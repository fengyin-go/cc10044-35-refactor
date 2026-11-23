"""表归档领域服务：规划表归档的执行计划、执行归档和归档安全检查。

数据链路位置：数据归档层，将历史数据表归档到低成本存储。

归档计划一旦签发就固定下来：后续保留期即使延长，旧清单仍可回查，
提交归档时只会在旧清单内按当前留存要求重新判定，既不会迁出仍受保护的
日期，也不会把签发时清单之外的日期补进来。确实需要继续归档时，必须从
旧单重新派生执行计划。
"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_dates import (
    ARCHIVE_PARTITION_LIMIT,
    expiry_cutoff,
    expired_dates,
    format_partition_path,
    is_expired,
    partition_age,
    parse_partition_date,
    resolve_as_of,
    unique_dates,
)
from app.store import store

# 平台示例数据统一使用的签发/归档时间戳。
ARCHIVE_TIMESTAMP = "2026-11-21T08:00:00Z"

# 已签发计划和实际归档记录都放在模块级注册表里：与各模块的内存数据仓库一致，
# 进程内跨服务实例共享，重启后随示例数据一起复位。
_ARCHIVE_PLANS: dict[int, dict[str, Any]] = {}
_ARCHIVE_RECORDS: dict[int, list[dict[str, Any]]] = {}
_PLAN_SEQUENCE = 0


def _next_plan_id() -> int:
    global _PLAN_SEQUENCE
    _PLAN_SEQUENCE += 1
    return _PLAN_SEQUENCE


class TableArchiveService(BaseModuleService):
    MODULE = "table_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["策略名称", "目标表", "归档条件", "归档存储", "原始保留天数"]
    REQUIRED_FIELDS = ["策略名称", "目标表", "归档条件"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用归档": "已启用", "停用归档": "已停用"}
    NEGATIVE_ACTIONS = ["停用归档"]

    STORAGE_TIERS = frozenset({"S3_Standard", "S3_IA", "S3_Glacier", "HDFS_COLD", "OSS_Archive"})

    def plan_archive_execution(self, entry_id: int, *, as_of: Any = None) -> dict[str, Any]:
        """规划并签发归档执行范围。

        根据归档条件计算需要归档的表分区列表，估算归档后节省的存储成本。
        签发时会保存原清单和签发时的留存依据，后续提交归档都以该清单为准。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}

        target_table = entry.get("目标表", "")
        archive_condition = entry.get("归档条件", "")
        archive_storage = entry.get("归档存储", "S3_IA")
        retention_days = self._effective_retention_days(entry)
        reference = resolve_as_of(as_of)

        dated_partitions = self._partition_dates(
            target_table, retention_days, reference
        )
        plan = self._issue_plan(
            entry=entry,
            retention_days=retention_days,
            reference=reference,
            dated_partitions=dated_partitions,
        )

        partitions_to_archive = plan["manifest_partitions"]
        return {
            "policy_id": entry_id,
            "policy_name": entry.get("策略名称"),
            "target_table": target_table,
            "archive_condition": archive_condition,
            "target_storage": archive_storage,
            "partitions_to_archive": len(partitions_to_archive),
            "partitions": partitions_to_archive[:30],
            "estimated_data_size_gb": len(partitions_to_archive) * 0.45,
            "storage_cost_savings_percent": 68 if archive_storage in ("S3_Glacier", "S3_IA") else 40,
            "plan_id": plan["plan_id"],
            "retention_basis": plan["retention_basis"],
        }

    def execute_archive(
        self,
        entry_id: int,
        confirm: bool = True,
        *,
        plan_id: int | None = None,
    ) -> dict[str, Any]:
        """提交并执行归档。

        提交请求先做安全检查；同一目标表仍有增量合并时先等待，不产生归档记录。
        确认办理后只按已签发清单执行：当前留存要求下仍受保护的日期转为待复核，
        清单之外的日期不会被补入。返回实际归档记录后，可继续回查原计划留存依据。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}
        if entry.get("status") != "已启用":
            return {"ok": False, "message": "只有已启用的策略才能执行归档"}

        # 先定位计划：显式给了 plan_id 却找不到时直接报错，不因安全检查掩盖问题。
        if plan_id is not None and self._resolve_plan(entry_id, plan_id) is None:
            return {
                "ok": False,
                "message": f"归档计划 {plan_id} 不存在或不属于该归档策略",
            }

        # 提交请求先做安全检查；被阻塞时不签发计划、不落归档记录。
        safety = self.check_archive_safety(entry_id)
        if not safety["safe_to_archive"]:
            return {
                "ok": False,
                "policy_id": entry_id,
                "plan_id": plan_id,
                "safe_to_archive": False,
                "blockers": safety["blockers"],
                "message": self._blocking_message(safety),
            }

        plan = self._resolve_plan(entry_id, plan_id)
        if plan is None:
            # 兼容旧调用：没有已签发计划时，先按当前留存依据签发一份。
            planned = self.plan_archive_execution(entry_id)
            if not planned.get("ok", True):
                return planned
            plan = _ARCHIVE_PLANS[int(planned["plan_id"])]

        current_retention = self._effective_retention_days(entry)
        archived_items, review_items = self._classify_manifest(
            plan, current_retention
        )

        if not confirm:
            return {
                "ok": True,
                "policy_id": entry_id,
                "plan_id": plan["plan_id"],
                "target_storage": plan["target_storage"],
                "awaiting_confirmation": True,
                "archived_partition_dates": [item["date"] for item in archived_items],
                "review_partition_dates": [item["date"] for item in review_items],
                "retention_basis": plan["retention_basis"],
                "current_retention_days": current_retention,
                "message": "安全检查已通过，确认后按原清单办理归档",
            }

        records = self._write_records(plan, archived_items, review_items, current_retention)
        archived_dates = [record["partition_date"] for record in records
                          if record["status"] == "archived"]
        review_dates = [record["partition_date"] for record in records
                        if record["status"] == "needs_review"]

        return {
            "ok": True,
            "policy_id": entry_id,
            "plan_id": plan["plan_id"],
            "archived_partitions": len(archived_dates),
            "archived_data_gb": len(archived_dates) * 0.45,
            "archive_records": records[:10],
            "archived_partition_dates": archived_dates,
            "review_partitions": len(review_dates),
            "review_partition_dates": review_dates,
            "restore_available": True,
            # 实际归档记录之后回查原计划：清单与签发时的留存依据保持不变。
            "original_plan": self.get_archive_plan(plan["plan_id"]),
            "message": self._archive_message(len(archived_dates), len(review_dates), plan),
        }

    def check_archive_safety(self, entry_id: int) -> dict[str, Any]:
        """归档安全性检查。

        在归档前检查目标表是否正在被其他流水线使用、
        是否有未完成的增量合并在进行中、是否有未满足的保留要求。
        增量合并只按目标表匹配，其他表的合并不影响本次提交。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}

        target_table = entry.get("目标表", "")
        pipeline_rows = store.rows("etl_pipeline")
        active_pipelines = [
            p for p in pipeline_rows
            if p.get("目标端类型") == target_table and p.get("status") in ("运行中",)
        ]
        merge_rows = store.rows("delta_merge")
        active_merges = [
            m for m in merge_rows
            if m.get("目标表") == target_table and self._is_active_merge(m)
        ]
        safe = len(active_pipelines) == 0 and len(active_merges) == 0
        result = {
            "safe_to_archive": safe,
            "blockers": {
                "active_pipelines": len(active_pipelines),
                "active_merges": len(active_merges),
            },
            "message": "可以安全归档" if safe else "存在活跃任务，请等待完成后归档",
        }
        if not safe:
            result["message"] = self._blocking_message(result)
        return result

    def get_archive_plan(self, plan_id: int) -> dict[str, Any]:
        """回查已签发计划：返回原清单和签发时的留存依据。

        保留期调整后旧计划不会被改写，这里始终返回签发时固化的内容。
        """
        plan = _ARCHIVE_PLANS.get(int(plan_id))
        if plan is None:
            return {"ok": False, "message": "归档计划不存在"}
        return {
            "ok": True,
            "plan_id": plan["plan_id"],
            "policy_id": plan["policy_id"],
            "policy_name": plan["policy_name"],
            "target_table": plan["target_table"],
            "target_storage": plan["target_storage"],
            "issued_at": plan["issued_at"],
            "derived_from_plan_id": plan.get("derived_from_plan_id"),
            "retention_basis": dict(plan["retention_basis"]),
            "manifest": [dict(item) for item in plan["manifest"]],
            "manifest_partitions": list(plan["manifest_partitions"]),
            "partitions_to_archive": len(plan["manifest"]),
        }

    def list_archive_plans(self, entry_id: int | None = None) -> dict[str, Any]:
        """列出已签发计划，可按归档策略过滤。"""
        plans = [
            plan for plan in _ARCHIVE_PLANS.values()
            if entry_id is None or plan["policy_id"] == entry_id
        ]
        plans.sort(key=lambda plan: plan["plan_id"], reverse=True)
        return {
            "total": len(plans),
            "plans": [
                {
                    "plan_id": plan["plan_id"],
                    "policy_id": plan["policy_id"],
                    "target_table": plan["target_table"],
                    "issued_at": plan["issued_at"],
                    "retention_basis": dict(plan["retention_basis"]),
                    "partitions_to_archive": len(plan["manifest"]),
                    "derived_from_plan_id": plan.get("derived_from_plan_id"),
                }
                for plan in plans
            ],
        }

    def get_archive_records(
        self,
        plan_id: int | None = None,
        *,
        entry_id: int | None = None,
    ) -> dict[str, Any]:
        """查询实际归档记录中的已迁出分区和需要复核的分区。

        不传 plan_id 时按归档策略汇总该策略下所有计划产生的记录。
        """
        if plan_id is not None:
            buckets = {int(plan_id): _ARCHIVE_RECORDS.get(int(plan_id), [])}
        elif entry_id is not None:
            relevant = [
                pid for pid, plan in _ARCHIVE_PLANS.items()
                if plan["policy_id"] == entry_id
            ]
            buckets = {pid: _ARCHIVE_RECORDS.get(pid, []) for pid in relevant}
        else:
            buckets = dict(_ARCHIVE_RECORDS)

        records = [record for bucket in buckets.values() for record in bucket]
        archived = [r for r in records if r["status"] == "archived"]
        needs_review = [r for r in records if r["status"] == "needs_review"]
        return {
            "ok": True,
            "plan_id": plan_id,
            "plan_ids": sorted(buckets),
            "policy_id": entry_id,
            "record_count": len(records),
            "archived_partition_dates": [r["partition_date"] for r in archived],
            "review_partition_dates": [r["partition_date"] for r in needs_review],
            "records": records,
        }

    def plan_remaining_from_plan(
        self,
        plan_id: int,
        *,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """从旧单派生可执行计划，只覆盖原清单中仍过期的原项。

        原清单里的每一项在签发时都已过期；这里沿用签发时的留存依据判定
        「仍过期」，因此待复核分区仍属于旧单的过期原项，可以派生出可执行计划。
        计划只包含原清单中尚未迁出的项，不会引入清单之外的日期。
        派生计划被提交时，仍会按当时生效的留存要求逐项复核，
        保留期放宽后受保护的日期依然不会迁出。
        """
        original = _ARCHIVE_PLANS.get(int(plan_id))
        if original is None:
            return {"ok": False, "message": "归档计划不存在"}

        entry = store.find(self.MODULE, original["policy_id"])
        if entry is None:
            return {"ok": False, "message": "归档策略不存在"}

        reference = resolve_as_of(as_of)
        current_retention = self._effective_retention_days(entry)
        original_basis = dict(original["retention_basis"])
        issue_reference = original_basis["as_of"]
        archived_partitions = self._archived_partitions_in_chain(original["plan_id"])
        pending = [
            item for item in original["manifest"]
            if item["partition"] not in archived_partitions
        ]
        # 原项按签发依据判定仍过期；待复核项在派生时单独标注。
        remaining = [
            item for item in pending
            if is_expired(item["date"], original_basis["retention_days"], issue_reference)
        ]
        review = [
            item for item in pending
            if not is_expired(item["date"], current_retention, reference)
        ]

        if not remaining:
            return {
                "ok": True,
                "executable": False,
                "original_plan_id": original["plan_id"],
                "plan_id": None,
                "policy_id": original["policy_id"],
                "retention_basis": original_basis,
                "review_partition_dates": [item["date"] for item in review],
                "partitions": [],
                "message": "原清单中没有仍过期且尚未迁出的原项",
            }

        dates = unique_dates([
            parse_partition_date(item["date"]) for item in remaining
        ])
        dated_partitions = [
            (date, format_partition_path(original["target_table"], date))
            for date in dates
        ]
        expected_partitions = [partition for _, partition in dated_partitions]
        existing = self._find_matching_derived_plan(
            original, original_basis, expected_partitions
        )
        derived = existing or self._issue_plan(
            entry=entry,
            retention_days=original_basis["retention_days"],
            reference=issue_reference,
            dated_partitions=dated_partitions,
            derived_from_plan_id=original["plan_id"],
            retention_basis=original_basis,
        )
        return {
            "ok": True,
            "executable": True,
            "original_plan_id": original["plan_id"],
            "plan_id": derived["plan_id"],
            "reused": existing is not None,
            "policy_id": original["policy_id"],
            "target_table": original["target_table"],
            "target_storage": original["target_storage"],
            "retention_basis": derived["retention_basis"],
            "current_retention_days": current_retention,
            "partitions_to_archive": len(derived["manifest_partitions"]),
            "partitions": derived["manifest_partitions"],
            "review_partition_dates": [item["date"] for item in review],
            "message": f"已从旧计划 {original['plan_id']} 派生可执行计划 {derived['plan_id']}",
        }

    def get_executable_plan_from_plan(
        self,
        plan_id: int,
        *,
        as_of: Any = None,
    ) -> dict[str, Any]:
        """``plan_remaining_from_plan`` 的语义化别名：从旧单取可执行计划。"""
        return self.plan_remaining_from_plan(plan_id, as_of=as_of)

    def get_archived_tables_inventory(self) -> dict[str, Any]:
        """获取已归档表的清单。

        供冷数据恢复模块查询哪些表有可恢复的归档数据。
        """
        all_policies = store.rows(self.MODULE)
        enabled = [p for p in all_policies if p.get("status") == "已启用"]
        return {
            "archived_table_count": len(enabled),
            "tables": [
                {
                    "table": p.get("目标表"),
                    "storage": p.get("归档存储"),
                    "policy_name": p.get("策略名称"),
                    "original_retention": p.get("原始保留天数"),
                }
                for p in enabled
            ],
        }

    # ------------------------------------------------------------------ 内部实现

    def _effective_retention_days(self, entry: dict[str, Any]) -> int:
        """当前生效的留存要求。

        归档策略的原始保留天数是基线；同一目标表上已生效的数据保留规则
        如果要求更长，按更长的保留期执行，避免把仍受合规保护的分区迁出。
        """
        retention_days = int(entry.get("原始保留天数", "30"))
        target_table = entry.get("目标表", "")
        for rule in store.rows("data_retention"):
            if rule.get("目标数据集") != target_table:
                continue
            if rule.get("status") != "已生效":
                continue
            retention_days = max(retention_days, int(rule.get("保留天数", "0") or 0))
        return retention_days

    def _partition_dates(
        self,
        table: str,
        retention_days: int,
        reference: Any,
    ) -> list[tuple[Any, str]]:
        """共享分区日期计算 → 带表名的归档路径。"""
        dates = expired_dates(
            retention_days,
            limit=ARCHIVE_PARTITION_LIMIT,
            as_of=reference,
        )
        return [
            (value, format_partition_path(table, value))
            for value in dates
        ]

    def _issue_plan(
        self,
        *,
        entry: dict[str, Any],
        retention_days: int,
        reference: Any,
        dated_partitions: list[tuple[Any, str]],
        derived_from_plan_id: int | None = None,
        retention_basis: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """固化并保存一份归档计划，返回内部计划对象。

        ``retention_basis`` 用于派生计划：沿用旧单的留存依据，
        保证「原项是否过期」的判定与签发时一致。
        """
        plan_id = _next_plan_id()
        manifest = [
            {
                "partition": partition,
                "date": value.isoformat(),
                "age_days": partition_age(value, reference),
            }
            for value, partition in dated_partitions
        ]
        basis = dict(retention_basis) if retention_basis else self._retention_basis(
            retention_days, reference
        )
        plan = {
            "plan_id": plan_id,
            "policy_id": entry["id"],
            "policy_name": entry.get("策略名称"),
            "target_table": entry.get("目标表", ""),
            "target_storage": entry.get("归档存储", "S3_IA"),
            "archive_condition": entry.get("归档条件", ""),
            "issued_at": ARCHIVE_TIMESTAMP,
            "derived_from_plan_id": derived_from_plan_id,
            "retention_basis": basis,
            "manifest": manifest,
            "manifest_partitions": [item["partition"] for item in manifest],
        }
        _ARCHIVE_PLANS[plan_id] = plan
        _ARCHIVE_RECORDS.setdefault(plan_id, [])
        return plan

    def _retention_basis(self, retention_days: int, reference: Any) -> dict[str, Any]:
        cutoff = expiry_cutoff(retention_days, reference)
        return {
            "retention_days": retention_days,
            "as_of": resolve_as_of(reference).isoformat(),
            "cutoff_date": cutoff.isoformat(),
        }

    def _find_matching_derived_plan(
        self,
        original: dict[str, Any],
        basis: dict[str, Any],
        partitions: list[str],
    ) -> dict[str, Any] | None:
        """复用同一旧单、同一留存依据下清单一致的派生计划，避免重复派生。"""
        for plan in _ARCHIVE_PLANS.values():
            if plan.get("derived_from_plan_id") != original["plan_id"]:
                continue
            if plan["retention_basis"] != basis:
                continue
            if plan["manifest_partitions"] == partitions:
                return plan
        return None

    def _archived_partitions_in_chain(self, plan_id: int) -> set[str]:
        """收集旧单及其派生链上所有已迁出的分区，避免派生时重复纳入。"""
        archived = {
            record["partition"]
            for record in _ARCHIVE_RECORDS.get(plan_id, [])
            if record["status"] == "archived"
        }
        children = [
            plan["plan_id"] for plan in _ARCHIVE_PLANS.values()
            if plan.get("derived_from_plan_id") == plan_id
        ]
        for child in children:
            archived |= self._archived_partitions_in_chain(child)
        return archived

    def _resolve_plan(self, entry_id: int, plan_id: int | None) -> dict[str, Any] | None:
        if plan_id is not None:
            plan = _ARCHIVE_PLANS.get(int(plan_id))
            if plan is None or plan["policy_id"] != entry_id:
                return None
            return plan
        candidates = [
            plan for plan in _ARCHIVE_PLANS.values()
            if plan["policy_id"] == entry_id
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda plan: plan["plan_id"])

    def _classify_manifest(
        self,
        plan: dict[str, Any],
        current_retention: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """按当前留存要求，把原清单拆成可迁出和待复核两组。

        年龄以签发时的基准日计算，保留期调整只改变保护边界；
        两组都严格来自签发清单，不做任何补充。
        """
        reference = plan["retention_basis"]["as_of"]
        dated = [
            (item, parse_partition_date(item["date"]))
            for item in plan["manifest"]
        ]
        archived = [
            (item, value) for item, value in dated
            if is_expired(value, current_retention, reference)
        ]
        review = [
            (item, value) for item, value in dated
            if not is_expired(value, current_retention, reference)
        ]
        archived_items = [
            {
                "partition": item["partition"],
                "date": value.isoformat(),
                "age_days": partition_age(value, reference),
            }
            for item, value in archived
        ]
        review_items = [
            {
                "partition": item["partition"],
                "date": value.isoformat(),
                "age_days": partition_age(value, reference),
            }
            for item, value in review
        ]
        return archived_items, review_items

    def _write_records(
        self,
        plan: dict[str, Any],
        archived_items: list[dict[str, Any]],
        review_items: list[dict[str, Any]],
        current_retention: int,
    ) -> list[dict[str, Any]]:
        # 已迁出的分区不因重复提交而回滚；未迁出的按最新判定重写，
        # 保证记录不重复累加，也不会凭空多出清单之外的日期。
        previous = _ARCHIVE_RECORDS.get(plan["plan_id"], [])
        already_archived = {
            record["partition"]: record
            for record in previous
            if record["status"] == "archived"
        }
        records: list[dict[str, Any]] = []
        for item in archived_items:
            record = already_archived.get(item["partition"])
            if record is None:
                record = {
                    "plan_id": plan["plan_id"],
                    "source_table": plan["target_table"],
                    "partition": item["partition"],
                    "partition_date": item["date"],
                    "storage": plan["target_storage"],
                    "archived_at": ARCHIVE_TIMESTAMP,
                    "status": "archived",
                    "retention_days_at_issue": plan["retention_basis"]["retention_days"],
                }
            record["retention_days_now"] = current_retention
            records.append(record)
        for item in review_items:
            records.append({
                "plan_id": plan["plan_id"],
                "source_table": plan["target_table"],
                "partition": item["partition"],
                "partition_date": item["date"],
                "storage": plan["target_storage"],
                "archived_at": None,
                "status": "needs_review",
                "retention_days_at_issue": plan["retention_basis"]["retention_days"],
                "retention_days_now": current_retention,
            })
        _ARCHIVE_RECORDS[plan["plan_id"]] = records
        return records

    def _is_active_merge(self, merge: dict[str, Any]) -> bool:
        return (
            merge.get("status") in ("排队中", "合并中")
            or merge.get("合并状态") in ("排队中", "合并中")
        )

    def _blocking_message(self, safety: dict[str, Any]) -> str:
        blockers = safety["blockers"]
        if blockers.get("active_merges"):
            return "同一目标表仍在增量合并，请等待合并结束后再提交归档"
        if blockers.get("active_pipelines"):
            return "目标表仍有运行中的流水线，请等待运行结束后再提交归档"
        return "存在活跃任务，请等待完成后归档"

    def _archive_message(self, archived: int, review: int, plan: dict[str, Any]) -> str:
        base = f"已归档 {archived} 个分区到 {plan['target_storage']}"
        if review:
            base += f"，{review} 个分区因保留期调整仍在保护期内，已转待复核"
        return base
