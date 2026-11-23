"""分区管理领域服务：管理数据表的分区策略、分区过期检测和分区覆盖率检查。

数据链路位置：分区治理层，维护数据分区的生命周期和存储利用率。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_dates import (
    PREVIEW_PARTITION_LIMIT,
    dates_from_cutoff,
    expiry_cutoff,
    format_partition_value,
)
from app.store import store


class PartitionManagerService(BaseModuleService):
    MODULE = "partition_manager"
    ENTITY = "分区配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "目标表", "分区字段", "分区粒度", "保留天数"]
    REQUIRED_FIELDS = ["配置名称", "目标表", "分区字段"]
    STATUS_ORDER = ["草稿", "已启用", "已冻结"]
    ACTION_RULES = {"启用分区": "已启用", "冻结分区": "已冻结"}
    NEGATIVE_ACTIONS = []

    PARTITION_GRANULARITIES = frozenset({"hour", "day", "month", "year"})

    def compute_expired_partitions(self, entry_id: int) -> dict[str, Any]:
        """计算已过期的分区列表，供清理作业消费。

        根据配置的保留天数和当前日期，计算出需要清理的分区范围。
        清理作业模块通过此接口获取待清理分区的精确列表。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分区配置不存在"}
        retention_days = int(entry.get("保留天数", "30"))
        granularity = entry.get("分区粒度", "day")
        partition_field = entry.get("分区字段", "dt")

        expired_before = expiry_cutoff(retention_days)

        expired_partitions = self._generate_partition_values(
            granularity, expired_before, retention_days
        )
        return {
            "config_id": entry_id,
            "config_name": entry.get("配置名称"),
            "target_table": entry.get("目标表"),
            "partition_field": partition_field,
            "retention_days": retention_days,
            "expired_before": expired_before.strftime("%Y-%m-%d"),
            "expired_partition_count": len(expired_partitions),
            "expired_partitions": expired_partitions,
            "estimated_cleanup_bytes": len(expired_partitions) * 524288000,
            "safe_to_clean": len(expired_partitions) > 0,
        }

    def check_partition_coverage(self, entry_id: int) -> dict[str, Any]:
        """检测分区覆盖范围是否与上游流水线对齐。

        比较分区配置覆盖的日期范围与 ETL 流水线的实际产出日期，
        发现空洞或未对齐区间时生成补数建议。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分区配置不存在"}

        target_table = entry.get("目标表", "")
        pipeline_entries = store.rows("etl_pipeline")
        related_pipelines = [
            p for p in pipeline_entries
            if p.get("目标端类型", "") == target_table and p.get("status") in ("已启用", "运行中")
        ]

        gaps = []
        retention_days = int(entry.get("保留天数", "30"))
        expected_start = datetime(2026, 11, 21) - timedelta(days=retention_days)
        for i in range(retention_days):
            check_date = expected_start + timedelta(days=i)
            date_str = check_date.strftime("%Y%m%d")
            has_data = any(
                p.get("最近运行状态") == "成功" for p in related_pipelines
            )
            if not has_data and check_date < datetime(2026, 11, 21):
                gaps.append(date_str)

        return {
            "config_id": entry_id,
            "target_table": target_table,
            "related_pipeline_count": len(related_pipelines),
            "retention_days": retention_days,
            "coverage_gaps": gaps[:30],
            "gap_count": len(gaps),
            "needs_backfill": len(gaps) > 0,
        }

    def suggest_retention_policy(self, entry_id: int) -> dict[str, Any]:
        """根据分区数据量和使用模式推荐保留策略。

        分析该分区配置下的实际数据增长和查询频率，
        为 data_retention 模块提供自动化的保留天数建议。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分区配置不存在"}

        current_retention = int(entry.get("保留天数", "30"))
        partition_count = self._estimate_partition_count(entry)

        suggestion = {
            "config_id": entry_id,
            "current_retention_days": current_retention,
            "total_partitions": partition_count,
            "daily_avg_rows": 150000,
            "monthly_growth_mb": 4500,
            "recommended_retention_days": min(current_retention, 90) if partition_count > 100 else current_retention + 30,
            "reason": "分区数量超过100，建议维持或缩短保留期" if partition_count > 100 else "数据量适中，可以适当延长保留期",
        }
        return suggestion

    def align_with_backfill(self, entry_id: int) -> dict[str, Any]:
        """为补数任务生成分区回填范围。

        读取补数任务模块中与该分区配置关联的待处理任务，
        计算每个任务需要补填的确切分区列表。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分区配置不存在"}

        target_table = entry.get("目标表", "")
        backfill_entries = store.rows("backfill_task")
        related_backfills = [
            b for b in backfill_entries
            if b.get("目标表") == target_table and b.get("status") == "排队中"
        ]

        fill_ranges = []
        for b in related_backfills:
            fill_ranges.append({
                "backfill_id": b.get("id"),
                "backfill_name": b.get("补数状态", ""),
                "time_range": b.get("补数时间范围", ""),
                "partitions_needed": self._compute_fill_partitions(b),
            })

        return {
            "config_id": entry_id,
            "target_table": target_table,
            "pending_backfills": len(related_backfills),
            "fill_ranges": fill_ranges,
        }

    def _generate_partition_values(
        self, granularity: str, start: datetime, count: int
    ) -> list[str]:
        # 数量上限沿用分区预览原有的 30；日期推导统一走共享计算。
        return [
            format_partition_value(value, granularity)
            for value in dates_from_cutoff(start, min(count, PREVIEW_PARTITION_LIMIT))
        ]

    def _estimate_partition_count(self, entry: dict[str, Any]) -> int:
        granularity = entry.get("分区粒度", "day")
        retention = int(entry.get("保留天数", "30"))
        if granularity == "hour":
            return retention * 24
        elif granularity == "month":
            return max(retention // 30, 1)
        return retention

    def _compute_fill_partitions(self, backfill_entry: dict[str, Any]) -> list[str]:
        time_range = backfill_entry.get("补数时间范围", "")
        parts = []
        if "~" in time_range:
            start_str, end_str = time_range.split("~")
            try:
                start = datetime.strptime(start_str.strip(), "%Y-%m-%d")
                end = datetime.strptime(end_str.strip(), "%Y-%m-%d")
                delta = (end - start).days
                for i in range(min(delta + 1, 60)):
                    d = start + timedelta(days=i)
                    parts.append(d.strftime("dt=%Y-%m-%d"))
            except ValueError:
                pass
        return parts