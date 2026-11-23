"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


# 业务动作接口：把领域服务里已有的操作暴露出来，路由层只做参数转交。
# 固定路径（如 /table_archive/plans）必须注册在通用 GET /{module}/{entry_id} 之前，
# 否则会被动态路径抢先匹配，entry_id 解析失败。


@router.post("/table_archive/{entry_id}/plan", response_model=dict)
def plan_archive(entry_id: int, as_of: str | None = Query(default=None)) -> dict[str, Any]:
    """签发归档计划，固化原清单和签发时的留存依据。"""
    service = require_service("table_archive")
    return service.plan_archive_execution(entry_id, as_of=as_of)


@router.get("/table_archive/plans", response_model=dict)
def list_archive_plans(entry_id: int | None = Query(default=None)) -> dict[str, Any]:
    """查询已签发归档计划，可按策略过滤。"""
    service = require_service("table_archive")
    return service.list_archive_plans(entry_id)


@router.get("/table_archive/plans/{plan_id}", response_model=dict)
def get_archive_plan(plan_id: int) -> dict[str, Any]:
    """回查已签发计划的原清单和留存依据。"""
    service = require_service("table_archive")
    plan = service.get_archive_plan(plan_id)
    if not plan.get("ok"):
        raise HTTPException(status_code=404, detail=plan["message"])
    return plan


@router.get("/table_archive/plans/{plan_id}/records", response_model=dict)
def get_archive_records(plan_id: int) -> dict[str, Any]:
    """查实际归档记录中的已迁出分区和需要复核的分区。"""
    service = require_service("table_archive")
    return service.get_archive_records(plan_id)


@router.post("/table_archive/plans/{plan_id}/remaining", response_model=dict)
def plan_remaining_from_plan(plan_id: int, as_of: str | None = Query(default=None)) -> dict[str, Any]:
    """从旧计划派生可执行计划，只覆盖原清单中仍过期的项。"""
    service = require_service("table_archive")
    result = service.plan_remaining_from_plan(plan_id, as_of=as_of)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/table_archive/{entry_id}/submit", response_model=dict)
def submit_archive(
    entry_id: int,
    plan_id: int | None = Query(default=None),
    confirm: bool = Query(default=True),
) -> dict[str, Any]:
    """提交归档：先做安全检查，确认后按已签发清单办理。"""
    service = require_service("table_archive")
    return service.execute_archive(entry_id, confirm=confirm, plan_id=plan_id)


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
