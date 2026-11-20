"""转换验证结果路由：样本批量验证、逐行结果查询、可交付分区消费与交付登记。

部署流水线通过本路由的 deliverable / deliver 接口消费转换验证结果，
只允许交付实际通过验证的目标分区，并登记发布数据范围与拒收行。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.transform_verification import (
    compare_batches,
    consume_partition_output,
    deliver_partition,
    get_row_detail,
    get_verification_batch,
    get_verification_rows,
    list_deliverable_partitions,
    list_deliveries,
    list_verification_batches,
    mapping_revision,
    rollback_delivery,
    submit_verification_batch,
)

router = APIRouter(prefix="/api/etl-mappings", tags=["转换验证"])


class SampleBatchPayload(BaseModel):
    samples: list[dict[str, Any]] = Field(default_factory=list)
    partition_field: str | None = None
    field_types: dict[str, str] | None = None
    required_fields: list[str] | None = None
    target_pk_field: str | None = None
    valid_target_pks: list[str] | None = None


class MappingUpdatePayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class DeliverPayload(BaseModel):
    environment: str | None = None
    operator: str | None = None
    deploy_task_id: int | None = None


class RollbackPayload(BaseModel):
    reason: str | None = ""


class ComparePayload(BaseModel):
    batch_ids: list[str] = Field(default_factory=list)


def _require_batch(batch_id: str) -> dict[str, Any]:
    batch = get_verification_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"验证批次 {batch_id} 不存在")
    return batch


@router.post("/{mapping_id}/verification-batches")
def submit_batch(mapping_id: int, payload: SampleBatchPayload) -> dict[str, Any]:
    """提交一批样本，逐行执行类型转换/必填字段/目标主键校验。"""
    try:
        return submit_verification_batch(mapping_id, payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/verification-batches")
def get_batches(mapping_id: int | None = None) -> dict[str, Any]:
    items = list_verification_batches(mapping_id)
    return {"items": items, "total": len(items)}


@router.get("/verification-batches/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    return _require_batch(batch_id)


@router.get("/verification-batches/{batch_id}/rows")
def get_batch_rows(batch_id: str, status: str | None = None) -> dict[str, Any]:
    _require_batch(batch_id)
    try:
        items = get_verification_rows(batch_id, status)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"batch_id": batch_id, "items": items, "total": len(items)}


@router.get("/verification-batches/{batch_id}/rows/{row_no}")
def get_row(batch_id: str, row_no: int) -> dict[str, Any]:
    _require_batch(batch_id)
    row = get_row_detail(batch_id, row_no)
    if row is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 第 {row_no} 行不存在")
    return row


@router.get("/deliverable-partitions")
def get_deliverable(target_table: str | None = None) -> dict[str, Any]:
    """部署流水线消费入口：列出各目标表实际通过验证、可交付的分区。"""
    items = list_deliverable_partitions(target_table)
    return {"items": items, "total": len(items)}


@router.get("/verification-batches/{batch_id}/partitions/{partition_value}/output")
def get_partition_output(batch_id: str, partition_value: str) -> dict[str, Any]:
    _require_batch(batch_id)
    try:
        return consume_partition_output(batch_id, partition_value)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/verification-batches/{batch_id}/partitions/{partition_value}/deliver")
def deliver(batch_id: str, partition_value: str, payload: DeliverPayload) -> dict[str, Any]:
    """登记分区交付：只发布通过验证的行，拒收行单独留档用于回溯源记录。"""
    _require_batch(batch_id)
    try:
        return deliver_partition(batch_id, partition_value, payload.model_dump(exclude_none=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/deliveries")
def get_deliveries(target_table: str | None = None) -> dict[str, Any]:
    items = list_deliveries(target_table)
    return {"items": items, "total": len(items)}


@router.post("/deliveries/{delivery_id}/rollback")
def rollback(delivery_id: str, payload: RollbackPayload) -> dict[str, Any]:
    try:
        return rollback_delivery(delivery_id, payload.reason or "")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/verification-batches/compare")
def compare(payload: ComparePayload) -> dict[str, Any]:
    """比较修正样本前后多个批次的逐行实际结果。"""
    if not payload.batch_ids:
        raise HTTPException(status_code=400, detail="batch_ids 不能为空")
    try:
        return compare_batches(payload.batch_ids)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/{mapping_id}")
def update_mapping(mapping_id: int, payload: MappingUpdatePayload) -> dict[str, Any]:
    """修改映射规则并递增规则修订版本，旧验证批次随之标记过期。"""
    service = service_for("etl_mapping")
    entry, message = service.update_mapping(mapping_id, payload.values)
    if entry is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "entry": entry}


@router.get("/{mapping_id}/rule-revision")
def get_rule_revision(mapping_id: int) -> dict[str, Any]:
    info = mapping_revision(mapping_id)
    if not info.get("exists"):
        raise HTTPException(status_code=404, detail=f"映射规则 {mapping_id} 不存在")
    return info
