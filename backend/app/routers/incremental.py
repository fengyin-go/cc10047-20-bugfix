"""增量提交回执路由：版本详情与检查点工作台通过同一组接口读取一致提交身份。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.incremental_receipt import build_business_identity, ledger
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api/incremental-receipts", tags=["增量提交回执"])


class IngestPayload(BaseModel):
    source: str | None = None
    table: str | None = None
    partition: str = "p0"
    start_offset: int = 0
    row_count: int = Field(gt=0)
    process_id: str | None = None
    batch_ref: str | None = None
    checkpoint_entry_id: int | None = None


class CheckpointPayload(BaseModel):
    source: str | None = None
    table: str | None = None
    partition: str | None = None
    start_offset: int | None = None
    row_count: int | None = None
    transaction_id: str | None = None
    confirmed_offset: int | None = None
    process_id: str | None = None


@router.get("")
def list_receipts(
    table: str | None = None,
    partition: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    items = ledger.list_receipts(table=table, partition=partition, source=source)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/by-transaction/{transaction_id}")
def get_by_transaction(transaction_id: str) -> dict[str, Any]:
    receipt = ledger.get_by_transaction(transaction_id)
    if receipt is None:
        return {"ok": False, "message": f"事务标识 {transaction_id} 没有对应回执"}
    return {"ok": True, "receipt": receipt}


@router.post("/commit/{entry_id}")
def register_ingest(entry_id: int, payload: IngestPayload) -> dict[str, Any]:
    """版本详情入口：按业务身份登记增量写入，返回一致的增量提交回执。"""
    svc = service_for("delta_commit")
    result = svc.domain_ingest_delta(
        entry_id,
        delta_records=payload.row_count,
        source=payload.source,
        table=payload.table,
        partition=payload.partition,
        start_offset=payload.start_offset,
        process_id=payload.process_id,
        batch_ref=payload.batch_ref,
        checkpoint_entry_id=payload.checkpoint_entry_id,
    )
    return result


@router.post("/checkpoint/{entry_id}/save")
def save_checkpoint(entry_id: int, payload: CheckpointPayload) -> dict[str, Any]:
    """检查点工作台入口：保存确认位置并回写同一份回执。"""
    svc = service_for("stream_checkpoint")
    return svc.domain_save_checkpoint(entry_id, payload.model_dump(exclude_none=True))


@router.post("/checkpoint/{entry_id}/recover")
def recover_checkpoint(entry_id: int, payload: CheckpointPayload) -> dict[str, Any]:
    """检查点工作台入口：区间恢复（同一区间只生效一次）。"""
    svc = service_for("stream_checkpoint")
    return svc.domain_recover_checkpoint(entry_id, payload.model_dump(exclude_none=True))


@router.get("/checkpoint/{entry_id}/view")
def checkpoint_view(entry_id: int) -> dict[str, Any]:
    """检查点工作台视角：哪组数据确实落库、尚需确认的偏移。"""
    svc = service_for("stream_checkpoint")
    return svc.domain_receipt_view(entry_id)


@router.get("/next-offset")
def next_offset(
    table: str,
    partition: str,
    source: str | None = None,
    source_start_offset: int = 0,
) -> dict[str, Any]:
    """证据优先级裁决检查点续点。"""
    svc = service_for("stream_checkpoint")
    return svc.domain_next_offset(
        table=table,
        partition=partition,
        source=source,
        source_start_offset=source_start_offset,
    )


@router.post("/identity/verify")
def verify_identity(payload: dict[str, Any]) -> dict[str, Any]:
    """校验增量写入批次的真实业务身份，返回稳定身份哈希与事务标识（不登记）。"""
    try:
        identity = build_business_identity(
            source=payload.get("source"),
            table=payload.get("table"),
            partition=payload.get("partition"),
            start_offset=payload.get("start_offset", 0),
            end_offset=payload.get("end_offset"),
            row_count=payload.get("row_count"),
        )
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}
    from app.incremental_receipt import identity_hash, transaction_id_for

    return {
        "ok": True,
        "business_identity": identity,
        "identity_hash": identity_hash(identity),
        "transaction_id": transaction_id_for(identity),
        "already_registered": ledger.get_by_identity(identity) is not None,
    }


@router.get("/health/state")
def ledger_state() -> dict[str, Any]:
    """运维视角：台账持久化状态，供重启一致性核对。"""
    items = ledger.list_receipts()
    return {
        "ok": True,
        "data_file": str(ledger._file),
        "receipt_total": len(items),
        "transaction_ids": [r["transaction_id"] for r in items],
    }
