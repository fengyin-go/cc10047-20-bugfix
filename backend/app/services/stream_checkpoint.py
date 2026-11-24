"""流计算检查点领域服务：检查点工作台与版本详情共享同一份增量提交身份。

职责：
    * 保存检查点时把确认位置回写增量提交回执（确认范围可追溯）；
    * 区间恢复只生效一次，再次进入不登记第二份回执；
    * 检查点续点按 检查点 > 落库回执 > 数据源起始偏移 的证据优先级裁决；
    * 普通登记字段变化后，仍能凭业务身份找到首次落库凭据。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.incremental_receipt import (
    EVIDENCE_LABELS,
    build_business_identity,
    ledger,
)
from app.services.base import BaseModuleService
from app.store import store


class StreamCheckpointService(BaseModuleService):
    MODULE = "stream_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["检查点标识", "关联管道", "检查点时间", "数据偏移", "状态大小", "保存路径", "保留时长", "检查状态"]
    REQUIRED_FIELDS = ["检查点标识", "关联管道", "检查点时间"]
    STATUS_ORDER = ["保存中", "已保存", "已恢复", "已过期"]
    ACTION_RULES = {"创建检查点": "保存中", "恢复检查点": "已恢复"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 身份解析：与 delta_commit 入口使用同一套业务身份规则
    # ------------------------------------------------------------------
    def _resolve_identity(self, entry: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        stored = entry.get("增量业务身份")
        if isinstance(stored, dict) and stored:
            return build_business_identity(**stored)
        source = str(payload.get("source") or entry.get("关联管道") or "").strip()
        table = str(payload.get("table") or entry.get("目标湖表") or source).strip()
        partition = str(payload.get("partition") or entry.get("写入分区") or "p0").strip()
        start_offset = int(payload.get("start_offset", entry.get("起始偏移", 0)))
        row_count = int(payload["row_count"])
        return build_business_identity(
            source=source,
            table=table,
            partition=partition,
            start_offset=start_offset,
            end_offset=start_offset + row_count - 1,
            row_count=row_count,
        )

    def domain_next_offset(
        self,
        *,
        table: str,
        partition: str,
        source: str | None = None,
        source_start_offset: int = 0,
    ) -> dict[str, Any]:
        """证据优先级裁决：检查点确认位置 > 回执落库区间 > 数据源起始偏移。"""
        def _lookup(tbl: str, part: str) -> dict[str, Any] | None:
            for row in store.rows(self.MODULE):
                identity = row.get("增量业务身份")
                if not isinstance(identity, dict):
                    # 未绑定增量身份的检查点（如种子数据）不能作为续点证据。
                    continue
                if identity.get("table") != tbl or identity.get("partition") != part:
                    continue
                # 指定来源时必须来源一致，避免命中同表另一摄入链路的检查点。
                if source is not None and identity.get("source") != source:
                    continue
                return row
            return None

        decision = ledger.resolve_next_offset(
            table=table,
            partition=partition,
            source_start_offset=source_start_offset,
            checkpoint_lookup=_lookup,
        )
        decision["ok"] = True
        decision["evidence_label"] = EVIDENCE_LABELS[decision["evidence"]]
        return decision

    def domain_save_checkpoint(self, entry_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """保存检查点：读取同一份提交身份，把确认位置回写回执与检查点待办。"""
        payload = payload or {}
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}

        result = {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "pipeline": entry.get("关联管道"),
            "checkpoint_time": entry.get("检查点时间"),
            "data_offset": entry.get("数据偏移"),
            "state_size_kb": int(entry.get("状态大小", 128)),
            "save_path": entry.get("保存路径", "/checkpoints"),
            "retention_hours": 24,
            "checksum": hashlib.sha256(str(entry).encode()).hexdigest()[:16],
        }

        transaction_id = payload.get("transaction_id")
        receipt = None
        if transaction_id:
            receipt = ledger.get_by_transaction(str(transaction_id))
        elif payload.get("row_count") is not None:
            try:
                identity = self._resolve_identity(entry, payload)
            except ValueError as exc:
                return {"ok": False, "message": str(exc)}
            receipt = ledger.get_by_identity(identity)

        if receipt is None:
            # 没有关联回执时保持基础检查点能力，但明确提示尚无落库依据。
            result["receipt_bound"] = False
            result["message"] = "未关联到增量提交回执，检查点仅保存运行状态"
            return result

        identity = receipt["business_identity"]
        confirmed_offset = int(payload.get("confirmed_offset", receipt["end_offset"]))
        receipt = ledger.confirm_checkpoint(
            identity,
            checkpoint_entry_id=entry_id,
            confirmed_offset=confirmed_offset,
        )
        # 检查点条目回写同一份提交身份，两个入口读到的事务标识必然一致。
        entry["增量业务身份"] = dict(identity)
        entry["事务标识"] = receipt["transaction_id"]
        entry["增量回执编号"] = receipt["receipt_id"]
        entry["数据偏移"] = confirmed_offset
        entry["确认偏移"] = confirmed_offset
        entry["待确认偏移"] = receipt["end_offset"] - confirmed_offset
        entry["status"] = "已保存"
        entry["pending"] = receipt["end_offset"] > confirmed_offset

        result.update(
            {
                "receipt_bound": True,
                "transaction_id": receipt["transaction_id"],
                "receipt_id": receipt["receipt_id"],
                "identity_hash": receipt["identity_hash"],
                "business_identity": identity,
                "confirmed_offset": confirmed_offset,
                "pending_offset": receipt["end_offset"] - confirmed_offset,
                "confirmation_scope": receipt["confirmation_scope"],
                "write_basis": receipt["write_basis"],
                "written_data": receipt["written_data"],
            }
        )
        return result

    def domain_recover_checkpoint(self, entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """恢复检查点区间：同一区间恢复只生效一次，恢复结果回写检查点待办。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}
        try:
            identity = self._resolve_identity(entry, payload)
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}

        try:
            receipt = ledger.recover_range(
                identity,
                checkpoint_entry_id=entry_id,
                process_id=payload.get("process_id"),
            )
        except KeyError as exc:
            return {"ok": False, "message": str(exc)}

        entry["增量业务身份"] = dict(identity)
        entry["事务标识"] = receipt["transaction_id"]
        entry["增量回执编号"] = receipt["receipt_id"]
        if receipt.get("recovery_effective", False):
            entry["status"] = "已恢复"
            entry["pending"] = False
            # 恢复生效时用回执确认范围回填检查点偏移，避免沿用条目上的陈旧偏移。
            confirmed = receipt["confirmation_scope"].get("confirmed_offset")
            entry["数据偏移"] = confirmed if confirmed is not None else receipt["end_offset"]
            entry["确认偏移"] = entry["数据偏移"]
        return {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "transaction_id": receipt["transaction_id"],
            "receipt_id": receipt["receipt_id"],
            "business_identity": identity,
            "recovery_effective": receipt.get("recovery_effective", False),
            "message": receipt.get("recovery_message", ""),
            "recovery": receipt.get("recovery"),
            "confirmation_scope": receipt["confirmation_scope"],
            "written_data": receipt["written_data"],
        }

    def domain_receipt_view(self, entry_id: int) -> dict[str, Any]:
        """检查点工作台视角：说明哪组数据确实落库、尚需确认的偏移。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}
        identity = entry.get("增量业务身份")
        receipt = ledger.get_by_identity(identity) if isinstance(identity, dict) else None
        if receipt is None:
            return {
                "ok": True,
                "checkpoint_id": entry.get("检查点标识"),
                "receipts": [],
                "message": "该检查点尚未绑定增量提交回执",
            }
        return {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "transaction_id": receipt["transaction_id"],
            "receipts": [receipt],
            "candidate_count": len(receipt["attempts"]),
            "landed_rows": receipt["written_data"]["landed_rows"],
            "confirmed_offset": receipt["confirmation_scope"]["confirmed_offset"],
            "pending_confirm_offset": (
                receipt["end_offset"] - (receipt["confirmation_scope"]["confirmed_offset"] or -1)
                if receipt["confirmation_scope"]["confirmed_offset"] is None
                else receipt["end_offset"] - receipt["confirmation_scope"]["confirmed_offset"]
            ),
        }
