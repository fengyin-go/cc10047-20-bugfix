"""提交版本领域服务：增量提交必须以批次真实业务身份为准，产出一致的增量提交回执。

修复点（共同根因）：
    旧版 ``transaction_id`` 用 ``hash(str(entry))`` 生成，Python 内置哈希带
    进程级随机盐，服务重启或换进程后同一批次得到不同事务标识，导致版本详情
    与检查点工作台对不上、重复回执候选。现在统一改为：
      * 业务身份（来源/湖表/分区/偏移区间/行数）→ sha256 → 确定性事务标识；
      * 登记走增量提交回执台账，同一身份全平台只有一份回执；
      * 条目回写提交身份，版本详情与检查点工作台读取同一份身份。
"""
from __future__ import annotations

from typing import Any

from app.incremental_receipt import build_business_identity, ledger
from app.services.base import BaseModuleService
from app.store import store


class DeltaCommitService(BaseModuleService):
    MODULE = "delta_commit"
    ENTITY = "版本提交"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["提交标识", "目标湖表", "提交类型", "新增行数", "更新行数", "删除行数", "提交水位", "提交时间"]
    REQUIRED_FIELDS = ["提交标识", "目标湖表", "提交类型"]
    STATUS_ORDER = ["提交中", "已提交", "提交冲突", "已回退"]
    ACTION_RULES = {"冲突处理": "提交冲突", "回退版本": "已提交"}
    NEGATIVE_ACTIONS = []

    def _resolve_identity(
        self,
        entry: dict[str, Any],
        *,
        source: str | None,
        table: str | None,
        partition: str | None,
        start_offset: int | None,
        row_count: int,
    ) -> dict[str, Any]:
        """从显式参数或条目已回写身份中解析业务身份；两个入口必须得到同一身份。"""
        table = str(table or entry.get("目标湖表") or "").strip()
        # 已回写过身份的条目，重复处理时直接采用原身份，普通登记字段变化也不影响。
        stored = entry.get("增量业务身份")
        if isinstance(stored, dict) and stored:
            return build_business_identity(
                source=stored["source"],
                table=stored["table"],
                partition=stored["partition"],
                start_offset=stored["start_offset"],
                end_offset=stored["end_offset"],
                row_count=stored["row_count"],
            )
        source = source or str(entry.get("来源流") or table)
        partition = partition or str(entry.get("写入分区") or "p0")
        if start_offset is None:
            start_offset = int(entry.get("起始偏移") or 0)
        start_offset = int(start_offset)
        row_count = int(row_count)
        return build_business_identity(
            source=source,
            table=table,
            partition=partition,
            start_offset=start_offset,
            end_offset=start_offset + row_count - 1,
            row_count=row_count,
        )

    def domain_commit(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行去重，联动调度和合并处理，产出标准化记录。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"提交记录 {entry_id} 不存在"}

        return {
            "ok": True,
            "commit_id": entry.get("提交标识"),
            "table": entry.get("目标湖表"),
            "new_rows": entry.get("新增行数", 0),
            "updated_rows": entry.get("更新行数", 0),
            "deleted_rows": entry.get("删除行数", 0),
            "watermark": entry.get("提交水位"),
            "version": entry["id"] + 1000,
        }

    def domain_ingest_delta(
        self,
        entry_id: int,
        delta_records: int = 500,
        *,
        source: str | None = None,
        table: str | None = None,
        partition: str | None = None,
        start_offset: int | None = None,
        process_id: str | None = None,
        batch_ref: str | None = None,
        checkpoint_entry_id: int | None = None,
    ) -> dict[str, Any]:
        """增量写入登记：校验真实业务身份，返回一致的增量提交回执。

        同一业务身份重复处理时返回同一份回执（``deduplicated=True``），
        不再因进程重启产生新的事务标识。
        """
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"提交记录 {entry_id} 不存在"}
        try:
            identity = self._resolve_identity(
                entry,
                source=source,
                table=table,
                partition=partition,
                start_offset=start_offset,
                row_count=delta_records,
            )
        except ValueError as exc:
            return {"ok": False, "message": str(exc)}

        receipt = ledger.register_write(
            identity,
            process_id=process_id,
            batch_ref=batch_ref,
            commit_entry_id=entry_id,
            checkpoint_entry_id=checkpoint_entry_id,
        )
        # 提交身份回写版本条目：版本详情入口与检查点工作台读同一份身份。
        entry["增量业务身份"] = dict(identity)
        entry["事务标识"] = receipt["transaction_id"]
        entry["增量回执编号"] = receipt["receipt_id"]
        entry["写入分区"] = identity["partition"]
        entry["起始偏移"] = identity["start_offset"]
        entry["确认偏移"] = receipt["confirmation_scope"]["confirmed_offset"]
        entry["实际落库行数"] = receipt["written_data"]["landed_rows"]

        return {
            "ok": True,
            "commit_id": entry.get("提交标识"),
            "table": identity["table"],
            "delta_records": delta_records,
            "operation": "merge",
            "delta_version": entry["id"] + 2000,
            # 事务标识完全由业务身份决定，重启/换进程保持一致。
            "transaction_id": receipt["transaction_id"],
            "receipt_id": receipt["receipt_id"],
            "identity_hash": receipt["identity_hash"],
            "business_identity": receipt["business_identity"],
            "partition": identity["partition"],
            "start_offset": identity["start_offset"],
            "end_offset": identity["end_offset"],
            "ingested_rows": receipt["written_data"]["landed_rows"],
            "pending_confirm_offset": receipt["confirmation_scope"]["confirmed_offset"],
            "write_basis": receipt["write_basis"],
            "written_data": receipt["written_data"],
            "confirmation_scope": receipt["confirmation_scope"],
            "deduplicated": receipt.get("deduplicated", False),
            "candidate_count": len(receipt["attempts"]),
        }
