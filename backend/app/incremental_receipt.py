"""增量提交回执台账：以批次真实业务身份为锚点，保证跨进程、跨重启的一致性。

共同根因背景：
    旧实现用 ``hash(str(条目))`` 生成事务标识，内置 ``hash`` 带进程级随机盐，
    服务一重启、或换一个进程重试，同一增量批次就会得到不同事务标识，进而：
      * 版本详情里同一批次显示成两个事务标识；
      * 检查点工作台关联不到原回执，确认位置丢失；
      * 同一批次重复登记出两份回执候选；
      * 不同分区的批次可能共用一份回执。

本模块的约束：
      * 业务身份 = 来源 + 目标湖表 + 分区 + 起始/结束偏移 + 行数，
        身份哈希用 sha256 计算，确定且与进程无关；
      * 同一业务身份全平台只有一份增量提交回执，重复登记只追加 attempt，
        首次落库凭据（write_basis / written_data）永不被覆盖；
      * 分区是身份的一部分，p1、p2 各写 9 行也各自独立成回执；
      * 回执落盘保存，重启后仍能查回原写入依据、实际写入数据和确认范围；
      * 同一区间恢复只生效一次；检查点续点按证据优先级裁决。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# 证据优先级：检查点从哪里继续，由证据可信度裁决，而不是由登记顺序决定。
EVIDENCE_PRIORITY = ("checkpoint", "receipt", "source")
EVIDENCE_LABELS = {
    "checkpoint": "已保存检查点确认位置",
    "receipt": "增量提交回执实际落库区间",
    "source": "流数据源起始偏移",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_business_identity(
    *,
    source: str,
    table: str,
    partition: str,
    start_offset: int,
    end_offset: int,
    row_count: int,
) -> dict[str, Any]:
    """校验并规范化增量写入批次的真实业务身份。"""
    source = str(source or "").strip()
    table = str(table or "").strip()
    partition = str(partition or "").strip()
    if not source or not table or not partition:
        raise ValueError("业务身份校验失败：来源、目标湖表、分区均不能为空")
    try:
        start_offset = int(start_offset)
        end_offset = int(end_offset)
        row_count = int(row_count)
    except (TypeError, ValueError) as exc:
        raise ValueError("业务身份校验失败：偏移与行数必须是整数") from exc
    if start_offset < 0 or end_offset < start_offset or row_count <= 0:
        raise ValueError("业务身份校验失败：偏移区间或行数不合法")
    # 偏移区间闭区间必须与行数自洽，杜绝“同一区间不同行数”的假批次。
    if end_offset - start_offset + 1 != row_count:
        raise ValueError(
            f"业务身份校验失败：区间 [{start_offset}, {end_offset}] 应为 "
            f"{end_offset - start_offset + 1} 行，实际申报 {row_count} 行"
        )
    return {
        "source": source,
        "table": table,
        "partition": partition,
        "start_offset": start_offset,
        "end_offset": end_offset,
        "row_count": row_count,
    }


def identity_hash(identity: dict[str, Any]) -> str:
    """业务身份 → 稳定哈希；同一批次在任何进程、任何重启后结果一致。"""
    import hashlib

    return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def transaction_id_for(identity: dict[str, Any]) -> str:
    return f"txn-{identity_hash(identity)[:16]}"


@dataclass
class ReceiptLedger:
    """增量提交回执台账：内存索引 + 落盘 JSONL，重启后从磁盘重建。"""

    data_dir: Path
    _lock: threading.RLock
    _receipts: dict[str, dict[str, Any]]

    @classmethod
    def default_path(cls) -> Path:
        env_dir = os.environ.get("LAKEHOUSE_DATA_DIR")
        if env_dir:
            return Path(env_dir)
        return Path(__file__).resolve().parent.parent / "data"

    def __init__(self, data_dir: str | Path | None = None) -> None:
        self.data_dir = Path(data_dir) if data_dir else self.default_path()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._receipts = {}
        self._load()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    @property
    def _file(self) -> Path:
        return self.data_dir / "incremental_receipts.jsonl"

    def _load(self) -> None:
        if not self._file.exists():
            return
        for line in self._file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            receipt = json.loads(line)
            self._receipts[receipt["identity_hash"]] = receipt

    def _flush(self) -> None:
        tmp = self._file.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            for receipt in self._receipts.values():
                handle.write(json.dumps(receipt, ensure_ascii=False) + "\n")
        os.replace(tmp, self._file)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_by_identity(self, identity: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            receipt = self._receipts.get(identity_hash(identity))
            return json.loads(json.dumps(receipt)) if receipt else None

    def get_by_transaction(self, transaction_id: str) -> dict[str, Any] | None:
        with self._lock:
            for receipt in self._receipts.values():
                if receipt["transaction_id"] == transaction_id:
                    return json.loads(json.dumps(receipt))
            return None

    def list_receipts(
        self,
        *,
        table: str | None = None,
        partition: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            items = [json.loads(json.dumps(r)) for r in self._receipts.values()]
        if table:
            items = [r for r in items if r["business_identity"]["table"] == table]
        if partition:
            items = [r for r in items if r["business_identity"]["partition"] == partition]
        if source:
            items = [r for r in items if r["business_identity"]["source"] == source]
        items.sort(key=lambda r: (r["committed_at"], r["receipt_id"]))
        return items

    # ------------------------------------------------------------------
    # 登记：同一业务身份幂等，只保留一份回执
    # ------------------------------------------------------------------
    def register_write(
        self,
        identity: dict[str, Any],
        *,
        process_id: str | None = None,
        batch_ref: str | None = None,
        commit_entry_id: int | None = None,
        checkpoint_entry_id: int | None = None,
    ) -> dict[str, Any]:
        """登记一次增量写入；重复处理同一批次时返回首次落库回执，不产生第二份。"""
        with self._lock:
            digest = identity_hash(identity)
            now = _utc_now()
            process_id = process_id or f"proc-{os.getpid()}"
            attempt = {
                "process_id": process_id,
                "batch_ref": batch_ref or f"batch-{digest[:10]}",
                "attempted_at": now,
            }
            existing = self._receipts.get(digest)
            if existing is not None:
                # 同一业务身份再次进入：只追加候选 attempt，绝不覆盖首次落库凭据。
                existing["attempts"].append(attempt)
                existing["last_attempt_at"] = now
                self._flush()
                receipt = json.loads(json.dumps(existing))
                receipt["deduplicated"] = True
                return receipt

            receipt = {
                "receipt_id": f"rcpt-{digest[:12]}",
                "transaction_id": transaction_id_for(identity),
                "identity_hash": digest,
                "business_identity": dict(identity),
                "pipeline": identity["source"],
                "table": identity["table"],
                "partition": identity["partition"],
                "start_offset": identity["start_offset"],
                "end_offset": identity["end_offset"],
                "row_count": identity["row_count"],
                # 原写入依据：申报来源、区间、批次、发起进程，重启后仍可查回。
                "write_basis": {
                    "source": identity["source"],
                    "requested_start_offset": identity["start_offset"],
                    "requested_end_offset": identity["end_offset"],
                    "requested_row_count": identity["row_count"],
                    "batch_ref": attempt["batch_ref"],
                    "process_id": process_id,
                    "requested_at": now,
                },
                # 实际写入数据：真正落库的分区、行偏移集合与落地路径。
                "written_data": {
                    "partition": identity["partition"],
                    "landed_rows": identity["row_count"],
                    "row_offsets": list(
                        range(identity["start_offset"], identity["end_offset"] + 1)
                    ),
                    "landing_path": (
                        f"lakehouse/{identity['table']}/{identity['partition']}/"
                        f"data-{digest[:12]}.parquet"
                    ),
                    "landed_at": now,
                },
                # 确认范围：本回执能覆盖的偏移区间，确认前后都保留。
                "confirmation_scope": {
                    "confirmable_from": identity["start_offset"],
                    "confirmable_to": identity["end_offset"],
                    "checkpoint_id": checkpoint_entry_id,
                    "confirmed_offset": None,
                    "confirmed_at": None,
                },
                "status": "landed",
                "recovered": False,
                "recovery": None,
                "commit_entry_id": commit_entry_id,
                "checkpoint_entry_id": checkpoint_entry_id,
                "committed_at": now,
                "last_attempt_at": now,
                "attempts": [attempt],
                "evidence_priority": list(EVIDENCE_PRIORITY),
            }
            self._receipts[digest] = receipt
            self._flush()
            return json.loads(json.dumps(receipt))

    # ------------------------------------------------------------------
    # 检查点确认：确认位置回写回执与检查点待办
    # ------------------------------------------------------------------
    def confirm_checkpoint(
        self,
        identity: dict[str, Any],
        *,
        checkpoint_entry_id: int,
        confirmed_offset: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            digest = identity_hash(identity)
            receipt = self._receipts.get(digest)
            if receipt is None:
                raise KeyError(f"业务身份 {digest[:12]} 没有可确认的增量提交回执")
            confirmed_offset = (
                receipt["end_offset"] if confirmed_offset is None else int(confirmed_offset)
            )
            if confirmed_offset < receipt["start_offset"]:
                raise ValueError("确认偏移不能早于回执实际落库区间起点")
            scope = receipt["confirmation_scope"]
            scope["checkpoint_id"] = checkpoint_entry_id
            scope["confirmed_offset"] = confirmed_offset
            scope["confirmed_at"] = _utc_now()
            receipt["checkpoint_entry_id"] = checkpoint_entry_id
            receipt["status"] = "confirmed"
            self._flush()
            return json.loads(json.dumps(receipt))

    # ------------------------------------------------------------------
    # 区间恢复：同一区间只生效一次
    # ------------------------------------------------------------------
    def recover_range(
        self,
        identity: dict[str, Any],
        *,
        checkpoint_entry_id: int | None = None,
        process_id: str | None = None,
    ) -> dict[str, Any]:
        """恢复指定区间；第二次进入不会登记第二份回执，也不会重复生效。"""
        with self._lock:
            digest = identity_hash(identity)
            receipt = self._receipts.get(digest)
            if receipt is None:
                raise KeyError(f"区间 {identity['start_offset']}-{identity['end_offset']} 无落库回执，无法恢复")
            if receipt["recovered"]:
                payload = json.loads(json.dumps(receipt))
                payload["recovery_effective"] = False
                payload["recovery_message"] = "该区间已恢复，恢复只生效一次，未登记第二份回执"
                return payload
            receipt["recovered"] = True
            receipt["recovery"] = {
                "checkpoint_id": checkpoint_entry_id,
                "process_id": process_id or f"proc-{os.getpid()}",
                "recovered_at": _utc_now(),
                "range": [identity["start_offset"], identity["end_offset"]],
            }
            if checkpoint_entry_id is not None:
                receipt["checkpoint_entry_id"] = checkpoint_entry_id
            self._flush()
            payload = json.loads(json.dumps(receipt))
            payload["recovery_effective"] = True
            payload["recovery_message"] = "区间恢复已生效，结果已回写检查点待办"
            return payload

    # ------------------------------------------------------------------
    # 证据优先级：检查点从哪里继续
    # ------------------------------------------------------------------
    def resolve_next_offset(
        self,
        *,
        table: str,
        partition: str,
        source_start_offset: int = 0,
        checkpoint_lookup: Any = None,
    ) -> dict[str, Any]:
        """按 检查点 > 落库回执 > 数据源起始偏移 的优先级裁决续点。

        ``checkpoint_lookup`` 为可调用对象，传入 (table, partition)，
        返回检查点行或 None；由领域服务注入，避免台账反向依赖 store。
        """
        with self._lock:
            partition_receipts = [
                r for r in self._receipts.values()
                if r["table"] == table and r["business_identity"]["partition"] == partition
            ]
            checkpoint_row = None
            if callable(checkpoint_lookup):
                checkpoint_row = checkpoint_lookup(table, partition)
            if checkpoint_row and checkpoint_row.get("数据偏移") is not None:
                offset = int(checkpoint_row["数据偏移"])
                return {
                    "next_offset": offset + 1,
                    "evidence": "checkpoint",
                    "evidence_label": EVIDENCE_LABELS["checkpoint"],
                    "evidence_detail": {
                        "checkpoint_id": checkpoint_row.get("id"),
                        "检查点标识": checkpoint_row.get("检查点标识"),
                        "confirmed_offset": offset,
                    },
                    "priority": list(EVIDENCE_PRIORITY),
                }
            if partition_receipts:
                latest = max(partition_receipts, key=lambda r: r["end_offset"])
                return {
                    "next_offset": latest["end_offset"] + 1,
                    "evidence": "receipt",
                    "evidence_label": EVIDENCE_LABELS["receipt"],
                    "evidence_detail": {
                        "receipt_id": latest["receipt_id"],
                        "transaction_id": latest["transaction_id"],
                        "landed_end_offset": latest["end_offset"],
                    },
                    "priority": list(EVIDENCE_PRIORITY),
                }
            return {
                "next_offset": int(source_start_offset),
                "evidence": "source",
                "evidence_label": EVIDENCE_LABELS["source"],
                "evidence_detail": {"source_start_offset": int(source_start_offset)},
                "priority": list(EVIDENCE_PRIORITY),
            }


# 进程级单例：数据目录固定，服务重启后构造即从磁盘重建全部回执。
ledger = ReceiptLedger()
