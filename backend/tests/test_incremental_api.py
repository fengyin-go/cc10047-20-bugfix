"""增量提交回执 HTTP 接口测试：验证两个入口经 API 读取同一份提交身份。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.seed import SEED_ROWS
from app.store import store


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """客户端在台账替换后再构造，确保路由句柄指向隔离台账。"""
    from app import incremental_receipt
    from app.routers import incremental as incremental_router
    from app.services import delta_commit as delta_commit_svc
    from app.services import stream_checkpoint as checkpoint_svc

    # 用例开始前恢复种子数据，避免其他测试文件对共享 store 的污染。
    for name, rows in SEED_ROWS.items():
        store._tables[name] = [dict(row) for row in rows]
    fresh = incremental_receipt.ReceiptLedger(data_dir=tmp_path / "receipts")
    monkeypatch.setattr(incremental_receipt, "ledger", fresh)
    monkeypatch.setattr(incremental_router, "ledger", fresh)
    monkeypatch.setattr(delta_commit_svc, "ledger", fresh)
    monkeypatch.setattr(checkpoint_svc, "ledger", fresh)
    with TestClient(app) as test_client:
        yield test_client
    for name, rows in SEED_ROWS.items():
        store._tables[name] = [dict(row) for row in rows]


class TestIncrementalReceiptApi:
    def test_register_and_deduplicate_via_api(self, client):
        payload = {
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
            "process_id": "proc-A", "batch_ref": "b-1",
        }
        first = client.post("/api/incremental-receipts/commit/1", json=payload)
        assert first.status_code == 200
        data = first.json()
        assert data["ok"] is True
        assert data["ingested_rows"] == 9
        txn = data["transaction_id"]

        # 换进程重试同一批次：事务标识不变，标记去重。
        payload["process_id"] = "proc-B"
        second = client.post("/api/incremental-receipts/commit/1", json=payload)
        data2 = second.json()
        assert data2["transaction_id"] == txn
        assert data2["deduplicated"] is True
        assert data2["candidate_count"] == 2

    def test_partitions_keep_separate_receipts_via_api(self, client):
        base = {"source": "stream-orders", "table": "lake_orders", "row_count": 9}
        r1 = client.post("/api/incremental-receipts/commit/1",
                         json={**base, "partition": "p1", "start_offset": 8}).json()
        r2 = client.post("/api/incremental-receipts/commit/2",
                         json={**base, "partition": "p2", "start_offset": 0}).json()
        assert r1["transaction_id"] != r2["transaction_id"]
        listing = client.get("/api/incremental-receipts").json()
        assert listing["total"] == 2

    def test_checkpoint_save_and_shared_identity_via_api(self, client):
        ingest = client.post("/api/incremental-receipts/commit/1", json={
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
        }).json()
        saved = client.post("/api/incremental-receipts/checkpoint/1/save", json={
            "transaction_id": ingest["transaction_id"], "confirmed_offset": 16,
        }).json()
        assert saved["ok"] is True
        assert saved["transaction_id"] == ingest["transaction_id"]
        assert saved["confirmed_offset"] == 16

        view = client.get("/api/incremental-receipts/checkpoint/1/view").json()
        assert view["transaction_id"] == ingest["transaction_id"]
        assert view["landed_rows"] == 9
        assert view["pending_confirm_offset"] == 0

    def test_recover_once_via_api(self, client):
        client.post("/api/incremental-receipts/commit/1", json={
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
        })
        body = {
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
        }
        first = client.post("/api/incremental-receipts/checkpoint/1/recover", json=body).json()
        assert first["recovery_effective"] is True
        second = client.post("/api/incremental-receipts/checkpoint/1/recover", json=body).json()
        assert second["recovery_effective"] is False
        assert client.get("/api/incremental-receipts").json()["total"] == 1

    def test_evidence_priority_via_api(self, client):
        # 仅数据源证据。
        decision = client.get(
            "/api/incremental-receipts/next-offset",
            params={"table": "lake_orders", "partition": "p1", "source_start_offset": 3},
        ).json()
        assert decision["evidence"] == "source"
        assert decision["next_offset"] == 3
        # 落库后切换为回执证据。
        client.post("/api/incremental-receipts/commit/1", json={
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
        })
        decision = client.get(
            "/api/incremental-receipts/next-offset",
            params={"table": "lake_orders", "partition": "p1", "source_start_offset": 3},
        ).json()
        assert decision["evidence"] == "receipt"
        assert decision["next_offset"] == 17
        # 检查点确认后检查点证据优先。
        client.post("/api/incremental-receipts/checkpoint/1/save", json={
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
            "confirmed_offset": 12,
        })
        decision = client.get(
            "/api/incremental-receipts/next-offset",
            params={"table": "lake_orders", "partition": "p1", "source_start_offset": 3},
        ).json()
        assert decision["evidence"] == "checkpoint"
        assert decision["next_offset"] == 13

    def test_identity_verify_rejects_bad_batch(self, client):
        ok = client.post("/api/incremental-receipts/identity/verify", json={
            "source": "s", "table": "t", "partition": "p1",
            "start_offset": 8, "end_offset": 16, "row_count": 9,
        }).json()
        assert ok["ok"] is True
        bad = client.post("/api/incremental-receipts/identity/verify", json={
            "source": "s", "table": "t", "partition": "p1",
            "start_offset": 8, "end_offset": 16, "row_count": 5,
        }).json()
        assert bad["ok"] is False
