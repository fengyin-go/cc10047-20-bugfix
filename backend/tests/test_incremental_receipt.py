"""增量提交回执一致性测试：覆盖重启幂等、双入口同身份、恢复单次生效、证据优先级。

叙事主线：
  进程A 把 p1 的偏移 8~16（9 行）写完后退出；换进程B 重试同一批次，
  再送 p2 的另外 9 行；切换版本详情与检查点工作台，核对两份回执、
  确认位置与实际行数。
"""
from __future__ import annotations

from app.incremental_receipt import (
    ReceiptLedger,
    build_business_identity,
    identity_hash,
    ledger as module_ledger,
    transaction_id_for,
)
from app.services import service_for
from app.store import store


P1_IDENTITY = dict(
    source="stream-orders", table="lake_orders", partition="p1",
    start_offset=8, end_offset=16, row_count=9,
)
P2_IDENTITY = dict(
    source="stream-orders", table="lake_orders", partition="p2",
    start_offset=0, end_offset=8, row_count=9,
)


def _p1_identity():
    return build_business_identity(**P1_IDENTITY)


def _p2_identity():
    return build_business_identity(**P2_IDENTITY)


class TestBusinessIdentity:
    def test_stable_identity_hash_across_processes(self, temp_ledger, reset_store):
        """同一业务身份在任意进程、任意重启后得到同一哈希与事务标识。"""
        first = identity_hash(_p1_identity())
        # 模拟另一进程：重新构造字符串再哈希，结果必须一致（不再依赖内置 hash 盐）。
        rebuilt = build_business_identity(
            source="stream-orders", table="lake_orders", partition="p1",
            start_offset=8, end_offset=16, row_count=9,
        )
        assert identity_hash(rebuilt) == first
        assert transaction_id_for(rebuilt) == f"txn-{first[:16]}"

    def test_different_partitions_get_different_receipts(self, temp_ledger, reset_store):
        """两个不同分区都写 9 行时不能共用一份回执。"""
        r1 = temp_ledger.register_write(_p1_identity(), process_id="proc-A")
        r2 = temp_ledger.register_write(_p2_identity(), process_id="proc-A")
        assert r1["receipt_id"] != r2["receipt_id"]
        assert r1["transaction_id"] != r2["transaction_id"]
        assert r1["partition"] == "p1"
        assert r2["partition"] == "p2"
        assert r1["written_data"]["landed_rows"] == 9
        assert r2["written_data"]["landed_rows"] == 9
        assert len(temp_ledger.list_receipts()) == 2

    def test_identity_validation_rejects_mismatched_rows(self, temp_ledger, reset_store):
        """真实业务身份校验：区间与行数不自洽必须拒绝。"""
        import pytest

        with pytest.raises(ValueError):
            build_business_identity(
                source="s", table="t", partition="p1",
                start_offset=8, end_offset=16, row_count=8,
            )
        with pytest.raises(ValueError):
            build_business_identity(
                source="", table="t", partition="p1",
                start_offset=0, end_offset=8, row_count=9,
            )


class TestIdempotentRegistration:
    def test_retry_same_batch_returns_single_receipt(self, temp_ledger, reset_store):
        """进程A 写完后进程B 重试：只有一份回执，第二份候选只记 attempt。"""
        first = temp_ledger.register_write(_p1_identity(), process_id="proc-A", batch_ref="b-1")
        second = temp_ledger.register_write(_p1_identity(), process_id="proc-B", batch_ref="b-1")
        assert second["deduplicated"] is True
        assert second["receipt_id"] == first["receipt_id"]
        assert second["transaction_id"] == first["transaction_id"]
        assert len(second["attempts"]) == 2
        assert {a["process_id"] for a in second["attempts"]} == {"proc-A", "proc-B"}
        assert len(temp_ledger.list_receipts()) == 1

    def test_first_landing_evidence_never_overwritten(self, temp_ledger, reset_store):
        """重复登记后，原写入依据与实际写入数据仍是首次落库凭据。"""
        first = temp_ledger.register_write(_p1_identity(), process_id="proc-A")
        temp_ledger.register_write(_p1_identity(), process_id="proc-B")
        again = temp_ledger.get_by_transaction(first["transaction_id"])
        assert again["write_basis"]["process_id"] == "proc-A"
        assert again["write_basis"]["requested_row_count"] == 9
        assert again["written_data"]["landed_at"] == first["written_data"]["landed_at"]
        assert again["written_data"]["row_offsets"] == list(range(8, 17))

    def test_plain_field_change_still_finds_first_receipt(self, temp_ledger, reset_store):
        """普通登记字段变化后，仍凭业务身份找到首次落库凭据。"""
        svc = service_for("delta_commit")
        result = svc.domain_ingest_delta(
            1, delta_records=9, source="stream-orders", table="lake_orders",
            partition="p1", start_offset=8, process_id="proc-A",
        )
        entry = store.find("delta_commit", 1)
        txn_before = entry["事务标识"]
        # 模拟普通登记字段被改动（与业务身份无关）。
        entry["提交时间"] = "2026-11-25"
        entry["提交水位"] = "wm-changed"
        retry = svc.domain_ingest_delta(
            1, delta_records=9, source="stream-orders", table="lake_orders",
            partition="p1", start_offset=8, process_id="proc-B",
        )
        assert retry["transaction_id"] == txn_before
        assert retry["deduplicated"] is True
        assert retry["write_basis"]["process_id"] == "proc-A"


class TestRestartPersistence:
    def test_rebuild_ledger_after_restart(self, tmp_path, reset_store):
        """服务中断重启：新台账实例从磁盘查回原写入依据、写入数据与确认范围。"""
        data_dir = tmp_path / "lakehouse-data"
        first = ReceiptLedger(data_dir=data_dir)
        receipt = first.register_write(_p1_identity(), process_id="proc-A")
        first.confirm_checkpoint(
            _p1_identity(), checkpoint_entry_id=1, confirmed_offset=12,
        )

        # 模拟重启：全新实例，内存为空，只能从磁盘恢复。
        restarted = ReceiptLedger(data_dir=data_dir)
        recovered = restarted.get_by_transaction(receipt["transaction_id"])
        assert recovered is not None
        assert recovered["receipt_id"] == receipt["receipt_id"]
        assert recovered["business_identity"] == P1_IDENTITY
        assert recovered["write_basis"]["source"] == "stream-orders"
        assert recovered["written_data"]["row_offsets"] == list(range(8, 17))
        assert recovered["confirmation_scope"]["confirmed_offset"] == 12
        assert recovered["confirmation_scope"]["checkpoint_id"] == 1


class TestTwoEntriesShareIdentity:
    def test_version_detail_and_checkpoint_read_same_identity(self, temp_ledger, reset_store):
        """版本详情与检查点工作台各入口读取同一份提交身份。"""
        commit_svc = service_for("delta_commit")
        ckpt_svc = service_for("stream_checkpoint")
        ingest = commit_svc.domain_ingest_delta(
            1, delta_records=9, source="stream-orders", table="lake_orders",
            partition="p1", start_offset=8, process_id="proc-A",
        )
        # 检查点入口通过事务标识关联同一份回执。
        saved = ckpt_svc.domain_save_checkpoint(1, {
            "transaction_id": ingest["transaction_id"],
            "confirmed_offset": 16,
        })
        commit_entry = store.find("delta_commit", 1)
        ckpt_entry = store.find("stream_checkpoint", 1)
        assert saved["transaction_id"] == ingest["transaction_id"]
        assert ckpt_entry["事务标识"] == commit_entry["事务标识"]
        assert ckpt_entry["增量回执编号"] == commit_entry["增量回执编号"]
        assert ckpt_entry["增量业务身份"] == commit_entry["增量业务身份"]

        view = ckpt_svc.domain_receipt_view(1)
        assert view["transaction_id"] == ingest["transaction_id"]
        assert view["landed_rows"] == 9
        assert view["confirmed_offset"] == 16
        assert view["pending_confirm_offset"] == 0

    def test_two_receipt_candidates_explained_after_restart(self, tmp_path, reset_store):
        """数据已写但确认位置未保存就中断，重启后两份候选必须说清落库组与待确认偏移。"""
        import app.services.delta_commit as delta_commit_svc
        import app.services.stream_checkpoint as checkpoint_svc

        data_dir = tmp_path / "lakehouse-data"
        led = ReceiptLedger(data_dir=data_dir)
        led.register_write(_p1_identity(), process_id="proc-A", batch_ref="batch-1")
        # 中断前未调用 confirm_checkpoint —— 确认位置缺失。
        restarted = ReceiptLedger(data_dir=data_dir)
        delta_commit_svc.ledger = restarted
        checkpoint_svc.ledger = restarted
        try:
            receipt = restarted.get_by_identity(_p1_identity())
            assert receipt["status"] == "landed"
            assert receipt["confirmation_scope"]["confirmed_offset"] is None
            # 另一进程重试同一批次：出现两个候选 attempt，但仍是同一份回执。
            retry = restarted.register_write(_p1_identity(), process_id="proc-B", batch_ref="batch-1")
            assert retry["deduplicated"] is True
            assert len(restarted.list_receipts()) == 1
            # 说清哪组数据确实落库。
            assert retry["written_data"]["partition"] == "p1"
            assert retry["written_data"]["landed_rows"] == 9
            # 说清尚需确认的偏移：落库到 16，确认位置为空，整段 8~16 待确认。
            assert retry["confirmation_scope"]["confirmable_from"] == 8
            assert retry["confirmation_scope"]["confirmable_to"] == 16

            ckpt_svc = service_for("stream_checkpoint")
            # 恢复时把检查点条目绑定到该身份并确认。
            bound = ckpt_svc.domain_save_checkpoint(2, {
                "source": "stream-orders", "table": "lake_orders",
                "partition": "p1", "start_offset": 8, "row_count": 9,
                "confirmed_offset": 16,
            })
            assert bound["ok"] is True
            assert bound["transaction_id"] == receipt["transaction_id"]
            assert store.find("stream_checkpoint", 2)["数据偏移"] == 16
        finally:
            from app.incremental_receipt import ledger as global_ledger
            delta_commit_svc.ledger = global_ledger
            checkpoint_svc.ledger = global_ledger


class TestRecoveryOnce:
    def test_recovery_effective_only_once(self, temp_ledger, reset_store):
        """同一区间恢复只生效一次，再次进入不能登记第二份回执。"""
        temp_ledger.register_write(_p1_identity(), process_id="proc-A")
        ckpt_svc = service_for("stream_checkpoint")
        first = ckpt_svc.domain_recover_checkpoint(1, {
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
            "process_id": "proc-B",
        })
        assert first["ok"] is True
        assert first["recovery_effective"] is True
        second = ckpt_svc.domain_recover_checkpoint(1, {
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
            "process_id": "proc-C",
        })
        assert second["recovery_effective"] is False
        assert "只生效一次" in second["message"]
        assert len(temp_ledger.list_receipts()) == 1
        receipt = temp_ledger.get_by_identity(_p1_identity())
        assert receipt["recovery"]["process_id"] == "proc-B"
        assert store.find("stream_checkpoint", 1)["status"] == "已恢复"


class TestEvidencePriority:
    def test_priority_checkpoint_receipt_source(self, temp_ledger, reset_store):
        """证据优先级决定检查点从哪里继续：检查点 > 回执 > 数据源。"""
        ckpt_svc = service_for("stream_checkpoint")
        # 无任何证据：使用数据源起始偏移。
        decision = ckpt_svc.domain_next_offset(
            table="lake_orders", partition="p1", source_start_offset=3)
        assert decision["evidence"] == "source"
        assert decision["next_offset"] == 3

        # 有落库回执但无检查点：续点取回执 end_offset + 1。
        temp_ledger.register_write(_p1_identity(), process_id="proc-A")
        decision = ckpt_svc.domain_next_offset(
            table="lake_orders", partition="p1", source_start_offset=3)
        assert decision["evidence"] == "receipt"
        assert decision["next_offset"] == 17

        # 检查点确认位置存在且更早：检查点证据优先，从确认位置之后继续。
        ckpt_svc.domain_save_checkpoint(1, {
            "source": "stream-orders", "table": "lake_orders",
            "partition": "p1", "start_offset": 8, "row_count": 9,
            "confirmed_offset": 12,
        })
        decision = ckpt_svc.domain_next_offset(
            table="lake_orders", partition="p1", source_start_offset=3)
        assert decision["evidence"] == "checkpoint"
        assert decision["next_offset"] == 13
        assert decision["priority"] == ["checkpoint", "receipt", "source"]


class TestFullNarrative:
    def test_p1_then_p2_across_processes(self, tmp_path, reset_store):
        """完整叙事：p1 8~16 写后退出 → 另一进程重试 → p2 另 9 行，核对两回执与行数。"""
        data_dir = tmp_path / "lakehouse-data"
        led = ReceiptLedger(data_dir=data_dir)
        import app.incremental_receipt as incr
        import app.services.delta_commit as delta_commit_svc
        import app.services.stream_checkpoint as checkpoint_svc
        original = incr.ledger
        incr.ledger = led
        delta_commit_svc.ledger = led
        checkpoint_svc.ledger = led
        try:
            commit_svc = service_for("delta_commit")
            ckpt_svc = service_for("stream_checkpoint")

            # 进程A：p1 偏移 8~16 共 9 行，写完即退出（未保存确认位置）。
            r_p1_a = commit_svc.domain_ingest_delta(
                1, delta_records=9, source="stream-orders", table="lake_orders",
                partition="p1", start_offset=8, process_id="proc-A", batch_ref="batch-p1",
            )
            assert r_p1_a["ingested_rows"] == 9
            assert r_p1_a["deduplicated"] is False

            # 模拟重启：新进程加载磁盘台账后重试同一批次。
            led_b = ReceiptLedger(data_dir=data_dir)
            incr.ledger = led_b
            delta_commit_svc.ledger = led_b
            checkpoint_svc.ledger = led_b
            r_p1_b = commit_svc.domain_ingest_delta(
                1, delta_records=9, source="stream-orders", table="lake_orders",
                partition="p1", start_offset=8, process_id="proc-B", batch_ref="batch-p1",
            )
            assert r_p1_b["transaction_id"] == r_p1_a["transaction_id"]
            assert r_p1_b["deduplicated"] is True

            # 进程B 再送 p2 的另外 9 行：独立回执。
            r_p2 = commit_svc.domain_ingest_delta(
                2, delta_records=9, source="stream-orders", table="lake_orders",
                partition="p2", start_offset=0, process_id="proc-B", batch_ref="batch-p2",
            )
            assert r_p2["deduplicated"] is False
            assert r_p2["transaction_id"] != r_p1_a["transaction_id"]

            # 两份回执、确认位置与实际行数核对。
            all_receipts = led_b.list_receipts()
            assert len(all_receipts) == 2
            by_partition = {r["partition"]: r for r in all_receipts}
            assert by_partition["p1"]["written_data"]["landed_rows"] == 9
            assert by_partition["p2"]["written_data"]["landed_rows"] == 9
            assert by_partition["p1"]["attempts"][0]["process_id"] == "proc-A"

            # 检查点工作台确认 p1（确认位置从丢失到补回 16）。
            saved = ckpt_svc.domain_save_checkpoint(1, {
                "transaction_id": r_p1_a["transaction_id"], "confirmed_offset": 16})
            assert saved["confirmed_offset"] == 16
            view_p1 = ckpt_svc.domain_receipt_view(1)
            assert view_p1["candidate_count"] == 2
            assert view_p1["pending_confirm_offset"] == 0

            # 切换版本详情入口，读到同一事务标识。
            commit_entry = store.find("delta_commit", 1)
            ckpt_entry = store.find("stream_checkpoint", 1)
            assert commit_entry["事务标识"] == ckpt_entry["事务标识"]

            # 再重启一次，两份回执与确认范围仍可查回。
            led_c = ReceiptLedger(data_dir=data_dir)
            assert len(led_c.list_receipts()) == 2
            recovered_p1 = led_c.get_by_transaction(r_p1_a["transaction_id"])
            assert recovered_p1["confirmation_scope"]["confirmed_offset"] == 16
            assert recovered_p1["write_basis"]["process_id"] == "proc-A"
        finally:
            incr.ledger = original
            delta_commit_svc.ledger = original
            checkpoint_svc.ledger = original
