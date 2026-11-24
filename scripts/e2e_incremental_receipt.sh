#!/usr/bin/env bash
# 端到端验证：模拟“进程A 写 p1 后中断 → 重启/换进程B 重试 → 再写 p2”。
# 每个 python 调用都是全新解释器进程，真实检验跨进程、跨重启的回执一致性。
set -euo pipefail

BACKEND_DIR="$(cd "$(dirname "$0")/../backend" && pwd)"
DATA_ROOT="$(mktemp -d)"
export LAKEHOUSE_DATA_DIR="$DATA_ROOT/lakehouse-e2e"
PY="$BACKEND_DIR/.venv/bin/python"
SCRIPT_DIR="$DATA_ROOT/scripts"
mkdir -p "$SCRIPT_DIR"

# -I 隔离模式下显式注入应用路径，避免任何外部目录的同名模块（如 json.py）被加载。
# 环境变量在命令前显式声明，确保每个全新解释器进程都指向同一个隔离数据目录。
run_py() { LAKEHOUSE_DATA_DIR="$LAKEHOUSE_DATA_DIR" "$PY" -I -c "import sys; sys.path.insert(0, '$BACKEND_DIR'); exec(open('$1').read())"; }

cat > "$SCRIPT_DIR/step1.py" <<'PY'
import os
from app.services import service_for
svc = service_for("delta_commit")
r = svc.domain_ingest_delta(1, delta_records=9, source="stream-orders",
                            table="lake_orders", partition="p1",
                            start_offset=8, process_id="proc-A",
                            batch_ref="batch-p1")
assert r["ok"] and not r["deduplicated"]
print(r["transaction_id"])
PY

cat > "$SCRIPT_DIR/step2.py" <<'PY'
import os
from app.services import service_for
expected = os.environ["P1_A"]
svc = service_for("delta_commit")
r = svc.domain_ingest_delta(1, delta_records=9, source="stream-orders",
                            table="lake_orders", partition="p1",
                            start_offset=8, process_id="proc-B",
                            batch_ref="batch-p1")
assert r["ok"] and r["deduplicated"], r
assert r["candidate_count"] == 2
assert r["transaction_id"] == expected
print(r["transaction_id"])
PY

cat > "$SCRIPT_DIR/step3.py" <<'PY'
from app.services import service_for
svc = service_for("delta_commit")
r = svc.domain_ingest_delta(2, delta_records=9, source="stream-orders",
                            table="lake_orders", partition="p2",
                            start_offset=0, process_id="proc-B",
                            batch_ref="batch-p2")
assert r["ok"] and not r["deduplicated"]
print(r["transaction_id"])
PY

cat > "$SCRIPT_DIR/step4.py" <<'PY'
import os
from app.incremental_receipt import ledger
from app.services import service_for
expected = os.environ["P1_A"]
# 检查点工作台：通过事务标识关联同一份落盘回执并补回确认位置。
svc = service_for("stream_checkpoint")
saved = svc.domain_save_checkpoint(1, {"transaction_id": expected,
                                       "confirmed_offset": 16})
assert saved["ok"] and saved["transaction_id"] == expected
assert saved["confirmed_offset"] == 16
view = svc.domain_receipt_view(1)
assert view["candidate_count"] == 2
assert view["landed_rows"] == 9 and view["pending_confirm_offset"] == 0
# 本进程内两个入口读到同一事务标识（跨进程持久化由落盘回执保证，见 step5）。
from app.store import store
assert store.find("stream_checkpoint", 1)["事务标识"] == expected
receipt = ledger.get_by_transaction(expected)
assert receipt["confirmation_scope"]["confirmed_offset"] == 16
print("检查点已关联同一回执，确认位置补回 16")
PY

cat > "$SCRIPT_DIR/step5.py" <<'PY'
import os
from app.incremental_receipt import ledger
from app.services import service_for
p1 = os.environ["P1_A"]
p2 = os.environ["P2"]
receipts = ledger.list_receipts()
assert len(receipts) == 2, receipts
by_part = {r["partition"]: r for r in receipts}
assert by_part["p1"]["transaction_id"] == p1
assert by_part["p2"]["transaction_id"] == p2
assert by_part["p1"]["written_data"]["landed_rows"] == 9
assert by_part["p2"]["written_data"]["landed_rows"] == 9
assert by_part["p1"]["confirmation_scope"]["confirmed_offset"] == 16
assert by_part["p1"]["write_basis"]["process_id"] == "proc-A"
svc = service_for("stream_checkpoint")
# 证据优先级：重启后内存检查点尚未重建，按来源精确查找无果，回退到回执证据，续点 17
decision = svc.domain_next_offset(table="lake_orders", partition="p1",
                                  source="stream-orders")
assert decision["next_offset"] == 17, decision
assert decision["evidence"] == "receipt", decision
# 恢复只生效一次（恢复会把检查点条目绑定到该身份）
body = {"source": "stream-orders", "table": "lake_orders",
        "partition": "p1", "start_offset": 8, "row_count": 9}
first = svc.domain_recover_checkpoint(2, body)
second = svc.domain_recover_checkpoint(2, body)
assert first["recovery_effective"] and not second["recovery_effective"]
assert len(ledger.list_receipts()) == 2
# 恢复绑定后检查点证据生效，确认位置 16 → 续点仍是 17，但证据来源升级为检查点
decision_after = svc.domain_next_offset(table="lake_orders", partition="p1",
                                        source="stream-orders")
assert decision_after["evidence"] == "checkpoint", decision_after
assert decision_after["next_offset"] == 17
print("重启后两份回执完整，恢复单次生效；恢复前回执证据、恢复后检查点证据，续点均为 17")
PY

echo "== 进程A：p1 偏移 8~16（9 行）写完后退出 =="
P1_A=$(run_py "$SCRIPT_DIR/step1.py")
echo "进程A 事务标识: $P1_A"
export P1_A

echo "== 进程B（全新进程，模拟服务重启）：重试同一批次 =="
P1_B=$(run_py "$SCRIPT_DIR/step2.py")
echo "进程B 重试事务标识: $P1_B （必须与进程A 相同）"

echo "== 进程B：再送 p2 的另外 9 行 =="
P2=$(run_py "$SCRIPT_DIR/step3.py")
export P2
echo "p2 事务标识: $P2 （必须与 p1 不同）"

echo "== 检查点工作台：补回 p1 确认位置 16，并核对落库/待确认 =="
run_py "$SCRIPT_DIR/step4.py"

echo "== 再次重启：两份回执、确认范围、首次落库依据仍可查回 =="
run_py "$SCRIPT_DIR/step5.py"

echo ""
echo "全部 E2E 断言通过。数据目录: $LAKEHOUSE_DATA_DIR"
rm -rf "$DATA_ROOT"
