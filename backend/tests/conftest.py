"""pytest 全局夹具：把增量回执台账隔离到临时目录，每个测试函数前后重置内存仓库。"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

# 必须在导入 app 业务模块之前指定台账数据目录，使进程级单例落到临时路径，
# 测试运行绝不污染后端真实落盘文件 backend/data。
_SESSION_DATA_DIR = Path(tempfile.mkdtemp(prefix="lakehouse-pytest-")) / "receipts"
os.environ["LAKEHOUSE_DATA_DIR"] = str(_SESSION_DATA_DIR)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import incremental_receipt  # noqa: E402
from app.seed import SEED_ROWS  # noqa: E402
from app.store import store  # noqa: E402


def _reset_store() -> None:
    for name, rows in SEED_ROWS.items():
        store._tables[name] = [dict(row) for row in rows]
    for name in list(store._tables):
        if name not in SEED_ROWS:
            store._tables.pop(name, None)


@pytest.fixture
def temp_ledger(tmp_path, monkeypatch):
    """台账指向用例专属临时目录，并替换各模块持有的单例引用。

    领域服务在模块加载时以 ``from app.incremental_receipt import ledger``
    绑定了单例，因此各服务模块、路由模块里的同名引用也要一起替换，
    否则服务仍写旧台账。
    """
    fresh = incremental_receipt.ReceiptLedger(data_dir=tmp_path / "receipts")
    monkeypatch.setattr(incremental_receipt, "ledger", fresh)
    import app.routers.incremental as incremental_router
    import app.services.delta_commit as delta_commit_svc
    import app.services.stream_checkpoint as checkpoint_svc

    monkeypatch.setattr(delta_commit_svc, "ledger", fresh)
    monkeypatch.setattr(checkpoint_svc, "ledger", fresh)
    monkeypatch.setattr(incremental_router, "ledger", fresh)
    return fresh


@pytest.fixture
def reset_store():
    """每个用例前后都把内存仓库恢复为种子数据，避免跨文件、跨用例污染。"""
    _reset_store()
    yield
    _reset_store()
