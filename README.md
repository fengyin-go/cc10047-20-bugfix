# 实时流数据湖构建与治理平台

面向实时流数据接入、Delta湖表构建、流批一体转换、分区合并和数据治理的流式数据湖平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8011/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5175/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8011`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源 | `stream_source` | 流数据源 | 来源标识、接入协议、主题名称 |
| 湖表定义 | `lake_table` | 湖表定义 | 表标识、表名称、主键列 |
| 流式摄入 | `stream_ingest` | 摄入管道 | 管道标识、来源流、目标湖表 |
| 提交版本 | `delta_commit` | 版本提交 | 提交标识、目标湖表、提交类型 |
| 分区合并 | `partition_merge` | 合并任务 | 任务标识、目标湖表、源分区 |
| 流Schema演进 | `stream_schema` | Schema变更 | 变更标识、目标湖表、变更类型 |
| 快照管理 | `snapshot_mgr` | 表快照 | 快照标识、目标湖表、快照时间 |
| 流质量校验 | `stream_quality` | 质量检查 | 检查标识、目标管道、校验字段 |
| 历史清理 | `vacuum_job` | 清理作业 | 作业标识、目标湖表、保留版本 |
| 批流合并 | `batch_merge` | 合并管道 | 管道标识、流来源、批来源 |
| 湖表血缘 | `lake_lineage` | 血缘链路 | 链路标识、上游表、下游表 |
| 保留策略 | `retention_policy` | 保留规则 | 规则标识、目标湖表、保留天数 |
| 压缩任务 | `compaction_task` | 压缩任务 | 任务标识、目标湖表、压缩算法 |
| 数据回填 | `backfill_pipeline` | 回填任务 | 任务标识、目标湖表、回填范围 |
| 流处理指标 | `stream_metric` | 处理指标 | 指标标识、关联管道、指标类型 |
| 格式迁移 | `iceberg_migrate` | 迁移计划 | 计划标识、源格式、目标格式 |
| 表目录同步 | `table_catalog` | 同步记录 | 记录标识、来源Catalog、目标Catalog |
| 湖仓审计 | `lake_audit` | 审计报告 | 报告标识、审计范围、审计时间 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 增量提交回执一致性

针对“相同来源的增量写入重复处理时，服务一重启事务标识就对不上”的共同根因，
平台以**批次真实业务身份**（来源 + 目标湖表 + 分区 + 起始/结束偏移 + 行数）为锚点：

- 事务标识由业务身份做 sha256 确定性生成（`app/incremental_receipt.py`），
  不再使用带进程随机盐的内置 `hash`，重启或换进程后同一批次事务标识不变；
- 同一业务身份全平台只有一份增量提交回执，重复处理只追加 `attempts` 候选，
  首次落库凭据（`write_basis`/`written_data`）永不覆盖；分区是身份的一部分，
  p1、p2 各写 9 行也各自独立成回执；
- 回执以 JSONL 落盘到 `backend/data/`（可用环境变量 `LAKEHOUSE_DATA_DIR` 改目录，
  该目录已在 `.gitignore` 忽略），服务重启后自动重建，仍可查回原写入依据、
  实际写入数据与确认范围；
- 版本详情（`delta_commit`）与检查点工作台（`stream_checkpoint`）两个入口
  回写并读取同一份提交身份；检查点确认位置回写回执待办；
- 同一区间恢复只生效一次，再次进入不登记第二份回执；
- 检查点续点按证据优先级裁决：已保存检查点确认位置 > 回执实际落库区间 >
  数据源起始偏移。

主要接口（前缀 `/api/incremental-receipts`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/commit/{entry_id}` | 版本详情入口：按业务身份登记增量写入，返回回执 |
| POST | `/checkpoint/{entry_id}/save` | 检查点入口：保存确认位置并回写同一回执 |
| POST | `/checkpoint/{entry_id}/recover` | 检查点入口：区间恢复（同一区间只生效一次） |
| GET | `/checkpoint/{entry_id}/view` | 检查点视角：哪组数据落库、尚需确认的偏移 |
| GET | `?table=&partition=&source=` | 回执工作台清单 |
| GET | `/by-transaction/{transaction_id}` | 按事务标识查回执 |
| GET | `/next-offset` | 证据优先级裁决检查点续点 |
| POST | `/identity/verify` | 校验业务身份并预览稳定事务标识（不登记） |

跨进程端到端验证脚本：`scripts/e2e_incremental_receipt.sh`
（每个 Python 调用都是全新解释器进程，真实模拟写后中断、换进程重试与重启查回）。

领域：数据工程
子方向：数仓与数据湖
