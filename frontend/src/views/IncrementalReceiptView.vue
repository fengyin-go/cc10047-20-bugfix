<template>
  <section class="page" data-module="incremental_receipt">
    <header class="page-head">
      <div>
        <h2>增量提交回执工作台</h2>
        <p class="page-desc">
          以批次真实业务身份（来源/湖表/分区/偏移区间/行数）为准，同一批次跨重启只有一份回执，
          版本详情与检查点工作台读取同一份提交身份。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-ingest" type="button" @click="openIngest">
          登记增量写入
        </button>
        <button class="btn" data-testid="refresh-receipts" type="button" @click="reload">
          刷新回执
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">回执总数（去重后）</span>
        <strong class="stat-value">{{ total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">存在重试候选的回执</span>
        <strong class="stat-value">{{ retriedCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待确认偏移的回执</span>
        <strong class="stat-value">{{ pendingCount }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>目标湖表</span>
        <input v-model="filters.table" data-testid="filter-table" placeholder="按湖表过滤" />
      </label>
      <label class="filter-item">
        <span>分区</span>
        <input v-model="filters.partition" data-testid="filter-partition" placeholder="如 p1" />
      </label>
      <label class="filter-item">
        <span>来源</span>
        <input v-model="filters.source" data-testid="filter-source" placeholder="按来源过滤" />
      </label>
      <button class="btn" data-testid="query-receipts" type="submit">查询</button>
    </form>

    <div class="decision-card" data-testid="decision-card">
      <h3>检查点续点裁决（证据优先级：检查点 &gt; 落库回执 &gt; 数据源起始偏移）</h3>
      <form class="filter-bar" @submit.prevent="runDecision">
        <label class="filter-item">
          <span>湖表</span>
          <input v-model="decisionForm.table" data-testid="decision-table" required />
        </label>
        <label class="filter-item">
          <span>分区</span>
          <input v-model="decisionForm.partition" data-testid="decision-partition" required />
        </label>
        <label class="filter-item">
          <span>来源（可选）</span>
          <input v-model="decisionForm.source" data-testid="decision-source" />
        </label>
        <button class="btn primary" data-testid="decision-run" type="submit">裁决续点</button>
      </form>
      <p v-if="decision" class="decision-result" data-testid="decision-result">
        下一偏移 = <strong>{{ decision.next_offset }}</strong>，
        采信证据：{{ decision.evidence_label }}
      </p>
    </div>

    <table class="data-table" data-testid="receipt-table">
      <thead>
        <tr>
          <th>事务标识（跨重启一致）</th>
          <th>回执编号</th>
          <th>湖表 / 分区</th>
          <th>偏移区间</th>
          <th>实际落库行数</th>
          <th>确认偏移 / 待确认</th>
          <th>重试候选</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in items" :key="receipt.receipt_id" :data-testid="`receipt-${receipt.receipt_id}`">
          <td class="txn-cell">{{ receipt.transaction_id }}</td>
          <td>{{ receipt.receipt_id }}</td>
          <td>{{ receipt.table }} / {{ receipt.partition }}</td>
          <td>[{{ receipt.start_offset }}, {{ receipt.end_offset }}]</td>
          <td>{{ receipt.written_data.landed_rows }}</td>
          <td>
            {{ receipt.confirmation_scope.confirmed_offset ?? '未确认' }}
            / 待确认 {{ pendingOf(receipt) }}
          </td>
          <td>
            <span :data-testid="`candidates-${receipt.receipt_id}`">{{ receipt.attempts.length }} 份</span>
            <ul v-if="receipt.attempts.length > 1" class="candidate-list">
              <li v-for="attempt in receipt.attempts" :key="attempt.attempted_at">
                {{ attempt.process_id }} · {{ attempt.batch_ref }} · {{ attempt.attempted_at }}
              </li>
            </ul>
          </td>
          <td>{{ receipt.recovered ? '已恢复' : receipt.status }}</td>
        </tr>
        <tr v-if="!items.length">
          <td colspan="8" class="empty-state">暂无增量提交回执，可先登记增量写入</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showIngest" class="modal-backdrop" data-testid="ingest-modal">
      <form class="modal" @submit.prevent="submitIngest">
        <h3>登记增量写入（校验真实业务身份）</h3>
        <label class="create-field">
          <span>版本提交条目 ID</span>
          <input v-model.number="ingestForm.commitEntryId" data-testid="ingest-commit-id" type="number" required />
        </label>
        <label class="create-field">
          <span>来源标识</span>
          <input v-model="ingestForm.source" data-testid="ingest-source" required />
        </label>
        <label class="create-field">
          <span>目标湖表</span>
          <input v-model="ingestForm.table" data-testid="ingest-table" required />
        </label>
        <label class="create-field">
          <span>分区</span>
          <input v-model="ingestForm.partition" data-testid="ingest-partition" required />
        </label>
        <label class="create-field">
          <span>起始偏移</span>
          <input v-model.number="ingestForm.startOffset" data-testid="ingest-start" type="number" required />
        </label>
        <label class="create-field">
          <span>行数（结束偏移自动推算）</span>
          <input v-model.number="ingestForm.rowCount" data-testid="ingest-rows" type="number" min="1" required />
        </label>
        <label class="create-field">
          <span>进程标识（模拟重启/换进程）</span>
          <input v-model="ingestForm.processId" data-testid="ingest-process" />
        </label>
        <p v-if="previewMessage" class="error-text" data-testid="ingest-preview">{{ previewMessage }}</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="showIngest = false">取消</button>
          <button class="btn primary" data-testid="submit-ingest" type="submit" :disabled="submitting">
            {{ submitting ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="lastReceipt" class="receipt-detail" data-testid="receipt-detail">
      <h3>最近一次登记回执</h3>
      <dl>
        <dt>事务标识</dt>
        <dd>{{ lastReceipt.transaction_id }}</dd>
        <dt>是否重复批次</dt>
        <dd>{{ lastReceipt.deduplicated ? '是（返回首次落库回执，未新增）' : '否（首次落库）' }}</dd>
        <dt>原写入依据</dt>
        <dd>
          {{ lastReceipt.write_basis.process_id }} · {{ lastReceipt.write_basis.batch_ref }} ·
          申报 {{ lastReceipt.write_basis.requested_row_count }} 行
        </dd>
        <dt>实际写入数据</dt>
        <dd>
          {{ lastReceipt.written_data.partition }} 落库
          {{ lastReceipt.written_data.landed_rows }} 行，路径 {{ lastReceipt.written_data.landing_path }}
        </dd>
        <dt>确认范围</dt>
        <dd>
          [{{ lastReceipt.confirmation_scope.confirmable_from }},
          {{ lastReceipt.confirmation_scope.confirmable_to }}]，
          已确认 {{ lastReceipt.confirmation_scope.confirmed_offset ?? '无' }}
        </dd>
      </dl>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 份去重回执</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  listReceipts,
  registerIngest,
  resolveNextOffset,
  type IngestReceipt,
  type NextOffsetDecision,
  type ReceiptListItem,
} from '@/api/incremental-receipt'

const items = ref<ReceiptListItem[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = reactive({ table: '', partition: '', source: '' })
const showIngest = ref(false)
const submitting = ref(false)
const previewMessage = ref('')
const lastReceipt = ref<IngestReceipt | null>(null)
const decision = ref<NextOffsetDecision | null>(null)
const decisionForm = reactive({ table: 'lake_orders', partition: 'p1', source: '' })

const ingestForm = reactive({
  commitEntryId: 1,
  source: 'stream-orders',
  table: 'lake_orders',
  partition: 'p1',
  startOffset: 8,
  rowCount: 9,
  processId: '',
})

const retriedCount = computed(
  () => items.value.filter((receipt) => receipt.attempts.length > 1).length,
)
const pendingCount = computed(
  () => items.value.filter((receipt) => pendingOf(receipt) > 0).length,
)

function pendingOf(receipt: ReceiptListItem): number {
  const confirmed = receipt.confirmation_scope.confirmed_offset
  if (confirmed === null || confirmed === undefined) {
    return receipt.written_data.landed_rows
  }
  return Math.max(0, receipt.end_offset - confirmed)
}

function openIngest() {
  previewMessage.value = ''
  showIngest.value = true
}

async function submitIngest() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const receipt = await registerIngest(ingestForm.commitEntryId, {
      source: ingestForm.source,
      table: ingestForm.table,
      partition: ingestForm.partition,
      start_offset: ingestForm.startOffset,
      row_count: ingestForm.rowCount,
      process_id: ingestForm.processId || undefined,
    })
    if (!receipt.ok) throw new Error(receipt.message || '登记失败')
    lastReceipt.value = receipt
    previewMessage.value = receipt.deduplicated
      ? `重复批次：返回事务标识 ${receipt.transaction_id} 的首次落库回执`
      : `首次落库：事务标识 ${receipt.transaction_id}`
    showIngest.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    submitting.value = false
  }
}

async function runDecision() {
  errorMessage.value = ''
  try {
    decision.value = await resolveNextOffset({
      table: decisionForm.table,
      partition: decisionForm.partition,
      source: decisionForm.source || undefined,
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '裁决失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listReceipts({
      table: filters.table || undefined,
      partition: filters.partition || undefined,
      source: filters.source || undefined,
    })
    items.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.decision-card,
.receipt-detail {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 16px;
  margin: 12px 0;
}
.decision-card h3,
.receipt-detail h3 {
  margin: 0 0 8px;
  font-size: 14px;
}
.decision-result {
  font-size: 13px;
}
.txn-cell {
  font-family: ui-monospace, monospace;
  font-size: 12px;
}
.candidate-list {
  margin: 4px 0 0;
  padding-left: 16px;
  font-size: 12px;
  color: var(--muted);
}
.receipt-detail dl {
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: 6px 12px;
  margin: 0;
  font-size: 13px;
}
.receipt-detail dt {
  color: var(--muted);
}
.receipt-detail dd {
  margin: 0;
}
</style>
