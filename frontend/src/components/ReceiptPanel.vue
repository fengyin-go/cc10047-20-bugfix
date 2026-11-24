<template>
  <section class="receipt-panel" :data-testid="`receipt-panel-${moduleKey}`">
    <header class="panel-head">
      <h3>增量提交身份（与另一入口共享同一份回执）</h3>
      <button class="btn" type="button" data-testid="reload-panel" @click="load">刷新</button>
    </header>
    <p v-if="message" class="panel-hint">{{ message }}</p>
    <table v-if="rows.length" class="data-table">
      <thead>
        <tr>
          <th>条目</th>
          <th>事务标识</th>
          <th>湖表 / 分区</th>
          <th>偏移区间</th>
          <th>落库行数</th>
          <th>确认偏移</th>
          <th>候选数</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.receipt_id">
          <td>{{ row.receipt_id }}</td>
          <td class="txn-cell">{{ row.transaction_id }}</td>
          <td>{{ row.table }} / {{ row.partition }}</td>
          <td>[{{ row.start_offset }}, {{ row.end_offset }}]</td>
          <td>{{ row.written_data.landed_rows }}</td>
          <td>{{ row.confirmation_scope.confirmed_offset ?? '未确认' }}</td>
          <td>{{ row.attempts.length }}</td>
        </tr>
      </tbody>
    </table>
    <p v-else class="empty-state">当前过滤条件下暂无关联回执</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listReceipts, type ReceiptListItem } from '@/api/incremental-receipt'

const props = defineProps<{ moduleKey: string }>()

const rows = ref<ReceiptListItem[]>([])
const message = ref('')

async function load() {
  message.value = ''
  try {
    // 两个入口都读同一份台账数据，只是默认过滤维度不同。
    const payload = await listReceipts()
    rows.value = payload.items
    message.value =
      props.moduleKey === 'delta_commit'
        ? '版本详情入口：事务标识由业务身份确定，重启/换进程保持不变'
        : '检查点工作台入口：确认位置回写同一份回执，恢复仅生效一次'
  } catch (error) {
    message.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

onMounted(load)
</script>

<style scoped>
.receipt-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 16px;
  margin: 16px 0;
}
.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.panel-head h3 {
  margin: 0;
  font-size: 14px;
}
.panel-hint {
  font-size: 12px;
  color: var(--muted);
}
.txn-cell {
  font-family: ui-monospace, monospace;
  font-size: 12px;
}
</style>
