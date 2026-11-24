import { request } from './client'

/** 增量写入批次的真实业务身份：事务标识完全由它决定，跨重启保持一致。 */
export type BusinessIdentity = {
  source: string
  table: string
  partition: string
  start_offset: number
  end_offset: number
  row_count: number
}

export type IngestPayload = {
  source?: string
  table?: string
  partition: string
  start_offset: number
  row_count: number
  process_id?: string
  batch_ref?: string
  checkpoint_entry_id?: number
}

export type IngestReceipt = {
  ok: boolean
  message?: string
  receipt_id: string
  transaction_id: string
  identity_hash: string
  business_identity: BusinessIdentity
  pipeline?: string
  table: string
  partition: string
  start_offset: number
  end_offset: number
  row_count?: number
  ingested_rows: number
  deduplicated: boolean
  candidate_count: number
  pending_confirm_offset: number | null
  write_basis: {
    source: string
    requested_start_offset: number
    requested_end_offset: number
    requested_row_count: number
    batch_ref: string
    process_id: string
    requested_at: string
  }
  written_data: {
    partition: string
    landed_rows: number
    row_offsets: number[]
    landing_path: string
    landed_at: string
  }
  confirmation_scope: {
    confirmable_from: number
    confirmable_to: number
    checkpoint_id: number | null
    confirmed_offset: number | null
    confirmed_at: string | null
  }
}

export type ReceiptListItem = IngestReceipt & {
  status: string
  recovered: boolean
  attempts: { process_id: string; batch_ref: string; attempted_at: string }[]
  committed_at: string
}

export type CheckpointActionPayload = {
  source?: string
  table?: string
  partition?: string
  start_offset?: number
  row_count?: number
  transaction_id?: string
  confirmed_offset?: number
  process_id?: string
}

export type NextOffsetDecision = {
  ok: boolean
  next_offset: number
  evidence: 'checkpoint' | 'receipt' | 'source'
  evidence_label: string
  evidence_detail: Record<string, string | number | null>
  priority: string[]
}

async function asJson<T>(response: Response): Promise<T> {
  return (await response.json()) as T
}

/** 版本详情入口：按业务身份登记增量写入，重复批次返回同一份回执。 */
export async function registerIngest(
  commitEntryId: number,
  payload: IngestPayload,
): Promise<IngestReceipt> {
  const response = await request(`/api/incremental-receipts/commit/${commitEntryId}`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return asJson<IngestReceipt>(response)
}

/** 检查点工作台入口：保存确认位置并回写同一份回执。 */
export async function saveCheckpointReceipt(
  checkpointEntryId: number,
  payload: CheckpointActionPayload,
): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/incremental-receipts/checkpoint/${checkpointEntryId}/save`,
    { method: 'POST', body: JSON.stringify(payload) },
  )
  return asJson(response)
}

/** 检查点工作台入口：区间恢复，同一区间只生效一次。 */
export async function recoverCheckpoint(
  checkpointEntryId: number,
  payload: CheckpointActionPayload,
): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/incremental-receipts/checkpoint/${checkpointEntryId}/recover`,
    { method: 'POST', body: JSON.stringify(payload) },
  )
  return asJson(response)
}

/** 检查点工作台视角：哪组数据确实落库、尚需确认的偏移。 */
export async function getCheckpointReceiptView(
  checkpointEntryId: number,
): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/incremental-receipts/checkpoint/${checkpointEntryId}/view`,
  )
  return asJson(response)
}

/** 回执工作台：列出全部增量提交回执（重启后仍可查回）。 */
export async function listReceipts(filters: {
  table?: string
  partition?: string
  source?: string
} = {}): Promise<{ ok: boolean; items: ReceiptListItem[]; total: number }> {
  const query = new URLSearchParams(
    Object.entries(filters).filter(([, value]) => value) as [string, string][],
  ).toString()
  const response = await request(`/api/incremental-receipts?${query}`)
  return asJson(response)
}

/** 按事务标识查回执：两个入口核对同一提交身份时使用。 */
export async function getReceiptByTransaction(
  transactionId: string,
): Promise<{ ok: boolean; receipt?: ReceiptListItem; message?: string }> {
  const response = await request(
    `/api/incremental-receipts/by-transaction/${encodeURIComponent(transactionId)}`,
  )
  return asJson(response)
}

/** 证据优先级裁决：检查点从哪里继续。 */
export async function resolveNextOffset(params: {
  table: string
  partition: string
  source?: string
  source_start_offset?: number
}): Promise<NextOffsetDecision> {
  const query = new URLSearchParams(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== '') as [
      string,
      string,
    ][],
  ).toString()
  const response = await request(`/api/incremental-receipts/next-offset?${query}`)
  return asJson(response)
}

/** 校验业务身份（不登记），用于登记前预览稳定事务标识。 */
export async function verifyIdentity(payload: {
  source: string
  table: string
  partition: string
  start_offset: number
  end_offset: number
  row_count: number
}): Promise<{
  ok: boolean
  message?: string
  identity_hash?: string
  transaction_id?: string
  already_registered?: boolean
  business_identity?: BusinessIdentity
}> {
  const response = await request('/api/incremental-receipts/identity/verify', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return asJson(response)
}
