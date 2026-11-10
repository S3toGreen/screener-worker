/**
 * CI/CD safety guard: detect and repair MISSING/SHORT screener histories.
 *
 *   bun safety.ts                 # detect only (exit 0, prints what is missing)
 *   bun safety.ts --repair        # detect + run the weekday backfill + upload
 *   bun safety.ts --repair --dry-run
 *   bun safety.ts --repair --months 36
 *
 * The daily ETL is a one-session append; it cannot rebuild a deleted 3-year
 * history. This guard runs BEFORE the daily seed in CI, lists the bucket's
 * `history/` objects, reads the single `history-manifest.json` index and repairs
 * only ABSENT or STALE (newest < snapshot session) series through the reverse
 * weekday gap walk in `backfill.ts`. A missing/unknown manifest entry falls
 * back to one per-object read; the manifest is then rebuilt once.
 *
 * Missing dates are contiguous. The reverse walk starts at the latest closed
 * weekday and stops the first time a date is already in the stored series. A
 * short but CURRENT history is healthy and is not rewritten.
 *
 * Taiwan sessions are Mon–Fri minus holidays/typhoon days. The backfill walks
 * WEEKDAYS backward and reads each exchange's DAILY RWD table; a holiday
 * answers a valid EMPTY table, which is skipped — it never becomes a
 * zero-volume session. A one-sided empty table or a report that fails after its
 * retry is skipped RESOURCE-WISE; the other exchange/report still writes.
 *
 * A newly listed symbol (listed within NEW_LISTING_DAYS) with no history is
 * exempt: the daily ETL starts it. A previously listed absent symbol is
 * rebuilt.
 *
 * Requires R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT (or
 * CLOUDFLARE_ACCOUNT_ID), the same credentials the ETL uses.
 */
import { appendFileSync } from 'node:fs'
import { join } from 'node:path'
import {
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  type ListObjectsV2CommandOutput
} from '@aws-sdk/client-s3'
import {
  SC,
  SCREENER_SNAPSHOT_KEY,
  isScreenerHistory,
  readSnapshotRows,
  type ScreenerRow
} from '@odf/screener-worker/screener'
import { dayStampOf, stampToMs } from '@odf/screener-worker/session'
import { findMissingHistories } from './src/safety'
import {
  HISTORY_MANIFEST_KEY,
  manifestLastDates,
  mergeHistoryManifest,
  readHistoryManifest,
  type HistoryManifest
} from './src/manifest'
import { uploadConfigFromEnv } from './seedFs'

const arg = (name: string): string | undefined => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 ? process.argv[i + 1] : undefined
}
const repair = process.argv.includes('--repair')
const dryRun = process.argv.includes('--dry-run')
const months = Math.max(1, Math.min(36, Number(arg('months') ?? 36) || 36))

/** Tell the workflow whether a repair was launched, so its following seed can
 *  FORCE a snapshot rebuild. Without this, a holiday repair leaves the
 *  snapshot's derived columns stale until the next trading session. */
function emitRepairOutput(repaired: boolean): void {
  const file = process.env.GITHUB_OUTPUT
  if (!file) return
  try {
    appendFileSync(file, `repaired=${repaired}\n`)
  } catch {
    // A local/CI output failure must never change the data outcome.
  }
}
/** A 61-column JSON history cannot legitimately be this small; an empty or
 *  truncated object must be READ and repaired, never trusted through the
 *  manifest's last-date claim. */
const SUSPECT_HISTORY_BYTES = 128

const isNoSuchKey = (err: unknown): boolean => {
  const e = err as { name?: string; Code?: string; $metadata?: { httpStatusCode?: number } }
  return e?.name === 'NoSuchKey' || e?.Code === 'NoSuchKey' || e?.$metadata?.httpStatusCode === 404
}

const config = uploadConfigFromEnv()
const client = new S3Client({
  region: 'auto',
  endpoint: config.endpoint,
  credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
  forcePathStyle: true,
  requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
})

async function readSnapshot(): Promise<{
  sessionDate: number
  rows: ScreenerRow[]
} | null> {
  try {
    const obj = await client.send(
      new GetObjectCommand({ Bucket: config.bucket, Key: SCREENER_SNAPSHOT_KEY })
    )
    const text = await obj.Body?.transformToString('utf8')
    if (!text) return null
    const raw = JSON.parse(text) as { sessionDate?: unknown; rows?: unknown }
    const sessionDate =
      typeof raw.sessionDate === 'number' && raw.sessionDate > 0 ? raw.sessionDate : 0
    const rows = readSnapshotRows(raw.rows)
    return sessionDate > 0 && rows ? { sessionDate, rows } : null
  } catch (err) {
    if (isNoSuchKey(err)) return null
    console.error(`[safety] snapshot read failed: ${String(err)}`)
    process.exit(1)
  }
}

async function listHistories(): Promise<Map<string, number>> {
  const out = new Map<string, number>()
  let token: string | undefined
  do {
    const page: ListObjectsV2CommandOutput = await client.send(
      new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: 'history/',
        ContinuationToken: token
      })
    )
    for (const obj of page.Contents ?? []) {
      const key = obj.Key ?? ''
      if (!key.startsWith('history/') || !key.endsWith('.json')) continue
      const symbol = key.slice('history/'.length, -'.json'.length)
      if (symbol) out.set(symbol, obj.Size ?? 0)
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined
  } while (token)
  return out
}

/** Reads the ONE manifest; absent/corrupt/unreadable starts empty (fallback). */
async function readManifest(s3: S3Client): Promise<HistoryManifest | null> {
  try {
    const obj = await s3.send(
      new GetObjectCommand({ Bucket: config.bucket, Key: HISTORY_MANIFEST_KEY })
    )
    const text = await obj.Body?.transformToString('utf8')
    if (!text) return null
    const manifest = readHistoryManifest(JSON.parse(text))
    if (!manifest) console.log('[safety] history manifest unreadable — per-object fallback')
    return manifest
  } catch (err) {
    if (!isNoSuchKey(err)) {
      console.log(`[safety] history manifest read failed — per-object fallback: ${String(err)}`)
    }
    return null
  }
}

async function writeManifest(s3: S3Client, manifest: HistoryManifest): Promise<void> {
  try {
    await s3.send(
      new PutObjectCommand({
        Bucket: config.bucket,
        Key: HISTORY_MANIFEST_KEY,
        Body: JSON.stringify(manifest),
        ContentType: 'application/json; charset=utf-8',
        CacheControl: 'no-store'
      })
    )
  } catch (err) {
    // Optimization only: the fallback scan still finds every missing history.
    console.log(`[safety] history manifest write failed: ${String(err)}`)
  }
}

/** The newest stored session for each readable history (one GET per symbol;
 *  a corrupt/missing object stays absent → repair). */
async function readLastDates(
  s3: S3Client,
  symbols: readonly string[]
): Promise<Map<string, number>> {
  const out = new Map<string, number>()
  for (let i = 0; i < symbols.length; i += 24) {
    await Promise.all(
      symbols.slice(i, i + 24).map(async (symbol) => {
        try {
          const obj = await s3.send(
            new GetObjectCommand({ Bucket: config.bucket, Key: `history/${symbol}.json` })
          )
          const text = await obj.Body?.transformToString('utf8')
          if (!text) return
          const raw = JSON.parse(text) as unknown
          if (!isScreenerHistory(raw, symbol)) return
          let last = 0
          for (const date of raw.dates) {
            const ms = typeof date === 'string' ? stampToMs(date) : 0
            if (ms > last) last = ms
          }
          if (last > 0) out.set(symbol, last)
        } catch {
          // Unreadable/absent stays a repair candidate.
        }
      })
    )
  }
  return out
}

const snapshot = await readSnapshot()
if (!snapshot) {
  console.log('[safety] no snapshot in R2 — nothing to compare yet')
  emitRepairOutput(false)
  client.destroy()
  process.exit(0)
}

const histories = await listHistories()
const manifest = await readManifest(client)
const stampOfDates = (dates: ReadonlyMap<string, number>): Map<string, string> => {
  const out = new Map<string, string>()
  for (const [symbol, ms] of dates) {
    const stamp = dayStampOf(ms)
    if (stamp) out.set(symbol, stamp)
  }
  return out
}
const manifestDates = manifest ? manifestLastDates(manifest) : new Map<string, number>()
const wanted = snapshot.rows
  .map((row) => String(row[SC.symbol] ?? ''))
  .filter((symbol) => symbol.length > 0 && histories.has(symbol))
// Trust the manifest for known symbols; read ONLY entries it does not have
// (a first adoption, a newly listed symbol, a newly created history).
const lastDates = new Map<string, number>()
const unknown: string[] = []
for (const symbol of wanted) {
  const ms = manifestDates.get(symbol) ?? 0
  // A manifest date is only a fast path for a plausible object. A suspiciously
  // small (or size-less) object is re-read; a corrupt/truncated file must never
  // survive because the index says its date is current.
  const size = histories.get(symbol) ?? 0
  if (ms > 0 && size >= SUSPECT_HISTORY_BYTES) lastDates.set(symbol, ms)
  else unknown.push(symbol)
}
if (unknown.length > 0) {
  const read = await readLastDates(client, unknown)
  for (const [symbol, ms] of read) lastDates.set(symbol, ms)
}
// Keep the index current even on a no-repair detection run: it is the next
// run's fast path. A dry run never mutates R2. `currentManifest` is carried to
// the post-repair write so unknown-but-healthy entries discovered above are not
// dropped when only the repaired symbols are re-read.
const currentManifest = mergeHistoryManifest(manifest, stampOfDates(lastDates), Date.now())
if (!dryRun) await writeManifest(client, currentManifest)
const missing = findMissingHistories(snapshot.rows, lastDates, snapshot.sessionDate)

console.log(
  `[safety] snapshot ${new Date(snapshot.sessionDate).toISOString().slice(0, 10)} · ` +
    `${snapshot.rows.length} symbols · ${histories.size} histories · ${missing.length} to repair`
)
if (missing.length === 0) {
  emitRepairOutput(false)
  client.destroy()
  process.exit(0)
}
console.log(
  `[safety] missing/stale: ${missing
    .slice(0, 60)
    .map((m) => `${m.symbol}(${m.reason})`)
    .join(' ')}${missing.length > 60 ? ` … +${missing.length - 60}` : ''}`
)
if (!repair) {
  console.log('[safety] detection only — rerun with --repair to rebuild these histories')
  emitRepairOutput(false)
  client.destroy()
  process.exit(0)
}
if (dryRun) {
  console.log('[safety] dry run — no backfill launched')
  emitRepairOutput(false)
  client.destroy()
  process.exit(0)
}

client.destroy()
const symbols = missing.map((m) => m.symbol)
console.log(`[safety] launching weekday backfill for ${symbols.length} symbol(s), ${months} months`)
const proc = Bun.spawn(
  [
    'bun',
    join(import.meta.dir, 'backfill.ts'),
    '--repair',
    '--histories-only',
    '--gap-fill',
    '--months',
    String(months),
    '--symbols',
    symbols.join(',')
  ],
  { cwd: import.meta.dir, env: process.env, stdout: 'inherit', stderr: 'inherit' }
)
const code = await proc.exited
if (code !== 0) {
  console.error(`[safety] backfill exited ${code} — the missing histories were NOT repaired`)
  process.exit(code || 1)
}
// A repair was attempted and uploaded (histories-only). The workflow's seed
// must force its snapshot rebuild even on a day with no new session.
emitRepairOutput(true)
// Re-list with a fresh client: a repair that still cannot produce rows (a
// suspended/new instrument) is surfaced, never treated as success.
const afterClient = new S3Client({
  region: 'auto',
  endpoint: config.endpoint,
  credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
  forcePathStyle: true,
  requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
})
const afterLast = await readLastDates(afterClient, symbols)
if (afterLast.size > 0) {
  await writeManifest(
    afterClient,
    mergeHistoryManifest(currentManifest, stampOfDates(afterLast), Date.now())
  )
}
afterClient.destroy()
const stillStale = symbols.filter((symbol) => (afterLast.get(symbol) ?? 0) < snapshot.sessionDate)
if (stillStale.length > 0) {
  console.warn(
    `[safety] backfill completed but ${stillStale.length} history object(s) still do not reach ` +
      `${new Date(snapshot.sessionDate).toISOString().slice(0, 10)}: ` +
      `${stillStale.slice(0, 40).join(' ')}${stillStale.length > 40 ? ' …' : ''}`
  )
} else {
  console.log(`[safety] verified ${symbols.length} repaired history object(s)`)
}
