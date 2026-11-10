/**
 * Operator-only seed for the market observation history.
 *
 *   bun seedMarketHistory.ts \
 *     --file ./market-history.json
 *   bun seedMarketHistory.ts --taifex 60      # TAIFEX split institution history
 *
 * The daily workflow can seed the CURRENT observation as a safety guard when
 * the series is missing; this script is the primary way to upload the full
 * prepared 60-session series. Existing observations are not erased.
 *
 * `--file` may carry:
 *   - a bare array of MarketHistoryPoint objects, or
 *   - a market object `{ asOf, credit, derivatives, history: [...] }`.
 * `--taifex N` reads N historical sessions from the exchange's dated
 * `/cht/3/futAndOptDate` HTML table (futures/options split) and fills only
 * dates the bucket does not already hold; the daily ETL/backfill never calls it.
 *
 * Existing `market-latest.json` is read from R2 and its current credit /
 * derivatives block is PRESERVED; only `history` is replaced with the uploaded
 * series (plus the current session). The publication stamp is rewritten so the
 * Pages edge cache cannot serve the pre-seed object.
 *
 *   ... --file history.json --dry-run    # validate + print, no R2 writes
 *
 * Requires R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT (or
 * CLOUDFLARE_ACCOUNT_ID) in `.dev.vars` / the environment.
 */
import { readFile } from 'node:fs/promises'
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import {
  readMarketContext,
  mergeMarketHistory,
  SCREENER_MARKET_KEY,
  type MarketContext,
  type MarketHistoryPoint
} from '@odf/screener-worker/screener'
import { DAY_MS } from '@odf/screener-worker/session'
import { latestClosedSessionMs, reverseWeekdays } from './src/backfillWindow'
import { taifexInstitutionHistory } from './src/sources/taifexHistory'
import { CACHE_VERSION_KEY, DATA_CACHE_CONTROL } from './src/keys'
import { uploadConfigFromEnv } from './seedFs'

const arg = (name: string): string => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : ''
}
const file = arg('file')
const taifexSessions = Math.max(0, Math.min(120, Number(arg('taifex') ?? 0) || 0))
const dryRun = process.argv.includes('--dry-run')

function fail(message: string): never {
  console.error(`[market-history] ${message}`)
  process.exit(1)
}

if (!file && taifexSessions === 0)
  fail('usage: seedMarketHistory.ts --file <history.json> | --taifex <sessions> [--dry-run]')

const raw = file ? (JSON.parse(await readFile(file, 'utf8')) as unknown) : null
const source = Array.isArray(raw)
  ? raw
  : raw && typeof raw === 'object' && Array.isArray((raw as { history?: unknown }).history)
    ? (raw as { history: unknown[] }).history
    : null
if (file && !source)
  fail('the file must be a MarketHistoryPoint[] or an object with a history[] array')

// The SAME reader the client/ETL use: malformed points are dropped, never
// fabricated; a future-dated point is bounded by the payload's own asOf.
const seeded = readMarketContext({ history: source ?? [] })
const fileHistory: MarketHistoryPoint[] = seeded?.history ?? []
if (file && fileHistory.length === 0) fail('no valid history points survived validation')

/** TAIFEX's dated futures/options split table is the historical institution
 *  half (the OpenAPI endpoints are latest-only). Operator-only: the daily ETL
 *  and backfill never call this. Holidays answer no table and are skipped. */
const generated: MarketHistoryPoint[] = []
if (taifexSessions > 0) {
  const latest = latestClosedSessionMs(Date.now())
  const days = reverseWeekdays(latest, latest - 200 * DAY_MS)
  for (const day of days) {
    if (generated.length >= taifexSessions) break
    try {
      const point = await taifexInstitutionHistory(day)
      if (point) generated.push(point)
    } catch (err) {
      // Resource-wise: one failed page never drops the other sessions.
      console.warn(`[market-history] TAIFEX ${day} skipped: ${String(err)}`)
    }
  }
  console.log(
    `[market-history] TAIFEX split history ${generated.length}/${taifexSessions} sessions`
  )
}
const seededHistory = [...fileHistory, ...generated]
if (seededHistory.length === 0 && taifexSessions === 0)
  fail('no valid history points survived validation')
if (seededHistory.length > 0) {
  console.log(
    `[market-history] read ${seededHistory.length} points ` +
      `${seededHistory[0]!.sessionDate} … ${seededHistory[seededHistory.length - 1]!.sessionDate}`
  )
}

const config = uploadConfigFromEnv()
const client = new S3Client({
  region: 'auto',
  endpoint: config.endpoint,
  credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
  forcePathStyle: true,
  requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
})

const isNoSuchKey = (err: unknown): boolean => {
  const e = err as { name?: string; Code?: string; $metadata?: { httpStatusCode?: number } }
  return e?.name === 'NoSuchKey' || e?.Code === 'NoSuchKey' || e?.$metadata?.httpStatusCode === 404
}

let currentText: string | null = null
try {
  const obj = await client.send(
    new GetObjectCommand({ Bucket: config.bucket, Key: SCREENER_MARKET_KEY })
  )
  currentText = (await obj.Body?.transformToString('utf8')) ?? null
} catch (err) {
  // A missing object is allowed when the file carries a full market context;
  // any other read failure aborts instead of overwriting blind.
  if (!isNoSuchKey(err)) fail(`cannot read ${SCREENER_MARKET_KEY} from R2: ${String(err)}`)
}

let r2Current: MarketContext | null = null
if (currentText) {
  try {
    r2Current = readMarketContext(JSON.parse(currentText))
  } catch {
    r2Current = null
  }
}
// A full market object in the upload file can bootstrap a genuinely empty
// bucket. A history-only array cannot invent the current credit/derivatives.
const fileCurrent = raw && !Array.isArray(raw) ? readMarketContext(raw) : null
const usableFileCurrent =
  fileCurrent && (fileCurrent.credit !== null || fileCurrent.derivatives !== null)
    ? fileCurrent
    : null
const current =
  r2Current && (!usableFileCurrent || r2Current.asOf >= usableFileCurrent.asOf)
    ? r2Current
    : usableFileCurrent
if (!current) {
  fail(
    `${SCREENER_MARKET_KEY} is absent/invalid and the file has no current credit/derivatives block`
  )
}
if (!r2Current) {
  console.log('[market-history] R2 has no usable current object — using the file’s market block')
}

// Merge, never erase: an existing R2 series survives; an uploaded point wins
// on a same-date collision (the map's later write). Never write an observation
// beyond the current object's anchor; a future point would be dropped by every
// reader and only bloat the object.
// A generated TAIFEX-only point must NEVER replace an existing full observation
// (credit + derivatives) under the same date; it only fills dates the bucket
// does not have. The file path keeps its documented same-date replacement.
const existingDates = new Set((r2Current?.history ?? []).map((point) => point.sessionDate))
const generatedNew = generated.filter((point) => !existingDates.has(point.sessionDate))
const combined = [...(r2Current?.history ?? []), ...fileHistory, ...generatedNew]
const history = mergeMarketHistory(combined, current).filter(
  (point) => point.sessionDate <= current.asOf
)
if (history.length === 0) fail('the seeded series is empty after merging the current session')
const next = JSON.stringify({ ...current, history })
console.log(
  `[market-history] will store ${history.length} points ` +
    `${history[0]!.sessionDate} … ${history[history.length - 1]!.sessionDate} ` +
    `(current ${new Date(current.asOf).toISOString()})`
)

if (dryRun) {
  console.log('[market-history] dry run — no R2 writes')
  client.destroy()
  process.exit(0)
}

try {
  await client.send(
    new PutObjectCommand({
      Bucket: config.bucket,
      Key: SCREENER_MARKET_KEY,
      Body: next,
      ContentType: 'application/json; charset=utf-8',
      CacheControl: DATA_CACHE_CONTROL
    })
  )
  // Publication stamp LAST: every non-history edge key embeds it.
  await client.send(
    new PutObjectCommand({
      Bucket: config.bucket,
      Key: CACHE_VERSION_KEY,
      Body: String(Date.now()),
      ContentType: 'text/plain; charset=utf-8',
      CacheControl: 'no-store'
    })
  )
  console.log(`[market-history] uploaded ${history.length} points to ${config.bucket}`)
} finally {
  client.destroy()
}
