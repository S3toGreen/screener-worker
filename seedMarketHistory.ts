/**
 * Operator-only seed for the market observation history.
 *
 *   bun seedMarketHistory.ts \
 *     --file ./market-history.json
 *   bun seedMarketHistory.ts --backfill 60    # RWD credit history
 *   bun seedMarketHistory.ts --taifex 60      # TAIFEX split institution history
 *   bun seedMarketHistory.ts --backfill 60 --taifex 60  # complete overview
 *
 * The daily workflow can seed the CURRENT observation as a safety guard when
 * the series is missing; this script is the primary way to upload the full
 * prepared 60-session series. Existing observations are not erased.
 *
 * `--backfill N` reconstructs the market half from the exchanges' per-date RWD
 * tables: TWSE `MI_MARGN` + `MI_INDEX` + `MI_QFIIS` + `BFIJ3U_TREND`, and TPEx
 * `margin/balance` summary + `otc` + `marketStats` + `highlight`, with the
 * dashboard used only for the TPEx creditAmount (09/10 onward). TotalAmount is
 * emitted with TDR/ETF and stock-only; market cap is stock-only. `--taifex N`
 * supplies the derivatives half; both flags merge by date. `--file` remains the
 * route for a fully prepared series.
 *
 * `--file` may carry:
 *   - a bare array of MarketHistoryPoint objects, or
 *   - a market object `{ asOf, credit, derivatives, history: [...] }`.
 * `--taifex N` reads N historical sessions from the exchange's dated
 * `/cht/3/futAndOptDate` HTML table (futures/options split) and fills only
 * dates the bucket does not already hold; the daily ETL/backfill never calls it.
 *
 * Existing `market-latest.json` is read from R2 and merged FIELD-WISE: the
 * newest credit-bearing point becomes the top-level credit anchor (a newer
 * RWD backfill wins over a stale R2 current), the newest derivatives half is
 * preserved separately, and `history` is the capped union of R2, the prepared
 * file and the reconstruction. The publication stamp is rewritten so the Pages
 * edge cache cannot serve the pre-seed object.
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
  SCREENER_MARKET_KEY,
  type MarketContext,
  type MarketHistoryPoint
} from '@odf/screener-worker/screener'
import { DAY_MS, dayStampOf } from '@odf/screener-worker/session'
import { latestClosedSessionMs, reverseWeekdays } from './src/backfillWindow'
import {
  buildMarketHistorySession,
  mergeCappedMarketHistory,
  newestCreditPoint
} from './src/marketHistory'
import { taifexInstitutionHistory } from './src/sources/taifexHistory'
import { twseOfficialTrend } from './src/sources/twse'
import type { MaintenancePoint } from './src/sources/types'
import { CACHE_VERSION_KEY, DATA_CACHE_CONTROL } from './src/keys'
import { uploadConfigFromEnv } from './seedFs'

const arg = (name: string): string => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : ''
}
const file = arg('file')
const taifexSessions = Math.max(0, Math.min(120, Number(arg('taifex') ?? 0) || 0))
const backfillSessions = Math.max(0, Math.min(120, Number(arg('backfill') ?? 0) || 0))
const dryRun = process.argv.includes('--dry-run')

function fail(message: string): never {
  console.error(`[market-history] ${message}`)
  process.exit(1)
}

if (!file && taifexSessions === 0 && backfillSessions === 0)
  fail(
    'usage: seedMarketHistory.ts --file <history.json> | --backfill <sessions> | --taifex <sessions> [--dry-run]'
  )

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
/** The full credit + market half: each session reconstructs from
 *  TWSE `MI_MARGN`/`MI_INDEX`/`MI_QFIIS` and TPEx
 *  `margin/balance`/`otc`/`marketStats`/`highlight`; `BFIJ3U_TREND` is
 *  prefetched ONCE for the whole window (official keepRate + credit amount).
 *  The TPEx dashboard is only the creditAmount source (09/10 onward); before
 *  that it is null, and no other market-level endpoint exists. */
const backfilled: MarketHistoryPoint[] = []
if (backfillSessions > 0) {
  const latest = latestClosedSessionMs(Date.now())
  // Enough calendar room for the requested trading sessions plus holidays.
  // 220 days covers 60 sessions; scale with the request instead of silently
  // ending the walk early for a deeper backfill.
  const days = reverseWeekdays(latest, latest - Math.max(220, backfillSessions * 2 + 40) * DAY_MS)
  const officialDays = Math.min(120, Math.max(backfillSessions + 20, 60))
  let officialByDate: Map<number, MaintenancePoint> = new Map()
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      officialByDate = await twseOfficialTrend(officialDays, latest)
      if (officialByDate.size > 0) break
    } catch (err) {
      console.warn(`[market-history] BFIJ3U attempt ${attempt}/3 failed: ${String(err)}`)
    }
    if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, 1_000 * attempt))
  }
  if (officialByDate.size === 0) {
    fail('BFIJ3U official trend unavailable; nothing was uploaded')
  }
  console.log(
    `[market-history] BFIJ3U official ${officialByDate.size} sessions (requested ${officialDays})`
  )
  let skippedUnpublished = false
  for (const day of days) {
    if (backfilled.length >= backfillSessions) break
    let result: Awaited<ReturnType<typeof buildMarketHistorySession>> | null = null
    for (let attempt = 1; attempt <= 3; attempt++) {
      result = await buildMarketHistorySession(day, undefined, officialByDate.get(day) ?? null)
      // An empty/not-yet-published session is final; only a rejected/thrown
      // request is retried.
      if (result.ok || result.reason === 'no-session' || !result.retryable) break
      console.warn(
        `[market-history] RWD ${dayStampOf(day)} attempt ${attempt}/3 failed: ${result.detail}`
      )
      if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, 1_000 * attempt))
    }
    if (!result) continue
    if (!result.ok) {
      if (result.reason === 'no-session') {
        console.log(`[market-history] RWD ${dayStampOf(day)} no session — skipped`)
        continue
      }
      // A rejected/thrown request that survived its retries is a real failure,
      // even on the newest day; never skip it as "not published".
      if (result.retryable) {
        fail(
          `RWD backfill failed for ${dayStampOf(day)} after retries: ${result.detail}. ` +
            'Nothing was uploaded; rerun to fill the session.'
        )
      }
      // The latest calendar session's MI_MARGN may not be published yet (the
      // cash close precedes the credit report). Skip exactly ONE unresolved
      // day; every later incomplete session is a real hole and aborts.
      if (backfilled.length === 0 && !skippedUnpublished && result.notPublished) {
        skippedUnpublished = true
        console.warn(
          `[market-history] latest RWD ${dayStampOf(day)} not published yet (${result.detail}) — skipped`
        )
        continue
      }
      fail(
        `RWD backfill incomplete for ${dayStampOf(day)}: ${result.detail}. ` +
          'Nothing was uploaded; rerun to fill the session.'
      )
    }
    backfilled.push(result.point)
    console.log(`[market-history] RWD ${backfilled.length}/${backfillSessions} ${dayStampOf(day)}`)
  }
  if (backfilled.length < backfillSessions) {
    fail(
      `RWD credit backfill produced ${backfilled.length}/${backfillSessions} sessions ` +
        'before the weekday window ended; nothing was uploaded'
    )
  }
  console.log(
    `[market-history] RWD credit history ${backfilled.length}/${backfillSessions} sessions`
  )
}
const seededHistory = [...fileHistory, ...generated, ...backfilled]
if (seededHistory.length === 0 && taifexSessions === 0 && backfillSessions === 0)
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
  if (!r2Current) {
    // A present-but-unreadable object is NOT absence: overwriting it would
    // silently discard the rolling history. Abort and let the operator repair
    // the object (or remove it deliberately).
    fail(
      `R2 ${SCREENER_MARKET_KEY} is present but unreadable; refusing to overwrite it. ` +
        'Repair/remove the object deliberately before re-running.'
    )
  }
}
// A full market object in the upload file can bootstrap a genuinely empty
// bucket. A history-only array cannot invent the current credit/derivatives.
const fileCurrent = raw && !Array.isArray(raw) ? readMarketContext(raw) : null
const usableFileCurrent =
  fileCurrent && (fileCurrent.credit !== null || fileCurrent.derivatives !== null)
    ? fileCurrent
    : null
let current =
  r2Current && (!usableFileCurrent || r2Current.asOf >= usableFileCurrent.asOf)
    ? r2Current
    : usableFileCurrent
// A derivatives-only current object must never shadow a newer credit
// observation: the top-level credit is the panel's headline, and the RWD
// backfill may be newer than the last daily ETL publication. Pick the newest
// point that actually carries credit; a newer derivatives-only point still
// enters the rolling history through the merge below.
const anchors: MarketHistoryPoint[] = []
for (const point of [r2Current, usableFileCurrent]) {
  if (point) {
    anchors.push({
      sessionDate: point.asOf,
      credit: point.credit,
      derivatives: point.derivatives
    })
  }
}
anchors.push(...(r2Current?.history ?? []), ...backfilled, ...fileHistory)
const newestCredit = newestCreditPoint(anchors)
if (newestCredit && (!current || !current.credit || newestCredit.sessionDate > current.asOf)) {
  current = {
    asOf: newestCredit.sessionDate,
    credit: newestCredit.credit,
    derivatives: newestCredit.derivatives
  }
}
if (!current) {
  const newest = [...fileHistory, ...generated, ...backfilled]
    .filter((point) => point.sessionDate > 0)
    .sort((a, b) => a.sessionDate - b.sessionDate)
    .at(-1)
  if (newest) {
    current = {
      asOf: newest.sessionDate,
      credit: newest.credit,
      derivatives: newest.derivatives
    }
  }
}
if (!current) {
  fail(`${SCREENER_MARKET_KEY} is absent/invalid and no seeded point carries a market block`)
}
// The top-level credit anchor stays the newest CREDIT observation, but the
// derivatives half has its own as-of and must not be lost when TAIFEX has
// already published a newer session than the credit backfill.
const derivativePoints: MarketHistoryPoint[] = [
  ...generated,
  ...(r2Current?.history ?? []),
  ...backfilled,
  ...fileHistory,
  ...(r2Current
    ? [
        {
          sessionDate: r2Current.asOf,
          credit: r2Current.credit,
          derivatives: r2Current.derivatives
        }
      ]
    : []),
  ...(usableFileCurrent
    ? [
        {
          sessionDate: usableFileCurrent.asOf,
          credit: usableFileCurrent.credit,
          derivatives: usableFileCurrent.derivatives
        }
      ]
    : [])
]
let newestDerivatives: MarketHistoryPoint | null = null
for (const point of derivativePoints) {
  if (!point.derivatives) continue
  const at = point.derivatives.asOf > 0 ? point.derivatives.asOf : point.sessionDate
  const bestAt =
    newestDerivatives?.derivatives?.asOf && newestDerivatives.derivatives.asOf > 0
      ? newestDerivatives.derivatives.asOf
      : (newestDerivatives?.sessionDate ?? -Infinity)
  if (!newestDerivatives || at > bestAt) newestDerivatives = point
}
if (newestDerivatives) {
  current = { ...current, derivatives: newestDerivatives.derivatives }
}
if (!r2Current) {
  console.log('[market-history] R2 has no usable current object — using the newest seeded point')
}

// Merge field-wise by date. TAIFEX contributes derivatives, the RWD backfill
// contributes credit, the prepared file wins over R2 on the same date, and the
// current object is merged again below so a live credit/derivatives block is
// never replaced by a historical reconstruction. Never write an observation
// beyond the current object's anchor.
const history = mergeCappedMarketHistory(
  // Source precedence for a same-date cell: prepared file > freshly
  // reconstructed RWD > stored R2 history. TAIFEX is first because it only
  // supplies the derivatives half.
  [...generated, ...(r2Current?.history ?? []), ...backfilled, ...fileHistory],
  current
)
if (history.length === 0) fail('the seeded series is empty after merging the current session')
// The history merge may have added derivatives to the current date (e.g. a
// TAIFEX-only observation while the RWD backfill supplied the credit); carry
// that merged point onto the top-level object.
const atCurrent = history.find((point) => point.sessionDate === current.asOf)
const currentDerivatives = current.derivatives
const mergedDerivatives = atCurrent?.derivatives ?? null
const currentDerAsOf = currentDerivatives?.asOf ?? -Infinity
const mergedDerAsOf = mergedDerivatives?.asOf ?? -Infinity
const currentOut = atCurrent
  ? {
      ...current,
      credit: atCurrent.credit,
      // Keep the newer TAIFEX half when it has its own as-of beyond the
      // credit anchor; otherwise take the merged same-date point.
      derivatives:
        currentDerivatives && currentDerAsOf >= mergedDerAsOf
          ? currentDerivatives
          : (mergedDerivatives ?? currentDerivatives)
    }
  : current
const next = JSON.stringify({ ...currentOut, history })
console.log(
  `[market-history] will store ${history.length} points ` +
    `${history[0]!.sessionDate} … ${history[history.length - 1]!.sessionDate} ` +
    `(current ${new Date(currentOut.asOf).toISOString()})`
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
