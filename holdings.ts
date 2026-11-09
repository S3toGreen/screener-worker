/**
 * The weekly TDCC 集保 collector (the same shape as `seed.ts`): scrape the
 * per-stock 千張大戶 share, store ONE R2 object, never touch the snapshot.
 *
 *   pnpm --filter @odf/screener-worker run holdings
 *   bun --env-file=.dev.vars holdings.ts --symbols 2330,0050   # smoke
 *   bun --env-file=.dev.vars holdings.ts --dry-run             # no write
 *
 * Exit codes: 0 written · 3 coverage under 50 % (the previous object is kept)
 * · 1 storage failure. The ETL joins `bigHolderPct` from this object on every
 * daily run; a missing / stale object simply leaves the cell `–`.
 */
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { SCREENER_CDN_DEFAULT } from '@odf/shared/protocol'
import { SC, screenerSnapshotUrl } from '@odf/shared/screener'
import { HOLDINGS_KEY } from './src/keys'
import { collectHoldings, mergeHoldings } from './src/holdings'
import { uploadConfigFromEnv } from './seedFs'

/** The live grid's symbols — the collector's universe (funds included: TDCC
 *  reports 受益權單位 for them). */
async function fetchUniverse(): Promise<string[]> {
  const res = await fetch(`${screenerSnapshotUrl(SCREENER_CDN_DEFAULT)}?t=${Date.now()}`)
  if (!res.ok) throw new Error(`snapshot HTTP ${res.status}`)
  const snap = (await res.json()) as { rows: Array<Array<string | number | null>> }
  return snap.rows.map((r) => String(r[SC.symbol] ?? '')).filter((s) => s.length > 0)
}

const arg = (name: string): string | null => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : null
}

const limit = arg('symbols')
const symbols = limit
  ? limit
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  : await fetchUniverse()
console.log(`[holdings] collecting ${symbols.length} symbols from TDCC`)
const collected = await collectHoldings(symbols)
const { week: collectedWeek, found, asked } = collected
console.log(
  `[holdings] week=${collectedWeek} found=${found}/${asked} (${((found / asked) * 100).toFixed(1)}%)`
)
if (found < asked * 0.5) {
  console.error('[holdings] under 50% coverage — the previous object is kept')
  process.exit(3)
}
if (process.argv.includes('--dry-run')) {
  console.log('[holdings] dry run — nothing written')
  process.exit(0)
}

const config = uploadConfigFromEnv()
const client = new S3Client({
  region: 'auto',
  endpoint: config.endpoint,
  credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
  forcePathStyle: true,
  requestHandler: { requestTimeout: 45_000, throwOnRequestTimeout: true }
})
// Fold the week into the stored series (the ETL needs the 4-week Δ).
let prior: unknown = null
try {
  const obj = await client.send(new GetObjectCommand({ Bucket: config.bucket, Key: HOLDINGS_KEY }))
  const text = await obj.Body?.transformToString('utf8')
  if (text) prior = JSON.parse(text)
} catch (err) {
  console.log(`[holdings] no prior series (${String(err).slice(0, 80)}) — starting one`)
}
const holdings = mergeHoldings(prior, collectedWeek, collected.symbols)
console.log(`[holdings] series weeks=${holdings.weeks.length} (newest ${holdings.weeks[0]?.week})`)
await client.send(
  new PutObjectCommand({
    Bucket: config.bucket,
    Key: HOLDINGS_KEY,
    Body: JSON.stringify(holdings),
    ContentType: 'application/json; charset=utf-8',
    CacheControl: 'public, max-age=300, s-maxage=3600, stale-while-revalidate=86400'
  })
)
client.destroy()
console.log(`[holdings] wrote ${HOLDINGS_KEY} (${JSON.stringify(holdings).length} bytes)`)
