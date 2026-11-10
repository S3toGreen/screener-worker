/**
 * Pre-deploy smoke — proves the PUBLISHED SHAPES before a worker deploy, with
 * no network: it reads the dry-run output in `.seed-out/` and runs BOTH sides
 * of the contract over it.
 *
 *   1. the stock snapshot is the full-contract grid (71 columns) and carries NO market block;
 *   2. the client's OWN reader (`createScreenerService`) accepts it — and
 *      still accepts a 61-column (pre-credit) grid by padding;
 *   3. `/api/market` is a separate object the client reader accepts;
 *   4. `serveApi` (the Pages function's body) answers /api/snapshot,
 *      /api/market and one history with CORS + the edge cache, and a bad path
 *      is a JSON 404.
 *
 *   bun smoke.ts                    # after: bun seed.ts --dry-run --keep --force
 *   bun smoke.ts --dir ../.seed-out
 *
 * Exit 0 = every check passed; exit 1 = at least one failed (printed).
 */
import { createHash } from 'node:crypto'
import { existsSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import {
  SC,
  SCREENER_FIELDS,
  readMarketContext,
  readSnapshotRows,
  screenerHistoryUrl,
  screenerMarketUrl,
  screenerSnapshotUrl,
  type ScreenerSnapshot
} from '@odf/screener-worker/screener'
import {
  createScreenerService,
  type ScreenerFetch,
  type ScreenerFetchResponse
} from '@odf/screener-worker/services/screenerService'
import type { StorageAdapter } from '@odf/screener-worker/services/storage'
import { serveApi } from './src/serve'
import { readHistoryManifest } from './src/manifest'
import { DATA_CACHE_CONTROL } from './src/keys'
import type { ScreenerEnv } from './src/env'

const arg = (name: string, fallback: string): string => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback
}
const root = arg('dir', join(import.meta.dir, '.seed-out'))
const failures: string[] = []
function check(ok: boolean, what: string, detail = ''): void {
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}${detail ? ` — ${detail}` : ''}`)
  if (!ok) failures.push(what)
}

/** The in-memory storage seam the client service binds (localStorage's shape). */
function memoryStorage(): StorageAdapter {
  const map = new Map<string, unknown>()
  return {
    async get<T>(key: string): Promise<T | null> {
      return (map.get(key) as T | undefined) ?? null
    },
    async set<T>(key: string, value: T): Promise<void> {
      map.set(key, JSON.parse(JSON.stringify(value)) as T)
    },
    async remove(key: string): Promise<void> {
      map.delete(key)
    }
  }
}

/** The client's fetch, served from the files the bucket will hold. */
function filesFetch(files: Map<string, unknown>): ScreenerFetch {
  return (async (url: string): Promise<ScreenerFetchResponse> => {
    const path = url.replace(/^https?:\/\/[^/]+/, '').split('?')[0]
    const body = files.get(path)
    if (body === undefined) return { ok: false, status: 404, json: async () => null }
    return { ok: true, status: 200, json: async () => body }
  }) as ScreenerFetch
}

/** The R2 binding over the same files (what the Pages function reads). */
function fsBucket(): ScreenerEnv['R2_BUCKET'] {
  return {
    async get(key: string) {
      const path = join(root, key)
      if (!existsSync(path)) return null
      const bytes = await readFile(path)
      return {
        httpEtag: `"${createHash('sha1').update(bytes).digest('hex')}"`,
        writeHttpMetadata(headers: Headers) {
          headers.set('content-type', 'application/json; charset=utf-8')
          headers.set('cache-control', DATA_CACHE_CONTROL)
        },
        body: bytes
      }
    }
  } as unknown as ScreenerEnv['R2_BUCKET']
}

// The edge cache, in memory (workerd's Cache API stores a Response).
const edge = new Map<string, Response>()
;(globalThis as unknown as { caches: unknown }).caches = {
  default: {
    match: async (req: Request) => edge.get(req.url),
    put: async (req: Request, res: Response) => {
      edge.set(req.url, res)
    }
  }
}

const snapshotText = await readFile(join(root, 'screener-latest.json'), 'utf8')
const marketText = existsSync(join(root, 'market-latest.json'))
  ? await readFile(join(root, 'market-latest.json'), 'utf8')
  : null
const snapshot = JSON.parse(snapshotText) as ScreenerSnapshot & { market?: unknown }
const market = marketText ? (JSON.parse(marketText) as unknown) : null

// 1. The published stock grid.
check(!('market' in snapshot), 'snapshot carries no market block')
const rows = readSnapshotRows(snapshot.rows)
check(rows !== null, 'snapshot rows read')
check(
  (snapshot.rows[0]?.length ?? 0) === SCREENER_FIELDS.length,
  `snapshot width = ${SCREENER_FIELDS.length}`,
  `${snapshot.rows[0]?.length}`
)
check(snapshot.rows.length > 500, 'snapshot row count', `${snapshot.rows.length}`)
check(
  typeof snapshot.benchmark?.close === 'number',
  'benchmark close present',
  `${snapshot.benchmark?.close}`
)
// The session stamp is Taipei MIDNIGHT — a mid-session instant (the stale-
// mirror probe used to echo `now`) breaks every downstream `=== sessionDate`
// gate (credit aggregate, valuation freshness).
const sessionIso = new Date(snapshot.sessionDate + 8 * 3_600_000).toISOString()
check(sessionIso.endsWith('T00:00:00.000Z'), 'session stamp is Taipei midnight', sessionIso)

// 2. The CLIENT reader over the same bytes (the deploy-order trap).
const files = new Map<string, unknown>([
  ['/api/snapshot', snapshot],
  ...(market ? [['/api/market', market] as [string, unknown]] : [])
])
const svc = createScreenerService(
  { base: 'https://smoke.test' },
  memoryStorage(),
  filesFetch(files)
)
const loaded = await svc.loadSnapshot().catch((err: unknown) => String(err))
check(
  typeof loaded !== 'string',
  'client accepts the snapshot',
  typeof loaded === 'string' ? loaded : ''
)
if (typeof loaded !== 'string') {
  check(loaded.rows.length === snapshot.rows.length, 'client row count', `${loaded.rows.length}`)
  check(
    loaded.rows[0].length === SCREENER_FIELDS.length,
    'client row width',
    `${loaded.rows[0].length}`
  )
  check(loaded.benchmark?.close === snapshot.benchmark?.close, 'client keeps the TAIEX level')
}

// An OLD 61-column grid must read too (the client ships independently).
const oldBody = {
  sessionDate: snapshot.sessionDate,
  benchmark: snapshot.benchmark,
  rows: snapshot.rows.slice(0, 50).map((r) => r.slice(0, 61))
}
const oldSvc = createScreenerService(
  { base: 'https://smoke.test' },
  memoryStorage(),
  filesFetch(new Map([['/api/snapshot', oldBody]]))
)
const oldLoaded = await oldSvc.loadSnapshot().catch((err: unknown) => String(err))
check(
  typeof oldLoaded !== 'string' && oldLoaded.rows[0].length === SCREENER_FIELDS.length,
  'client pads a 61-column snapshot',
  typeof oldLoaded === 'string' ? oldLoaded : `${oldLoaded.rows[0].length} cells`
)

// 3. The market context (its own object).
if (market === null) {
  check(false, 'market-latest.json written (run the dry-run with the new ETL)')
} else {
  const ctx = readMarketContext(market)
  check(ctx !== null, 'market context reads')
  check(
    ctx !== null && (ctx.credit !== null || ctx.derivatives !== null),
    'market context has a credit or derivatives half'
  )
  const clientCtx = await svc.loadMarketContext()
  check(clientCtx?.asOf === ctx?.asOf, 'client reads /api/market', `${clientCtx?.asOf}`)
}

// 4. The Pages function body over the bucket.
const env = { R2_BUCKET: fsBucket() } as unknown as ScreenerEnv
const ctx = { waitUntil: () => undefined }
const get = (path: string): Promise<Response> =>
  serveApi(new Request(`https://smoke.test${path}`), env, ctx)

const snapRes = await get('/api/snapshot')
const snapBody = await snapRes.text()
check(snapRes.status === 200, '/api/snapshot status', `${snapRes.status}`)
check(snapRes.headers.get('access-control-allow-origin') === '*', '/api/snapshot CORS')
check(snapBody === snapshotText, '/api/snapshot serves the object byte-for-byte')
check(snapRes.headers.get('x-edge-cache') === 'miss', 'first read is an edge miss')
const hit = await get('/api/snapshot')
check(hit.headers.get('x-edge-cache') === 'hit', 'second read is an edge hit')
// The client's `?t=` is a BROWSER cache-buster; the edge key deliberately
// ignores client query strings and follows the published version stamp. A
// forced client pull must not evict or bypass the edge entry.
const forced = await get(`/api/snapshot?t=${Date.now()}`)
check(forced.headers.get('x-edge-cache') === 'hit', 'client ?t= keeps the edge entry')

// The derived-history index is part of the published contract (the safety
// guard reads it instead of thousands of objects). It must exist and validate.
const manifestPath = join(root, 'history-manifest.json')
const manifestText = existsSync(manifestPath) ? await readFile(manifestPath, 'utf8') : null
check(manifestText !== null, 'history manifest written')
if (manifestText !== null) {
  let parsed: unknown = null
  try {
    parsed = JSON.parse(manifestText)
  } catch {
    check(false, 'history manifest parses')
  }
  if (parsed !== null) {
    const manifest = readHistoryManifest(parsed)
    check(manifest !== null, 'history manifest shape')
    check(
      manifest !== null && Object.keys(manifest.symbols).length > 500,
      'history manifest coverage',
      manifest ? `${Object.keys(manifest.symbols).length} symbols` : ''
    )
  }
}

const marketRes = await get('/api/market')
check(marketRes.status === 200, '/api/market status', `${marketRes.status}`)
if (marketText !== null) {
  check((await marketRes.text()) === marketText, '/api/market serves the object byte-for-byte')
}

const symbol = snapshot.rows
  .map((r) => String(r[SC.symbol] ?? ''))
  .find((s) => existsSync(join(root, 'history', `${s}.json`)))
if (symbol) {
  const histRes = await get(screenerHistoryUrl('', symbol).replace(/^https?:\/\/[^/]+/, ''))
  check(histRes.status === 200, `history route (${symbol})`, `${histRes.status}`)
}

check((await get('/api/nope')).status === 404, 'unknown path is a JSON 404')
check((await get('/api/history/!!')).status === 404, 'malformed history symbol is a 404')

// The shared URL builders must agree with the served routes.
check(screenerSnapshotUrl('https://x') === 'https://x/api/snapshot', 'snapshot URL builder')
check(screenerMarketUrl('https://x') === 'https://x/api/market', 'market URL builder')

if (failures.length > 0) {
  console.error(`\n${failures.length} check(s) FAILED:\n  ${failures.join('\n  ')}`)
  process.exit(1)
}
console.log(
  `\npre-deploy smoke passed — ${snapshot.rows.length} rows × ${SCREENER_FIELDS.length} cols`
)
