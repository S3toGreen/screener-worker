/**
 * The public data API — five cached reads over the bucket the ETL owns.
 *
 *   GET /api/snapshot          → the day's 72-column STOCK grid (`screener-latest.json`)
 *   GET /api/market            → the market-wide credit/derivatives object
 *   GET /api/history/{symbol}  → that symbol's rolling history (on demand only)
 *   GET /api/holdings          → the weekly TDCC 集保 object (`holdings.json`)
 *   GET /api/dividends         → the accumulated ex-dividend calendar
 *
 * Cached at the edge through the Cache API (the static payload changes once per
 * session, so a colo answers from cache and R2 sees ~one read per object per
 * half hour), with CORS open because the desktop/mobile clients call it from
 * `file://`-ish origins. The R2 objects carry their own `Cache-Control`
 * (`DATA_CACHE_CONTROL`: browser 5 min, edge 30 min, stale-while-revalidate 1 day).
 *
 * Publication invalidation is AUTOMATIC: the ETL writes `cache-version.json`
 * after the snapshot, and every NON-history edge key embeds that stamp (read
 * once a minute per isolate). A new session therefore replaces the cached grid
 * at every colo within a minute — no purge API, no client cache-buster. The
 * per-symbol histories are excluded on purpose: a publication must not evict
 * thousands of history entries from every colo; they follow their own TTL.
 */
import {
  SCREENER_DIVIDENDS_PATH,
  SCREENER_HISTORY_PREFIX,
  SCREENER_HOLDINGS_PATH,
  SCREENER_MARKET_KEY,
  SCREENER_MARKET_PATH,
  SCREENER_SNAPSHOT_KEY,
  SCREENER_SNAPSHOT_PATH
} from '@odf/shared/screener'
import type { ScreenerEnv } from './env'
import {
  CACHE_VERSION_KEY,
  DATA_CACHE_CONTROL,
  DIVIDENDS_KEY,
  HISTORY_KEY_PREFIX,
  HOLDINGS_KEY,
  historyKey
} from './keys'
import { isScreenerSymbol } from './universe'

const HISTORY_ROUTE = new RegExp(`^${SCREENER_HISTORY_PREFIX}([^/]+)$`)

const CORS = {
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, HEAD, OPTIONS',
  'access-control-allow-headers': 'content-type, if-none-match',
  'access-control-max-age': '86400'
} as const

/** The ONE context member the handler uses — satisfied by both the Worker's
 *  `ExecutionContext` and the Pages `EventContext`. */
export interface Background {
  waitUntil(promise: Promise<unknown>): void
}

/** Bump to invalidate every cached object at every colo (a payload SHAPE
 *  change must never be served from an older entry). v7 = the 61-column
 *  snapshot; v8 = 72 columns + the `market` block inside the snapshot;
 *  v9 = the market context split into its own `/api/market`; v10 = the
 *  official maintenance ratio (BFIJ3U) and no 融資金額 family. */
const CACHE_VERSION = '11'

/** One R2 read per minute per isolate — the stamp only moves once a session. */
const VERSION_TTL_MS = 60_000
let versionMemo = { at: 0, value: '' }

async function edgeVersion(env: ScreenerEnv): Promise<string> {
  const now = Date.now()
  if (now - versionMemo.at < VERSION_TTL_MS) return versionMemo.value
  let value = ''
  try {
    const obj = await env.R2_BUCKET.get(CACHE_VERSION_KEY)
    if (obj) value = (await obj.text()).trim().slice(0, 32)
  } catch {
    // A version read failure must never fail the data read: fall back to the
    // static key (the object's own TTL still bounds staleness).
  }
  versionMemo = { at: now, value }
  return value
}

export async function serveApi(
  request: Request,
  env: ScreenerEnv,
  ctx: Background
): Promise<Response> {
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS })
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    return error('method not allowed', 405)
  }

  const url = new URL(request.url)
  const object = objectKey(url.pathname)
  if (object === null) return error('not found', 404)

  // The Cache API is the edge tier; a hit never touches R2. The key is the URL
  // path plus CACHE_VERSION plus the publication stamp (never the client's
  // query). The stamp is skipped for histories so a publish does not evict the
  // whole per-symbol archive; they follow `DATA_CACHE_CONTROL`'s TTL.
  const isHistory = object.startsWith(HISTORY_KEY_PREFIX)
  const stamp = isHistory ? '' : await edgeVersion(env)
  const cache = caches.default
  const key = new Request(
    `${url.origin}${url.pathname}?edge=${CACHE_VERSION}${stamp ? `-${stamp}` : ''}`
  )
  const hit = await cache.match(key)
  if (hit) {
    const headers = new Headers(hit.headers)
    headers.set('x-edge-cache', 'hit')
    return new Response(hit.body, { status: hit.status, headers })
  }

  // One delayed retry: an R2 blip must not 500 a cold colo with a bodiless
  // Pages error (no CORS, no retry hint). A second failure answers a JSON 503
  // the clients can read and retry — never a silent empty grid.
  let obj: R2ObjectBody | null = null
  try {
    obj = await env.R2_BUCKET.get(object)
    if (!obj) {
      await new Promise((resolve) => setTimeout(resolve, 200))
      obj = await env.R2_BUCKET.get(object)
    }
  } catch (err) {
    console.log(`[serve] r2 ${object} failed: ${String(err)}`)
    return unavailable()
  }
  if (!obj) return error('not found', 404)

  // A browser revalidation (max-age expiry) returns the small 304 instead of
  // re-streaming the object. A 304 is never cached (it has no body to store).
  if (request.headers.get('if-none-match') === obj.httpEtag) {
    const headers = new Headers(CORS)
    obj.writeHttpMetadata(headers)
    headers.set('etag', obj.httpEtag)
    if (!headers.has('cache-control')) headers.set('cache-control', DATA_CACHE_CONTROL)
    return new Response(null, { status: 304, headers })
  }

  const headers = new Headers(CORS)
  obj.writeHttpMetadata(headers)
  headers.set('etag', obj.httpEtag)
  headers.set('x-edge-cache', 'miss')
  if (!headers.has('cache-control')) headers.set('cache-control', DATA_CACHE_CONTROL)
  const response = new Response(request.method === 'HEAD' ? null : obj.body, { headers })
  if (request.method === 'GET') ctx.waitUntil(cache.put(key, response.clone()))
  return response
}

function objectKey(pathname: string): string | null {
  if (pathname === SCREENER_SNAPSHOT_PATH) return SCREENER_SNAPSHOT_KEY
  if (pathname === SCREENER_MARKET_PATH) return SCREENER_MARKET_KEY
  if (pathname === SCREENER_HOLDINGS_PATH) return HOLDINGS_KEY
  if (pathname === SCREENER_DIVIDENDS_PATH) return DIVIDENDS_KEY
  const m = HISTORY_ROUTE.exec(pathname)
  if (!m) return null
  // A malformed percent sequence must answer 404, not a bodiless 500.
  let symbol: string
  try {
    symbol = decodeURIComponent(m[1])
  } catch {
    return null
  }
  // Same universe rule as the ETL: a listed ordinary share, nothing else.
  return isScreenerSymbol(symbol) ? historyKey(symbol) : null
}

/** A bucket outage the client can retry — JSON + CORS + `Retry-After`, never a
 *  bodiless platform 500. */
function unavailable(): Response {
  return new Response(JSON.stringify({ error: 'data temporarily unavailable' }), {
    status: 503,
    headers: {
      ...CORS,
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'retry-after': '60'
    }
  })
}

function error(message: string, status: number): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: {
      ...CORS,
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    }
  })
}
