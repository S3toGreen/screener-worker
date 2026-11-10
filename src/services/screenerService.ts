import {
  HOLDINGS_CACHE_WEEKS,
  SCREENER_HISTORY_FIELDS,
  isDividendsObject,
  isHoldingsObject,
  isScreenerHistory,
  readMarketContext,
  readSnapshotRows,
  screenerDividendsUrl,
  screenerHistoryUrl,
  screenerHoldingsUrl,
  screenerMarketUrl,
  screenerSnapshotUrl,
  type DividendsObject,
  type HoldingsObject,
  type MarketContext,
  type ScreenerBenchmark,
  type ScreenerHistory,
  type ScreenerSnapshot
} from '../screener'
import type { StorageAdapter } from './storage'

/** The snapshot origin (the deployed Worker's route or an R2 custom domain). */
export interface ScreenerCdn {
  /** Base URL; a trailing slash is tolerated. */
  base: string
}

/** The fetch slice this module needs — shared is DOM-lib free by convention;
 *  web, Node 18+, Bun and workerd all satisfy it structurally. */
export interface ScreenerFetchResponse {
  ok: boolean
  status: number
  json(): Promise<unknown>
}
export type ScreenerFetch = (url: string) => Promise<ScreenerFetchResponse>

const defaultFetch: ScreenerFetch = (url) =>
  (globalThis as unknown as { fetch: ScreenerFetch }).fetch(url)

const SNAPSHOT_CACHE = 'odf-screener-cache'
/** The market-wide context (its own object — never part of the stock grid). */
const MARKET_CACHE = 'odf-market-cache'
const HOLDINGS_CACHE = 'odf-screener-holdings'
const DIVIDENDS_CACHE = 'odf-screener-dividends'
const HISTORY_CACHE = 'odf-screener-hist:'
/** The history-retention index: an array of symbols, oldest first, capped —
 *  `loadHistory` caches EVERY fetch, so an uncapped window grew one blob per
 *  opened symbol for the profile's lifetime. The evicted symbol's cache key is
 *  removed with the index entry (a re-select simply re-fetches). */
const HISTORY_LRU = 'odf-screener-hist-lru'
/** Cached histories kept. MEASURED: one 260-session history is ≈73 K chars
 *  while the whole origin quota is ≈5.2 M chars (Chromium, verified) — the
 *  grid cache alone is ≈1.47 M, so 96 entries (≈7 M) guaranteed a quota
 *  failure that silently dropped the GRID write. 32 (≈2.3 M) leaves room for
 *  the grid and every other preference. */
const HISTORY_LRU_CAP = 32
/** The CACHED sessions per history: the full window is up to 780 sessions and
 *  a single 780-session body is ≈200 KB — the cache keeps the newest 260 while
 *  the returned body stays complete. */
const HISTORY_CACHE_SESSIONS = 260

/** The storage copy of a history, trimmed to the cache window (older first). */
/** The localStorage copy of the weekly TDCC object keeps only the newest
 *  window: the source is 3 years (~10 MB), which would blow the origin quota
 *  on its own. The fetched object (and the preview) stays COMPLETE. */
function cacheableHoldings(h: HoldingsObject): HoldingsObject {
  const weeks = h.weeks.slice(0, HOLDINGS_CACHE_WEEKS)
  return weeks.length === h.weeks.length ? h : { ...h, weeks }
}

function cacheableHistory(h: ScreenerHistory): ScreenerHistory {
  const drop = h.dates.length - HISTORY_CACHE_SESSIONS
  if (drop <= 0) return h
  const out = { ...h, dates: h.dates.slice(drop) } as ScreenerHistory
  const write = out as unknown as Record<string, unknown[]>
  const read = h as unknown as Record<string, unknown[]>
  for (const field of SCREENER_HISTORY_FIELDS) {
    const src = read[field]
    // An older history may legitimately omit an optional column; keep it
    // absent instead of throwing (which silently skipped caching entirely).
    if (Array.isArray(src)) write[field] = src.slice(drop)
  }
  return out
}

/** The cached snapshot carries the fetch time (the store's "pulled at" line). */
export interface CachedSnapshot extends ScreenerSnapshot {
  pulledAt: number
}

export interface ScreenerService {
  /** The last cached snapshot (an instant grid on boot), or null. */
  cachedSnapshot(): Promise<CachedSnapshot | null>
  /** Fetch the snapshot; on a network failure the cache is served when one
   *  exists, otherwise the error propagates. */
  /** The newest published grid. Reads through the browser/edge cache: a
   *  publication invalidates the edge entry by its own version stamp, so a
   *  client-side cache-buster would only multiply R2 reads. `force` (a manual
   *  Pull) adds one query stamp so the BROWSER's HTTP cache cannot serve the
   *  object the user asked to replace; the edge still keys on its stamp. */
  loadSnapshot(force?: boolean): Promise<CachedSnapshot>
  loadHistory(symbol: string): Promise<ScreenerHistory>
  /** The market-wide credit + TAIFEX context (`/api/market`, its own object);
   *  null when neither the CDN nor the cache answers a valid shape. */
  loadMarketContext(): Promise<MarketContext | null>
  /** The weekly TDCC object (one market-wide read, edge-cached); null when
   *  neither the CDN nor the cache answers a valid shape — the preview simply
   *  hides its weekly section. */
  loadHoldings(): Promise<HoldingsObject | null>
  /** The accumulated ex-dividend calendar; null when unavailable. */
  loadDividends(): Promise<DividendsObject | null>
}

/** A cached/fetched snapshot body: the session stamp plus a row grid. Older
 *  rows are padded to the current contract (append-only schema) — a server and
 *  a client deploy independently, so a narrower grid is NOT corruption. */
function readSnapshotBody(raw: unknown): CachedSnapshot | null {
  if (typeof raw !== 'object' || raw === null) return null
  const v = raw as Record<string, unknown>
  if (typeof v.sessionDate !== 'number') return null
  const rows = readSnapshotRows(v.rows)
  if (!rows) return null
  return {
    sessionDate: v.sessionDate,
    benchmark: readBenchmark(v.benchmark),
    rows,
    pulledAt: typeof v.pulledAt === 'number' ? v.pulledAt : 0
  }
}

/** The TAIEX context: unknown cells become null (the UI dashes them). */
function readBenchmark(raw: unknown): ScreenerBenchmark | null {
  if (!raw || typeof raw !== 'object') return null
  const b = raw as Record<string, unknown>
  const pick = (key: keyof ScreenerBenchmark): number | null => {
    const v = b[key]
    return typeof v === 'number' && Number.isFinite(v) ? v : null
  }
  return {
    close: pick('close') ?? undefined,
    ret1d: pick('ret1d'),
    ret5d: pick('ret5d'),
    ret20d: pick('ret20d'),
    ret60d: pick('ret60d'),
    ret240d: pick('ret240d')
  }
}

export function createScreenerService(
  cdn: ScreenerCdn,
  storage: StorageAdapter,
  fetchImpl: ScreenerFetch = defaultFetch
): ScreenerService {
  /** Bumps the symbol to the LRU tail and evicts the oldest past the cap. */
  async function touchHistoryLru(symbol: string): Promise<void> {
    const raw = await storage.get<unknown>(HISTORY_LRU)
    const list = Array.isArray(raw) ? raw.filter((s): s is string => typeof s === 'string') : []
    const at = list.indexOf(symbol)
    if (at >= 0) list.splice(at, 1)
    list.push(symbol)
    while (list.length > HISTORY_LRU_CAP) {
      const evict = list.shift()
      if (evict !== undefined) await storage.remove(HISTORY_CACHE + evict)
    }
    await storage.set(HISTORY_LRU, list)
  }

  /** Drops every cached preview history — they are re-fetched on demand,
   *  unlike the GRID (which must survive for the next boot). The LRU index
   *  names them. */
  async function evictHistories(): Promise<void> {
    const raw = await storage.get<unknown>(HISTORY_LRU)
    if (Array.isArray(raw)) {
      for (const symbol of raw) {
        if (typeof symbol === 'string') await storage.remove(HISTORY_CACHE + symbol)
      }
    }
    await storage.remove(HISTORY_LRU)
  }

  /** Persists a CRITICAL cache object (the grid, the market context): a full
   *  store must NOT silently drop it, so the disposable histories are evicted
   *  and the write retried once. */
  async function persistCritical(key: string, value: unknown): Promise<void> {
    try {
      await storage.set(key, value)
      return
    } catch {
      // quota — evict the disposable half and retry
    }
    await evictHistories()
    await storage.set(key, value)
  }

  const base = cdn.base

  /** ONE small-object read with the cache as the fallback — a corrupt or
   *  failed payload never propagates: the caller renders the section as
   *  absent, never as zeros. */
  async function loadSmall<T>(
    url: string,
    key: string,
    reader: (raw: unknown) => T | null,
    /** The CACHE copy (a quota-safe projection); the returned value stays full. */
    cacheable: (value: T) => T = (value) => value
  ): Promise<T | null> {
    try {
      const res = await fetchImpl(url)
      if (res.ok) {
        const body = (await res.json()) as unknown
        const value = reader(body)
        if (value) {
          await persistCritical(key, cacheable(value))
          return value
        }
      }
    } catch {
      // A network failure falls through to the cached copy below.
    }
    return reader(await storage.get<unknown>(key))
  }

  async function cachedSnapshot(): Promise<CachedSnapshot | null> {
    return readSnapshotBody(await storage.get<unknown>(SNAPSHOT_CACHE))
  }

  return {
    cachedSnapshot,

    async loadSnapshot(force = false): Promise<CachedSnapshot> {
      const url = force ? `${screenerSnapshotUrl(base)}?t=${Date.now()}` : screenerSnapshotUrl(base)
      let res: ScreenerFetchResponse
      try {
        res = await fetchImpl(url)
      } catch (err) {
        const cached = await cachedSnapshot()
        if (cached) return cached
        throw err
      }
      if (!res.ok) throw new Error(`snapshot ${res.status}`)
      const body = (await res.json()) as Record<string, unknown>
      const snap = readSnapshotBody(body)
      if (!snap) {
        const cached = await cachedSnapshot()
        if (cached) return cached
        throw new Error('snapshot corrupt')
      }
      snap.pulledAt = Date.now()
      try {
        await persistCritical(SNAPSHOT_CACHE, snap)
      } catch {
        // The grid still renders this pull; only the NEXT boot's cache is lost.
      }
      return snap
    },

    async loadMarketContext(): Promise<MarketContext | null> {
      return loadSmall(screenerMarketUrl(base), MARKET_CACHE, readMarketContext)
    },

    async loadHoldings(): Promise<HoldingsObject | null> {
      return loadSmall(
        screenerHoldingsUrl(base),
        HOLDINGS_CACHE,
        (raw) => (isHoldingsObject(raw) ? raw : null),
        cacheableHoldings
      )
    },

    async loadDividends(): Promise<DividendsObject | null> {
      return loadSmall(screenerDividendsUrl(base), DIVIDENDS_CACHE, (raw) =>
        isDividendsObject(raw) ? raw : null
      )
    },

    async loadHistory(symbol: string): Promise<ScreenerHistory> {
      const key = HISTORY_CACHE + symbol
      let res: ScreenerFetchResponse
      try {
        res = await fetchImpl(screenerHistoryUrl(base, symbol))
      } catch (err) {
        const cached = await storage.get<unknown>(key)
        if (isScreenerHistory(cached)) return cached
        throw err
      }
      if (res.status === 404) throw new Error(`history ${symbol} 404`)
      if (!res.ok) throw new Error(`history ${symbol} ${res.status}`)
      const body = (await res.json()) as unknown
      if (!isScreenerHistory(body)) throw new Error(`history ${symbol} corrupt`)
      // Best-effort: a full store must never fail a preview read. The LRU
      // index goes FIRST — a history written but never indexed could never be
      // evicted (a permanent orphan in the quota).
      try {
        await touchHistoryLru(symbol)
        await storage.set(key, cacheableHistory(body))
      } catch {
        // the cache is disposable
      }
      return body
    }
  }
}
