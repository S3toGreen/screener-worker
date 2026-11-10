import { describe, expect, test } from 'bun:test'
import {
  HOLDINGS_CACHE_WEEKS,
  HOLDINGS_WEEKS,
  SC,
  SCREENER_BASE_FIELDS,
  SCREENER_FIELDS
} from '../screener'
import type { StorageAdapter } from './storage'
import {
  createScreenerService,
  type ScreenerFetch,
  type ScreenerFetchResponse
} from './screenerService'

/** A 61-column row — exactly what the deployed (pre-credit) ETL still writes. */
function oldRow(symbol: string, close: number): Array<string | number | null> {
  const out: Array<string | number | null> = new Array(SCREENER_BASE_FIELDS).fill(null)
  out[0] = symbol
  out[1] = symbol
  out[2] = 'TWSE'
  out[4] = close
  return out
}

function memoryStorage(): StorageAdapter & { map: Map<string, unknown> } {
  const map = new Map<string, unknown>()
  return {
    map,
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

/** A fetch stub: `routes` maps a path to its JSON body (or a status). */
function stubFetch(
  routes: Record<string, unknown | { status: number }>
): ScreenerFetch & { calls: string[] } {
  const calls: string[] = []
  const impl = (async (url: string): Promise<ScreenerFetchResponse> => {
    calls.push(url)
    const path = url.replace(/^https?:\/\/[^/]+/, '').split('?')[0]
    const body = routes[path]
    if (body === undefined) return { ok: false, status: 404, json: async () => null }
    if (typeof body === 'object' && body !== null && 'status' in body) {
      return { ok: false, status: (body as { status: number }).status, json: async () => null }
    }
    return { ok: true, status: 200, json: async () => body }
  }) as ScreenerFetch
  return Object.assign(impl, { calls })
}

const BASE = 'https://cdn.test'

const OLD_SNAPSHOT = {
  sessionDate: 1_793_635_200_000,
  benchmark: {
    close: 22_450.15,
    ret1d: -1.8,
    ret5d: null,
    ret20d: null,
    ret60d: null,
    ret240d: null
  },
  rows: [oldRow('2330', 100_000), oldRow('2317', 7_500)]
}

describe('loadSnapshot', () => {
  test('a FULL store evicts the disposable histories and still writes the grid', async () => {
    // The production failure: the history LRU filled localStorage, the grid
    // write failed SILENTLY, and the next boot re-applied the stale snapshot.
    const storage = memoryStorage()
    await storage.set('odf-screener-hist:2330', { dates: [] })
    await storage.set('odf-screener-hist:2317', { dates: [] })
    await storage.set('odf-screener-hist-lru', ['2330', '2317'])
    const write = storage.set.bind(storage)
    let quotaThrown = false
    storage.set = async (key, value) => {
      if (key === 'odf-screener-cache' && !quotaThrown) {
        quotaThrown = true
        throw new Error('QuotaExceededError')
      }
      return write(key, value)
    }
    const svc = createScreenerService(
      { base: BASE },
      storage,
      stubFetch({ '/api/snapshot': OLD_SNAPSHOT })
    )
    const snap = await svc.loadSnapshot()
    expect(snap.rows.length).toBe(2)
    expect(storage.map.has('odf-screener-cache')).toBe(true)
    // The disposable half is gone; the grid survived.
    expect(storage.map.has('odf-screener-hist:2330')).toBe(false)
    expect(storage.map.has('odf-screener-hist:2317')).toBe(false)
    expect(storage.map.has('odf-screener-hist-lru')).toBe(false)
  })

  test('an older (61-column) snapshot is ACCEPTED and padded — not "corrupt"', async () => {
    const storage = memoryStorage()
    const svc = createScreenerService(
      { base: BASE },
      storage,
      stubFetch({ '/api/snapshot': OLD_SNAPSHOT })
    )
    const snap = await svc.loadSnapshot()
    expect(snap.rows.length).toBe(2)
    expect(snap.rows[0].length).toBe(SCREENER_FIELDS.length)
    expect(snap.rows[0][SC.close]).toBe(100_000)
    expect(snap.rows[0][SC.marginBuy]).toBeNull()
    expect(snap.sessionDate).toBe(OLD_SNAPSHOT.sessionDate)
  })

  test('the TAIEX level survives the read (the pill needs close)', async () => {
    const svc = createScreenerService(
      { base: BASE },
      memoryStorage(),
      stubFetch({ '/api/snapshot': OLD_SNAPSHOT })
    )
    const snap = await svc.loadSnapshot()
    expect(snap.benchmark?.close).toBe(22_450.15)
    expect(snap.benchmark?.ret1d).toBe(-1.8)
    expect(snap.pulledAt).toBeGreaterThan(0)
  })

  test('a full-width snapshot passes through unchanged', async () => {
    const full: Array<string | number | null> = new Array(SCREENER_FIELDS.length).fill(null)
    full[SC.symbol] = '2330'
    full[SC.close] = 100_000
    const svc = createScreenerService(
      { base: BASE },
      memoryStorage(),
      stubFetch({ '/api/snapshot': { sessionDate: 1, rows: [full] } })
    )
    const snap = await svc.loadSnapshot()
    expect(snap.rows[0].length).toBe(SCREENER_FIELDS.length)
  })

  test('a corrupt payload throws when no cache exists…', async () => {
    const svc = createScreenerService(
      { base: BASE },
      memoryStorage(),
      stubFetch({ '/api/snapshot': { rows: [] } })
    )
    await expect(svc.loadSnapshot()).rejects.toThrow('snapshot corrupt')
  })

  test('…and falls back to the cached grid when one does', async () => {
    const storage = memoryStorage()
    await storage.set('odf-screener-cache', {
      sessionDate: 42,
      rows: [oldRow('2330', 100_000)],
      pulledAt: 7
    })
    const svc = createScreenerService(
      { base: BASE },
      storage,
      stubFetch({ '/api/snapshot': { sessionDate: 'x', rows: 'nope' } })
    )
    const snap = await svc.loadSnapshot()
    expect(snap.sessionDate).toBe(42)
    expect(snap.rows[0].length).toBe(SCREENER_FIELDS.length)
    expect(snap.pulledAt).toBe(7)
  })

  test('cachedSnapshot pads an older cached copy too', async () => {
    const storage = memoryStorage()
    await storage.set('odf-screener-cache', {
      sessionDate: 42,
      benchmark: { close: 1, ret1d: null, ret5d: null, ret20d: null, ret60d: null, ret240d: null },
      rows: [oldRow('2330', 100_000)],
      pulledAt: 9
    })
    const snap = await createScreenerService({ base: BASE }, storage).cachedSnapshot()
    expect(snap?.rows[0].length).toBe(SCREENER_FIELDS.length)
    expect(snap?.benchmark?.close).toBe(1)
  })
})

describe('loadMarketContext', () => {
  const MARKET = {
    asOf: 1_793_635_200_000,
    credit: { scope: 'TWSE', sessionDate: 1_793_635_200_000, twseMarginBalanceLots: 5_997_744 },
    derivatives: {
      asOf: 1_793_635_200_000,
      institutions: [],
      pcr: [{ dateMs: 1, oiRatio: 150 }],
      vix: null
    }
  }

  test('reads its OWN route and never touches the snapshot', async () => {
    const storage = memoryStorage()
    const fetchImpl = stubFetch({ '/api/market': MARKET })
    const svc = createScreenerService({ base: BASE }, storage, fetchImpl)
    const ctx = await svc.loadMarketContext()
    expect(ctx?.credit?.twseMarginBalanceLots).toBe(5_997_744)
    expect(fetchImpl.calls.map((u) => u.replace(/^https?:\/\/[^/]+/, ''))).toEqual(['/api/market'])
    expect(storage.map.has('odf-screener-cache')).toBe(false)
    expect(storage.map.has('odf-market-cache')).toBe(true)
  })

  test('a deployed API without the route answers null (no throw, no cache)', async () => {
    const svc = createScreenerService({ base: BASE }, memoryStorage(), stubFetch({}))
    expect(await svc.loadMarketContext()).toBeNull()
  })

  test('a failed fetch falls back to the cached context', async () => {
    const storage = memoryStorage()
    await storage.set('odf-market-cache', MARKET)
    const svc = createScreenerService({ base: BASE }, storage, stubFetch({}))
    const ctx = await svc.loadMarketContext()
    expect(ctx?.asOf).toBe(MARKET.asOf)
  })
})

describe('loadHoldings — 3-year retention, quota-safe cache', () => {
  const holdings = (weeks: number): unknown => ({
    updatedAt: 1,
    weeks: Array.from({ length: weeks }, (_, i) => ({
      week: `2026${String(40 + i).padStart(2, '0')}01`.slice(0, 8),
      symbols: { '2330': [80 + i, 10, 3_000_000] }
    }))
  })

  test('the fetched object stays FULL (up to HOLDINGS_WEEKS = 3 years)', async () => {
    const storage = memoryStorage()
    const svc = createScreenerService(
      { base: BASE },
      storage,
      stubFetch({ '/api/holdings': holdings(40) })
    )
    const full = await svc.loadHoldings()
    expect(full?.weeks.length).toBe(40)
    // The localStorage copy is trimmed to the quota-safe window…
    const cached = storage.map.get('odf-screener-holdings') as { weeks: unknown[] }
    expect(cached.weeks.length).toBe(HOLDINGS_CACHE_WEEKS)
    // …while the SOURCE retention is the 3-year cap.
    expect(HOLDINGS_WEEKS).toBe(156)
  })

  test('a payload within the cache window is stored unchanged', async () => {
    const storage = memoryStorage()
    const svc = createScreenerService(
      { base: BASE },
      storage,
      stubFetch({ '/api/holdings': holdings(3) })
    )
    await svc.loadHoldings()
    const cached = storage.map.get('odf-screener-holdings') as { weeks: unknown[] }
    expect(cached.weeks.length).toBe(3)
  })
})
