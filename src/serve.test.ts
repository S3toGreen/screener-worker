import { afterEach, beforeEach, describe, expect, setSystemTime, test } from 'bun:test'
import type { ScreenerEnv } from './env'
import { serveApi, type Background } from './serve'

/** The R2 slice `serveApi` reads: `get(key)` → an object body or null, plus the
 *  `.text()` the cache-version reader uses. Reads are recorded for assertions. */
function fakeBucket(objects: Map<string, string>, reads: string[]): object {
  return {
    async get(key: string) {
      reads.push(key)
      const body = objects.get(key)
      if (body === undefined) return null
      return {
        httpEtag: `"${key}"`,
        writeHttpMetadata: (headers: Headers) => headers.set('content-type', 'application/json'),
        body: new Blob([body]).stream(),
        text: async () => body
      }
    }
  }
}

function fakeCache(): { map: Map<string, Response>; default: Cache } {
  const map = new Map<string, Response>()
  return {
    map,
    default: {
      async match(key: Request) {
        const hit = map.get(key.url)
        return hit ? hit.clone() : undefined
      },
      async put(key: Request, response: Response) {
        map.set(key.url, response)
      }
    } as unknown as Cache
  }
}

function context(): Background & { pending: Promise<unknown>[] } {
  return {
    pending: [],
    waitUntil(promise: Promise<unknown>) {
      this.pending.push(promise)
    }
  }
}

const SNAPSHOT = JSON.stringify({ sessionDate: 1, rows: [] })
const ENV = (objects: Map<string, string>, reads: string[]): ScreenerEnv =>
  ({ R2_BUCKET: fakeBucket(objects, reads) }) as unknown as ScreenerEnv

describe('the edge cache', () => {
  let objects: Map<string, string>
  let reads: string[]
  let cache: ReturnType<typeof fakeCache>
  const realCaches = globalThis.caches

  beforeEach(() => {
    objects = new Map()
    reads = []
    cache = fakeCache()
    ;(globalThis as { caches?: unknown }).caches = cache
    setSystemTime(new Date('2026-11-05T00:00:00Z'))
  })

  afterEach(() => {
    ;(globalThis as { caches?: unknown }).caches = realCaches
    setSystemTime()
  })

  test('the snapshot key carries the publication stamp; a hit never re-reads R2', async () => {
    objects.set('screener-latest.json', SNAPSHOT)
    objects.set('cache-version.json', 'v1')
    const first = context()
    const res = await serveApi(
      new Request('https://odf-trading.pages.dev/api/snapshot'),
      ENV(objects, reads),
      first
    )
    await Promise.all(first.pending)
    expect(res.status).toBe(200)
    expect([...cache.map.keys()]).toEqual(['https://odf-trading.pages.dev/api/snapshot?edge=11-v1'])
    expect(reads).toEqual(['cache-version.json', 'screener-latest.json'])

    const second = context()
    const hit = await serveApi(
      new Request('https://odf-trading.pages.dev/api/snapshot'),
      ENV(objects, reads),
      second
    )
    expect(hit.headers.get('x-edge-cache')).toBe('hit')
    // The stamp is memoized per isolate; the object came from the edge cache.
    expect(reads.filter((k) => k === 'cache-version.json').length).toBe(1)
    expect(reads.includes('screener-latest.json')).toBe(true)
    expect(reads.filter((k) => k === 'screener-latest.json').length).toBe(1)
  })

  test('a new publication stamp mints a new edge key (no purge API)', async () => {
    objects.set('screener-latest.json', SNAPSHOT)
    objects.set('cache-version.json', 'v1')
    const first = context()
    await serveApi(
      new Request('https://odf-trading.pages.dev/api/snapshot'),
      ENV(objects, reads),
      first
    )
    await Promise.all(first.pending)

    objects.set('cache-version.json', 'v2')
    setSystemTime(new Date('2026-11-05T00:01:01Z'))
    const second = context()
    const res = await serveApi(
      new Request('https://odf-trading.pages.dev/api/snapshot'),
      ENV(objects, reads),
      second
    )
    expect(res.headers.get('x-edge-cache')).toBe('miss')
    expect([...cache.map.keys()]).toContain('https://odf-trading.pages.dev/api/snapshot?edge=11-v2')
  })

  test('history entries are NOT versioned (a publish must not evict the archive)', async () => {
    objects.set('history/2330.json', '{"symbol":"2330"}')
    objects.set('cache-version.json', 'v1')
    const first = context()
    const res = await serveApi(
      new Request('https://odf-trading.pages.dev/api/history/2330'),
      ENV(objects, reads),
      first
    )
    await Promise.all(first.pending)
    expect(res.status).toBe(200)
    expect([...cache.map.keys()]).toEqual([
      'https://odf-trading.pages.dev/api/history/2330?edge=11'
    ])
    // The history path never even reads the stamp object.
    expect(reads).toEqual(['history/2330.json'])
  })
})
