/**
 * History backfill (run once, before the daily schedule takes over) — a pure
 * rebuild from the EXCHANGES' official per-date/per-period tables. No third
 * party: every column below has an official route (TWSE/TPEx rwd per-date
 * tables, MOPS quarterly/monthly reports), so a 3-year rebuild needs ~40 MOPS
 * requests plus the per-session day walk — never a per-symbol vendor pull.
 *
 *   pnpm --filter @odf/screener-worker run backfill -- --months 36 --dry-run
 *   pnpm --filter @odf/screener-worker run backfill -- --from 2023-10-01 --to 2026-10-22
 *   pnpm --filter @odf/screener-worker run backfill -- --months 1 --symbols 2330,6488 --dry-run
 *
 * Flags: `--months N` (default 36) · `--from/--to YYYY-MM-DD` ·
 * `--symbols a,b,c` / `--sample N` (a SMOKE run: the write set is limited and
 * NOTHING is uploaded) · `--api <base>` (the snapshot to learn the universe
 * from, default the deployed Pages API) · `--dry-run` (never upload) ·
 * `--keep` (keep .seed-out/).
 *
 * Flow: universe (the live snapshot's market caps imply the share count) →
 * the MOPS collector (quarters + months, concurrently with the day walk, both
 * paced by the shared polite gate) → one exchange read per session → per-symbol
 * column arrays → settled-or-fail gate → histories → benchmark → the SAME
 * `runEtl` the daily job runs (history first, snapshot second) → upload.
 */
import { join } from 'node:path'
import {
  SC,
  SCREENER_HISTORY_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  type ScreenerHistory,
  type ScreenerHistoryField
} from '@odf/shared/screener'
import { DAY_MS, dayStampOf } from '@odf/shared/session'
import { mergeBenchmark, twseMonthIndex } from './src/benchmark'
import {
  tpexDayBars,
  tpexDayFlows,
  tpexDayForeign,
  tpexDayMargin,
  tpexDaySblBal,
  tpexDayTrade,
  tpexDayValuation,
  twseDayBars,
  twseDayForeign,
  twseDayValuation,
  twseMarginDay,
  twseDaySblBal
} from './src/history'
import {
  expectRows,
  mopsMonthRevenue,
  mopsQuarterMargins,
  type QuarterMargins,
  type RevenueDelta
} from './src/sources/mops'
import { runEtl } from './src/etl'
import type {
  DayBar,
  ForeignHold,
  InstSrc,
  MarginSrc,
  SblSrc,
  ValuationRow
} from './src/sources/types'
import { twseDayTrade, twseInstitutional } from './src/sources/twse'
import { createFsEnv, resetSeedRoot, SEED_ROOT, uploadSeedRoot } from './seedFs'

const DEFAULT_API = 'https://odf-trading.pages.dev'

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 ? process.argv[i + 1] : undefined
}

const dryRun = process.argv.includes('--dry-run')
const apiBase = (arg('api') ?? DEFAULT_API).replace(/\/+$/, '')
const fromMs = arg('from') ? Date.parse(`${arg('from')}T00:00:00+08:00`) : 0
const toMs = arg('to') ? Date.parse(`${arg('to')}T00:00:00+08:00`) : Date.now()
const months = Number(arg('months') ?? 36)
const startMs = fromMs || toMs - Math.round(months * 30.44) * DAY_MS
const symbolsArg = arg('symbols')
const sampleCount = Number(arg('sample') ?? 0) || 0

/** The generic view the field loops write through (the contract's exact types
 *  differ per column; the loops are one shape). */
type AccColumns = Record<ScreenerHistoryField, (number | null)[]>
/** One symbol's column arrays, index-aligned on `dates` — the exact contract
 *  the history file carries, so the writer needs no per-field conversion. */
type Acc = { dates: string[] } & Pick<ScreenerHistory, ScreenerHistoryField>

function columns(a: Acc): AccColumns {
  return a as unknown as AccColumns
}

function acc(): Acc {
  const out = { dates: [] as string[] } as unknown as Acc
  for (const field of SCREENER_HISTORY_FIELDS) columns(out)[field] = []
  return out
}

/** A plain backoff sleep — the day-read retry shares it. */
function delay(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>()
  setTimeout(resolve, ms)
  return promise
}

/** A per-day exchange read: `failed` = the read itself broke after every
 *  retry (a holiday answers a VALID EMPTY table — never a failure). */
type DayRead<T> = { failed: true; value: null } | { failed: false; value: T }

/** A null sub-source on a SESSION day is retried once: the exchange CDNs
 *  occasionally answer an empty payload (a transient edge miss) that the very
 *  next read resolves — the day is known to trade because its bars are in the
 *  same walk. Two attempts, then the cell stays null. */
async function readDaySoft<T>(
  name: string,
  read: () => Promise<T | null>
): Promise<DayRead<T | null>> {
  const first = await readDay(name, read)
  if (first.failed || first.value !== null) return first
  await delay(2_000)
  return readDay(name, read)
}

/** Retries one per-day exchange read (timeouts and ECONNRESET are transient —
 *  an unguarded failure used to leave a PERMANENT hole in the histories). */
async function readDay<T>(name: string, read: () => Promise<T>): Promise<DayRead<T>> {
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      return { failed: false, value: await read() }
    } catch (err) {
      console.warn(`[backfill] ${name} attempt ${attempt}/5 failed: ${String(err)}`)
      if (attempt < 5) await delay(10_000 * attempt)
    }
  }
  return { failed: true, value: null }
}

/** The universe + the issued-share count the snapshot already implies. */
async function loadUniverse(): Promise<Map<string, number>> {
  let lastErr: unknown = null
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      // Cache-busted: the edge tier holds the previous snapshot for up to an
      // hour, and a stale universe would silently skip every symbol it lacks
      // (the ETF rows arrived with a later snapshot).
      const res = await fetch(`${apiBase}/api/snapshot?t=${Date.now()}`)
      if (!res.ok) throw new Error(`snapshot HTTP ${res.status}`)
      const snap = (await res.json()) as { rows: Array<Array<string | number | null>> }
      const out = new Map<string, number>()
      for (const row of snap.rows) {
        const symbol = String(row[SC.symbol] ?? '')
        if (!symbol) continue
        const marketCap = Number(row[SC.marketCap])
        const close = Number(row[SC.close])
        // marketCap = shares × close / 100 → shares; 0 when either is unusable.
        out.set(symbol, marketCap > 0 && close > 0 ? (marketCap * 100) / close : 0)
      }
      console.log(`[backfill] universe ${out.size} symbols from ${apiBase}`)
      return out
    } catch (err) {
      lastErr = err
      console.warn(`[backfill] universe attempt ${attempt}/3 failed: ${String(err)}`)
      if (attempt < 3) await delay(10_000 * attempt)
    }
  }
  throw new Error(`[backfill] universe unavailable after 3 attempts: ${String(lastErr)}`)
}

/** The weekday calendar. The per-date readers decide whether a day actually
 *  published, so a holiday simply answers an empty table and is skipped — no
 *  external calendar service is needed (or wanted). */
function tradingDays(): number[] {
  const days: number[] = []
  for (let ms = startMs; ms <= toMs; ms += DAY_MS) {
    const weekday = new Date(ms + 8 * 3_600_000).getUTCDay()
    if (weekday !== 0 && weekday !== 6) days.push(ms)
  }
  console.log(`[backfill] calendar ${days.length} weekdays`)
  return days
}

/**
 * The day walk and the final ETL overlap on the ANCHOR session: the walk builds
 * the per-symbol histories from the exchange's per-date tables, then `runEtl`
 * publishes the snapshot — from the SAME tables. ONE in-process response memo
 * makes that overlap FREE (the walk's last day is the ETL's anchor): keyed by
 * method + URL + request body, successful responses only, raw BYTES (a Big5
 * MOPS page must replay byte-identically), LRU-bounded so a month-long run
 * never grows without limit.
 */
const MEMO_MAX_BYTES = 96 * 1024 * 1024

function installFetchMemo(): void {
  const memo = new Map<string, { bytes: Uint8Array; type: string }>()
  let bytes = 0
  const real = globalThis.fetch
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
    const body = typeof init?.body === 'string' ? init.body : ''
    const key = `${init?.method ?? 'GET'} ${url} ${body}`
    const hit = memo.get(key)
    if (hit) {
      memo.delete(key)
      memo.set(key, hit)
      return new Response(hit.bytes, {
        status: 200,
        headers: hit.type ? { 'content-type': hit.type } : {}
      })
    }
    const res = await real(input as never, init)
    if (!res.ok) return res
    const copy = new Uint8Array(await res.clone().arrayBuffer())
    memo.set(key, { bytes: copy, type: res.headers.get('content-type') ?? '' })
    bytes += copy.byteLength
    for (const [oldKey, old] of memo) {
      if (bytes <= MEMO_MAX_BYTES) break
      memo.delete(oldKey)
      bytes -= old.bytes.byteLength
    }
    return res
  }) as typeof fetch
}

installFetchMemo()

const universe = await loadUniverse()
/** The write set (a smoke run limits it): null = every symbol the day walk
 *  produces. A limited run NEVER uploads. */
const writeSet = ((): Set<string> | null => {
  if (symbolsArg) {
    const list = symbolsArg
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
    console.log(`[backfill] smoke filter — ${list.length} symbols from --symbols, not uploaded`)
    return new Set(list)
  }
  if (sampleCount > 0) {
    const all = [...universe.keys()]
    // Stride-spread so the sample includes BOTH markets (a head-only slice is
    // all TWSE).
    const stride = Math.max(1, Math.floor(all.length / sampleCount))
    const picked = all.filter((_, i) => i % stride === 0).slice(0, sampleCount)
    console.log(`[backfill] smoke filter — ${picked.length} sampled symbols, not uploaded`)
    return new Set(picked)
  }
  return null
})()
const smoke = writeSet !== null

// ── MOPS collector state ────────────────────────────────────────────────────
/** symbol → MOPS PUBLISH stamp (`YYYYMMDD`) → the report. Keyed by publish (not
 *  quarter end): a value applies once technically published, no look-ahead. */
const marginsOf = new Map<string, Map<string, QuarterMargins>>()
const revenueOf = new Map<string, Map<string, RevenueDelta>>()

function putInto<T>(
  map: Map<string, Map<string, T>>,
  symbol: string,
  stamp: string,
  value: T
): void {
  let per = map.get(symbol)
  if (!per) {
    per = new Map()
    map.set(symbol, per)
  }
  per.set(stamp, value)
}

/** The newest entry at or before `stamp`. */
function latestBefore<T>(map: Map<string, T> | undefined, stamp: string): T | null {
  if (!map) return null
  let best: string | null = null
  for (const key of map.keys()) {
    if (key <= stamp && (best === null || key > best)) best = key
  }
  return best === null ? null : (map.get(best) as T)
}

const pad2 = (n: number): string => String(n).padStart(2, '0')
const gregStamp = (y: number, m: number, d: number): string => `${y}${pad2(m)}${pad2(d)}`

/** The MOPS publish day (verbatim from the report note): Q1 May 16, Q2 Aug 15,
 *  Q3 Nov 15, the annual report Apr 1 of the FOLLOWING year. */
function quarterPublishStamp(yearCn: number, season: '01' | '02' | '03' | '04'): string {
  const y = yearCn + 1911
  switch (season) {
    case '01':
      return gregStamp(y, 5, 16)
    case '02':
      return gregStamp(y, 8, 15)
    case '03':
      return gregStamp(y, 11, 15)
    case '04':
      return gregStamp(y + 1, 4, 1)
  }
}

/** A month's revenue publishes from the 10th of the NEXT month. */
function monthPublishStamp(year: number, month: number): string {
  return month === 12 ? gregStamp(year + 1, 1, 10) : gregStamp(year, month + 1, 10)
}

const SEASONS = ['01', '02', '03', '04'] as const
const MARKETS_SRC = ['sii', 'otc'] as const

/** The quarters whose publish day has passed AND that can still apply to the
 *  window (one extra quarter before `startMs` is the carry-in value). */
function quartersFor(today: string): Array<{ yearCn: number; season: (typeof SEASONS)[number] }> {
  const startY = new Date(startMs + 8 * 3_600_000).getUTCFullYear() - 1911
  const endY = new Date(toMs + 8 * 3_600_000).getUTCFullYear() - 1911
  const floor = dayStampOf(startMs - 200 * DAY_MS)
  const out: Array<{ yearCn: number; season: (typeof SEASONS)[number] }> = []
  for (let yearCn = startY - 1; yearCn <= endY; yearCn++) {
    for (const season of SEASONS) {
      const publish = quarterPublishStamp(yearCn, season)
      // STRICTLY before today: a report published today may not be up yet,
      // and an empty parse must never fail the run.
      if (publish < today && publish >= floor) out.push({ yearCn, season })
    }
  }
  return out
}

/** The months whose revenue has published AND can still apply to the window. */
function monthsFor(today: string): Array<{ year: number; month: number }> {
  const out: Array<{ year: number; month: number }> = []
  const start = new Date(startMs - 70 * DAY_MS + 8 * 3_600_000)
  let year = start.getUTCFullYear()
  let month = start.getUTCMonth() + 1
  for (;;) {
    const publish = monthPublishStamp(year, month)
    // A month publishing TODAY is an empty shell for part of the day — only
    // completed publishes are read (a 0-row parse would fail the run).
    if (publish >= today) break
    out.push({ year, month })
    if (year > new Date(toMs + 8 * 3_600_000).getUTCFullYear() + 1) break
    month += 1
    if (month > 12) {
      month = 1
      year += 1
    }
  }
  return out
}

/** One MOPS task = one report fetch. A failure is retried ONCE (a 2nd round
 *  after the day walk) and then blocks the publish (settled-or-fail). */
interface MopsTask {
  label: string
  minRows: number
  run: () => Promise<void>
}

function mopsTasks(today: string): MopsTask[] {
  const tasks: MopsTask[] = []
  for (const { yearCn, season } of quartersFor(today)) {
    const publish = quarterPublishStamp(yearCn, season)
    for (const market of MARKETS_SRC) {
      tasks.push({
        label: `mops Q${yearCn}${season} ${market}`,
        minRows: 300,
        run: async () => {
          const rows = expectRows(
            await mopsQuarterMargins(yearCn, season, market),
            300,
            `mops Q${yearCn}${season} ${market}`
          )
          for (const [symbol, value] of rows) putInto(marginsOf, symbol, publish, value)
        }
      })
    }
  }
  for (const { year, month } of monthsFor(today)) {
    const yearCn = year - 1911
    const publish = monthPublishStamp(year, month)
    for (const market of MARKETS_SRC) {
      for (const foreign of [false, true]) {
        tasks.push({
          label: `mops ${yearCn}/${month} ${market}${foreign ? ' KY' : ''}`,
          // The domestic file always holds the whole market; a KY file can be
          // legitimately empty in an early month, so only the domestic read
          // asserts a floor (the parser drift it catches is shared by both).
          minRows: foreign ? 0 : 1,
          run: async () => {
            const label = `mops ${yearCn}/${month} ${market}${foreign ? ' KY' : ''}`
            const rows = await mopsMonthRevenue(yearCn, month, market, foreign)
            if (!foreign) expectRows(rows, 1, label)
            if (rows.size === 0) {
              console.log(`[backfill] ${label}: no rows`)
              return
            }
            for (const [symbol, value] of rows) putInto(revenueOf, symbol, publish, value)
          }
        })
      }
    }
  }
  return tasks
}

/** Runs every MOPS task; a failed one is re-attempted once in a second round.
 *  A surviving failure is returned (the caller refuses to publish). */
async function collectMops(): Promise<string[]> {
  const today = dayStampOf(Date.now())
  const tasks = mopsTasks(today)
  console.log(`[backfill] mops ${tasks.length} reports`)
  let pending = tasks
  for (let round = 1; round <= 2 && pending.length > 0; round++) {
    const failed: MopsTask[] = []
    for (const task of pending) {
      try {
        await task.run()
      } catch (err) {
        console.warn(`[backfill] ${task.label} round ${round} failed: ${String(err)}`)
        failed.push(task)
      }
    }
    pending = failed
    if (pending.length > 0 && round < 2) await delay(30_000)
  }
  console.log(
    `[backfill] mops margins=${marginsOf.size} revenue=${revenueOf.size}` +
      (pending.length > 0 ? ` — FAILED: ${pending.map((t) => t.label).join(' ')}` : '')
  )
  return pending.map((t) => t.label)
}

// ── the day walk ────────────────────────────────────────────────────────────
const days = tradingDays()
const accs = new Map<string, Acc>()
let sessions = 0
const sourceDays = {
  twse: 0,
  tpex: 0,
  twseFlow: 0,
  tpexFlow: 0,
  twseMargin: 0,
  tpexMargin: 0,
  twseForeign: 0,
  tpexForeign: 0,
  valuation: 0,
  tpexSbl: 0
}
/** Days whose market read STILL failed after the re-queue — a missing real
 *  session shifts every rolling column, so the run refuses to publish such
 *  histories (nonzero exit, nothing uploaded) instead of guessing. */
const failedDays: Array<{ market: 'TWSE' | 'TPEX'; dateMs: number; what: string }> = []
/** Idempotent per-(market, day) counters — the re-queue re-walks whole days. */
const counted = {
  twse: new Set<number>(),
  tpex: new Set<number>(),
  twseFlow: new Set<number>(),
  tpexFlow: new Set<number>(),
  twseMargin: new Set<number>(),
  tpexMargin: new Set<number>(),
  twseForeign: new Set<number>(),
  tpexForeign: new Set<number>(),
  valuation: new Set<number>(),
  tpexSbl: new Set<number>()
}

/** ONE market's per-date payloads for a session (every read optional). */
interface DayMarket {
  bars: Map<string, DayBar>
  flows: Map<string, InstSrc> | null
  margin: Map<string, MarginSrc> | null
  foreign: Map<string, ForeignHold> | null
  valuation: Map<string, ValuationRow> | null
  sbl: Map<string, SblSrc> | null
  /** 當沖 volume in shares (each exchange's own per-date report). */
  dayTrade: Map<string, number> | null
}

/** One session's walk: the per-date exchange tables → per-symbol acc rows.
 *  IDEMPOTENT per (symbol, stamp) — the re-queue re-runs whole days safely,
 *  inserting at the sorted position when later sessions already accumulated
 *  and patching still-null cells (e.g. flows that failed before) in place.
 *  A FAILED market read (never a holiday's empty table) lands in failedDays. */
async function processDay(dateMs: number, index: number, total: number): Promise<void> {
  const stamp = dayStampOf(dateMs)
  if (!stamp) return

  // An EMPTY map is "no session" too: TPEx answers a valid, empty table on a
  // holiday, so truthiness alone would count the day as a session.
  const twseRead = await readDay(`${stamp} TWSE bars`, () => twseDayBars(dateMs))
  if (twseRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'bars' })
  const tpexRead = await readDay(`${stamp} TPEx bars`, () => tpexDayBars(dateMs))
  if (tpexRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'bars' })
  const twseBars = twseRead.value && twseRead.value.size > 0 ? twseRead.value : null
  const tpexBars = tpexRead.value && tpexRead.value.size > 0 ? tpexRead.value : null
  if (!twseBars && !tpexBars) {
    if (index % 20 === 0) console.log(`[backfill] ${stamp} no session (skipped)`)
    return
  }

  const markets: Array<['TWSE' | 'TPEX', DayMarket | null]> = []
  if (twseBars) {
    const [flowsRead, marginRead, sblRead, foreignRead, valRead, dayTradeRead] = await Promise.all([
      readDay(`${stamp} TWSE flows`, () => twseInstitutional(dateMs)),
      readDay(`${stamp} TWSE margin`, () => twseMarginDay(dateMs).then((r) => r?.stocks ?? null)),
      readDaySoft(`${stamp} TWSE sbl`, () => twseDaySblBal(dateMs)),
      readDaySoft(`${stamp} TWSE foreign`, () => twseDayForeign(dateMs)),
      readDay(`${stamp} TWSE valuation`, () => twseDayValuation(dateMs)),
      readDaySoft(`${stamp} TWSE dayTrade`, () => twseDayTrade(dateMs))
    ])
    if (flowsRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'flows' })
    if (marginRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'margin' })
    if (sblRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'sbl' })
    if (foreignRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'foreign' })
    if (valRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'valuation' })
    if (dayTradeRead.failed) failedDays.push({ market: 'TWSE', dateMs, what: 'dayTrade' })
    markets.push([
      'TWSE',
      {
        bars: twseBars,
        flows: flowsRead.value && flowsRead.value.rows > 0 ? flowsRead.value.data : null,
        margin: marginRead.value,
        foreign: foreignRead.value,
        valuation: valRead.value,
        sbl: sblRead.value,
        dayTrade: dayTradeRead.value
      }
    ])
    if (!counted.twse.has(dateMs)) {
      counted.twse.add(dateMs)
      sourceDays.twse++
    }
  }
  if (tpexBars) {
    const [flowsRead, marginRead, foreignRead, valRead, sblRead, dayTradeRead] = await Promise.all([
      readDay(`${stamp} TPEx flows`, () => tpexDayFlows(dateMs)),
      readDay(`${stamp} TPEx margin`, () => tpexDayMargin(dateMs)),
      readDaySoft(`${stamp} TPEx foreign`, () => tpexDayForeign(dateMs)),
      readDay(`${stamp} TPEx valuation`, () => tpexDayValuation(dateMs)),
      readDay(`${stamp} TPEx sbl`, () => tpexDaySblBal(dateMs)),
      readDaySoft(`${stamp} TPEx dayTrade`, () => tpexDayTrade(dateMs))
    ])
    if (flowsRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'flows' })
    if (marginRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'margin' })
    if (foreignRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'foreign' })
    if (valRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'valuation' })
    if (sblRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'sbl' })
    if (dayTradeRead.failed) failedDays.push({ market: 'TPEX', dateMs, what: 'dayTrade' })
    markets.push([
      'TPEX',
      {
        bars: tpexBars,
        flows: flowsRead.value,
        margin: marginRead.value,
        foreign: foreignRead.value,
        valuation: valRead.value,
        sbl: sblRead.value,
        dayTrade: dayTradeRead.value
      }
    ])
    if (!counted.tpex.has(dateMs)) {
      counted.tpex.add(dateMs)
      sourceDays.tpex++
    }
  }

  let rows = 0
  for (const [marketName, day] of markets) {
    if (!day) continue
    if (day.flows && !counted[marketName === 'TWSE' ? 'twseFlow' : 'tpexFlow'].has(dateMs)) {
      counted[marketName === 'TWSE' ? 'twseFlow' : 'tpexFlow'].add(dateMs)
      if (marketName === 'TWSE') sourceDays.twseFlow++
      else sourceDays.tpexFlow++
    }
    if (day.margin) {
      const set = marketName === 'TWSE' ? counted.twseMargin : counted.tpexMargin
      if (!set.has(dateMs)) {
        set.add(dateMs)
        if (marketName === 'TWSE') sourceDays.twseMargin++
        else sourceDays.tpexMargin++
      }
    }
    if (day.foreign) {
      const set = marketName === 'TWSE' ? counted.twseForeign : counted.tpexForeign
      if (!set.has(dateMs)) {
        set.add(dateMs)
        if (marketName === 'TWSE') sourceDays.twseForeign++
        else sourceDays.tpexForeign++
      }
    }
    if (day.valuation && !counted.valuation.has(dateMs)) {
      counted.valuation.add(dateMs)
      sourceDays.valuation++
    }
    if (day.sbl && !counted.tpexSbl.has(dateMs)) {
      counted.tpexSbl.add(dateMs)
      sourceDays.tpexSbl++
    }

    for (const [symbol, bar] of day.bars) {
      if (writeSet && !writeSet.has(symbol)) continue
      const baseShares = universe.get(symbol)
      if (baseShares === undefined) continue
      const price = bar.close / 100
      const flow = day.flows?.get(symbol)
      // The history stores institutional flow in NT$ (shares × close), exactly
      // like `appendSession` does for a live session.
      const nt = (shares: number | undefined): number | null =>
        flow && shares !== undefined && Number.isFinite(shares) ? shares * price : null
      const shares = bar.shares > 0 ? bar.shares : baseShares
      const values: Record<ScreenerHistoryField, number | null> = {
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        tickets: bar.tickets,
        tradeValue: bar.tradeValue,
        turnoverRate: shares > 0 && bar.volume > 0 ? (bar.volume / shares) * 100 : null,
        foreign: nt(flow?.foreign),
        trust: nt(flow?.trust),
        dealer: nt(flow?.dealer),
        per: day.valuation?.get(symbol)?.per ?? null,
        pbr: day.valuation?.get(symbol)?.pbr ?? null,
        divYield: day.valuation?.get(symbol)?.divYield ?? null,
        marginBal: day.margin?.get(symbol)?.marginBal ?? null,
        marginQuota: day.margin?.get(symbol)?.quota ?? null,
        shortBal: day.margin?.get(symbol)?.shortBal ?? null,
        sblBal: day.sbl?.get(symbol)?.sblBal ?? null,
        riskFlags: null, // the risk lists publish no per-date history — daily accrual
        grossMargin: latestBefore(marginsOf.get(symbol), stamp)?.gross ?? null,
        opMargin: latestBefore(marginsOf.get(symbol), stamp)?.op ?? null,
        pretaxMargin: latestBefore(marginsOf.get(symbol), stamp)?.pretax ?? null,
        netMargin: latestBefore(marginsOf.get(symbol), stamp)?.net ?? null,
        revenueMom: latestBefore(revenueOf.get(symbol), stamp)?.mom ?? null,
        revenueYoy: latestBefore(revenueOf.get(symbol), stamp)?.yoy ?? null,
        revenueCumYoy: latestBefore(revenueOf.get(symbol), stamp)?.cumYoy ?? null,
        revenueMonth: latestBefore(revenueOf.get(symbol), stamp)?.revenueMonth ?? null,
        revenueYm: latestBefore(revenueOf.get(symbol), stamp)?.revenueYm ?? null,
        foreignShares: day.foreign?.get(symbol)?.shares ?? null,
        foreignRatio: day.foreign?.get(symbol)?.ratio ?? null,
        dayTradeShares: day.dayTrade ? (day.dayTrade.get(symbol) ?? 0) : null
      }
      let a = accs.get(symbol)
      if (!a) {
        a = acc()
        accs.set(symbol, a)
      }
      const cols = columns(a)
      const i = a.dates.indexOf(stamp)
      if (i >= 0) {
        // The re-queue may complete a day whose bars landed earlier — patch
        // every still-null cell instead of inserting a duplicate.
        for (const field of SCREENER_HISTORY_FIELDS) {
          const value = values[field]
          if (value !== null && cols[field][i] === null) cols[field][i] = value
        }
        continue
      }
      // A re-queued day can arrive after later sessions accumulated — insert
      // at the sorted position, never a blind push.
      const at = a.dates.findIndex((d) => d > stamp)
      const idx = at < 0 ? a.dates.length : at
      a.dates.splice(idx, 0, stamp)
      for (const field of SCREENER_HISTORY_FIELDS) cols[field].splice(idx, 0, values[field])
      rows++
    }
  }
  sessions++
  if (index % 10 === 0 || index === total - 1) {
    console.log(`[backfill] ${stamp} rows=${rows} sessions=${sessions}/${total}`)
  }
}

/** The per-day exchange walk — builds `accs` on the exchanges' OWN budget
 *  (polite gate + bandwidth); it never touches a third-party API. Runs
 *  CONCURRENTLY with `collectMops` (different host, same gate). */
async function walkDays(): Promise<void> {
  for (let i = 0; i < days.length; i++) await processDay(days[i], i, days.length)
  // One re-queue round: transient outages often clear within minutes — a
  // second walk over just the failed days catches them without a full re-run.
  // Dedupe per (market, day): several sources of one day share the re-walk.
  const retry = [...new Map(failedDays.map((f) => [`${f.market}:${f.dateMs}`, f])).values()]
  if (retry.length > 0) {
    console.log(`[backfill] re-queuing ${retry.length} failed day reads after a 60 s cool-down`)
    await delay(60_000)
    failedDays.splice(0)
    for (const failed of retry) await processDay(failed.dateMs, -1, days.length)
  }
  // Per-source coverage is the ONE line that exposes a silently failing reader
  // (a title-format change once dropped every TWSE session for ten months).
  console.log(
    `[backfill] coverage twse=${sourceDays.twse} tpex=${sourceDays.tpex} ` +
      `twseFlow=${sourceDays.twseFlow} tpexFlow=${sourceDays.tpexFlow} ` +
      `twseMargin=${sourceDays.twseMargin} tpexMargin=${sourceDays.tpexMargin} ` +
      `valuation=${sourceDays.valuation} ` +
      `twseForeign=${sourceDays.twseForeign} tpexForeign=${sourceDays.tpexForeign} ` +
      `tpexSbl=${sourceDays.tpexSbl} of ${sessions} sessions`
  )
}

// Both phases run at once: the MOPS host and the exchanges are different
// endpoints, and the polite gate serializes only the requests themselves.
const mopsTask = collectMops()
await walkDays()
const failedMops = await mopsTask

// THE SETTLED-OR-FAIL GATE: a missing real session (or a missing report) shifts
// every rolling column, so hole-y histories are never written, never uploaded.
if (failedDays.length > 0) {
  console.error(
    `[backfill] ${failedDays.length} day reads still failed after the re-queue (${failedDays
      .map((f) => `${f.market}:${dayStampOf(f.dateMs)}:${f.what}`)
      .join(' ')}) — refusing to publish hole-y histories`
  )
  process.exit(1)
}
if (failedMops.length > 0) {
  console.error(`[backfill] MOPS reports failed: ${failedMops.join(' ')} — refusing to publish`)
  process.exit(1)
}
// The write gate: every column must stay index-aligned on `dates` — a splice
// bug would corrupt every rolling derivation downstream, so refuse to write.
for (const [symbol, a] of accs) {
  const cols = columns(a)
  for (const field of SCREENER_HISTORY_FIELDS) {
    if (cols[field].length !== a.dates.length) {
      console.error(`[backfill] ${symbol} history column ${field} misaligned — aborting`)
      process.exit(1)
    }
  }
}

if (!process.argv.includes('--keep')) await resetSeedRoot()

if (sessions === 0) {
  console.error('[backfill] no sessions fetched — nothing written')
  process.exit(1)
}
if (sourceDays.twse === 0 || sourceDays.tpex === 0) {
  console.error('[backfill] a whole market produced no sessions — aborting')
  process.exit(1)
}
if (writeSet) {
  const missing = [...writeSet].filter((s) => !accs.has(s))
  if (missing.length > 0) {
    console.warn(`[backfill] requested symbols without rows: ${missing.join(' ')}`)
  }
}

let written = 0
for (const [symbol, a] of accs) {
  if (a.dates.length === 0) continue
  const drop = Math.max(0, a.dates.length - SCREENER_HISTORY_SESSIONS)
  const slice = <T>(list: T[]): T[] => (drop > 0 ? list.slice(drop) : list)
  const history: ScreenerHistory = {
    symbol,
    dates: slice(a.dates),
    open: slice(a.open),
    high: slice(a.high),
    low: slice(a.low),
    close: slice(a.close),
    volume: slice(a.volume),
    tickets: slice(a.tickets),
    tradeValue: slice(a.tradeValue),
    turnoverRate: slice(a.turnoverRate),
    foreign: slice(a.foreign),
    trust: slice(a.trust),
    dealer: slice(a.dealer),
    per: slice(a.per),
    pbr: slice(a.pbr),
    divYield: slice(a.divYield),
    marginBal: slice(a.marginBal),
    marginQuota: slice(a.marginQuota),
    shortBal: slice(a.shortBal),
    sblBal: slice(a.sblBal),
    riskFlags: slice(a.riskFlags),
    grossMargin: slice(a.grossMargin),
    opMargin: slice(a.opMargin),
    pretaxMargin: slice(a.pretaxMargin),
    netMargin: slice(a.netMargin),
    revenueMom: slice(a.revenueMom),
    revenueYoy: slice(a.revenueYoy),
    revenueCumYoy: slice(a.revenueCumYoy),
    revenueMonth: slice(a.revenueMonth),
    revenueYm: slice(a.revenueYm),
    foreignShares: slice(a.foreignShares),
    foreignRatio: slice(a.foreignRatio),
    dayTradeShares: slice(a.dayTradeShares)
  }
  await Bun.write(join(SEED_ROOT, 'history', `${symbol}.json`), JSON.stringify(history))
  written++
}
console.log(`[backfill] wrote ${written} histories under ${SEED_ROOT}`)

// The TAIEX series the header's benchmark returns read: one FMTQIK request per
// calendar month in the window.
const monthStarts: number[] = []
for (let ms = startMs; ms <= toMs; ms += DAY_MS) {
  const y = new Date(ms + 8 * 3_600_000).getUTCFullYear()
  const m = new Date(ms + 8 * 3_600_000).getUTCMonth()
  const first = Date.UTC(y, m, 1) - 8 * 3_600_000
  if (monthStarts.at(-1) !== first) monthStarts.push(first)
}
const indexRows = []
for (const monthMs of monthStarts) {
  indexRows.push(...(await twseMonthIndex(monthMs).catch(() => [])))
}
const series = mergeBenchmark(null, indexRows)
await Bun.write(join(SEED_ROOT, 'benchmark.json'), JSON.stringify(series))
console.log(
  `[backfill] benchmark ${series.dates.length} sessions (${series.dates[0]} -> ${series.dates.at(-1)})`
)

// History first, snapshot second: the ETL reads those histories, appends the
// live session, and re-derives every rolling column from them.
const started = Date.now()
const result = await runEtl(createFsEnv(), Date.now(), { force: true })
console.log(`[backfill] etl ${JSON.stringify(result)} in ${Date.now() - started} ms`)
if (result.skipped) {
  console.error('[backfill] the ETL skipped — the snapshot was not rebuilt')
  process.exit(1)
}

if (smoke) {
  console.log('[backfill] smoke filter active — not uploaded')
  process.exit(0)
}
if (dryRun) {
  console.log('[backfill] dry run — objects left in .seed-out/')
} else {
  const { files, failed } = await uploadSeedRoot()
  console.log(`[backfill] done: ${files - failed}/${files} uploaded, ${failed} failed`)
  if (failed > 0) process.exit(1)
}
