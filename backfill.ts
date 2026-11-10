/**
 * History backfill (run once, before the daily schedule takes over) — a pure
 * rebuild from the EXCHANGES' official DAILY per-date tables (TWSE + TPEx).
 * No third party, no MOPS monthly/quarterly scrape: a 3-year rebuild is the
 * weekday day walk only.
 *
 *   bun backfill.ts --months 36 --dry-run
 *   bun backfill.ts --from 2023-10-01 --to 2026-10-22
 *   bun backfill.ts --months 1 --symbols 2330,6488 --dry-run
 *
 * Flags: `--months N` (default 36, the absolute horizon) · `--from/--to` ·
 * `--symbols a,b,c` / `--sample N` (a SMOKE run: limited and NEVER uploaded) ·
 * `--repair` (an explicit symbol set is uploadable) · `--histories-only`
 * (upload histories + benchmark only, no snapshot) · `--gap-fill` (reverse
 * walk, stop at the first stored date) · `--api <base>` · `--dry-run` ·
 * `--keep`.
 *
 * Flow: universe (the live snapshot's market caps imply the share count) →
 * a REVERSE weekday walk over the exchanges' DAILY RWD tables only (TWSE +
 * TPEx; no MOPS monthly/quarterly fetch) → per-symbol column arrays → merge
 * with the stored histories → benchmark → (full mode) the SAME `runEtl` the
 * daily job runs, history first.
 *
 * Each exchange and each report is an independent RESOURCE: a late/absent
 * side or report writes null for that resource and never drops the bar
 * session, the other market or the other reports.
 *
 * Quarterly/monthly MOPS-derived history columns are NOT re-fetched here. The
 * merge carries the last published value forward across the repaired gap; the
 * daily seed then rebuilds every derived row column from the merged histories.
 */
import { join } from 'node:path'
import {
  SC,
  SCREENER_HISTORY_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  isScreenerHistory,
  type ScreenerHistory,
  type ScreenerHistoryField
} from '@odf/screener-worker/screener'
import { DAY_MS, dayStampOf, stampToMs } from '@odf/screener-worker/session'
import { mergeBenchmark, twseMonthIndex, type IndexRow } from './src/benchmark'
import { gapStop, latestClosedSessionMs, reverseWeekdays } from './src/backfillWindow'
import { mergeHistorySeries } from './src/historyMerge'
import { HISTORY_MANIFEST_KEY, mergeHistoryManifest } from './src/manifest'
import { historyKey } from './src/keys'
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
import { runEtl } from './src/etl'
import { classifyBarDay } from './src/safety'
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
/** Safety-guard mode: upload the rebuilt histories + benchmark only and let
 *  the following daily seed rebuild the snapshot. This avoids a second ETL
 *  publishing a partial grid after the daily run. */
const historiesOnly = process.argv.includes('--histories-only')
/** Reverse gap repair: start at the most recent closed weekday, bridge to the
 *  stored series, and stop the first time a date already exists. */
const gapFill = process.argv.includes('--gap-fill')
/** Repair mode: the symbol filter is a real write set, not a smoke run. Used
 *  by the CI safety guard to rebuild ONLY the histories it detected as absent
 *  or short; the backfill still walks official weekday tables, so a holiday's
 *  valid empty response is skipped and never becomes a zero session. */
const repair = process.argv.includes('--repair')
const apiBase = (arg('api') ?? DEFAULT_API).replace(/\/+$/, '')
const fromMs = arg('from') ? Date.parse(`${arg('from')}T00:00:00+08:00`) : 0
const toMs = arg('to') ? Date.parse(`${arg('to')}T00:00:00+08:00`) : Date.now()
const months = Math.max(1, Math.min(36, Number(arg('months') ?? 36) || 36))
const startMs = fromMs || toMs - Math.round(months * 30.44) * DAY_MS
/** The gap walk NEVER crosses the absolute 36-calendar-month horizon. */
const reverseFloor = gapFill ? Math.max(startMs, toMs - Math.round(36 * 30.44) * DAY_MS) : startMs
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

/** Retries one per-day exchange read ONCE with a short exponential backoff.
 *  A second failure returns `failed`; the caller skips that RESOURCE (null
 *  cells) rather than aborting the whole market/run. */
async function readDay<T>(name: string, read: () => Promise<T>): Promise<DayRead<T>> {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return { failed: false, value: await read() }
    } catch (err) {
      console.warn(`[backfill] ${name} attempt ${attempt}/2 failed: ${String(err)}`)
      if (attempt < 2) await delay(1_000 * 2 ** (attempt - 1))
    }
  }
  return { failed: true, value: null }
}

/** Listing-day epochs from the same snapshot (the reverse walk never probes
 *  before a symbol existed). */
const listedDates = new Map<string, number>()

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
        const listed = row[SC.listedDate]
        const listedMs = typeof listed === 'string' ? stampToMs(listed) : 0
        if (listedMs > 0) listedDates.set(symbol, listedMs)
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

/** The REVERSE weekday calendar. The per-date readers decide whether a day
 *  actually published, so a holiday simply answers an empty table and is
 *  skipped — no external calendar service is needed (or wanted). */
function reverseCalendar(): number[] {
  const latest = latestClosedSessionMs(toMs)
  const days = reverseWeekdays(latest, reverseFloor)
  console.log(
    `[backfill] reverse calendar ${days.length} weekdays from ${dayStampOf(latest)} to ${dayStampOf(reverseFloor)}`
  )
  return days
}

/**
 * The day walk and the final ETL overlap on the ANCHOR session: the walk builds
 * the per-symbol histories from the exchange's per-date tables, then `runEtl`
 * publishes the snapshot — from the SAME tables. ONE in-process response memo
 * makes that overlap FREE (the walk's last day is the ETL's anchor): keyed by
 * method + URL + request body, successful responses only, raw BYTES,
 * LRU-bounded so a long run never grows without limit.
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
    console.log(
      repair
        ? `[backfill] repair set — ${list.length} symbols from --symbols, uploaded`
        : `[backfill] smoke filter — ${list.length} symbols from --symbols, not uploaded`
    )
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
const smoke = writeSet !== null && !repair
if (repair && writeSet === null) {
  console.error('[backfill] --repair requires --symbols (an explicit repair set)')
  process.exit(1)
}
if (historiesOnly && smoke) {
  console.error('[backfill] --histories-only requires --repair (an uploadable repair set)')
  process.exit(1)
}
if (gapFill && writeSet === null) {
  console.error('[backfill] --gap-fill requires --symbols (an explicit repair set)')
  process.exit(1)
}

/** Reverse-gap state: the stored series for each target, its dates for the
 *  local stop, and the mutable active set the day walk records from. */
const existingHistories = new Map<string, ScreenerHistory>()
const existingDates = new Map<string, Set<string>>()
let activeRepair: Set<string> | null = null
let recordSet: Set<string> | null = writeSet
const EMPTY_DATES: ReadonlySet<string> = new Set()
if (gapFill && writeSet) {
  if (!process.argv.includes('--keep')) await resetSeedRoot()
  activeRepair = new Set(writeSet)
  recordSet = activeRepair
  const repairEnv = createFsEnv()
  for (const symbol of writeSet) {
    try {
      const obj = await repairEnv.R2_BUCKET.get(historyKey(symbol))
      if (!obj) continue
      const text = await obj.text()
      let parsed: unknown
      try {
        parsed = JSON.parse(text)
      } catch {
        console.warn(`[backfill] ${symbol} stored history unreadable — rebuilding from scratch`)
        continue
      }
      if (!isScreenerHistory(parsed, symbol)) {
        console.warn(`[backfill] ${symbol} stored history invalid — rebuilding from scratch`)
        continue
      }
      existingHistories.set(symbol, parsed)
      existingDates.set(symbol, new Set(parsed.dates))
    } catch (err) {
      // A storage read failure must never be mistaken for an absent history:
      // abort before any write rather than replace a good series with a rebuild.
      console.error(`[backfill] ${symbol} stored history read failed — aborting: ${String(err)}`)
      process.exit(1)
    }
  }
  console.log(
    `[backfill] gap target ${writeSet.size}; ${existingHistories.size} with stored history`
  )
}

// ── the day walk ────────────────────────────────────────────────────────────
const days = reverseCalendar()
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

/** One market's daily bars. A holiday/typhoon answers a VALID EMPTY table;
 *  a transient edge miss on a real session can look identical, so an empty
 *  result is read ONCE more after a short pause. Two empties = no session. */
async function readBarsDay(
  name: string,
  read: () => Promise<Map<string, DayBar> | null>
): Promise<DayRead<Map<string, DayBar> | null>> {
  const first = await readDay(name, read)
  if (first.failed || (first.value && first.value.size > 0)) return first
  await delay(2_000)
  return readDay(name, read)
}

/** One session's walk: the per-date exchange tables → per-symbol acc rows.
 *  IDEMPOTENT per (symbol, stamp) — the re-queue re-runs whole days safely,
 *  inserting at the sorted position when later sessions already accumulated
 *  and patching still-null cells (e.g. flows that failed before) in place.
 *  Each market/report is an independent resource: an absent side is skipped
 *  for that day, never a reason to discard the session. */
async function processDay(dateMs: number, index: number, total: number): Promise<void> {
  const stamp = dayStampOf(dateMs)
  if (!stamp) return

  // An EMPTY map is "no session" too: TPEx answers a valid, empty table on a
  // holiday, so truthiness alone would count the day as a session.
  const twseRead = await readBarsDay(`${stamp} TWSE bars`, () => twseDayBars(dateMs))
  const tpexRead = await readBarsDay(`${stamp} TPEx bars`, () => tpexDayBars(dateMs))
  const twseBars = twseRead.value && twseRead.value.size > 0 ? twseRead.value : null
  const tpexBars = tpexRead.value && tpexRead.value.size > 0 ? tpexRead.value : null
  const kind = classifyBarDay(!!twseBars, !!tpexBars)
  if (kind === 'closed') {
    if (twseRead.failed || tpexRead.failed) {
      console.log(`[backfill] ${stamp} both bar resources unavailable — skipped`)
    } else if (index % 20 === 0) {
      console.log(`[backfill] ${stamp} no session (skipped)`)
    }
    return
  }
  // Each exchange is its OWN resource: a missing/empty side is skipped for
  // this day, never allowed to drop the other side's session. The exchanges
  // publish separate RWD tables, so one market's late/absent file is normal.
  if (kind === 'twse-missing') console.log(`[backfill] ${stamp} TWSE bars empty — skipped`)
  if (kind === 'tpex-missing') console.log(`[backfill] ${stamp} TPEx bars empty — skipped`)

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
    // Resource-wise: a missing report writes null cells for that resource; it
    // never discards the bar session or the other resources that DID answer.
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
    // Resource-wise: use every report that answered; leave absent ones null.
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
      if (recordSet && !recordSet.has(symbol)) continue
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
        grossMargin: null,
        opMargin: null,
        pretaxMargin: null,
        netMargin: null,
        revenueMom: null,
        revenueYoy: null,
        revenueCumYoy: null,
        revenueMonth: null,
        revenueYm: null,
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

const bridged = new Set<string>()

/** The per-day exchange walk — builds `accs` on the exchanges' OWN budget
 *  (polite gate + bandwidth); it never touches a third-party API. */
async function walkDays(): Promise<void> {
  for (let i = 0; i < days.length; i++) {
    const dateMs = days[i]
    const stamp = dayStampOf(dateMs)
    if (activeRepair) {
      // LOCAL CHECK FIRST: a date already in the stored series bridges the
      // contiguous gap for that symbol — never fetch/record it again.
      for (const symbol of [...activeRepair]) {
        const action = gapStop(stamp, dateMs, {
          existingDates: existingDates.get(symbol) ?? EMPTY_DATES,
          listedMs: listedDates.get(symbol) ?? 0,
          collected: sessions,
          quota: existingHistories.has(symbol) ? 0 : SCREENER_HISTORY_SESSIONS
        })
        if (action === 'continue') continue
        activeRepair.delete(symbol)
        if (action === 'bridged') bridged.add(symbol)
        if (action !== 'bridged') {
          console.log(`[backfill] ${symbol} ${action} at ${stamp} — stopping its scan`)
        }
      }
      if (activeRepair.size === 0) {
        console.log(`[backfill] all gap targets stopped at ${stamp}`)
        break
      }
    }
    await processDay(dateMs, i, days.length)
  }
  // A stored series MUST reach its own last date before the horizon. Otherwise
  // writing the collected prefix would create a hole between it and the old
  // series — refuse the whole repair instead.
  if (gapFill) {
    const unbridged = [...existingHistories.keys()].filter((s) => !bridged.has(s))
    if (unbridged.length > 0) {
      console.error(
        `[backfill] horizon reached before bridging stored histories: ${unbridged.join(' ')} — aborting`
      )
      process.exit(1)
    }
    if (sessions === 0) {
      console.log('[backfill] no missing trading session — nothing to write')
      process.exit(0)
    }
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

await walkDays()

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

if (sessions === 0 && !gapFill) {
  console.error('[backfill] no sessions fetched — nothing written')
  process.exit(1)
}
if (writeSet && !gapFill) {
  const missing = [...writeSet].filter((s) => !accs.has(s))
  if (missing.length > 0) {
    console.warn(`[backfill] requested symbols without rows: ${missing.join(' ')}`)
  }
}

function historyFromAcc(symbol: string, a: Acc): ScreenerHistory {
  const drop = Math.max(0, a.dates.length - SCREENER_HISTORY_SESSIONS)
  const slice = <T>(list: T[]): T[] => (drop > 0 ? list.slice(drop) : list)
  return {
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
}

const toWrite: ScreenerHistory[] = []
if (gapFill && writeSet) {
  for (const symbol of writeSet) {
    const a = accs.get(symbol)
    if (!a || a.dates.length === 0) {
      if (!existingHistories.has(symbol)) console.warn(`[backfill] ${symbol} produced no rows`)
      continue
    }
    const next = historyFromAcc(symbol, a)
    const merged = mergeHistorySeries(existingHistories.get(symbol) ?? null, next)
    if (merged) toWrite.push(merged)
  }
} else {
  for (const [symbol, a] of accs) {
    if (a.dates.length === 0) continue
    toWrite.push(historyFromAcc(symbol, a))
  }
}

let written = 0
for (const history of toWrite) {
  await Bun.write(join(SEED_ROOT, 'history', `${history.symbol}.json`), JSON.stringify(history))
  written++
}
console.log(`[backfill] wrote ${written} histories under ${SEED_ROOT}`)

// The safety guard's ONE index: the repaired targets' newest session. The
// uploader merges this partial map into the bucket's existing manifest, so a
// gap repair never erases entries for symbols it did not touch.
const manifestUpdates = new Map<string, string>()
for (const history of toWrite) {
  const last = history.dates.at(-1)
  if (last) manifestUpdates.set(history.symbol, last)
}
if (manifestUpdates.size > 0) {
  await Bun.write(
    join(SEED_ROOT, HISTORY_MANIFEST_KEY),
    JSON.stringify(mergeHistoryManifest(null, manifestUpdates))
  )
}

// The TAIEX series the header's benchmark returns read: one FMTQIK request per
// calendar month in the window.
const monthStarts: number[] = []
for (let ms = startMs; ms <= toMs; ms += DAY_MS) {
  const y = new Date(ms + 8 * 3_600_000).getUTCFullYear()
  const m = new Date(ms + 8 * 3_600_000).getUTCMonth()
  const first = Date.UTC(y, m, 1) - 8 * 3_600_000
  if (monthStarts.at(-1) !== first) monthStarts.push(first)
}
const indexRows: IndexRow[] = []
let benchmarkFailed = false
for (const monthMs of monthStarts) {
  try {
    const rows = await twseMonthIndex(monthMs)
    if (rows.length === 0) throw new Error('empty monthly table')
    indexRows.push(...rows)
  } catch (err) {
    benchmarkFailed = true
    console.error(`[backfill] benchmark month ${dayStampOf(monthMs)} failed: ${String(err)}`)
  }
}
if (benchmarkFailed && !smoke && !dryRun) {
  console.error('[backfill] benchmark incomplete — refusing to write/upload a partial series')
  process.exit(1)
}
const series = mergeBenchmark(null, indexRows)
if (series.dates.length < 240 && !smoke && !dryRun) {
  console.error(`[backfill] benchmark has only ${series.dates.length} sessions (< 240) — aborting`)
  process.exit(1)
}
await Bun.write(join(SEED_ROOT, 'benchmark.json'), JSON.stringify(series))
console.log(
  `[backfill] benchmark ${series.dates.length} sessions (${series.dates[0]} -> ${series.dates.at(-1)})`
)

// The safety guard writes only the repaired histories + benchmark. The daily
// seed follows and rebuilds the snapshot from these objects, so a partial
// second ETL cannot overwrite a complete daily grid.
if (historiesOnly) {
  if (dryRun) {
    console.log('[backfill] histories-only dry run — objects left in .seed-out/')
    process.exit(0)
  }
  const { files, failed } = await uploadSeedRoot()
  console.log(
    `[backfill] histories-only done: ${files - failed}/${files} uploaded, ${failed} failed`
  )
  process.exit(failed > 0 ? 1 : 0)
}

// History first, snapshot second: the ETL reads those histories, appends the
// live session, and re-derives every rolling column from them.
const started = Date.now()
const result = await runEtl(createFsEnv(), Date.now(), { force: true })
console.log(`[backfill] etl ${JSON.stringify(result)} in ${Date.now() - started} ms`)
if (result.skipped) {
  console.error('[backfill] the ETL skipped — the snapshot was not rebuilt')
  process.exit(1)
}
if (result.partial.length > 0 && !dryRun) {
  console.error(
    `[backfill] the ETL is partial (${result.partial.join(',')}) — refusing to upload a partial rebuild`
  )
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
