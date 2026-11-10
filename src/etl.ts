/**
 * The daily screener ETL — the hub's SQLite pull ported to R2.
 *
 * Flow: read the previous snapshot (freshness) → DISCOVER the anchor from the
 * exchanges' per-date rwd tables (the OpenAPI mirrors regenerate the next
 * morning; they are only dated fallbacks) → fetch every official source
 * (per-date first, mirror second; a failure leaves its columns null, never a
 * previous session's value) → filter to the stock universe (`^[1-9][0-9]{3}$`) →
 * per symbol, append today's session to ITS 780-session history object and
 * derive the 73-column row from the rolling window → PUT the histories, then
 * the snapshot LAST (a half-written run never publishes a grid whose history
 * is missing).
 *
 * There is no database: `history/{symbol}.json` IS the rolling store, and the
 * snapshot is a pure projection of it plus today's source payloads.
 */
import {
  MARKETS,
  SC,
  SCREENER_FIELDS,
  SCREENER_CREDIT_STATE_KEY,
  SCREENER_HISTORY_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  SCREENER_MARKET_KEY,
  SCREENER_SNAPSHOT_KEY,
  instDeltaRatio,
  isScreenerHistory,
  mergeMarketContext,
  mergeMarketHistory,
  readMarketContext,
  returnOver,
  screenNum,
  type DividendsObject,
  type MarketCredit,
  type MarketDerivatives,
  type ScreenerHistory,
  type ScreenerHistoryField,
  type MarketContext,
  type ScreenerRow,
  type ScreenerSnapshot
} from '@odf/shared/screener'
import {
  estimatedMaintenanceRatio,
  MARGIN_COST_STATE_VERSION,
  rollAverageMarginCost,
  sanitizeCreditState,
  type CreditStateFile
} from '@odf/shared/credit'
import {
  DAY_MS,
  STOCK_CLOSE_TOD_MS,
  TAIPEI_OFFSET_MS,
  dayStampOf,
  stampToMs,
  todOf
} from '@odf/shared/session'
import { isFundSymbol } from '@odf/shared/screener'
import type { ScreenerEnv } from './env'
import {
  tpexDayFlows,
  tpexDayForeign,
  tpexDayTrade,
  tpexDayMargin,
  tpexDayQuotes,
  tpexDaySblBal,
  tpexDayValuation,
  twseDayForeign,
  twseDayQuotes,
  twseDayValuation,
  twseMarginDay,
  twseDaySblBal
} from './history'
import { isScreenerSymbol } from './universe'
import { exDivInDays, mergeDividends, readDividends } from './dividends'
import { readHoldings } from './holdings'
import {
  twseDaily,
  twseMaintenanceRatio,
  twseDayTrade,
  twseInstitutional,
  twseUpcomingDividends,
  twseRatios,
  twseRevenue,
  twseRiskFlags,
  twseUniverse,
  twseValuation
} from './sources/twse'
import {
  tpexCreditSummary,
  tpexDaily,
  tpexInstitutional,
  tpexMargin,
  tpexRatios,
  tpexRevenue,
  tpexRiskFlags,
  tpexSbl,
  tpexUniverse,
  tpexValuation
} from './sources/tpex'
import {
  BENCHMARK_KEY,
  benchmarkReturns,
  mergeBenchmark,
  shapedBenchmark,
  twseMonthIndex
} from './benchmark'
import { DATA_CACHE_CONTROL, DIVIDENDS_KEY, historyKey } from './keys'
import { taifexDerivatives } from './sources/taifex'
import type {
  CreditSummary,
  DailyRow,
  ForeignHold,
  MaintenancePoint,
  InstSrc,
  MarginSrc,
  RatioRow,
  RevenueRow,
  SblSrc,
  Sourced,
  TpexCreditSummary,
  UniverseRow,
  ValuationRow
} from './sources/types'

type Market = (typeof MARKETS)[number]

/** The after-hours publish boundary: before the session's own next day at
 *  15:00 Taipei the exchanges cannot have published a newer session. */
const ROLL_TOD_MS = 15 * 3_600_000
/** A market-wide daily table must hold at least this many stock rows — a
 *  truncated payload (~430 rows once) is a broken table, not a quiet day: the
 *  market falls back to the carried rows exactly like a stale session. */
const TWSE_DAILY_MIN_ROWS = 900
const TPEX_DAILY_MIN_ROWS = 700
/** A listing universe below these row counts is a truncated payload, not a
 *  quiet market. Keep the daily rows (their own identity/name) instead of
 *  rejecting every symbol the truncated universe omits. The counts sit below
 *  the normal universes (~1 300 TWSE / ~800 TPEx) and above a partial page. */
const TWSE_UNIVERSE_MIN_ROWS = 900
const TPEX_UNIVERSE_MIN_ROWS = 500
/** The 營益分析 mirror is regenerated on every filing batch, so its 出表日期 is
 *  (near) the run day — a stamp further than this from the anchor means the
 *  feed stopped and the values may predate a quarterly publish. */
const RATIOS_MAX_SKEW_MS = 30 * DAY_MS
/** A session dated TODAY is only FINAL this long after the stock close — the
 *  exchanges' "latest" files refresh intraday (TPEx reports today's date at
 *  noon), and writing a mid-session anchor would stamp a partial day into
 *  history. 14:00 Taipei = 13:30 close + the EOD publish lag. */
const SESSION_FINAL_TOD_MS = STOCK_CLOSE_TOD_MS + 30 * 60_000
/** The most missed sessions a daily run repairs from the per-date tables —
 *  a longer outage needs the 3-year backfill (logged + `partial`). */
const CATCHUP_MAX = 10
/** The evening catch-up boundary (Taipei): a same-session run after this hour
 *  is a REPAIR pass, not the morning guard. It re-reads every source so a
 *  source that published late (day-trade, credit, TAIFEX, a regenerated file)
 *  is merged, while the byte guards keep every unchanged write a no-op. */
const LATE_REPAIR_TOD_MS = 20 * 3_600_000
/** The next-morning guard (Taipei 03:45) that repairs a still-late session. */
const MORNING_GUARD_TOD_MS = 3 * 3_600_000 + 45 * 60_000
/** Weekdays the anchor discovery walks back from today (a long weekend plus a
 *  holiday). The first date ANY market answers is the anchor. */
const PROBE_MAX_DAYS = 5
/** Concurrent R2 get+put pair per symbol. Latency-bound from CI (a runner is
 *  ~0.5–1 s from R2), so 48 halves the per-symbol phase's wall time; the
 *  exchange HTTP reads are polite-gated upstream and unaffected. */
const R2_CONCURRENCY = 48

/** A plain backoff sleep — shared by every retry path in the ETL. */
function delay(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>()
  setTimeout(resolve, ms)
  return promise
}

/** Retries a storage/network call; the LAST attempt's error propagates, so the
 *  caller decides failure-vs-absence (absence is a plain null, never a throw). */
async function withRetry<T>(name: string, attempts: number, read: () => Promise<T>): Promise<T> {
  let err: unknown
  for (let attempt = 1; ; attempt++) {
    try {
      return await read()
    } catch (e) {
      err = e
      if (attempt >= attempts) break
      console.log(`[etl] ${name} attempt ${attempt}/${attempts} failed: ${String(e)}`)
      await delay(750 * attempt)
    }
  }
  throw err
}

export interface EtlResult {
  sessionDate: number
  symbols: number
  skipped: boolean
  /** Sources that answered CLEANLY with "not this session yet" (a holiday, a
   *  file not regenerated, an archive still serving the previous session).
   *  Retrying seconds later cannot help — the LATER cron slot repairs them, so
   *  this list never triggers the forced retry (only the exit code). */
  late: string[]
  /** Factors whose read FAILED (after its own retry) or whose data was
   *  rejected (`history-put`, a stale mirror, ...). A non-empty list makes the
   *  caller retry ONCE with `--force` and then exit nonzero. */
  partial: string[]
}

/** The trading weekdays strictly between two sessions — a holiday answers no
 *  session in the per-date readers and simply drops out of the walk. */
export function missingSessions(prevMs: number, anchorMs: number): number[] {
  const out: number[] = []
  for (let ms = prevMs + DAY_MS; prevMs > 0 && ms < anchorMs; ms += DAY_MS) {
    const weekday = new Date(ms + 8 * 3_600_000).getUTCDay()
    if (weekday !== 0 && weekday !== 6) out.push(ms)
  }
  return out
}

/** ONE anchor seed plus the sessions it missed — the history is appended in
 *  chronological order in ONE read/write pair per symbol. */
interface SeedChain {
  seed: Seed
  gap: Array<{ stamp: string; seed: Seed }>
}

export async function runEtl(
  env: ScreenerEnv,
  nowMs: number,
  opts: { force?: boolean } = {}
): Promise<EtlResult> {
  const force = opts.force === true
  /** Degraded factors — ONE token each, reported through `EtlResult.partial`
   *  (the caller retries once and exits nonzero when they survive). */
  const degraded = new Set<string>()
  const late = new Set<string>()
  const { snap: prev, text: prevText } = await readSnapshot(env)
  // The evening catch-up is a REPAIR pass on the same session: the stock grid
  // is current, but sources that published after the first run (day-trade,
  // credit, TAIFEX, a regenerated table) must still be merged. Treat it as a
  // local force of the same session only — never a payload-behind override.
  const repair = prev !== null && lateRepairWindow(nowMs, prev.sessionDate)
  if (repair) console.log('[etl] evening repair window — re-reading every source')

  // Fast path — no request at all: the stored session is the latest the
  // exchanges can have published (its next day's 15:00 has not passed yet).
  if (!force && !repair && prev && nowMs < prev.sessionDate + DAY_MS + ROLL_TOD_MS) {
    return finish(
      prev.sessionDate,
      prev.rows.length,
      true,
      'cached session still current',
      degraded,
      late
    )
  }

  // rwd FIRST. The OpenAPI mirrors are NOT the session of record: they
  // regenerate ~05:20 Taipei the NEXT morning (measured 2026-11-05 02:00:
  // STOCK_DAY_ALL/ BWIBBU_ALL still held 11-03 while MI_INDEX?date=11-04
  // answered), so a mirror-based anchor would publish the wrong session or
  // skip the evening entirely. The walk starts at today and steps back to the
  // newest weekday an exchange's own per-date table answers; a mirror only
  // supplies a date hint (and the fallback payload for a market whose table is
  // not out yet).
  const [mirrorTwse, mirrorTpex] = await Promise.all([
    tryRead('twse.daily', () => twseDaily()),
    tryRead('tpex.daily', () => tpexDaily())
  ])
  const probed = await probeAnchor(nowMs)
  const sessionDate = Math.max(probed.dateMs, mirrorTwse?.dateMs ?? 0, mirrorTpex?.dateMs ?? 0)
  if (sessionDate === 0) {
    return finish(0, 0, true, 'no session date in any daily payload', degraded, late)
  }
  console.log(
    `[etl] anchor ${dayStampOf(sessionDate)} — per-date=${stampOrDash(probed.dateMs)}` +
      ` mirrors twse=${stampOrDash(mirrorTwse?.dateMs)} tpex=${stampOrDash(mirrorTpex?.dateMs)}`
  )
  const twseDay = await anchorDaily(
    'TWSE',
    mirrorTwse,
    sessionDate,
    TWSE_DAILY_MIN_ROWS,
    probed.twse,
    twseDayQuotes,
    degraded,
    late
  )
  const tpexDay = await anchorDaily(
    'TPEX',
    mirrorTpex,
    sessionDate,
    TPEX_DAILY_MIN_ROWS,
    probed.tpex,
    tpexDayQuotes,
    degraded,
    late
  )
  // A session dated TODAY is not publishable before it has CLOSED: the
  // exchanges' latest files refresh intraday, and a noon run would stamp a
  // partial day (TWSE would even be carried from yesterday, mixing sessions).
  if (
    !force &&
    dayStampOf(nowMs) === dayStampOf(sessionDate) &&
    todOf(nowMs) < SESSION_FINAL_TOD_MS
  ) {
    return finish(
      sessionDate,
      prev?.rows.length ?? 0,
      true,
      'session not final yet',
      degraded,
      late
    )
  }
  // The published session must never move BACKWARDS. The exchange CDNs flap:
  // minutes after serving the anchor, one mirror answered the PREVIOUS session,
  // and a `--force` re-run then replaced a freshly published 10/30 with 10/29.
  // A re-run repairs the SAME session; it never rewinds (and this holds for
  // `force` too — the flap is the payloads' fault, not the operator's).
  if (prev && sessionDate < prev.sessionDate) {
    return finish(
      prev.sessionDate,
      prev.rows.length,
      true,
      `payloads behind the stored session (${dayStampOf(sessionDate)}) — skip`,
      degraded,
      late
    )
  }
  // `force` bypasses the remaining freshness gate — the workflow's partial
  // retry and the backfill's same-stamp re-run exist precisely to redo today's
  // session. The evening repair bypasses ONLY this same-session gate.
  if (!force && !repair && prev && sessionDate <= prev.sessionDate) {
    return finish(sessionDate, prev.rows.length, true, 'no new session, skip', degraded, late)
  }

  // Wave 2 — every other source. The polite gate serializes the HTTP calls, so
  // a single Promise.all is the whole parallelism that exists.
  // The VALUATION mirrors are NOT read here: a per-date-first source fetches
  // its mirror lazily (`perDateFirst`) only when the exchange's own table has
  // no answer — a normal evening reads each dataset exactly ONCE.
  const [twseUni, tpexUni, twseRat, tpexRat, twseRev, tpexRev] = await Promise.all([
    readSafe('twse.universe', degraded, late, () => twseUniverse()),
    readSafe('tpex.universe', degraded, late, () => tpexUniverse()),
    readSafe('twse.ratios', degraded, late, () => twseRatios()),
    readSafe('tpex.ratios', degraded, late, () => tpexRatios()),
    readSafe('twse.revenue', degraded, late, () => twseRevenue()),
    readSafe('tpex.revenue', degraded, late, () => tpexRevenue())
  ])
  // TWSE's margin OpenAPI mirror carries NO session date — it is never used
  // (the per-date table is the only honest source; a stale mirror stamped onto
  // the anchor is the bug the 03:45 guard had to repair). TPEx's dated mirrors
  // (margin / SBL / institutional) are the per-date readers' LAZY fallback, so
  // the normal evening never fetches them twice.
  const [twseRisk, tpexRisk, twseInst] = await Promise.all([
    readSafe('twse.risk', degraded, late, () => twseRiskFlags(sessionDate)),
    readSafe('tpex.risk', degraded, late, () => tpexRiskFlags()),
    readSafe('twse.inst', degraded, late, () => twseInstitutional(sessionDate))
  ])
  // 外資持股 has no OpenAPI mirror at all — the exchanges' per-date reports ARE
  // the daily source (they answer the anchor session as promptly as the rest).
  const [twseForeign, tpexForeign, twseDayTrades, tpexDayTrades] = await Promise.all([
    readSafe('twse.foreign', degraded, late, () => twseDayForeign(sessionDate)),
    readSafe('tpex.foreign', degraded, late, () => tpexDayForeign(sessionDate)),
    // 當沖: each exchange's own per-session report (TWSE `TWTB4U` /
    // TPEx `intraday/stat`, both per-stock).
    // A null day-trade answer is retried once: both exchanges' archives are
    // complete per session, so an empty payload is an edge miss, not a gap.
    readSafe('twse.dayTrade', degraded, late, () => dayTradeRetry(() => twseDayTrade(sessionDate))),
    readSafe('tpex.dayTrade', degraded, late, () => dayTradeRetry(() => tpexDayTrade(sessionDate)))
  ])

  // Market-wide context is AUXILIARY: one retry, then a log — a failed read
  // leaves the section absent and never degrades the equity grid's publish.
  // The TWSE margin read rides the same wave for latency but is NOT auxiliary:
  // it also feeds the per-stock columns, so its failure is a degraded source.
  const [tpexSummary, maintenance, derivatives, twseMarginNow] = await Promise.all([
    auxiliary('TPEx credit summary', () => tpexCreditSummary(sessionDate)),
    auxiliary('market maintenance ratio', () => twseMaintenanceRatio(sessionDate)),
    auxiliary('taifex context', () => taifexDerivatives()),
    // ONE MI_MARGN response feeds BOTH halves of the 上市 credit report: the
    // market summary (whole-market lots + money rows) and the per-stock margin
    // columns. They used to be two readers fetching the same payload.
    readSafe('twse.margin', degraded, late, () => twseMarginDay(sessionDate))
  ])

  // A fundamental mirror that answers a FOREIGN session is worse than absent:
  // a frozen `BWIBBU_ALL`/`t187ap17` once entered history as the anchor's
  // value. Every per-date-first source reads the exchange's own table for the
  // anchor and falls back to a mirror ONLY when that mirror's own date is the
  // anchor; a periodicity sanity covers the undated quarterly/monthly mirrors.
  const [twseValNow, tpexValNow, tpexMarNow, tpexInstNow, tpexSblNow, twseSblNow] =
    await Promise.all([
      perDateFirst(
        'twse.valuation',
        () => readSafe('twse.valuation', degraded, late, () => twseValuation()),
        sessionDate,
        twseDayValuation,
        degraded,
        late
      ),
      perDateFirst(
        'tpex.valuation',
        () => readSafe('tpex.valuation', degraded, late, () => tpexValuation()),
        sessionDate,
        tpexDayValuation,
        degraded,
        late
      ),
      perDateFirst(
        'tpex.margin',
        () => readSafe('tpex.margin', degraded, late, () => tpexMargin()),
        sessionDate,
        tpexDayMargin,
        degraded,
        late
      ),
      perDateFirst(
        'tpex.inst',
        () => readSafe('tpex.inst', degraded, late, () => tpexInstitutional()),
        sessionDate,
        tpexDayFlows,
        degraded,
        late
      ),
      perDateFirst(
        'tpex.sbl',
        () => readSafe('tpex.sbl', degraded, late, () => tpexSbl()),
        sessionDate,
        tpexDaySblBal,
        degraded,
        late
      ),
      // TWSE's 借券賣出 balance has its own DATED report (`TWT93U`, 2007+) —
      // the undated `SBL/TWT96U` (可借券賣出股數, a different quantity) is gone.
      perDateFirst('twse.sbl', null, sessionDate, twseDaySblBal, degraded, late)
    ])

  // The 上市 summary and the per-stock margin columns come from ONE response.
  const creditSummary = twseMarginNow?.summary ?? null
  const tpexTotals = tpexMarNow ? marginTotalsOf(tpexMarNow.data) : null

  const sources: Record<Market, MarketSources> = {
    TWSE: {
      daily: twseDay,
      universe: twseUni,
      valuation: twseValNow,
      ratios: saneRatios('twse.ratios', twseRat, sessionDate, late),
      revenue: saneRevenue('twse.revenue', twseRev, sessionDate, late),
      margin: twseMarginNow?.stocks
        ? { dateMs: sessionDate, rows: twseMarginNow.stocks.size, data: twseMarginNow.stocks }
        : null,
      sbl: twseSblNow,
      risk: twseRisk,
      inst: anchorOnly('twse.inst', twseInst, sessionDate, late),
      foreign: twseForeign,
      dayTrade: twseDayTrades
    },
    TPEX: {
      daily: tpexDay,
      universe: tpexUni,
      valuation: tpexValNow,
      ratios: saneRatios('tpex.ratios', tpexRat, sessionDate, late),
      revenue: saneRevenue('tpex.revenue', tpexRev, sessionDate, late),
      margin: tpexMarNow,
      sbl: tpexSblNow,
      risk: tpexRisk,
      inst: tpexInstNow,
      foreign: tpexForeign,
      dayTrade: tpexDayTrades
    }
  }
  // A universe read can answer a valid-shaped but truncated page. It is only
  // the identity/name gate — fall back to the daily table's own identity for
  // every row instead of dropping the symbols the truncated page omitted.
  for (const market of MARKETS) {
    const src = sources[market]
    const min = market === 'TWSE' ? TWSE_UNIVERSE_MIN_ROWS : TPEX_UNIVERSE_MIN_ROWS
    if (src.universe && src.universe.data.size < min) {
      console.log(
        `[etl] ${market} universe truncated (${src.universe.data.size} rows < ${min}) — ` +
          'daily identity fallback'
      )
      src.universe = null
      degraded.add(`universe:${market.toLowerCase()}`)
    }
  }
  if (late.size > 0) console.log(`[etl] sources late: ${[...late].join(' ')}`)
  if (degraded.size > 0) console.log(`[etl] sources failed: ${[...degraded].join(' ')}`)

  const stamp = dayStampOf(sessionDate)
  // A market whose daily payload is an EARLIER session (the exchange has not
  // published today yet) must not be stamped with the anchor date: its prices
  // would enter history as today's. Treat it exactly like a failed read — and
  // so does a table that IS the anchor but lost most of its rows (a silently
  // truncated payload once dropped ~430 symbols with an `ok` exit).
  // The weekly TDCC object — ABSENT is normal (the weekly job may not have run
  // yet), so it is not a partial token: the cell simply stays null.
  const stored = await readHoldings(env).catch((err: unknown) => {
    console.log(`[etl] holdings unreadable: ${String(err).slice(0, 120)}`)
    return null
  })
  // A weekly object older than three publish cycles means the weekly job has
  // stopped: join nothing rather than present a quarter-old 大戶 figure as
  // current (the column reads `–` until the job heals).
  const latestWeek = stored?.weeks[0] ?? null
  const holdings =
    latestWeek && sessionDate - stampToMs(latestWeek.week) <= 21 * DAY_MS ? stored : null
  if (stored && !holdings) {
    console.log(
      `[etl] holdings stale (week ${latestWeek?.week ?? '–'}) — the TDCC columns stay null`
    )
  }
  /** The 4-weeks-ago week (the 大戶 Δ's base) — null until the series is deep. */
  const holdings4w = holdings?.weeks[4] ?? null
  // The ex-dividend calendar: fold this run's 3-week window into the stored
  // object (a failure keeps the stored one — an event column must never fail
  // the run).
  const calendar = await refreshDividends(env, sessionDate)
  const stale: Market[] = []
  const seeds: Seed[] = []
  for (const market of MARKETS) {
    const src = sources[market]
    if (!src.daily) {
      // No daily at all is a carried market too: its prior rows are merged
      // into the grid, so its rows must never enter the anchor's credit sums.
      console.log(`[etl] ${market} daily unavailable — carried, credit excluded`)
      stale.push(market)
      degraded.add(`carried:${market.toLowerCase()}`)
      continue
    }
    const minRows = market === 'TWSE' ? TWSE_DAILY_MIN_ROWS : TPEX_DAILY_MIN_ROWS
    if (src.daily.dateMs !== sessionDate || src.daily.rows < minRows) {
      if (src.daily.dateMs === sessionDate) {
        console.log(
          `[etl] ${market} daily holds only ${src.daily.rows} rows (< ${minRows}) — carried`
        )
      }
      stale.push(market)
      // A carried market is a DEGRADED publish (its rows keep the previous
      // session while the rest of the grid is fresh) — the token makes the
      // workflow retry it and stay red if the market is still behind.
      degraded.add(`carried:${market.toLowerCase()}`)
      continue
    }
    let kept = 0
    let funds = 0
    let rejectedShape = 0
    let rejectedUniverse = 0
    for (const [symbol, daily] of src.daily.data) {
      // Warrants (`03`–`08`+6 digits on TWSE, `7xxxxx` on TPEx), 特別股
      // (`2881A`, `2887Z1`), TDRs (`91xxxx`) and indices match neither shape;
      // 興櫃 names never reach these two mainboard tables at all.
      if (!isScreenerSymbol(symbol)) {
        rejectedShape++
        continue
      }
      // When the universe answered, it is the gate (it also drops symbols the
      // daily table lists but the exchange does not call ordinary shares).
      // A failed universe read falls back to the daily row's own identity.
      // A FUND (ETF/ETN/REIT) has no company record in the listing table —
      // the daily row is its identity (par/paidIn stay null, so its
      // turnoverRate/marketCap render `–`). A STOCK the universe does not
      // list is a suspended/renamed name and stays out.
      const info = src.universe?.data.get(symbol) ?? null
      if (src.universe && !info && !isFundSymbol(symbol)) {
        rejectedUniverse++
        continue
      }
      kept++
      if (isFundSymbol(symbol)) funds++
      seeds.push({
        symbol,
        market,
        name: info?.name || daily.name,
        industry: src.revenue?.data.get(symbol)?.industry || null,
        par: finiteOrNull(info?.par) ?? null,
        paidIn: finiteOrNull(info?.paidIn) ?? null,
        daily,
        valuation: src.valuation?.data.get(symbol) ?? null,
        ratios: src.ratios?.data.get(symbol) ?? null,
        revenue: src.revenue?.data.get(symbol) ?? null,
        margin: src.margin?.data.get(symbol) ?? null,
        sbl: src.sbl?.data.get(symbol) ?? null,
        // A FAILED risk read stores null (unknown), never 0 — the history
        // column would otherwise log a silent no-flags day per outage.
        risk: src.risk === null ? null : (src.risk.data.get(symbol) ?? 0),
        inst: src.inst?.data.get(symbol) ?? null,
        foreign: src.foreign?.get(symbol) ?? null,
        // A non-null day-trade map is COMPLETE for the session (its rows sum
        // to the market total), so a symbol it does not name traded zero.
        dayTrade: src.dayTrade ? (src.dayTrade.get(symbol) ?? 0) : null,
        marginCost: null,
        bigHolder: latestWeek?.symbols[symbol]?.[0] ?? null,
        retail: latestWeek?.symbols[symbol]?.[1] ?? null,
        holders: latestWeek?.symbols[symbol]?.[2] ?? null,
        bigHolder4w:
          latestWeek && holdings4w && latestWeek.symbols[symbol] && holdings4w.symbols[symbol]
            ? latestWeek.symbols[symbol][0] - holdings4w.symbols[symbol][0]
            : null,
        exDiv: exDivInDays(calendar, symbol, sessionDate),
        listed: info?.listedMs ? dayStampOf(info.listedMs) || null : null
      })
    }
    console.log(
      `[etl] ${market} universe: ${kept} rows (${funds} ETF/ETN/REIT) — ` +
        `rejected ${rejectedShape} warrant/特別股/DR shapes, ${rejectedUniverse} unlisted`
    )
  }
  if (seeds.length === 0) {
    return finish(
      sessionDate,
      prev?.rows.length ?? 0,
      true,
      'no stock rows in the payloads',
      degraded,
      late
    )
  }

  // The ONE stateful derived value: the rolling average margin cost. It is
  // rolled ONCE per session (a forced re-run reuses the stored estimate) and
  // persisted in its own small object — no per-session history is kept. The
  // roll needs the symbol's HISTORY (the cold-start VWAP prior), so it runs
  // inside the per-symbol task below; the state is written once afterwards.
  const credit = await openCreditState(env, degraded)

  // A missed run leaves a GAP: the stored session is older than the anchor and
  // nothing else would ever append those sessions. Repair them from the
  // exchanges' per-date tables (bounded) before the anchor, ONE history read +
  // write per symbol.
  const missing = prev ? missingSessions(prev.sessionDate, sessionDate) : []
  let gaps = new Map<string, Array<{ stamp: string; seed: Seed }>>()
  if (missing.length > 0) {
    if (missing.length <= CATCHUP_MAX) {
      console.log(`[etl] repairing ${missing.length} missed session(s) before the anchor`)
      gaps = await readGapDays(missing, new Map(seeds.map((s) => [s.symbol, s])), degraded, late)
    } else {
      console.log(
        `[etl] ${missing.length} missed sessions (> ${CATCHUP_MAX}) — run the backfill to repair`
      )
      degraded.add('gap-too-old')
    }
  }

  /** Same-session re-run outcomes: cells kept because this run's source was
   *  absent, and symbols whose history was byte-identical (no write). */
  const sameSession = prev !== null && prev.sessionDate === sessionDate
  const prevRows = new Map<string, ScreenerRow>()
  for (const row of prev?.rows ?? []) prevRows.set(String(row[SC.symbol]), row)
  const stats = { preserved: 0, unchanged: 0 }
  const chains: SeedChain[] = seeds.map((seed) => ({ seed, gap: gaps.get(seed.symbol) ?? [] }))
  const fresh = await mapLimit(chains, R2_CONCURRENCY, async (entry): Promise<SeedOutcome> => {
    const seed = entry.seed
    let prior: PriorHistory | null = null
    try {
      prior = await readHistory(env, seed.symbol, degraded)
    } catch (err) {
      // A failed prior read must NEVER write: appending onto an assumed-empty
      // history would replace the symbol's whole rolling store with one
      // session. The merge below carries the previous row instead.
      console.log(`[etl] history ${seed.symbol} read failed — row carried: ${String(err)}`)
      degraded.add('history-read')
      return { ok: false, seed }
    }
    let history: ScreenerHistory | null = prior?.h ?? null
    for (const missed of entry.gap) {
      history = appendSession(history, seed.symbol, missed.stamp, missed.seed, stats)
    }
    const merged = appendSession(history, seed.symbol, stamp, seed, stats)
    rollSeedMarginCost(seed, merged, credit, stamp)
    const row = buildRow(seed, merged, stamp)
    // A same-session re-run may have lost a live source (a late/regenerated
    // reader failed this pass). The earlier row is the SAME session's published
    // truth, so fill only its null/empty cells; new finite values always win.
    if (sameSession) {
      const priorRow = prevRows.get(seed.symbol)
      if (priorRow) preserveRowCells(row, priorRow)
    }
    const body = JSON.stringify(merged)
    // A guard run re-appends the SAME session: when the serialized bytes match
    // the stored object, NOTHING is written. That makes the 03:45 guard a
    // zero-write no-op on a healthy day, and bounds a late-source repair to
    // exactly the symbols that changed (per-symbol history + last-session
    // files are the entire upload cost).
    if (prior !== null && body === prior.text) {
      stats.unchanged++
      return { ok: true, row }
    }
    try {
      await withRetry(`history ${seed.symbol} put`, 3, () =>
        env.R2_BUCKET.put(historyKey(seed.symbol), body, {
          httpMetadata: {
            contentType: 'application/json; charset=utf-8',
            cacheControl: DATA_CACHE_CONTROL
          }
        })
      )
    } catch (err) {
      console.log(`[etl] history ${seed.symbol} NOT persisted — row carried: ${String(err)}`)
      degraded.add('history-put')
      return { ok: false, seed }
    }
    return { ok: true, row }
  })
  await saveCreditState(env, credit, stamp, degraded)
  if (stats.preserved > 0) {
    console.log(`[etl] preserved ${stats.preserved} prior cells on the same-session re-run`)
  }
  if (stats.unchanged > 0) {
    console.log(`[etl] unchanged histories skipped ${stats.unchanged}/${chains.length} (no write)`)
  }

  // A market whose daily payload failed keeps its previous rows instead of
  // losing half the grid to one transient host error — and so does a symbol
  // whose history read or write failed. Merge order: carried markets first,
  // then per-seed outcomes (a failure overrides back to its previous row, or
  // to a daily-only row for a symbol the previous snapshot never held).
  const rowsBySymbol = new Map<string, ScreenerRow>()
  // A same-session repair may have a truncated daily table/universe: symbols
  // absent from this run's seeds still belong to the session. Seed from the
  // published grid first; the fresh rows below overwrite their symbols.
  if (sameSession) {
    for (const [symbol, row] of prevRows) rowsBySymbol.set(symbol, row)
  }
  const carried: Market[] = [...stale]
  for (const market of MARKETS) {
    if (sources[market].daily && !stale.includes(market)) continue
    if (!carried.includes(market)) carried.push(market)
    for (const row of prevRows.values()) {
      if (row[SC.market] === market) rowsBySymbol.set(String(row[SC.symbol]), row)
    }
  }
  for (const out of fresh) {
    if (out.ok) {
      rowsBySymbol.set(String(out.row[SC.symbol]), out.row)
      continue
    }
    const seed = out.seed
    const priorRow = prevRows.get(seed.symbol)
    rowsBySymbol.set(
      seed.symbol,
      priorRow ??
        buildRow(seed, appendSession(null, seed.symbol, stamp, seed, { preserved: 0 }), stamp)
    )
  }

  const rows = [...rowsBySymbol.values()].sort((a, b) =>
    String(a[SC.symbol]).localeCompare(String(b[SC.symbol]))
  )

  // The TAIEX benchmark: one ~15-row monthly table, merged into the stored
  // series. A failure never fails the run — the stored series still answers
  // (one session behind at worst).
  let benchmark: ScreenerSnapshot['benchmark'] = null
  try {
    let prior = null as ReturnType<typeof mergeBenchmark> | null
    let priorText: string | null = null
    const stored = await withRetry('benchmark read', 3, () => env.R2_BUCKET.get(BENCHMARK_KEY))
    if (stored) {
      priorText = await stored.text()
      try {
        const raw = JSON.parse(priorText) as unknown
        if (shapedBenchmark(raw)) prior = raw
      } catch {
        priorText = null
      }
    }
    const month = await twseMonthIndex(sessionDate).catch(() => [])
    if (month.length === 0) console.log(`[etl] benchmark month unavailable for ${stamp}`)
    const series = mergeBenchmark(prior, month)
    benchmark = benchmarkReturns(series)
    const body = JSON.stringify(series)
    if (priorText === body) {
      console.log('[etl] benchmark unchanged — not rewritten')
    } else {
      await withRetry('benchmark put', 3, () =>
        env.R2_BUCKET.put(BENCHMARK_KEY, body, {
          httpMetadata: {
            contentType: 'application/json; charset=utf-8',
            cacheControl: DATA_CACHE_CONTROL
          }
        })
      )
    }
  } catch (err) {
    console.log(`[etl] benchmark unavailable: ${String(err)}`)
  }

  // Market-wide context (own object; both halves independently nullable). It
  // is published before the snapshot so the grid never points at a missing
  // context. The rolling history is appended here — one observation per
  // session, same-session re-runs merge without erasing a published cell.
  const market = buildMarketContext(
    creditSummary,
    tpexSummary,
    maintenance,
    derivatives,
    rows,
    sessionDate,
    tpexTotals,
    stale
  )
  if (market) {
    let priorText: string | null = null
    let priorReady = true
    try {
      const priorObj = await withRetry('market read', 3, () =>
        env.R2_BUCKET.get(SCREENER_MARKET_KEY)
      )
      if (priorObj) priorText = await withRetry('market text', 3, () => priorObj.text())
    } catch (err) {
      // A failed READ is not absence: the prior object (and its history) is
      // still the only truth. Leave it untouched; the next run retries.
      priorReady = false
      degraded.add('market-read')
      console.log(`[etl] market context read failed — prior object left untouched: ${String(err)}`)
    }
    let priorContext: MarketContext | null = null
    if (priorReady && priorText) {
      try {
        priorContext = readMarketContext(JSON.parse(priorText))
      } catch (err) {
        // A truncated/corrupt body is never a licence to erase the rolling
        // series; retry the read on the next run.
        priorReady = false
        degraded.add('market-corrupt')
        console.log(`[etl] market context unreadable — prior object left untouched: ${String(err)}`)
      }
    }
    if (priorReady) {
      const merged =
        priorContext && priorContext.asOf === market.asOf
          ? mergeMarketContext(priorContext, market)
          : market
      merged.history = mergeMarketHistory(priorContext?.history, merged, priorContext)
      const body = JSON.stringify(merged)
      if (priorText === body) {
        console.log('[etl] market context unchanged — not rewritten')
      } else {
        await withRetry('market put', 3, () =>
          env.R2_BUCKET.put(SCREENER_MARKET_KEY, body, {
            httpMetadata: {
              contentType: 'application/json; charset=utf-8',
              cacheControl: DATA_CACHE_CONTROL
            }
          })
        )
      }
    }
  }
  // The publication itself retries: every history PUT above already landed,
  // so a single socket blip here must not strand the run one session behind.
  const snapshot: ScreenerSnapshot = { sessionDate, benchmark, rows }
  const snapshotBody = JSON.stringify(snapshot)
  // Publication is skipped when the grid is byte-identical — a guard run that
  // found nothing new leaves R2 untouched (and the edge cache unbothered).
  if (prevText === snapshotBody) {
    console.log('[etl] snapshot unchanged — not rewritten')
  } else {
    await withRetry('snapshot put', 3, () =>
      env.R2_BUCKET.put(SCREENER_SNAPSHOT_KEY, snapshotBody, {
        httpMetadata: {
          contentType: 'application/json; charset=utf-8',
          cacheControl: DATA_CACHE_CONTROL
        }
      })
    )
  }

  return finish(
    sessionDate,
    rows.length,
    false,
    carried.length > 0 ? `carried forward ${carried.join(' ')}` : 'ok',
    degraded,
    late
  )
}

/** Whether a run at `nowMs` is the same session's evening repair pass. A
 *  complete first run leaves no stale cache; this pass still re-reads the
 *  sources so late-published data is never skipped for the day. */
export function lateRepairWindow(nowMs: number, sessionDateMs: number): boolean {
  if (!(nowMs > 0) || !(sessionDateMs > 0)) return false
  const evening =
    dayStampOf(nowMs) === dayStampOf(sessionDateMs) && todOf(nowMs) >= LATE_REPAIR_TOD_MS
  // The next-morning 03:45 guard is the second repair slot; it must not wait
  // for the next session's first run. Only inside the fast-path window —
  // after 15:00 the next session's own run takes over.
  const morning =
    dayStampOf(nowMs) === dayStampOf(sessionDateMs + DAY_MS) &&
    todOf(nowMs) >= MORNING_GUARD_TOD_MS &&
    nowMs < sessionDateMs + DAY_MS + ROLL_TOD_MS
  return evening || morning
}

/** The result line every path logs — one shape, greppable in `wrangler tail`. */

/** Sessions the cold-start margin cost averages over (Σ成交金額 ÷ Σ成交股數). */
export const MARGIN_COST_VWAP_SESSIONS = 60

/**
 * The trailing VWAP of a symbol's own history (NT$ per share): Σ成交金額 ÷
 * Σ成交股數 over the newest `sessions` sessions that traded. This is the
 * cold-start prior for the rolling margin cost — seeding at the CLOSE made
 * every symbol read exactly 100/0.6 = 166.7 %, which is not an estimate.
 */
export function vwapOf(h: ScreenerHistory, sessions: number): number | null {
  let value = 0
  let volume = 0
  for (let i = h.dates.length - 1; i >= 0 && h.dates.length - i <= sessions; i--) {
    const v = h.volume[i]
    const t = h.tradeValue[i]
    if (typeof v !== 'number' || typeof t !== 'number' || !(v > 0) || !(t > 0)) continue
    value += t
    volume += v
  }
  if (!(volume > 0)) return null
  const vwap = value / volume
  return Number.isFinite(vwap) && vwap > 0 ? vwap : null
}

/** The shared credit-state handle: read ONCE, rolled per symbol, written once. */
interface CreditStateSession {
  state: CreditStateFile
  rolled: number
  resets: number
}

/** Reads the persisted estimate state (a corrupt/older-version object is an
 *  empty one — the symbols re-initialize from their warm priors). */
async function openCreditState(
  env: ScreenerEnv,
  degraded: Set<string>
): Promise<CreditStateSession> {
  let state: CreditStateFile = { version: MARGIN_COST_STATE_VERSION, asOf: '', symbols: {} }
  try {
    const obj = await withRetry('credit state read', 3, () =>
      env.R2_BUCKET.get(SCREENER_CREDIT_STATE_KEY)
    )
    if (obj) state = sanitizeCreditState(await obj.json(), Date.now())
  } catch (err) {
    console.log(`[etl] credit state unreadable — estimates re-initialize: ${String(err)}`)
    degraded.add('credit-state')
  }
  return { state, rolled: 0, resets: 0 }
}

/** ONE symbol's roll for this session (mutates the shared state + the seed).
 *  The gate is PER SYMBOL: the first run of a session can miss a late margin
 *  source, and the evening repair must still roll the symbols it missed. A
 *  global `state.asOf` gate could never do that. */
function rollSeedMarginCost(
  seed: Seed,
  merged: ScreenerHistory,
  credit: CreditStateSession,
  stamp: string
): void {
  const entry = credit.state.symbols[seed.symbol]
  if (entry && entry.asOf === stamp) {
    // Already rolled this session (a repair): reuse the same estimate.
    seed.marginCost = entry.warm !== false ? entry.cost : null
    return
  }
  // Never roll a back-dated repair onto a newer persisted estimate.
  if (entry && entry.asOf > stamp) {
    seed.marginCost = null
    return
  }
  const m = seed.margin
  const closeNtd = seed.daily.close > 0 ? seed.daily.close / 100 : 0
  if (!m || !(closeNtd > 0)) return
  const next = rollAverageMarginCost(
    entry ?? null,
    {
      prevBal: m.marginPrev,
      buy: m.marginBuy,
      sell: m.marginSell,
      cashRepay: m.marginCashRepay,
      bal: m.marginBal,
      close: closeNtd
    },
    vwapOf(merged, MARGIN_COST_VWAP_SESSIONS)
  )
  if (!next) return
  credit.state.symbols[seed.symbol] = { cost: next.cost, asOf: stamp, warm: next.warm }
  credit.rolled++
  if (next.reset) credit.resets++
  // A close-anchored initialization is NOT an estimate (the ratio is the
  // constant 100/0.6): the state bootstraps, the column stays null.
  seed.marginCost = next.warm ? next.cost : null
}

/** Persists the rolled state ONCE per session. A write failure only degrades
 *  the run (`credit-state`) — the grid still publishes. */
async function saveCreditState(
  env: ScreenerEnv,
  credit: CreditStateSession,
  stamp: string,
  degraded: Set<string>
): Promise<void> {
  // No symbol rolled => nothing changed; do NOT advance the state's date, or
  // a later same-session repair would believe the session was already done.
  if (credit.rolled === 0) return
  credit.state.asOf = stamp
  try {
    await withRetry('credit state put', 3, () =>
      env.R2_BUCKET.put(SCREENER_CREDIT_STATE_KEY, JSON.stringify(credit.state), {
        httpMetadata: {
          contentType: 'application/json; charset=utf-8',
          cacheControl: DATA_CACHE_CONTROL
        }
      })
    )
    console.log(
      `[etl] credit cost state ${stamp}: ${credit.rolled} symbols, ${credit.resets} (re)initialized`
    )
  } catch (err) {
    console.log(`[etl] credit state NOT persisted: ${String(err)}`)
    degraded.add('credit-state')
  }
}

/** The panel's provenance lines — the 上市櫃融資維持率 estimate, per scope.
 *  Definition (財經M平方-style, more price-sensitive than the exchange's
 *  whole-account 擔保維持率): the financed shares' market value ÷ the
 *  announced loan balance. Both announced balances include ETFs; neither
 *  exchange publishes a per-stock loan amount, so the shares' cost basis can
 *  only come from the rolling estimate (`estMaintenanceRatio`). */
const MAINTENANCE_NOTE =
  '上市櫃融資維持率 (est.) — Σ(margin balance lots × 10 × close) ÷ (TWSE 融資金額 + TPEx 融資餘額); financed value (ETFs included) over the announced loan balance — more price-sensitive than 全市場擔保維持率'
const MAINTENANCE_NOTE_TWSE =
  'TWSE 融資維持率 (est.) — Σ(margin balance lots × 10 × close) ÷ 融資金額 (ETFs included); the TPEx half was unavailable'

/** One market's row sums for the combined credit measures. */
interface MarketSums {
  financedNtd: number
  marketCapNtd: number
  marginLots: number
  shortLots: number
  hasFinanced: boolean
  hasMargin: boolean
  hasShort: boolean
}

const emptySums = (): MarketSums => ({
  financedNtd: 0,
  marketCapNtd: 0,
  marginLots: 0,
  shortLots: 0,
  hasFinanced: false,
  hasMargin: false,
  hasShort: false
})

/** Σ a market's rows: financed value (lots × 1000 shares × NT$/share; the
 *  close is raw ×100, so lots × close × 10), market cap, and the lot totals. */
function sumRows(rows: ScreenerRow[], market: 'TWSE' | 'TPEX'): MarketSums {
  const out = emptySums()
  for (const row of rows) {
    if (row[SC.market] !== market) continue
    const cap = screenNum(row, 'marketCap')
    if (cap !== null) out.marketCapNtd += cap
    const bal = screenNum(row, 'marginBal')
    if (bal !== null) {
      out.marginLots += bal
      out.hasMargin = true
    }
    const close = screenNum(row, 'close')
    if (bal !== null && close !== null && close > 0) {
      out.financedNtd += bal * close * 10
      out.hasFinanced = true
    }
    const short = screenNum(row, 'shortBal')
    if (short !== null) {
      out.shortLots += short
      out.hasShort = true
    }
  }
  return out
}

/** A summary cell: NaN (a blank exchange cell) → null. */
function summaryNum(v: number): number | null {
  return Number.isFinite(v) ? v : null
}

/** The whole-market 融資融券 totals of a per-stock margin table. EVERY row is
 *  summed (ETFs included): a stock-universe sum silently undercounts the
 *  market the exchange publishes. */
interface MarginTotals {
  marginBal: number
  marginPrev: number
  marginBuy: number
  marginSell: number
  marginCashRepay: number
  shortBal: number
  shortPrev: number
  shortSell: number
  shortCover: number
  shortCashRepay: number
}

const MARGIN_TOTAL_KEYS: ReadonlyArray<keyof MarginTotals> = [
  'marginBal',
  'marginPrev',
  'marginBuy',
  'marginSell',
  'marginCashRepay',
  'shortBal',
  'shortPrev',
  'shortSell',
  'shortCover',
  'shortCashRepay'
]

function marginTotalsOf(data: Map<string, MarginSrc>): MarginTotals | null {
  if (data.size === 0) return null
  const out: MarginTotals = {
    marginBal: 0,
    marginPrev: 0,
    marginBuy: 0,
    marginSell: 0,
    marginCashRepay: 0,
    shortBal: 0,
    shortPrev: 0,
    shortSell: 0,
    shortCover: 0,
    shortCashRepay: 0
  }
  for (const row of data.values()) {
    for (const key of MARGIN_TOTAL_KEYS) {
      const value = row[key]
      if (Number.isFinite(value)) out[key] += value
    }
  }
  return out
}

/**
 * Builds the market-context object (`market-latest.json`, its OWN product —
 * never part of the stock grid). `credit` needs the per-date TWSE summary to
 * BE the anchor (a stale aggregate is never stamped as today's); the TPEx
 * summary joins the money measures only when both its halves exist. The
 * derivatives half carries its own as-of and stays absent when no endpoint
 * answered.
 */
export function buildMarketContext(
  summary: CreditSummary | null,
  tpexSummary: TpexCreditSummary | null,
  maintenance: MaintenancePoint | null,
  derivatives: MarketDerivatives | null,
  rows: ScreenerRow[],
  sessionDate: number,
  tpexTotals: MarginTotals | null = null,
  /** Markets whose daily payload was carried: their rows are the PRIOR
   *  session's, so they must not enter this session's financed value, market
   *  cap or ratio. */
  staleMarkets: readonly ('TWSE' | 'TPEX')[] = []
): MarketContext | null {
  let credit: MarketCredit | null = null
  if (summary && summary.dateMs === sessionDate) {
    // The OTC summary is from the same session or it is not used at all: a
    // stale dashboard must never be stamped as the anchor's credit.
    const tpexCurrent = tpexSummary?.dateMs === sessionDate ? tpexSummary : null
    const twse = staleMarkets.includes('TWSE') ? emptySums() : sumRows(rows, 'TWSE')
    const tpex = staleMarkets.includes('TPEX') ? emptySums() : sumRows(rows, 'TPEX')
    const loanTwse = summaryNum(summary.marginAmountNtd)
    const loanTpex = tpexCurrent?.marginBalanceNtd ?? null
    // TPEx joins the ratio only when BOTH halves are present: its announced
    // loan balance and its row value. A numerator without a denominator (or
    // the reverse) would bias the ratio — the scope names what was covered.
    const scope: MarketCredit['scope'] =
      twse.hasFinanced && tpex.hasFinanced && loanTpex !== null && loanTpex > 0 && loanTwse !== null
        ? 'TWSE+TPEX'
        : 'TWSE'
    const financed = scope === 'TWSE+TPEX' ? twse.financedNtd + tpex.financedNtd : twse.financedNtd
    const loan = scope === 'TWSE+TPEX' ? (loanTwse ?? 0) + (loanTpex ?? 0) : loanTwse
    // The market cap denominator uses the SAME row universe as `financed`:
    // the grid's rows for both markets. The OTC dashboard's whole-market cap
    // (which includes funds) is carried separately and is not mixed in.
    const cap = scope === 'TWSE+TPEX' ? twse.marketCapNtd + tpex.marketCapNtd : twse.marketCapNtd
    credit = {
      scope,
      sessionDate,
      twseMarginBuyLots: summaryNum(summary.margin.buy),
      twseMarginSellLots: summaryNum(summary.margin.sell),
      twseMarginCashRepayLots: summaryNum(summary.margin.repay),
      twseMarginBalanceLots: summaryNum(summary.margin.bal),
      twseMarginPrevLots: summaryNum(summary.margin.prev),
      twseMarginChangeLots: summaryNum(summary.margin.bal - summary.margin.prev),
      twseShortSellLots: summaryNum(summary.short.sell),
      twseShortCoverLots: summaryNum(summary.short.buy),
      twseShortCashRepayLots: summaryNum(summary.short.repay),
      twseShortBalanceLots: summaryNum(summary.short.bal),
      twseShortPrevLots: summaryNum(summary.short.prev),
      twseShortChangeLots: summaryNum(summary.short.bal - summary.short.prev),
      twseMarginBalanceNtd: summaryNum(summary.marginAmountNtd),
      twseMarginBuyNtd: summaryNum(summary.marginAmountBuyNtd),
      twseMarginSellNtd: summaryNum(summary.marginAmountSellNtd),
      twseMarginCashRepayNtd: summaryNum(summary.marginAmountRepayNtd),
      twseMarginPrevNtd: summaryNum(summary.marginAmountPrevNtd),
      // The chosen per-stock table is the WHOLE market (ETFs included) — its
      // totals are the announced market figures; `sumRows` is the fallback.
      tpexMarginBuyLots: tpexTotals ? tpexTotals.marginBuy : null,
      tpexMarginSellLots: tpexTotals ? tpexTotals.marginSell : null,
      tpexMarginCashRepayLots: tpexTotals ? tpexTotals.marginCashRepay : null,
      tpexMarginBalanceLots: tpexTotals
        ? tpexTotals.marginBal
        : tpex.hasMargin
          ? tpex.marginLots
          : null,
      tpexMarginPrevLots: tpexTotals ? tpexTotals.marginPrev : null,
      tpexMarginChangeLots: tpexTotals ? tpexTotals.marginBal - tpexTotals.marginPrev : null,
      tpexShortSellLots: tpexTotals ? tpexTotals.shortSell : null,
      tpexShortCoverLots: tpexTotals ? tpexTotals.shortCover : null,
      tpexShortCashRepayLots: tpexTotals ? tpexTotals.shortCashRepay : null,
      tpexShortBalanceLots: tpexTotals
        ? tpexTotals.shortBal
        : tpex.hasShort
          ? tpex.shortLots
          : null,
      tpexShortPrevLots: tpexTotals ? tpexTotals.shortPrev : null,
      tpexShortChangeLots: tpexTotals ? tpexTotals.shortBal - tpexTotals.shortPrev : null,
      tpexMarginBalanceNtd: loanTpex,
      tpexCreditAmountNtd: tpexCurrent?.creditAmountNtd ?? null,
      tpexMarketCapNtd: tpexCurrent?.marketCapNtd ?? null,
      // A value the panel divides by: never 0 for a missing half.
      financedValueNtd: financed > 0 ? financed : null,
      loanBalanceNtd: loan !== null && loan > 0 ? loan : null,
      marketCapNtd: cap > 0 ? cap : null,
      marginToMarketCap: loan !== null && loan > 0 && cap > 0 ? (loan / cap) * 100 : null,
      maintenanceRatio: loan !== null && loan > 0 && financed > 0 ? (financed / loan) * 100 : null,
      maintenanceRatioKind: 'estimate',
      maintenanceRatioNote: scope === 'TWSE+TPEX' ? MAINTENANCE_NOTE : MAINTENANCE_NOTE_TWSE,
      officialMaintenanceRatio:
        maintenance && maintenance.dateMs === sessionDate ? maintenance.keepRate : null
    }
  }
  const hasDer =
    derivatives !== null &&
    (derivatives.institutions.length > 0 ||
      derivatives.futures.length > 0 ||
      derivatives.options.length > 0 ||
      derivatives.largeTraders.length > 0 ||
      derivatives.pcr.length > 0 ||
      derivatives.vix !== null)
  if (credit === null && !hasDer) return null
  return { asOf: sessionDate, credit, derivatives: hasDer ? derivatives : null }
}

function finish(
  sessionDate: number,
  symbols: number,
  skipped: boolean,
  note: string,
  degraded: Set<string>,
  late: Set<string>
): EtlResult {
  const partial = [...degraded]
  const waiting = [...late]
  console.log(
    `[etl] session=${sessionDate === 0 ? '-' : dayStampOf(sessionDate)} symbols=${symbols} skipped=${skipped} ${note}` +
      (waiting.length > 0 ? ` late=${waiting.join(',')}` : '') +
      (partial.length > 0 ? ` partial=${partial.join(',')}` : '')
  )
  return { sessionDate, symbols, skipped, late: waiting, partial }
}

interface MarketSources {
  daily: Sourced<DailyRow> | null
  universe: Sourced<UniverseRow> | null
  valuation: Sourced<ValuationRow> | null
  ratios: Sourced<RatioRow> | null
  revenue: Sourced<RevenueRow> | null
  margin: Sourced<MarginSrc> | null
  sbl: Sourced<SblSrc> | null
  risk: Sourced<number> | null
  inst: Sourced<InstSrc> | null
  /** 外資持股 — a plain map (the per-date report IS the daily source). */
  foreign: Map<string, ForeignHold> | null
  /** 當沖 volume (shares) — each exchange's own per-session report. */
  dayTrade: Map<string, number> | null
}

/** One symbol's today payloads — the row builder's only input besides history. */
export interface Seed {
  symbol: string
  market: Market
  name: string
  industry: string | null
  par: number | null
  paidIn: number | null
  daily: DailyRow
  valuation: ValuationRow | null
  ratios: RatioRow | null
  revenue: RevenueRow | null
  margin: MarginSrc | null
  sbl: SblSrc | null
  /** Risk bits; null = the risk read FAILED (unknown ⊃ not clean). */
  risk: number | null
  inst: InstSrc | null
  foreign: ForeignHold | null
  /** 當沖 volume in shares (null = not published that session). */
  dayTrade: number | null
  /** The rolling margin-cost estimate (NT$ per share) for THIS session — set
   *  after the credit-state roll; null when the symbol has no fresh estimate. */
  marginCost: number | null
  /** The weekly TDCC split (null = no object / not covered). */
  bigHolder: number | null
  retail: number | null
  holders: number | null
  /** Δ (pp) of the 大戶 share over the last 4 weeks — null until then. */
  bigHolder4w: number | null
  /** Days to the next ex-dividend date (0 = today), null when none is known. */
  exDiv: number | null
  /** 上市日期 as a `YYYYMMDD` stamp (null when the table omits it). */
  listed: string | null
}

/** One seed's result: a built row, or a failure that degrades to a carry. */
type SeedOutcome = { ok: true; row: ScreenerRow } | { ok: false; seed: Seed }

/** One extra read when the archive answered empty (an edge miss). */
async function dayTradeRetry<T>(read: () => Promise<T | null>): Promise<T | null> {
  const first = await read()
  if (first !== null) return first
  await delay(2_000)
  return read()
}

/** A source read with the run's TWO absence kinds separated:
 *  - a CLEAN `null` = "not published for this session yet" → `late` (the later
 *    cron repairs it; no retry, no forced second pass);
 *  - a THROWN read = a failure → ONE retry, then `degraded` (the caller's
 *    forced retry may heal it). */
export async function readSafe<T>(
  name: string,
  degraded: Set<string>,
  late: Set<string>,
  read: () => Promise<T | null>
): Promise<T | null> {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const value = await read()
      if (value === null) {
        late.add(name)
        return null
      }
      return value
    } catch (err) {
      console.log(`[etl] source ${name} attempt ${attempt}/2 failed: ${String(err)}`)
      if (attempt < 2) await delay(1500)
    }
  }
  degraded.add(name)
  return null
}

/** `YYYYMMDD` or `-` — the one anchor-log formatter. */
function stampOrDash(ms: number | undefined): string {
  return ms && ms > 0 ? dayStampOf(ms) || '-' : '-'
}

/** The per-date daily adapter each market reads for the anchor. */
type DailyReader = (dateMs: number) => Promise<Sourced<DailyRow> | null>

/** The newest weekdays at/before `nowMs`, today first (weekends skipped) —
 *  the anchor walk's domain. */
export function probeDays(nowMs: number, max = PROBE_MAX_DAYS): number[] {
  const out: number[] = []
  if (!(nowMs > 0)) return out
  for (let ms = stampToMs(dayStampOf(nowMs)); out.length < max; ms -= DAY_MS) {
    const weekday = new Date(ms + TAIPEI_OFFSET_MS).getUTCDay()
    if (weekday !== 0 && weekday !== 6) out.push(ms)
  }
  return out
}

interface AnchorProbe {
  dateMs: number
  twse: Sourced<DailyRow> | null
  tpex: Sourced<DailyRow> | null
}

/** One market's probe: a thrown read logs and answers null — the walk goes on
 *  and never degrades the run (a holiday simply answers nothing). */
async function probeOne(
  market: Market,
  stamp: string,
  read: () => Promise<Sourced<DailyRow> | null>
): Promise<Sourced<DailyRow> | null> {
  try {
    return await read()
  } catch (err) {
    console.log(`[etl] ${market} per-date probe ${stamp} failed: ${String(err).slice(0, 120)}`)
    return null
  }
}

/** The anchor from the exchanges' own per-date tables: the first weekday (from
 *  today) that ANY market answers. A market that misses that date falls back
 *  to its own read in `anchorDaily`. */
async function probeAnchor(nowMs: number): Promise<AnchorProbe> {
  for (const dateMs of probeDays(nowMs)) {
    const stamp = dayStampOf(dateMs)
    const [twse, tpex] = await Promise.all([
      probeOne('TWSE', stamp, () => twseDayQuotes(dateMs)),
      probeOne('TPEX', stamp, () => tpexDayQuotes(dateMs))
    ])
    const twseOk = twse && twse.dateMs === dateMs ? twse : null
    const tpexOk = tpex && tpex.dateMs === dateMs ? tpex : null
    if (twseOk || tpexOk) return { dateMs, twse: twseOk, tpex: tpexOk }
  }
  return { dateMs: 0, twse: null, tpex: null }
}

/** A mirror (date hint) read that never degrades the run: the per-date tables
 *  are the source of record, and a mirror outage must not fail the session. */
async function tryRead<T>(name: string, read: () => Promise<T>): Promise<T | null> {
  try {
    return await read()
  } catch (err) {
    console.log(`[etl] ${name} mirror unavailable: ${String(err).slice(0, 120)}`)
    return null
  }
}

/** An AUXILIARY market-context read: retried ONCE, logged, and never a partial
 *  token — a lost section is repaired by the next slot and the context object
 *  already records what it carries. */
async function auxiliary<T>(name: string, read: () => Promise<T>): Promise<T | null> {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      return await read()
    } catch (err) {
      if (attempt === 2) {
        console.log(`[etl] ${name} unavailable: ${String(err)}`)
        return null
      }
      await delay(1_000)
    }
  }
  return null
}

/** The per-date table omits names that did NOT trade that session (a
 *  suspended stock has no row at all), while the OpenAPI mirror lists them
 *  with blank prices — the grid must not lose a listed instrument to a quiet
 *  day. Union the mirror's missing symbols in ONLY when the mirror holds the
 *  SAME session; the per-date values win for a symbol both carry. */
function unionDaily(
  base: Sourced<DailyRow>,
  mirror: Sourced<DailyRow> | null,
  anchorMs: number
): Sourced<DailyRow> {
  if (!mirror || mirror.dateMs !== anchorMs || mirror.rows === 0) return base
  let added = 0
  for (const [symbol, row] of mirror.data) {
    if (base.data.has(symbol)) continue
    base.data.set(symbol, row)
    base.rows++
    added++
  }
  if (added > 0) {
    console.log(`[etl] daily union: ${added} no-trade rows restored from the mirror`)
  }
  return base
}

/**
 * The market's daily for the anchor session — rwd FIRST: the probe's own
 * payload for that date, else one fresh per-date read. The OpenAPI mirror only
 * stands in when it holds THE anchor and the per-date table failed (a slow
 * publish). A mirror holding another session comes back unchanged, so the
 * stale-market carry applies exactly as before.
 */
export async function anchorDaily(
  market: Market,
  mirror: Sourced<DailyRow> | null,
  anchorMs: number,
  minRows: number,
  probed: Sourced<DailyRow> | null,
  at: DailyReader,
  degraded: Set<string>,
  late: Set<string>
): Promise<Sourced<DailyRow> | null> {
  const usable = (s: Sourced<DailyRow> | null): boolean =>
    s !== null && s.dateMs === anchorMs && s.rows >= minRows
  if (usable(probed) && probed) return unionDaily(probed, mirror, anchorMs)
  let failed = false
  let fallback: Sourced<DailyRow> | null = null
  for (let attempt = 1; attempt <= 2 && !fallback; attempt++) {
    try {
      fallback = await at(anchorMs)
    } catch (err) {
      failed = true
      console.log(`[etl] ${market} per-date daily attempt ${attempt}/2 failed: ${String(err)}`)
      if (attempt < 2) await delay(1_000)
    }
  }
  if (usable(fallback) && fallback) {
    console.log(
      `[etl] ${market} daily from the per-date table — ${fallback.rows} rows` +
        ` (mirror ${stampOrDash(mirror?.dateMs)})`
    )
    return unionDaily(fallback, mirror, anchorMs)
  }
  if (usable(mirror) && mirror) {
    console.log(
      `[etl] ${market} daily: the per-date table is not out — the mirror holds the anchor`
    )
    return mirror
  }
  if (mirror && mirror.dateMs === anchorMs) {
    console.log(`[etl] ${market} daily holds only ${mirror.rows} rows (< ${minRows}) — carried`)
  } else if (mirror) {
    console.log(
      `[etl] ${market} daily mirror holds session ${stampOrDash(mirror.dateMs)} (anchor ${dayStampOf(anchorMs)}) — carried`
    )
  }
  const token = market === 'TWSE' ? 'twse.daily' : 'tpex.daily'
  // A clean "not out yet" (or a market that answered nothing) is LATE; a
  // thrown read or a truncated table is a failure.
  ;(failed || (fallback !== null && fallback.rows > 0) ? degraded : late).add(token)
  return mirror
}

/** The per-date adapters for the valuation / margin / flow / SBL readers. */
type MapReader<T> = (dateMs: number) => Promise<Map<string, T> | null>

/**
 * A sourced value for the anchor — the exchange's own per-date table FIRST
 * (the OpenAPI mirrors regenerate the NEXT morning; measured 2026-11-05 02:00:
 * TWSE's STOCK_DAY_ALL and BWIBBU_ALL still held 11-03). The dated mirror is
 * the fallback for a slow publish, and only when it holds THE anchor; anything
 * else is absent + named in `partial` (a stale value must never be stamped on
 * the session).
 */
export async function perDateFirst<T>(
  name: string,
  /** The dated mirror — read LAZILY, only when the exchange's own per-date
   *  table has no answer. A normal evening therefore fetches each dataset
   *  exactly once (the per-date read); the mirror is the fallback, never a
   *  parallel duplicate of the same session's data. */
  mirror: (() => Promise<Sourced<T> | null>) | null,
  anchorMs: number,
  at: MapReader<T>,
  degraded: Set<string>,
  late: Set<string>
): Promise<Sourced<T> | null> {
  // Retry a FAILED read once (the user's rule: retry failures, skip a clean
  // "not out yet"); either way the dated mirror below is the fallback.
  let map: Map<string, T> | null = null
  let failed = false
  for (let attempt = 1; attempt <= 2 && map === null; attempt++) {
    try {
      map = await at(anchorMs)
      failed = false
    } catch (err) {
      failed = true
      console.log(`[etl] ${name} per-date attempt ${attempt}/2 failed: ${String(err)}`)
      if (attempt < 2) await delay(1_000)
    }
  }
  if (map && map.size > 0) return { dateMs: anchorMs, rows: map.size, data: map }
  const primary = mirror ? await mirror() : null
  if (primary && (primary.dateMs === 0 || primary.dateMs === anchorMs) && primary.rows > 0) {
    console.log(`[etl] ${name} per-date table not out yet — using the mirror`)
    return primary
  }
  if (primary) {
    console.log(
      `[etl] ${name} mirror holds session ${stampOrDash(primary.dateMs)} (anchor ${dayStampOf(anchorMs)}) — absent`
    )
  } else {
    console.log(`[etl] ${name} unavailable for the anchor — absent`)
  }
  ;(failed ? degraded : late).add(name)
  return null
}

/** A source that carries its OWN session date must be the anchor's (an absent
 *  date cannot be checked; every source without one is read per-date). */
function anchorOnly<T>(
  name: string,
  src: Sourced<T> | null,
  anchorMs: number,
  late: Set<string>
): Sourced<T> | null {
  if (!src || src.dateMs === 0 || src.dateMs === anchorMs) return src
  console.log(
    `[etl] ${name} holds session ${dayStampOf(src.dateMs)} (anchor ${dayStampOf(anchorMs)}) — absent`
  )
  late.add(name)
  return null
}

/** The mirror's own 出表日期 is the freshness signal (it is regenerated on
 *  every filing batch, so it normally sits within a day of the anchor): a skew
 *  beyond `RATIOS_MAX_SKEW_MS` means a frozen feed whose values may predate a
 *  quarterly publish. A frozen mirror INSIDE one quarter's window is not
 *  detectable from the payload (the rows carry no quarter) — that residual
 *  risk is documented, not guessed around. */
function saneRatios(
  name: string,
  src: Sourced<RatioRow> | null,
  anchorMs: number,
  late: Set<string>
): Sourced<RatioRow> | null {
  if (!src || src.dateMs === 0) return src
  const skew = anchorMs - src.dateMs
  if (Math.abs(skew) <= RATIOS_MAX_SKEW_MS) return src
  console.log(
    `[etl] ${name} filing date ${dayStampOf(src.dateMs)} is ${Math.round(skew / DAY_MS)}d off the anchor — absent`
  )
  late.add(name)
  return null
}

/** `YYYYMM` shifted by `delta` months. */
function shiftYm(ym: number, delta: number): number {
  const total = Math.floor(ym / 100) * 12 + (ym % 100) - 1 + delta
  return Math.floor(total / 12) * 100 + (total % 12) + 1
}

/** A month's revenue publishes from the 10th of the NEXT month, so the latest
 *  published month is anchor−1 from the 10th onward and anchor−2 before it —
 *  the payload's 資料年月 must sit in that window (older = a frozen mirror; a
 *  future month is impossible). */
function saneRevenue(
  name: string,
  src: Sourced<RevenueRow> | null,
  anchorMs: number,
  late: Set<string>
): Sourced<RevenueRow> | null {
  if (!src || src.dateMs === 0) return src
  const first = src.data.values().next().value as RevenueRow | undefined
  if (!first || first.monthMs === 0) return src
  const ym = Number(dayStampOf(first.monthMs).slice(0, 6))
  const anchorYm = Number(dayStampOf(anchorMs).slice(0, 6))
  if (ym <= anchorYm && ym >= shiftYm(anchorYm, -2)) return src
  console.log(`[etl] ${name} reports ${ym} (anchor ${anchorYm}) — absent`)
  late.add(name)
  return null
}

/**
 * Repairs the sessions a missed run never appended: for each traded weekday
 * between the stored session and the anchor, read the exchange's own per-date
 * tables and build a seed per symbol. Fundamentals (ratios/revenue) come from
 * the anchor payloads — the value AS KNOWN at the run — and risk flags are
 * unknown (null) for a past session. A bounded read (CATCHUP_MAX sessions):
 * a longer outage is the backfill's job, reported as `partial`.
 */
async function readGapDays(
  days: number[],
  seedsBySymbol: Map<string, Seed>,
  degraded: Set<string>,
  late: Set<string>
): Promise<Map<string, Array<{ stamp: string; seed: Seed }>>> {
  const out = new Map<string, Array<{ stamp: string; seed: Seed }>>()
  for (const dateMs of days) {
    const stamp = dayStampOf(dateMs)
    const [
      twseDay,
      tpexDay,
      twseFlow,
      tpexFlow,
      twseMar,
      twseSbl,
      tpexSbl,
      twseFor,
      tpexFor,
      twseVal,
      tpexVal
    ] = await Promise.all([
      readSafe(`gap ${stamp} twse.daily`, degraded, late, () => twseDayQuotes(dateMs)),
      readSafe(`gap ${stamp} tpex.daily`, degraded, late, () => tpexDayQuotes(dateMs)),
      readSafe(`gap ${stamp} twse.inst`, degraded, late, () => twseInstitutional(dateMs)),
      readSafe(`gap ${stamp} tpex.inst`, degraded, late, () => tpexDayFlows(dateMs)),
      readSafe(`gap ${stamp} twse.margin`, degraded, late, () =>
        twseMarginDay(dateMs).then((r) => r?.stocks ?? null)
      ),
      readSafe(`gap ${stamp} twse.sbl`, degraded, late, () => twseDaySblBal(dateMs)),
      readSafe(`gap ${stamp} tpex.sbl`, degraded, late, () => tpexDaySblBal(dateMs)),
      readSafe(`gap ${stamp} twse.foreign`, degraded, late, () => twseDayForeign(dateMs)),
      readSafe(`gap ${stamp} tpex.foreign`, degraded, late, () => tpexDayForeign(dateMs)),
      readSafe(`gap ${stamp} twse.valuation`, degraded, late, () => twseDayValuation(dateMs)),
      readSafe(`gap ${stamp} tpex.valuation`, degraded, late, () => tpexDayValuation(dateMs))
    ])
    const markets: Array<
      [
        Map<string, DailyRow> | null,
        Map<string, InstSrc> | null,
        Map<string, MarginSrc> | null,
        Map<string, SblSrc> | null,
        Map<string, ForeignHold> | null,
        Map<string, ValuationRow> | null
      ]
    > = [
      [twseDay?.data ?? null, twseFlow?.data ?? null, twseMar, twseSbl, twseFor, twseVal],
      [tpexDay?.data ?? null, tpexFlow, null, tpexSbl, tpexFor, tpexVal]
    ]
    let rows = 0
    for (const [daily, inst, margin, sbl, foreign, valuation] of markets) {
      if (!daily) continue
      for (const [symbol, day] of daily) {
        const base = seedsBySymbol.get(symbol)
        if (!base) continue
        const list = out.get(symbol) ?? []
        list.push({
          stamp,
          seed: {
            ...base,
            daily: day,
            valuation: valuation?.get(symbol) ?? null,
            margin: margin?.get(symbol) ?? null,
            sbl: sbl?.get(symbol) ?? null,
            inst: inst?.get(symbol) ?? null,
            foreign: foreign?.get(symbol) ?? null,
            risk: null
          }
        })
        out.set(symbol, list)
        rows++
      }
    }
    console.log(`[etl] gap ${stamp} rows=${rows}`)
  }
  return out
}

/** The stored snapshot AND its raw bytes: the bytes are what lets the writer
 *  prove a re-run changed nothing (a guard run must be a ZERO-write no-op). */
async function readSnapshot(
  env: ScreenerEnv
): Promise<{ snap: ScreenerSnapshot | null; text: string | null }> {
  const obj = await withRetry('snapshot read', 3, () => env.R2_BUCKET.get(SCREENER_SNAPSHOT_KEY))
  if (!obj) return { snap: null, text: null }
  const text = await obj.text()
  try {
    const parsed = JSON.parse(text) as ScreenerSnapshot
    const ok = Array.isArray(parsed.rows) && typeof parsed.sessionDate === 'number'
    return { snap: ok ? parsed : null, text }
  } catch {
    return { snap: null, text }
  }
}

/** A corrupt history is QUARANTINED, never treated as absence: the append path
 *  would otherwise replace the symbol's whole rolling store with one session
 *  (the silent wipe). The original bytes are kept beside the key for triage. */
/** A prior history plus its exact bytes — the append path compares the
 *  re-serialized result against `text` and skips the PUT when nothing moved. */
interface PriorHistory {
  h: ScreenerHistory
  text: string
}

async function readHistory(
  env: ScreenerEnv,
  symbol: string,
  degraded: Set<string>
): Promise<PriorHistory | null> {
  const obj = await withRetry(`history ${symbol} read`, 3, () =>
    env.R2_BUCKET.get(historyKey(symbol))
  )
  if (!obj) return null
  const text = await obj.text()
  let parsed: unknown = null
  try {
    parsed = JSON.parse(text)
  } catch {
    parsed = null
  }
  if (isScreenerHistory(parsed, symbol)) return { h: parsed, text }
  console.log(`[etl] history ${symbol} UNREADABLE — quarantined as .corrupt`)
  try {
    await env.R2_BUCKET.put(`${historyKey(symbol)}.corrupt`, text, {
      httpMetadata: { contentType: 'application/json; charset=utf-8' }
    })
  } catch (err) {
    console.log(`[etl] history ${symbol} quarantine write failed: ${String(err)}`)
  }
  degraded.add('history-corrupt')
  return null
}

function emptyHistory(symbol: string): ScreenerHistory {
  const h = { symbol, dates: [] as string[] } as unknown as Record<string, unknown>
  for (const field of SCREENER_HISTORY_FIELDS) h[field] = []
  return h as unknown as ScreenerHistory
}

/** The history's aligned column for one contract field (typed read). */
function columnOf(h: ScreenerHistory, field: ScreenerHistoryField): (number | null)[] {
  return (h as unknown as Record<string, (number | null)[]>)[field]
}

/** One session's values for EVERY history column — the append/prune/sort
 *  loops iterate the shared field list, so a new column is one contract line
 *  plus one `HistoryValues` member, never a fourth duplicated block. */
export type HistoryValues = Record<ScreenerHistoryField, number | null>

/** Appends (or REPLACES, a forced same-session re-run) one session, keeps the
 *  dates ascending and prunes to the retained window. A prior from BEFORE the
 *  extended columns is normalized in place (missing arrays become nulls), so
 *  an older file appends aligned instead of being treated as corrupt. */
function appendSession(
  prior: ScreenerHistory | null,
  symbol: string,
  stamp: string,
  seed: Seed,
  stats: { preserved: number }
): ScreenerHistory {
  const h = prior ?? emptyHistory(symbol)
  const write = h as unknown as Record<string, unknown[]>
  for (const field of SCREENER_HISTORY_FIELDS) {
    if (write[field] === undefined) write[field] = h.dates.map(() => null)
  }
  const close = finiteOrNull(seed.daily.close) ?? 0
  // A session without a usable close is not a session: leave the history as is.
  if (close <= 0) return h
  const shares = shareCount(seed)
  const price = close / 100
  const nt = (shares: number | undefined): number | null =>
    seed.inst && Number.isFinite(shares) ? (shares as number) * price : null
  const values: HistoryValues = {
    open: finiteOrNull(seed.daily.open) ?? close,
    high: finiteOrNull(seed.daily.high) ?? close,
    low: finiteOrNull(seed.daily.low) ?? close,
    close,
    volume: finiteOrNull(seed.daily.volume) ?? 0,
    tickets: finiteOrNull(seed.daily.transactions) ?? 0,
    tradeValue: finiteOrNull(seed.daily.tradeValue) ?? 0,
    turnoverRate:
      shares > 0 && Number.isFinite(seed.daily.volume) ? (seed.daily.volume / shares) * 100 : null,
    foreign: nt(seed.inst?.foreign),
    trust: nt(seed.inst?.trust),
    dealer: nt(seed.inst?.dealer),
    per: finiteOrNull(seed.valuation?.per),
    pbr: finiteOrNull(seed.valuation?.pbr),
    divYield: finiteOrNull(seed.valuation?.divYield),
    marginBal: finiteOrNull(seed.margin?.marginBal),
    marginQuota: finiteOrNull(seed.margin?.quota),
    shortBal: finiteOrNull(seed.margin?.shortBal),
    sblBal: finiteOrNull(seed.sbl?.sblBal),
    riskFlags: seed.risk,
    grossMargin: finiteOrNull(seed.ratios?.grossMargin) ?? lastPublished(h, 'grossMargin'),
    opMargin: finiteOrNull(seed.ratios?.opMargin) ?? lastPublished(h, 'opMargin'),
    pretaxMargin: finiteOrNull(seed.ratios?.pretaxMargin) ?? lastPublished(h, 'pretaxMargin'),
    netMargin: finiteOrNull(seed.ratios?.netMargin) ?? lastPublished(h, 'netMargin'),
    revenueMom: finiteOrNull(seed.revenue?.revenueMom),
    revenueYoy: finiteOrNull(seed.revenue?.revenueYoy),
    revenueCumYoy: finiteOrNull(seed.revenue?.revenueCumYoy),
    revenueMonth: finiteOrNull(seed.revenue?.revenueMonth),
    revenueYm:
      seed.revenue?.monthMs && seed.revenue.monthMs > 0
        ? Number(dayStampOf(seed.revenue.monthMs).slice(0, 6)) || null
        : null,
    foreignShares: finiteOrNull(seed.foreign?.shares),
    foreignRatio: finiteOrNull(seed.foreign?.ratio),
    dayTradeShares: finiteOrNull(seed.dayTrade)
  }

  return mergeSession(h, stamp, values, stats)
}

/**
 * Merges ONE session into a history — the reconciliation rule in one place:
 * a new stamp APPENDS; a same-stamp re-run REPLACES finite cells but never
 * ERASES one (a source that failed this time keeps the prior cell — the
 * contract's "value AS KNOWN that session"; a max-of read would revive a stale
 * column forever, so the carry is one-way). The oldest sessions past the
 * retention are dropped. Exported for the reconciliation test.
 */
export function mergeSession(
  h: ScreenerHistory,
  stamp: string,
  values: HistoryValues,
  stats: { preserved: number }
): ScreenerHistory {
  let i = h.dates.indexOf(stamp)
  if (i < 0) {
    i = h.dates.length
    h.dates.push(stamp)
    for (const field of SCREENER_HISTORY_FIELDS) columnOf(h, field).push(values[field])
    sortHistory(h)
  } else {
    for (const field of SCREENER_HISTORY_FIELDS) {
      const col = columnOf(h, field)
      const next = values[field]
      if (next === null && Number.isFinite(col[i])) {
        stats.preserved++
        continue
      }
      col[i] = next
    }
  }
  const drop = h.dates.length - SCREENER_HISTORY_SESSIONS
  if (drop > 0) {
    h.dates.splice(0, drop)
    for (const field of SCREENER_HISTORY_FIELDS) columnOf(h, field).splice(0, drop)
  }
  return h
}

/** Shares outstanding from the listing capital (paid-in ÷ par). */
function shareCount(seed: Seed): number {
  const par = seed.par ?? 0
  const paidIn = seed.paidIn ?? 0
  return par > 0 && paidIn > 0 ? paidIn / par : 0
}

/** Reorders every column when an out-of-order session was appended (a forced
 *  back-dated repair); normally the append order is already ascending. */
function sortHistory(h: ScreenerHistory): void {
  const last = h.dates.length - 1
  if (last < 1 || h.dates[last - 1] <= h.dates[last]) return
  const order = h.dates.map((_, i) => i).sort((a, b) => h.dates[a].localeCompare(h.dates[b]))
  const write = h as unknown as Record<string, unknown[]>
  h.dates = order.map((i) => h.dates[i])
  for (const field of SCREENER_HISTORY_FIELDS) {
    const col = columnOf(h, field)
    write[field] = order.map((i) => col[i])
  }
}

/** Reads, folds and stores the ex-dividend calendar; a failed fetch keeps the
 *  stored one (the column is an event, never a reason to degrade the run). */
async function refreshDividends(
  env: ScreenerEnv,
  sessionDate: number
): Promise<DividendsObject | null> {
  const prior = await readDividends(env).catch(() => null)
  const fresh = await twseUpcomingDividends().catch((err: unknown) => {
    console.log(`[etl] dividends calendar unavailable: ${String(err).slice(0, 120)}`)
    return []
  })
  if (fresh.length === 0 && !prior) return null
  const merged = mergeDividends(prior, fresh, sessionDate)
  // `mergeDividends` stamps updatedAt=now; keep the prior stamp when the
  // event set is unchanged so the object's bytes (and its R2 write) are a
  // no-op on a repeat run.
  const eventsUnchanged =
    prior !== null && JSON.stringify(merged.events) === JSON.stringify(prior.events)
  const next =
    eventsUnchanged && prior !== null ? { ...merged, updatedAt: prior.updatedAt } : merged
  try {
    if (prior === null || JSON.stringify(next) !== JSON.stringify(prior)) {
      await env.R2_BUCKET.put(DIVIDENDS_KEY, JSON.stringify(next), {
        httpMetadata: {
          contentType: 'application/json; charset=utf-8',
          cacheControl: DATA_CACHE_CONTROL
        }
      })
    }
  } catch (err) {
    console.log(`[etl] dividends calendar write failed: ${String(err).slice(0, 120)}`)
  }
  return next
}

/** The 73-column row: today's payloads + the rolling window of `history`. */
/** The newest NON-NULL value of a QUARTERLY history column. The exchange's
 *  quarterly tables publish in WAVES (`t187ap17_L` holds only the newest
 *  filing batch — ~46 rows while the Q3 season runs), so a company that has
 *  not filed yet must keep its last published quarter; nulling it would erase
 *  a real filing from the grid (and zero out 基本面 screens market-wide). */
function lastPublished(
  h: ScreenerHistory,
  field: 'grossMargin' | 'opMargin' | 'pretaxMargin' | 'netMargin'
): number | null {
  const arr = h[field]
  if (!Array.isArray(arr)) return null
  for (let i = arr.length - 1; i >= 0; i--) if (arr[i] !== null) return arr[i]
  return null
}

/** Fills a same-session re-run's null/empty cells from the row already
 *  published for that session. The new row's finite values always win. */
export function preserveRowCells(next: ScreenerRow, prior: ScreenerRow): ScreenerRow {
  for (let i = 0; i < next.length && i < prior.length; i++) {
    const cur = next[i]
    const old = prior[i]
    if ((cur === null || cur === '') && old !== null && old !== '') next[i] = old
  }
  return next
}

export function buildRow(seed: Seed, h: ScreenerHistory, stamp: string): ScreenerRow {
  const n = h.dates.length
  const last = n > 0 ? n - 1 : -1
  // A same-stamp re-run may have LOST a live source this pass (a late reader,
  // a regenerated file the gate rejected): the history already holds what the
  // earlier pass published, so the row falls back to that cell instead of
  // publishing a null over a known value. Never for a NEW session — the prior
  // session's value is not this session's.
  const cur = last >= 0 && h.dates[last] === stamp ? last : -1
  const carried = (field: ScreenerHistoryField): number | null => {
    if (cur < 0) return null
    const v = columnOf(h, field)[cur]
    return typeof v === 'number' && Number.isFinite(v) ? v : null
  }
  const close = finiteOrNull(seed.daily.close)
  const tradeValue = finiteOrNull(seed.daily.tradeValue)
  const shares = shareCount(seed)
  const price = close === null ? null : close / 100
  const nt = (v: number | undefined): number | null =>
    price !== null && v !== undefined && Number.isFinite(v) ? v * price : null
  const instForeign = nt(seed.inst?.foreign) ?? carried('foreign')
  const instTrust = nt(seed.inst?.trust) ?? carried('trust')
  const instDealer = nt(seed.inst?.dealer) ?? carried('dealer')
  const instTotal =
    instForeign === null && instTrust === null && instDealer === null
      ? null
      : (instForeign ?? 0) + (instTrust ?? 0) + (instDealer ?? 0)

  const tailValue = h.tradeValue.slice(Math.max(0, n - 20)).filter(Number.isFinite)
  const tailTickets = h.tickets.slice(Math.max(0, n - 20)).filter(Number.isFinite)
  const avgTradeValue5 = median(h.tradeValue.slice(Math.max(0, n - 5)))
  const avgTradeValue20 = median(tailValue)
  const avgTradeTickets20 = median(tailTickets)

  // The institutional series, newest-first, skipping sessions the exchange did
  // not publish flow for (a missing flow day is not a zero-flow day). ONE
  // definition of "total" — the sum of the three nets — feeds deltaRatio and
  // the 5-session accumulations alike.
  let trustStreak: number | null = null
  for (let i = last; i >= 0; i--) {
    const t = h.trust[i]
    // A session with no published trust flow is NOT a zero-flow day: skip it
    // so an outage cannot reset a genuine buying streak to 0.
    if (t === null) continue
    if (t > 0) trustStreak = (trustStreak ?? 0) + 1
    else {
      trustStreak ??= 0
      break
    }
  }
  const inst = { foreign: 0, trust: 0, dealer: 0 }
  let instCount = 0
  for (let i = last; i >= 0 && instCount < 5; i--) {
    const f = h.foreign[i]
    const t = h.trust[i]
    const d = h.dealer[i]
    if (f === null && t === null && d === null) continue
    inst.foreign += f ?? 0
    inst.trust += t ?? 0
    inst.dealer += d ?? 0
    instCount++
  }
  const instTotal5 = instCount > 0 ? inst.foreign + inst.trust + inst.dealer : null

  // 20-session volatility (population stdev of daily % changes) and the
  // distance from the 20-session closing high.
  const closes = h.close.slice(Math.max(0, n - 21))
  const changes: number[] = []
  for (let i = 1; i < closes.length; i++) {
    if (closes[i - 1] > 0) changes.push(((closes[i] - closes[i - 1]) / closes[i - 1]) * 100)
  }
  const volatility20 = changes.length >= 6 ? stdev(changes.slice(-20)) : null
  const high20 = h.close.slice(Math.max(0, n - 20))
  let distFrom20dHigh: number | null = null
  if (high20.length >= 6) {
    const max = Math.max(...high20)
    const latest = high20[high20.length - 1]
    if (max > 0) distFrom20dHigh = ((max - latest) / max) * 100
  }

  const v: ScreenerRow = new Array(SCREENER_FIELDS.length).fill(null)
  v[SC.symbol] = seed.symbol
  v[SC.name] = seed.name
  v[SC.market] = seed.market
  v[SC.industry] = seed.industry
  v[SC.close] = close
  v[SC.changePct] = finiteOrNull(seed.daily.changePct) ?? null
  v[SC.volume] = finiteOrNull(seed.daily.volume) ?? null
  v[SC.tradeValue] = tradeValue
  v[SC.transactions] = finiteOrNull(seed.daily.transactions) ?? null
  v[SC.avgTradeValue5] = avgTradeValue5
  v[SC.avgTradeValue20] = avgTradeValue20
  v[SC.avgTradeTickets20] = avgTradeTickets20
  v[SC.sessions] = Math.min(n, 20)
  v[SC.per] = finiteOrNull(seed.valuation?.per) ?? carried('per')
  v[SC.pbr] = finiteOrNull(seed.valuation?.pbr) ?? carried('pbr')
  v[SC.divYield] = finiteOrNull(seed.valuation?.divYield) ?? carried('divYield')
  v[SC.marketCap] = close !== null && shares > 0 ? (shares * close) / 100 : null
  const credit = seed.margin
  const marginBal = finiteOrNull(credit?.marginBal)
  const marginPrev = finiteOrNull(credit?.marginPrev)
  const shortBal = finiteOrNull(credit?.shortBal)
  const shortPrev = finiteOrNull(credit?.shortPrev)
  v[SC.marginBal] = marginBal ?? carried('marginBal')
  v[SC.marginQuota] = finiteOrNull(credit?.quota) ?? carried('marginQuota')
  v[SC.shortBal] = shortBal ?? carried('shortBal')
  // The per-stock columns that used to be parsed and then dropped. The
  // 券限額 cell is NOT a column of its own: the exchange prints the SAME
  // 次一營業日限額 in both blocks (verified identical on 2 221/2 221 rows),
  // so `marginQuota` already carries it.
  v[SC.marginOffset] = finiteOrNull(credit?.offset) ?? null
  v[SC.marginNote] = credit?.note ? credit.note : null
  v[SC.sblBal] = finiteOrNull(seed.sbl?.sblBal) ?? carried('sblBal')
  // 信用交易 flows: the exchange's own columns; the CHANGE is balance −
  // previous balance (both published, so it survives a missing flow cell).
  v[SC.marginBuy] = finiteOrNull(credit?.marginBuy) ?? null
  v[SC.marginSell] = finiteOrNull(credit?.marginSell) ?? null
  v[SC.marginCashRepay] = finiteOrNull(credit?.marginCashRepay) ?? null
  v[SC.marginChange] = marginBal !== null && marginPrev !== null ? marginBal - marginPrev : null
  v[SC.shortSell] = finiteOrNull(credit?.shortSell) ?? null
  v[SC.shortCover] = finiteOrNull(credit?.shortCover) ?? null
  v[SC.shortCashRepay] = finiteOrNull(credit?.shortCashRepay) ?? null
  v[SC.shortChange] = shortBal !== null && shortPrev !== null ? shortBal - shortPrev : null
  // The stock-level estimates (never official): the rolling cost and the
  // maintenance ratio at the general 60 % ratio.
  v[SC.avgMarginCost] = seed.marginCost
  v[SC.estMaintenanceRatio] =
    seed.marginCost === null || close === null
      ? null
      : estimatedMaintenanceRatio(close / 100, seed.marginCost)
  v[SC.instForeign] = instForeign
  v[SC.instTrust] = instTrust
  v[SC.instDealer] = instDealer
  v[SC.instTotal] = instTotal
  // The session's margin AS KNOWN (the appended history row already carries
  // the last published quarter when the newest filing batch omitted it).
  const margin = (
    field: 'grossMargin' | 'opMargin' | 'pretaxMargin' | 'netMargin'
  ): number | null =>
    (last >= 0 ? h[field][last] : null) ?? finiteOrNull(seed.ratios?.[field]) ?? null
  v[SC.grossMargin] = margin('grossMargin')
  v[SC.opMargin] = margin('opMargin')
  v[SC.pretaxMargin] = margin('pretaxMargin')
  v[SC.netMargin] = margin('netMargin')
  v[SC.revenueMom] = finiteOrNull(seed.revenue?.revenueMom) ?? carried('revenueMom')
  v[SC.revenueYoy] = finiteOrNull(seed.revenue?.revenueYoy) ?? carried('revenueYoy')
  v[SC.revenueCumYoy] = finiteOrNull(seed.revenue?.revenueCumYoy) ?? carried('revenueCumYoy')
  v[SC.riskFlags] = seed.risk ?? carried('riskFlags')
  v[SC.paidInCapital] = seed.paidIn
  v[SC.instTrustStreak] = trustStreak
  v[SC.turnoverRate] =
    shares > 0 && Number.isFinite(seed.daily.volume)
      ? (seed.daily.volume / shares) * 100
      : carried('turnoverRate')
  v[SC.deltaRatio] = instDeltaRatio(instTotal, tradeValue)
  v[SC.open] = finiteOrNull(seed.daily.open) ?? null
  v[SC.high] = finiteOrNull(seed.daily.high) ?? null
  v[SC.low] = finiteOrNull(seed.daily.low) ?? null
  v[SC.volatility20] = volatility20
  v[SC.distFrom20dHigh] = distFrom20dHigh
  v[SC.instTotal5] = instTotal5
  v[SC.instForeign5] = instCount > 0 ? inst.foreign : null
  v[SC.instTrust5] = instCount > 0 ? inst.trust : null
  v[SC.instDealer5] = instCount > 0 ? inst.dealer : null
  v[SC.volRatio5] =
    avgTradeValue5 !== null && avgTradeValue5 > 0 && tradeValue !== null
      ? tradeValue / avgTradeValue5
      : null
  v[SC.ret1d] = returnOver(h.close, 1)
  v[SC.ret5d] = returnOver(h.close, 5)
  v[SC.ret20d] = returnOver(h.close, 20)
  v[SC.ret60d] = returnOver(h.close, 60)
  v[SC.ret240d] = returnOver(h.close, 240)
  v[SC.foreignRatio] = finiteOrNull(seed.foreign?.ratio) ?? carried('foreignRatio')
  const dayTradeShares = finiteOrNull(seed.dayTrade) ?? carried('dayTradeShares')
  const sessionVolume = finiteOrNull(seed.daily.volume)
  v[SC.dayTradeRatio] =
    dayTradeShares !== null && sessionVolume !== null && sessionVolume > 0
      ? (dayTradeShares / sessionVolume) * 100
      : null
  v[SC.bigHolderPct] = seed.bigHolder
  v[SC.retailPct] = seed.retail
  v[SC.holderCount] = seed.holders
  v[SC.bigHolder4w] = seed.bigHolder4w
  v[SC.exDivInDays] = seed.exDiv
  v[SC.listedDate] = seed.listed
  // 營收 TTM — the trailing 12 DISTINCT report months (the history repeats a
  // month's level on every session, so dedupe by `revenueYm`).
  const revenueByYm = new Map<number, number>()
  for (let i = last; i >= 0 && revenueByYm.size < 12; i--) {
    const ym = h.revenueYm[i]
    const level = h.revenueMonth[i]
    if (ym === null || level === null || revenueByYm.has(ym)) continue
    revenueByYm.set(ym, level)
  }
  v[SC.revenueTtm] =
    revenueByYm.size === 12 ? [...revenueByYm.values()].reduce((a, b) => a + b, 0) : null
  return v
}

function finiteOrNull(v: number | undefined | null): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null
}

function median(values: number[]): number | null {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b)
  if (sorted.length === 0) return null
  const mid = sorted.length >> 1
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

/** Population standard deviation. */
function stdev(values: number[]): number | null {
  if (values.length === 0) return null
  let sum = 0
  for (const v of values) sum += v
  const mean = sum / values.length
  let acc = 0
  for (const v of values) acc += (v - mean) * (v - mean)
  return Math.sqrt(acc / values.length)
}

/** Ordered, bounded-concurrency map (one R2 get+put pair per symbol). */
async function mapLimit<T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const out = new Array<R>(items.length)
  let next = 0
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const i = next++
      if (i >= items.length) return
      out[i] = await fn(items[i], i)
    }
  })
  await Promise.all(workers)
  return out
}
