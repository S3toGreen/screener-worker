/**
 * Stock screener wire contract — shared by the ETL worker (which builds it)
 * and the renderer (which renders it). The hub no longer builds rows.
 *
 * The grid crosses the wire as POSITIONAL rows (`ScreenerRow[]`, indexed by
 * `SC`): ~2 000 rows × 73 columns as objects would be a stringly-keyed blob
 * that every consumer re-walks. Prices are RAW ×100 ints (the app-wide
 * convention, `src/bars.ts`); NT$ money amounts are plain
 * numbers; a field the sources did not answer is `null` (never 0 — the UI
 * prints `–`).
 */

/** Column order of a `ScreenerRow` — the ONE schema the grid, the preview and
 *  the sort dropdown all index through. */
export const SCREENER_FIELDS = [
  'symbol',
  'name',
  'market',
  'industry',
  'close',
  'changePct',
  'volume',
  'tradeValue',
  'transactions',
  'avgTradeValue5',
  'avgTradeValue20',
  'avgTradeTickets20',
  'sessions',
  'per',
  'pbr',
  'divYield',
  'marketCap',
  'marginBal',
  'marginQuota',
  'shortBal',
  'sblBal',
  'instForeign',
  'instTrust',
  'instDealer',
  'instTotal',
  'grossMargin',
  'opMargin',
  'pretaxMargin',
  'netMargin',
  'revenueMom',
  'revenueYoy',
  'riskFlags',
  'paidInCapital',
  'instTrustStreak',
  'turnoverRate',
  'deltaRatio',
  'open',
  'high',
  'low',
  'volatility20',
  'distFrom20dHigh',
  'instTotal5',
  'instForeign5',
  'instTrust5',
  'instDealer5',
  'volRatio5',
  'ret1d',
  'ret5d',
  'ret20d',
  'ret60d',
  'ret240d',
  'foreignRatio',
  'dayTradeRatio',
  'bigHolderPct',
  'retailPct',
  'holderCount',
  'bigHolder4w',
  'revenueCumYoy',
  'exDivInDays',
  'revenueTtm',
  'listedDate',
  // 信用交易 flows (the exchange's own units; the snapshot's latest session).
  // 融券 (short sale) is DISTINCT from 借券賣出 (`sblBal` above).
  'marginBuy',
  'marginSell',
  'marginCashRepay',
  'marginChange',
  'shortSell',
  'shortCover',
  'shortCashRepay',
  'shortChange',
  // Estimates — see `./credit` (never presented as official): the
  // rolling average margin cost (NT$/share) and the stock-level maintenance
  // ratio at the general 60 % financing ratio. Both are CALCULATED: the
  // exchanges publish per-stock LOTS only (no 融資金額), so a per-stock cost
  // basis cannot be read — the rolling estimate is the one honest form.
  'avgMarginCost',
  'estMaintenanceRatio',
  'marginOffset',
  'marginNote'
] as const
export type ScreenerField = (typeof SCREENER_FIELDS)[number]
export type ScreenerRow = Array<string | number | null>

/** Field → column index (named, so no call site carries a magic number). */
export const SC = Object.fromEntries(SCREENER_FIELDS.map((f, i) => [f, i])) as Record<
  ScreenerField,
  number
>

/** The width of the row contract before the credit-trading extension — i.e.
 *  `SC[SCREENER_FIELDS[SCREENER_BASE_FIELDS]]` is the first appended column.
 *  Columns are APPEND-ONLY and every column past this point is nullable, so a
 *  snapshot written by an older server is still readable: `readSnapshotRows`
 *  pads the tail with nulls instead of rejecting the payload (the client and
 *  the ETL deploy independently — a hard width guard made every older snapshot
 *  read as "corrupt"). A row SHORTER than this is not a snapshot row. */
export const SCREENER_BASE_FIELDS = 61

/** The snapshot rows as the readers expect them: every row validated against
 *  the base width and padded to the full contract. Null when the payload is
 *  not a row grid at all (the caller falls back to its cache / errors). */
export function readSnapshotRows(raw: unknown): ScreenerRow[] | null {
  if (!Array.isArray(raw) || raw.length === 0) return null
  const out: ScreenerRow[] = []
  for (const item of raw) {
    if (!Array.isArray(item) || item.length < SCREENER_BASE_FIELDS) return null
    out.push(item.length >= SCREENER_FIELDS.length ? (item as ScreenerRow) : padRow(item))
  }
  return out
}

/** One row widened to the contract: the appended cells are null (unknown). */
function padRow(row: unknown[]): ScreenerRow {
  const full: ScreenerRow = row.slice() as ScreenerRow
  while (full.length < SCREENER_FIELDS.length) full.push(null)
  return full
}

/** A listed fund (ETF / ETN / REIT): `0[0-2]` + 2–4 digits + an optional
 *  letter. Funds have no company record, so `paidInCapital`/`marketCap`/
 *  `turnoverRate` stay null by construction and the fundamentals (PER, the
 *  three margins, revenue) do not exist — the grid tags the row instead of
 *  leaving a block of blank cells unexplained. */
const FUND_CODE = /^0[0-2][0-9]{2,4}[A-Z]?$/

export function isFundSymbol(symbol: string): boolean {
  return FUND_CODE.test(symbol)
}

/** The exchange vocabulary — ONE owner (the profile's market, the facet
 *  chips and the symbol search all speak it). */
export const MARKETS = ['TWSE', 'TPEX'] as const
export type MarketName = (typeof MARKETS)[number]

/** Numeric column value — the server guarantees `number | null` per field
 *  (shared contract: missing is null, never 0); anything else is UNKNOWN. */
export function screenNum(row: ScreenerRow, field: ScreenerField): number | null {
  const v = row[SC[field]]
  return typeof v === 'number' && Number.isFinite(v) ? v : null
}

/** String column value (symbol/name/market/industry); absent = ''. */
export function screenStr(row: ScreenerRow, field: ScreenerField): string {
  const v = row[SC[field]]
  return typeof v === 'string' ? v : ''
}

/** Risk / method-change flags, one bit per official list. A set bit means the
 *  name is excluded by default (`excludeRisk` in the client filter). */
export const RISK_NOTICE = 1 // 公布注意
export const RISK_DISPOSAL = 2 // 處置
export const RISK_ALTERED = 4 // 變更交易 (the 全額交割 mechanism)
export const RISK_STOP_FUNDS = 8 // 停資停券
export const RISK_STOP_TRADING = 16 // 暫停交易
export const RISK_MANAGED = 32 // 管理股票
export const RISK_PERIODIC = 64 // 分盤/定期定價
export const RISK_ANY = 127

export interface StockProfile {
  symbol: string
  /** The exchange's ABBREVIATION (TWSE 公司簡稱 / TPEx CompanyAbbreviation) —
   *  the legal registered name is a different column and is never shown. */
  name: string
  market: MarketName
  industry: string
  /** Funds have no company record — null, never a fabricated 0. */
  paidInCapital: number | null
  close: number
  changePct: number
  /** Fundamental cells are `null` when the session's source did not answer —
   *  the UI prints `–`, never a fabricated 0. */
  per: number | null
  pbr: number | null
  divYield: number | null
  marketCap: number | null
  grossMargin: number | null
  opMargin: number | null
  netMargin: number | null
  revenueYoy: number | null
  pretaxMargin: number | null
  /** 營收: the month-over-month %, the cumulative YoY % and the trailing-12-
   *  month level (NT$ thousands — the report's own unit). */
  revenueMom: number | null
  revenueCumYoy: number | null
  revenueTtm: number | null
  /** 籌碼: foreign holding (%), 當沖 volume share (%), TDCC weekly split. */
  foreignRatio: number | null
  dayTradeRatio: number | null
  bigHolderPct: number | null
  retailPct: number | null
  holderCount: number | null
  bigHolder4w: number | null
  /** 信用交易 balances and flows (the exchange's own units). 融券 (`short*`)
   *  is DISTINCT from 借券賣出 (`sblBal`). */
  marginBal: number | null
  marginQuota: number | null
  marginBuy: number | null
  marginSell: number | null
  marginCashRepay: number | null
  marginChange: number | null
  shortBal: number | null
  shortSell: number | null
  shortCover: number | null
  shortCashRepay: number | null
  shortChange: number | null
  sblBal: number | null
  /** ESTIMATED credit metrics (see `./credit`) — NT$ per share, the
   *  general financing ratio (percent) and the resulting maintenance ratio. */
  avgMarginCost: number | null
  estMaintenanceRatio: number | null
  /** Days to the next known ex-date (0 = today) and the listing date as a
   *  `YYYYMMDD` stamp (funds: null). */
  exDivInDays: number | null
  listedDate: string | null
  riskFlags: number
  /** The session the row's prices belong to (ms of Taipei midnight). */
  sessionDate: number
}

/** One session of institutional net flow (NT$; TWSE's own unit is shares for
 *  some rows, so the reader normalizes to NT$ via the close). */
export interface FlowRow {
  sessionDate: number
  foreign: number
  trust: number
  dealer: number
  total: number
}

/** One daily candle (raw ×100 prices) — the preview chart's input. */
export interface Candle1D {
  tsMs: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

/** One session's non-price extras for the preview's lower pane — index-aligned
 *  with the bundle's candles. Each is a published number or the first-
 *  principles quotient of two published levels; null = that session's source
 *  was absent (the pane skips the point, never draws a zero). */
export interface PreviewSession {
  /** 當沖 shares ÷ session volume × 100. */
  dayTradePct: number | null
  /** 外資持股比率 (%). */
  foreignPct: number | null
  /** 累計營收年增率 (%), the value AS KNOWN that session. */
  cumRevYoy: number | null
  /** 融資使用率 = 融資餘額 ÷ the exchange's NEXT-session 融資限額 × 100. */
  marginUse: number | null
  /** 券資比 = 融券餘額 ÷ 融資餘額 × 100. */
  shortRatio: number | null
}

export interface StockPreviewBundle {
  symbol: string
  profile: StockProfile | null
  flow: FlowRow[]
  candles: Candle1D[]
  /** Index-aligned with `candles`. */
  sessions: PreviewSession[]
}

/** The R2 object key + URL of the daily snapshot (the ONE snapshot seam). */
export const SCREENER_SNAPSHOT_KEY = 'screener-latest.json'
/** The market-wide credit/derivatives object (separate from the stock grid). */
export const SCREENER_MARKET_KEY = 'market-latest.json'
/** Rolling market observations retained inside the market object (≈3 months
 *  of sessions). The per-stock histories are the longer archive; this series
 *  is the overview's change/trend source and stays small enough for the
 *  client's critical cache. */
export const MARKET_HISTORY_SESSIONS = 60

/** The rolling per-symbol margin-cost state (one small object; the ETL is its
 *  only writer). No history is stored — see `./credit`. */
export const SCREENER_CREDIT_STATE_KEY = 'credit-state.json'

/** The public API routes (`serve.ts` maps them to the bucket keys above). */
export const SCREENER_SNAPSHOT_PATH = '/api/snapshot'
export const SCREENER_MARKET_PATH = '/api/market'
export const SCREENER_HISTORY_PREFIX = '/api/history/'

/** The public API route (`/api/snapshot`); the R2 object key stays
 *  `SCREENER_SNAPSHOT_KEY` — the Pages Function maps route → key. */
export function screenerSnapshotUrl(base: string): string {
  return `${base.replace(/\/+$/, '')}${SCREENER_SNAPSHOT_PATH}`
}

/** The market-context route (`/api/market`). */
export function screenerMarketUrl(base: string): string {
  return `${base.replace(/\/+$/, '')}${SCREENER_MARKET_PATH}`
}

/** TAIEX returns over the SAME trading-session horizons as the row columns
 *  (`ret1d` = 1 session, `ret5d` = 5 sessions ≈ a week, … `ret240d` ≈ a year). */
export interface ScreenerBenchmark {
  /** The index LEVEL at the snapshot session (e.g. 22 450.15) — the pill's
   *  headline; absent on a snapshot written before the field existed. */
  close?: number
  ret1d: number | null
  ret5d: number | null
  ret20d: number | null
  ret60d: number | null
  ret240d: number | null
}

/** One session's MARKET-WIDE credit statistics — the 上市 (TWSE) + 上櫃 (TPEx)
 *  combination.
 *
 *  The 12 lot fields are TWSE's own 信用交易統計 (table 0 of its per-date
 *  `MI_MARGN`), unchanged in meaning. TPEx publishes per-stock LOTS and a
 *  market summary in MONEY (億元) on its dashboard — the lot totals below are
 *  the Σ of its per-stock balance table (the only lot totals it offers).
 *
 *  `maintenanceRatio` is OUR estimate of the 上市櫃 融資維持率:
 *      Σ(margin balance lots × 10 × close) ÷ (TWSE 融資金額 + TPEx 融資餘額)
 *  — i.e. the financed market value (ETFs included) over the announced loan
 *  balance. The exchanges' announced balances include ETFs too. `scope` names
 *  the markets both halves actually covered (a missing TPEx summary falls back
 *  to the TWSE-only estimate rather than inflating the ratio).
 *
 *  `officialMaintenanceRatio` is TWSE's OWN published 全市場擔保維持率
 *  (`BFIJ3U_TREND` keepRate) — the exchange's figure, TWSE-only, carried only
 *  when it is the session's. */
export interface MarketCredit {
  scope: 'TWSE+TPEX' | 'TWSE'
  sessionDate: number
  // The per-market blocks are SYMMETRIC: {market}{side}{measure}{unit} —
  // `Lots` = 交易單位, `Ntd` = NT$ — and each block lists the same series in
  // the same order (買進 · 賣出 · 現金償還 · 餘額 · 前日 · 增減).
  /** 上市 — the TWSE credit summary's whole-market rows. */
  twseMarginBuyLots: number | null
  twseMarginSellLots: number | null
  twseMarginCashRepayLots: number | null
  twseMarginBalanceLots: number | null
  twseMarginPrevLots: number | null
  twseMarginChangeLots: number | null
  twseShortSellLots: number | null
  twseShortCoverLots: number | null
  twseShortCashRepayLots: number | null
  twseShortBalanceLots: number | null
  twseShortPrevLots: number | null
  twseShortChangeLots: number | null
  /** The SAME report row in MONEY (仟元 ×1000): 融資金額 — the loan
   *  denominator's TWSE half — plus its four flow columns. */
  twseMarginBalanceNtd: number | null
  twseMarginBuyNtd: number | null
  twseMarginSellNtd: number | null
  twseMarginCashRepayNtd: number | null
  twseMarginPrevNtd: number | null
  /** 上櫃 — the per-stock table's WHOLE-MARKET totals (ETFs included). */
  tpexMarginBuyLots: number | null
  tpexMarginSellLots: number | null
  tpexMarginCashRepayLots: number | null
  tpexMarginBalanceLots: number | null
  tpexMarginPrevLots: number | null
  tpexMarginChangeLots: number | null
  tpexShortSellLots: number | null
  tpexShortCoverLots: number | null
  tpexShortCashRepayLots: number | null
  tpexShortBalanceLots: number | null
  tpexShortPrevLots: number | null
  tpexShortChangeLots: number | null
  /** The OTC dashboard's money summary (億元 ×1e8). */
  tpexMarginBalanceNtd: number | null
  tpexCreditAmountNtd: number | null
  tpexMarketCapNtd: number | null
  /** Σ margin balance lots × price over the markets in `scope` (NT$). */
  financedValueNtd: number | null
  /** The announced loan balance: TWSE 融資金額 (+ TPEx 融資餘額), NT$. */
  loanBalanceNtd: number | null
  /** Σ the SNAPSHOT ROWS' market caps over the markets in `scope`. Listed funds
   *  without a share count are excluded from BOTH halves; the OTC dashboard's
   *  whole-market cap is carried separately (`tpexMarketCapNtd`) and is never
   *  mixed into this denominator. */
  marketCapNtd: number | null
  /** loanBalanceNtd ÷ marketCapNtd × 100. */
  marginToMarketCap: number | null
  /** financedValueNtd ÷ loanBalanceNtd × 100 — an ESTIMATE (see the doc above). */
  maintenanceRatio: number | null
  maintenanceRatioKind: 'estimate'
  /** Provenance line the panel prints under the value. */
  maintenanceRatioNote: string
  /** TWSE's own 整戶擔保維持率 (`BFIJ3U_TREND` keepRate), same session only. */
  officialMaintenanceRatio: number | null
}

/** One 三大法人 row (futures and options are reported SEPARATELY). */
export interface TaifexInstitution {
  item: string
  futuresLong: number | null
  futuresShort: number | null
  futuresNet: number | null
  futuresOiLong: number | null
  futuresOiShort: number | null
  futuresOiNet: number | null
  optionsLong: number | null
  optionsShort: number | null
  optionsNet: number | null
  optionsOiLong: number | null
  optionsOiShort: number | null
  optionsOiNet: number | null
}

/** One contract's institutional position (futures, or one option side). */
export interface TaifexContractPosition {
  /** The exchange's published contract name (`臺股期貨`, `臺指選擇權`). */
  contract: string
  item: string
  long: number | null
  short: number | null
  net: number | null
  oiLong: number | null
  oiShort: number | null
  oiNet: number | null
  /** Net OI value in NT$ (the report's 仟元 ×1000). */
  oiValueNtd: number | null
}

export interface TaifexOptionSide extends TaifexContractPosition {
  callPut: 'CALL' | 'PUT'
}

/** One large-trader OI row. `traderType` is the exchange's own bucket label
 *  ('0' / '1' — the 5/10-trader classification); `marketOi` is the published
 *  market-wide OI the percentages are taken against. The DAILY context carries
 *  the exchange's ALL-MONTHS aggregate row only (its `999912` settlement
 *  bucket) — per-month detail is never stored. */
export interface TaifexLargeTrader {
  contract: string
  name: string
  product: 'futures' | 'options'
  callPut: 'CALL' | 'PUT' | null
  traderType: string
  top5Buy: number | null
  top5Sell: number | null
  top10Buy: number | null
  top10Sell: number | null
  marketOi: number | null
}

/** 臺指選擇權 Put/Call statistics — volume- and OI-based ratios are DISTINCT. */
export interface TaifexPcr {
  dateMs: number
  putVolume: number | null
  callVolume: number | null
  /** 賣權成交量 ÷ 買權成交量 × 100 (the exchange's own published %). */
  volumeRatio: number | null
  putOi: number | null
  callOi: number | null
  /** 賣權未平倉 ÷ 買權未平倉 × 100. */
  oiRatio: number | null
}

/** One observation of the TAIFEX context — the market object retains a rolling
 *  series of these observations for the overview's changes and trend. */
export interface MarketDerivatives {
  /** The session the TAIFEX payloads report (ms) — may lag the equity anchor. */
  asOf: number
  institutions: TaifexInstitution[]
  /** Futures positions by contract, the app's index roster first. */
  futures: TaifexContractPosition[]
  /** Option positions by contract and side (calls and puts separate). */
  options: TaifexOptionSide[]
  largeTraders: TaifexLargeTrader[]
  /** Newest first (the endpoint's own ~19-session window). */
  pcr: TaifexPcr[]
  vix: { dateMs: number; close: number } | null
}

/** One past session's market observation — the same credit/derivatives halves
 *  the live context carries, retained so the overview can show changes and a
 *  trend instead of only today's level. */
export interface MarketHistoryPoint {
  sessionDate: number
  credit: MarketCredit | null
  derivatives: MarketDerivatives | null
}

/** The standalone market-context object (`market-latest.json`, `/api/market`).
 *  The STOCK SCREENER snapshot carries per-stock rows only — market-wide credit
 *  and TAIFEX context is its own object with its own route, cache and refresh,
 *  so neither payload can bloat or block the other. */
export interface MarketContext {
  /** The equity session the credit half belongs to (Taipei midnight, ms); the
   *  derivatives half carries its own, possibly lagging, session. */
  asOf: number
  credit: MarketCredit | null
  derivatives: MarketDerivatives | null
  /** Rolling observations, oldest first. The daily writer stores the current
   *  observation when the series is missing; a full series can be uploaded
   *  out-of-band (`seedMarketHistory.ts`). The daily writer appends/replaces
   *  the current session thereafter. Capped to `MARKET_HISTORY_SESSIONS`. */
  history?: MarketHistoryPoint[]
}

/** The snapshot payload `screener-latest.json` carries: the session's per-stock
 *  rows plus the TAIEX reference. Nothing market-wide lives here. */
export interface ScreenerSnapshot {
  /** ms of Taipei midnight for the session the rows belong to. */
  sessionDate: number
  /** The TAIEX context the preview header shows; absent on an older snapshot. */
  benchmark?: ScreenerBenchmark | null
  rows: ScreenerRow[]
}

/** `benchmark.json` — the TAIEX close series the returns are derived from,
 *  oldest first, index-aligned (the row-history shape without the per-stock
 *  columns). One object, not one per day. */
export interface BenchmarkSeries {
  dates: string[]
  close: number[]
}

/** Per-session history columns — the ONE list the ETL appends, the backfill
 *  writes and the shape guard walks (the price/volume/flow columns are
 *  REQUIRED: a file missing them predates the contract and is rebuilt; the
 *  fundamental/chip columns are OPTIONAL at read time so an older file never
 *  reads as corrupt). Every array is index-aligned on `dates`. */
export const SCREENER_HISTORY_FIELDS = [
  'open',
  'high',
  'low',
  'close',
  'volume',
  'tickets',
  'tradeValue',
  'turnoverRate',
  'foreign',
  'trust',
  'dealer',
  'per',
  'pbr',
  'divYield',
  'marginBal',
  'marginQuota',
  'shortBal',
  'sblBal',
  'riskFlags',
  'grossMargin',
  'opMargin',
  'pretaxMargin',
  'netMargin',
  'revenueMom',
  'revenueYoy',
  'foreignShares',
  'foreignRatio',
  'dayTradeShares',
  'revenueCumYoy',
  'revenueMonth',
  'revenueYm'
] as const
export type ScreenerHistoryField = (typeof SCREENER_HISTORY_FIELDS)[number]
/** The columns a history file has ALWAYS carried (open..dealer = 11) — the
 *  guard requires these; a missing one predates the contract and is rebuilt. */
export const SCREENER_HISTORY_CORE = SCREENER_HISTORY_FIELDS.slice(0, 11) as [
  'open',
  'high',
  'low',
  'close',
  'volume',
  'tickets',
  'tradeValue',
  'turnoverRate',
  'foreign',
  'trust',
  'dealer'
]

/** The ONE history retention window (≈3 trading years): the ETL prunes to it,
 *  the backfill rebuilds it, the client trims its CACHED copy further (local
 *  storage quota) — never a second cap. */
export const SCREENER_HISTORY_SESSIONS = 780

/** `history/{symbol}.json` — up to `SCREENER_HISTORY_SESSIONS` TRADING
 *  sessions (a closed day writes no stamp), oldest first, every array
 *  index-aligned on `dates`. Prices are raw ×100; margin/SBL balances are the
 *  EXCHANGE's units (the 融資 units the daily readers use); the quarterly
 *  margin/revenue columns carry the value AS KNOWN that session (a null =
 *  that session's source was absent). The core columns exist since the first
 *  history; the rest accrue per session. */
export interface ScreenerHistory {
  symbol: string
  /** `YYYYMMDD` session stamps (the Taipei calendar day). */
  dates: string[]
  open: number[]
  high: number[]
  low: number[]
  close: number[]
  volume: number[]
  tickets: number[]
  /** NT$ turnover. */
  tradeValue: number[]
  turnoverRate: (number | null)[]
  foreign: (number | null)[]
  trust: (number | null)[]
  dealer: (number | null)[]
  per: (number | null)[]
  pbr: (number | null)[]
  divYield: (number | null)[]
  marginBal: (number | null)[]
  marginQuota: (number | null)[]
  shortBal: (number | null)[]
  sblBal: (number | null)[]
  riskFlags: (number | null)[]
  grossMargin: (number | null)[]
  opMargin: (number | null)[]
  pretaxMargin: (number | null)[]
  netMargin: (number | null)[]
  revenueMom: (number | null)[]
  revenueYoy: (number | null)[]
  /** 外資持股: shares held by foreign investors and the holding ratio (%) —
   *  the exchanges' own per-date reports (TWSE `MI_QFIIS` / TPEx `qfii`), read
   *  by the daily ETL AND the backfill. */
  foreignShares: (number | null)[]
  foreignRatio: (number | null)[]
  /** 當沖 (day-trade) volume in SHARES — the exchange's own per-session report
   *  (TWSE `TWTB4U` / TPEx `intraday/stat`). `0` = the symbol traded but with
   *  no day-trade that session (the per-stock rows sum to the market total, so
   *  absence is a real zero); `null` = the session's report was absent. The
   *  visible ratio derives against `volume`. */
  dayTradeShares: (number | null)[]
  /** 累計營收年增率 (%) — the MOPS report's own 前期比較增減, the value AS
   *  KNOWN that session (it steps once a month, on the 10th). */
  revenueCumYoy: (number | null)[]
  /** 當月營收 (NT$ thousands, the report's own unit) and the report's month
   *  (`YYYYMM`) — the primitive the snapshot's trailing-12-month revenue
   *  derives from. */
  revenueMonth: (number | null)[]
  revenueYm: (number | null)[]
}

export function screenerHistoryUrl(base: string, symbol: string): string {
  return `${base.replace(/\/+$/, '')}${SCREENER_HISTORY_PREFIX}${encodeURIComponent(symbol)}`
}

/** The weekly TDCC 集保 object's route (`holdings.json` — ONE market-wide
 *  object, not one per symbol: the weekly job scrapes every listing). */
export const SCREENER_HOLDINGS_PATH = '/api/holdings'
/** The ex-dividend calendar's route (`dividends.json` — the exchange's 3-week
 *  window accumulated across runs). */
export const SCREENER_DIVIDENDS_PATH = '/api/dividends'

export function screenerHoldingsUrl(base: string): string {
  return `${base.replace(/\/+$/, '')}${SCREENER_HOLDINGS_PATH}`
}

export function screenerDividendsUrl(base: string): string {
  return `${base.replace(/\/+$/, '')}${SCREENER_DIVIDENDS_PATH}`
}

/** [千張大戶 %, ≤50張散戶 %, 集保總戶數] — the TDCC docs' own denominator is
 *  the 集保 custody balance (not issued shares), so the two shares do not sum
 *  to 100. `retail` is null on a v1 object (before the bracket split). */
export type HoldingsRow = [big: number, retail: number | null, holders: number]

export interface HoldingsWeek {
  /** `YYYYMMDD` — the TDCC reporting week. */
  week: string
  symbols: Record<string, HoldingsRow>
}

/** `holdings.json` — newest week first, at most `HOLDINGS_WEEKS` (156 = THREE
 *  YEARS of weekly publishes, matching the 780-session history window). The
 *  per-symbol history deliberately does NOT carry this: a weekly datum
 *  repeated across every session would be noise in the 780-session window. */
export interface HoldingsObject {
  updatedAt: number
  weeks: HoldingsWeek[]
}

/** Three years of weekly TDCC publishes. The object is market-wide, so it
 *  grows ~60 KB/week (all stocks) — the CLIENT caches only the newest
 *  `HOLDINGS_CACHE_WEEKS` in localStorage (the source stays complete). */
export const HOLDINGS_WEEKS = 156
/** The localStorage copy's horizon (a quota-safe window; the fetched object is
 *  always full). */
export const HOLDINGS_CACHE_WEEKS = 26

export function isHoldingsObject(raw: unknown): raw is HoldingsObject {
  if (!raw || typeof raw !== 'object') return false
  const o = raw as HoldingsObject
  if (!Array.isArray(o.weeks) || o.weeks.length === 0) return false
  for (const w of o.weeks) {
    if (!w || typeof w !== 'object') return false
    if (typeof w.week !== 'string' || !/^\d{8}$/.test(w.week)) return false
    const symbols = w.symbols as Record<string, unknown> | undefined
    if (!symbols || typeof symbols !== 'object') return false
    for (const v of Object.values(symbols)) {
      if (!Array.isArray(v) || v.length < 3) return false
      if (typeof v[0] !== 'number' || !Number.isFinite(v[0])) return false
      if (v[1] !== null && (typeof v[1] !== 'number' || !Number.isFinite(v[1]))) return false
      if (typeof v[2] !== 'number' || !Number.isFinite(v[2])) return false
    }
  }
  return true
}

/** One announced ex-dividend event. `cash` is NT$ per share (null when the
 *  issuer has not announced — ETFs often show "待公告"), `stockRatio` the
 *  無償配股率 (shares per share), `kind` the exchange's 權 | 息 | 權息 cell. */
export interface DividendEvent {
  symbol: string
  /** `YYYYMMDD` ex-date. */
  exDate: string
  cash: number | null
  stockRatio: number | null
  kind: string
}

/** `dividends.json` — events sorted by ex-date, newest run's window folded in. */
export interface DividendsObject {
  updatedAt: number
  events: DividendEvent[]
}

/** One symbol's weekly TDCC point — the object projected onto one code. */
export interface HoldingPoint {
  week: string
  big: number
  retail: number | null
  holders: number
}

/** One symbol's weekly points in CHART order (oldest first) — a week the
 *  portal did not answer for the symbol is skipped, never zero-filled. */
export function holdingsSeriesOf(holdings: HoldingsObject | null, symbol: string): HoldingPoint[] {
  if (!holdings) return []
  const out: HoldingPoint[] = []
  for (let i = holdings.weeks.length - 1; i >= 0; i--) {
    const w = holdings.weeks[i]
    const v = w.symbols[symbol]
    if (!v) continue
    out.push({ week: w.week, big: v[0], retail: v[1], holders: v[2] })
  }
  return out
}

/** The symbol's announced ex-dividend events, oldest first (the calendar folds
 *  every run's window in, so one ex-date never appears twice). */
export function dividendsOf(calendar: DividendsObject | null, symbol: string): DividendEvent[] {
  if (!calendar) return []
  return calendar.events
    .filter((e) => e.symbol === symbol)
    .sort((a, b) => a.exDate.localeCompare(b.exDate))
}

export function isDividendsObject(raw: unknown): raw is DividendsObject {
  if (!raw || typeof raw !== 'object') return false
  const d = raw as DividendsObject
  if (!Array.isArray(d.events)) return false
  for (const e of d.events) {
    if (!e || typeof e !== 'object') return false
    if (typeof e.symbol !== 'string' || e.symbol.length === 0) return false
    if (typeof e.exDate !== 'string' || !/^\d{8}$/.test(e.exDate)) return false
  }
  return true
}

/** The ONE history shape guard: the core arrays MUST be index-aligned on
 *  `dates` and present; later-added columns may be absent (an older file) but
 *  when present must align too. The same contract every consumer walks. */
export function isScreenerHistory(raw: unknown, symbol?: string): raw is ScreenerHistory {
  if (!raw || typeof raw !== 'object') return false
  const h = raw as ScreenerHistory
  if (symbol !== undefined && h.symbol !== symbol) return false
  if (typeof h.symbol !== 'string' || !Array.isArray(h.dates)) return false
  const n = h.dates.length
  for (const field of SCREENER_HISTORY_CORE) {
    const list = h[field]
    if (!Array.isArray(list) || list.length !== n) return false
  }
  for (const field of SCREENER_HISTORY_FIELDS) {
    if (SCREENER_HISTORY_CORE.includes(field as never)) continue
    const list = (h as unknown as Record<string, unknown>)[field]
    if (list === undefined) continue
    if (!Array.isArray(list) || list.length !== n) return false
  }
  return true
}

/** Return over `sessions` TRADING SESSIONS of an index-aligned close series —
 *  null until the series holds N+1 entries (the row columns and the TAIEX
 *  benchmark share this ONE formula). */
export function returnOver(closes: readonly number[], sessions: number): number | null {
  const last = closes.length - 1
  const from = last - sessions
  if (from < 0) return null
  const a = closes[from]
  const b = closes[last]
  return a > 0 && b > 0 ? (b / a - 1) * 100 : null
}

/** Institutional net flow as a share of the session's turnover (%), the
 *  snapshot's `deltaRatio` column — ONE definition, the worker's only writer. */
export function instDeltaRatio(flow: number | null, tradeValue: number | null): number | null {
  if (flow === null || tradeValue === null || !(tradeValue > 0)) return null
  return (flow / tradeValue) * 100
}

/** A wire number: finite numbers only, everything else null. */
function marketNum(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null
}

/** A wire string ('' when absent). */
function marketStr(v: unknown): string {
  return typeof v === 'string' ? v : ''
}

/**
 * Reads the snapshot's `market` block defensively. The ETL writes it typed;
 * this exists for a cached/older snapshot and for the client's own storage —
 * a malformed section degrades to null, never to fabricated values.
 */
export function readMarketContext(raw: unknown): MarketContext | null {
  if (!raw || typeof raw !== 'object') return null
  const v = raw as Record<string, unknown>
  const credit =
    v.credit && typeof v.credit === 'object' ? (v.credit as Record<string, unknown>) : null
  const der =
    v.derivatives && typeof v.derivatives === 'object'
      ? (v.derivatives as Record<string, unknown>)
      : null
  const out: MarketContext = { asOf: 0, credit: null, derivatives: null }
  if (
    credit &&
    (credit.scope === 'TWSE+TPEX' || credit.scope === 'TWSE') &&
    typeof credit.sessionDate === 'number'
  ) {
    // Migration: the pre-split object carried the OFFICIAL ratio in
    // `maintenanceRatio` (kind 'official'). It is moved to its own slot — an
    // official number is never re-labelled as our estimate.
    const official =
      credit.maintenanceRatioKind === 'official'
        ? marketNum(credit.maintenanceRatio)
        : marketNum(credit.officialMaintenanceRatio)
    out.credit = {
      scope: credit.scope,
      sessionDate: credit.sessionDate,
      twseMarginBuyLots: marketNum(credit.twseMarginBuyLots),
      twseMarginSellLots: marketNum(credit.twseMarginSellLots),
      twseMarginCashRepayLots: marketNum(credit.twseMarginCashRepayLots),
      twseMarginBalanceLots: marketNum(credit.twseMarginBalanceLots),
      twseMarginPrevLots: marketNum(credit.twseMarginPrevLots),
      twseMarginChangeLots: marketNum(credit.twseMarginChangeLots),
      twseShortSellLots: marketNum(credit.twseShortSellLots),
      twseShortCoverLots: marketNum(credit.twseShortCoverLots),
      twseShortCashRepayLots: marketNum(credit.twseShortCashRepayLots),
      twseShortBalanceLots: marketNum(credit.twseShortBalanceLots),
      twseShortPrevLots: marketNum(credit.twseShortPrevLots),
      twseShortChangeLots: marketNum(credit.twseShortChangeLots),
      twseMarginBalanceNtd: marketNum(credit.twseMarginBalanceNtd),
      twseMarginBuyNtd: marketNum(credit.twseMarginBuyNtd),
      twseMarginSellNtd: marketNum(credit.twseMarginSellNtd),
      twseMarginCashRepayNtd: marketNum(credit.twseMarginCashRepayNtd),
      twseMarginPrevNtd: marketNum(credit.twseMarginPrevNtd),
      tpexMarginBuyLots: marketNum(credit.tpexMarginBuyLots),
      tpexMarginSellLots: marketNum(credit.tpexMarginSellLots),
      tpexMarginCashRepayLots: marketNum(credit.tpexMarginCashRepayLots),
      tpexMarginBalanceLots: marketNum(credit.tpexMarginBalanceLots),
      tpexMarginPrevLots: marketNum(credit.tpexMarginPrevLots),
      tpexMarginChangeLots: marketNum(credit.tpexMarginChangeLots),
      tpexShortSellLots: marketNum(credit.tpexShortSellLots),
      tpexShortCoverLots: marketNum(credit.tpexShortCoverLots),
      tpexShortCashRepayLots: marketNum(credit.tpexShortCashRepayLots),
      tpexShortBalanceLots: marketNum(credit.tpexShortBalanceLots),
      tpexShortPrevLots: marketNum(credit.tpexShortPrevLots),
      tpexShortChangeLots: marketNum(credit.tpexShortChangeLots),
      tpexMarginBalanceNtd: marketNum(credit.tpexMarginBalanceNtd),
      tpexCreditAmountNtd: marketNum(credit.tpexCreditAmountNtd),
      tpexMarketCapNtd: marketNum(credit.tpexMarketCapNtd),
      financedValueNtd: marketNum(credit.financedValueNtd),
      loanBalanceNtd: marketNum(credit.loanBalanceNtd),
      marketCapNtd: marketNum(credit.marketCapNtd),
      marginToMarketCap: marketNum(credit.marginToMarketCap),
      // Never re-label: only a writer that declares the ESTIMATE may publish a
      // computed ratio (an older object's value was official and lives above).
      maintenanceRatio:
        credit.maintenanceRatioKind === 'estimate' ? marketNum(credit.maintenanceRatio) : null,
      maintenanceRatioKind: 'estimate',
      maintenanceRatioNote: marketStr(credit.maintenanceRatioNote),
      officialMaintenanceRatio: official
    }
  }
  if (der && typeof der.asOf === 'number') {
    const list = (key: string): Array<Record<string, unknown>> =>
      Array.isArray(der[key])
        ? (der[key] as unknown[]).filter(
            (x): x is Record<string, unknown> => !!x && typeof x === 'object'
          )
        : []
    const position = (r: Record<string, unknown>): TaifexContractPosition => ({
      contract: marketStr(r.contract),
      item: marketStr(r.item),
      long: marketNum(r.long),
      short: marketNum(r.short),
      net: marketNum(r.net),
      oiLong: marketNum(r.oiLong),
      oiShort: marketNum(r.oiShort),
      oiNet: marketNum(r.oiNet),
      oiValueNtd: marketNum(r.oiValueNtd)
    })
    out.derivatives = {
      asOf: der.asOf,
      institutions: list('institutions').map((r) => ({
        item: marketStr(r.item),
        futuresLong: marketNum(r.futuresLong),
        futuresShort: marketNum(r.futuresShort),
        futuresNet: marketNum(r.futuresNet),
        futuresOiLong: marketNum(r.futuresOiLong),
        futuresOiShort: marketNum(r.futuresOiShort),
        futuresOiNet: marketNum(r.futuresOiNet),
        optionsLong: marketNum(r.optionsLong),
        optionsShort: marketNum(r.optionsShort),
        optionsNet: marketNum(r.optionsNet),
        optionsOiLong: marketNum(r.optionsOiLong),
        optionsOiShort: marketNum(r.optionsOiShort),
        optionsOiNet: marketNum(r.optionsOiNet)
      })),
      futures: list('futures').map(position),
      options: list('options').map((r) => ({
        ...position(r),
        callPut: r.callPut === 'PUT' ? 'PUT' : 'CALL'
      })),
      largeTraders: list('largeTraders').map((r) => ({
        contract: marketStr(r.contract),
        name: marketStr(r.name),
        product: r.product === 'options' ? 'options' : 'futures',
        callPut: r.callPut === 'CALL' || r.callPut === 'PUT' ? r.callPut : null,
        traderType: marketStr(r.traderType),
        top5Buy: marketNum(r.top5Buy),
        top5Sell: marketNum(r.top5Sell),
        top10Buy: marketNum(r.top10Buy),
        top10Sell: marketNum(r.top10Sell),
        marketOi: marketNum(r.marketOi)
      })),
      pcr: list('pcr')
        .map((r) => ({
          dateMs: marketNum(r.dateMs) ?? 0,
          putVolume: marketNum(r.putVolume),
          callVolume: marketNum(r.callVolume),
          volumeRatio: marketNum(r.volumeRatio),
          putOi: marketNum(r.putOi),
          callOi: marketNum(r.callOi),
          oiRatio: marketNum(r.oiRatio)
        }))
        .filter((r) => r.dateMs > 0),
      vix:
        der.vix && typeof der.vix === 'object'
          ? (() => {
              const x = der.vix as Record<string, unknown>
              const close = marketNum(x.close)
              const dateMs = marketNum(x.dateMs)
              return close !== null && dateMs !== null ? { dateMs, close } : null
            })()
          : null
    }
  }
  // A history point is parsed by the SAME reader: a malformed/old point
  // degrades to absent, never to fabricated values. The recursion passes only
  // the two halves, so a point can never re-open a nested history.
  const points: MarketHistoryPoint[] = []
  if (Array.isArray(v.history)) {
    for (const item of v.history) {
      if (!item || typeof item !== 'object') continue
      const p = item as Record<string, unknown>
      const point = readMarketContext({
        asOf: p.sessionDate,
        credit: p.credit,
        derivatives: p.derivatives
      })
      if (!point) continue
      points.push({
        sessionDate: point.asOf,
        credit: point.credit,
        derivatives: point.derivatives
      })
    }
  }
  const byDate = new Map<number, MarketHistoryPoint>()
  for (const point of points) byDate.set(point.sessionDate, point)
  const history = [...byDate.values()]
    .sort((a, b) => a.sessionDate - b.sessionDate)
    .slice(-MARKET_HISTORY_SESSIONS)
  if (out.credit === null && out.derivatives === null && history.length === 0) return null
  // The object's own date (the equity anchor); an older writer may omit it, so
  // the halves' own stamps stand in — never a fabricated date.
  out.asOf =
    marketNum(v.asOf) ??
    out.credit?.sessionDate ??
    out.derivatives?.asOf ??
    history[history.length - 1]?.sessionDate ??
    0
  if (out.asOf <= 0) return null
  const bounded = history.filter((point) => point.sessionDate <= out.asOf)
  if (bounded.length > 0) out.history = bounded
  return out
}

/** Same-session re-run merge for the market object: a source that failed this
 *  pass must never erase a cell it published earlier that session. New finite
 *  values win; only null/empty cells fall back to the prior observation. */
export function mergeMarketCredit(prior: MarketCredit, next: MarketCredit): MarketCredit {
  // A scope downgrade (the repair lost one market) invalidates the derived
  // financed/loan/cap/ratio block. Keep the prior SELF-CONSISTENT observation
  // for this same session; a later repair that restores both markets can
  // upgrade it. A scope upgrade uses the new complete block.
  if (prior.scope !== next.scope && next.scope !== 'TWSE+TPEX') return prior
  const out = { ...next } as Record<string, unknown>
  for (const [key, old] of Object.entries(prior)) {
    const cur = out[key]
    if ((cur === null || cur === undefined || cur === '') && old !== null && old !== '') {
      out[key] = old
    }
  }
  return out as unknown as MarketCredit
}

/** Fills only null/empty cells of ONE row from its prior-session row. */
function mergeRow<T extends object>(prior: T, next: T): T {
  const out = { ...next } as Record<string, unknown>
  for (const [key, old] of Object.entries(prior as Record<string, unknown>)) {
    const cur = out[key]
    if ((cur === null || cur === undefined || cur === '') && old !== null && old !== '') {
      out[key] = old
    }
  }
  return out as T
}

/** Keyed row merge: new rows win, missing rows are preserved, and a row that
 *  exists in both keeps its published non-null cells. */
function mergeRows<T extends object>(prior: T[], next: T[], key: (row: T) => string): T[] {
  if (next.length === 0) return prior
  if (prior.length === 0) return next
  const byKey = new Map(prior.map((row) => [key(row), row]))
  const seen = new Set<string>()
  const out = next.map((row) => {
    const k = key(row)
    seen.add(k)
    const old = byKey.get(k)
    return old ? mergeRow(old, row) : row
  })
  for (const old of prior) {
    const k = key(old)
    if (!seen.has(k)) out.push(old)
  }
  return out
}

/** Same-session re-run merge for the derivatives half. */
export function mergeMarketDerivatives(
  prior: MarketDerivatives,
  next: MarketDerivatives
): MarketDerivatives {
  // TAIFEX readers are latest-only and can flap/still hold the previous
  // session. Never move a same-session observation backwards in time.
  if (prior.asOf > 0 && (next.asOf <= 0 || next.asOf < prior.asOf)) return prior
  return {
    asOf: next.asOf > 0 ? next.asOf : prior.asOf,
    institutions: mergeRows(prior.institutions, next.institutions, (row) => row.item),
    futures: mergeRows(prior.futures, next.futures, (row) => `${row.contract}\u0000${row.item}`),
    options: mergeRows(
      prior.options,
      next.options,
      (row) => `${row.contract}\u0000${row.callPut}\u0000${row.item}`
    ),
    largeTraders: mergeRows(
      prior.largeTraders,
      next.largeTraders,
      (row) =>
        `${row.contract}\u0000${row.product}\u0000${row.callPut ?? ''}\u0000${row.traderType}`
    ),
    pcr: mergeRows(prior.pcr, next.pcr, (row) => String(row.dateMs)).sort(
      (a, b) => b.dateMs - a.dateMs
    ),
    vix: next.vix ?? prior.vix
  }
}

/** The same-session merge of a whole context (history excluded). */
export function mergeMarketContext(prior: MarketContext, next: MarketContext): MarketContext {
  return {
    asOf: next.asOf > 0 ? next.asOf : prior.asOf,
    credit: next.credit
      ? prior.credit
        ? mergeMarketCredit(prior.credit, next.credit)
        : next.credit
      : prior.credit,
    derivatives: next.derivatives
      ? prior.derivatives
        ? mergeMarketDerivatives(prior.derivatives, next.derivatives)
        : next.derivatives
      : prior.derivatives
  }
}

function marketPoint(ctx: MarketContext): MarketHistoryPoint {
  return { sessionDate: ctx.asOf, credit: ctx.credit, derivatives: ctx.derivatives }
}

/**
 * Appends/replaces one session in the rolling observation series.
 *
 * The full 60-session series is normally uploaded out-of-band with
 * `seedMarketHistory.ts`. As a SAFETY guard the daily writer can also start a
 * missing series: the prior object (when it has no history yet) contributes one
 * observation and the current session is added/merged. An absent prior object
 * still stores the current observation, so a fresh bucket can never leave the
 * panel permanently empty. The current session replaces a same-session point
 * through the non-erasing merge; the series stays ascending and capped.
 */
export function mergeMarketHistory(
  prior: readonly MarketHistoryPoint[] | undefined,
  current: MarketContext,
  fallback?: MarketContext | null
): MarketHistoryPoint[] {
  const points = new Map<number, MarketHistoryPoint>()
  for (const point of prior ?? []) {
    if (point && point.sessionDate > 0) points.set(point.sessionDate, point)
  }
  if (points.size === 0 && fallback && fallback.asOf > 0) {
    points.set(fallback.asOf, marketPoint(fallback))
  }
  if (current.asOf > 0) {
    const old = points.get(current.asOf)
    if (old) {
      const merged = mergeMarketContext(
        { asOf: old.sessionDate, credit: old.credit, derivatives: old.derivatives },
        current
      )
      points.set(current.asOf, marketPoint(merged))
    } else {
      points.set(current.asOf, marketPoint(current))
    }
  }
  return [...points.values()]
    .sort((a, b) => a.sessionDate - b.sessionDate)
    .slice(-MARKET_HISTORY_SESSIONS)
}
