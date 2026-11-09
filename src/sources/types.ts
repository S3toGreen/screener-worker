/**
 * Normalized source rows for the screener ETL.
 *
 * Both exchanges' readers answer the SAME shapes so the pipeline can merge
 * them without caring which host a row came from. Prices are raw ×100 ints
 * (the app convention); NT$ amounts and share counts are plain numbers; a
 * field a payload did not carry is `NaN` (the pipeline stores NULL).
 */

/** A reader's answer: the rows by symbol, the session the PAYLOAD reports
 *  (0 when the table carries no date — the ETL then attributes it to the
 *  anchor session) and how many raw rows it walked. */
export interface Sourced<T> {
  dateMs: number
  rows: number
  data: Map<string, T>
}

export function sourced<T>(dateMs: number, data: Map<string, T> = new Map()): Sourced<T> {
  return { dateMs, rows: 0, data }
}

/** One whole-market daily snapshot row. */
export interface DailyRow {
  symbol: string
  name: string
  open: number
  high: number
  low: number
  close: number
  changePct: number
  volume: number
  tradeValue: number
  transactions: number
}

/** ONE session's per-stock bar from a per-date table (the backfill's row).
 *  `change` is the exchange's own 漲跌 in DISPLAY points, signed; NaN when the
 *  session has no comparable change (an ex-rights `X` day). */
export interface DayBar {
  name: string
  open: number
  high: number
  low: number
  close: number
  change: number
  volume: number
  tickets: number
  tradeValue: number
  shares: number
}

/** A listing's static identity. */
export interface UniverseRow {
  symbol: string
  /** The exchange's ABBREVIATION (公司簡稱 / CompanyAbbreviation). */
  name: string
  /** 普通股每股面額 (NT$ per share) — the market-cap denominator. */
  par: number
  /** 實收資本額 (NT$). */
  paidIn: number
  /** 上市日期 (ms), 0 when the table omits it. */
  listedMs: number
}

export interface ValuationRow {
  per: number
  divYield: number
  pbr: number
}

/** The quarterly 營益分析 ratios (%). */
export interface RatioRow {
  grossMargin: number
  opMargin: number
  pretaxMargin: number
  netMargin: number
}

export interface RevenueRow {
  /** The industry NAME (the revenue table spells it; the universe tables carry
   *  codes) — the display string the industry filter uses. */
  industry: string
  /** The reported month (ms of Taipei midnight). */
  monthMs: number
  revenueMonth: number
  /** 上月比較增減 (%). */
  revenueMom: number
  /** 去年同月增減 (%). */
  revenueYoy: number
  /** 前期比較增減 (%) — the cumulative year-to-date growth. */
  revenueCumYoy: number
}

/** One symbol's 信用交易 row. Balances are the exchange's margin units
 *  (交易單位). The flows are the payload's own daily columns — NaN when the
 *  table leaves a cell blank (a 融券 row with no short activity at all).
 *  `marginChange`/`shortChange` are derived, never read. */
export interface MarginSrc {
  marginBal: number
  shortBal: number
  quota: number
  marginPrev: number
  marginBuy: number
  marginSell: number
  marginCashRepay: number
  shortQuota: number
  shortPrev: number
  shortSell: number
  shortCover: number
  shortCashRepay: number
  /** 資券互抵 / 資券相抵 (TWSE MI_MARGN col 14, TPEx 資券相抵) — lots. */
  offset: number
  /** The exchange's raw 註記 / 備註 cell ('' when absent). */
  note: string
}

/** The exchange's OWN market-wide 擔保維持率 for one session (`BFIJ3U_TREND`). */
export interface MaintenancePoint {
  dateMs: number
  /** `keepRate` — percent (e.g. 182.63). */
  keepRate: number
}

/** The market-wide 信用交易 aggregate (TWSE's own 信用交易統計 table). Lots
 *  are 交易單位; the amount rows are 融資金額 in NT$ (the report's 仟元 ×1000). */
export interface CreditSummary {
  dateMs: number
  margin: { buy: number; sell: number; repay: number; prev: number; bal: number }
  short: { buy: number; sell: number; repay: number; prev: number; bal: number }
  /** 融資金額 今日餘額 (NT$) — the announced LOAN balance: the denominator of
   *  the market maintenance-ratio estimate. NaN when the row is absent. */
  marginAmountNtd: number
  /** The SAME row's other four columns (NT$): the day's 融資金額 flows and the
   *  previous balance — the market payload carries them, so they are never
   *  dropped just because the panel only prints the balance. NaN = absent. */
  marginAmountBuyNtd: number
  marginAmountSellNtd: number
  marginAmountRepayNtd: number
  marginAmountPrevNtd: number
}

/** TPEx's own market summary (its dashboard — money in 億元, converted here
 *  to NT$). It carries NO 上櫃-specific 擔保維持率 (its `keepRate` field is
 *  the exchange-wide number, deliberately NOT read) — only the money and lot
 *  totals the 上市櫃 estimate needs. The route answers recent sessions only;
 *  an older date is null. */
export interface TpexCreditSummary {
  dateMs: number
  /** 融資餘額 (NT$) — the announced OTC loan balance. */
  marginBalanceNtd: number
  /** 融券餘額 (交易單位). */
  shortBalanceLots: number
  /** 信用交易成交金額 (NT$). */
  creditAmountNtd: number
  /** 上櫃市場總市值 (NT$). */
  marketCapNtd: number
}

export interface SblSrc {
  sblBal: number
}

/** 外資持股: the shares foreign investors hold and their ratio of issued
 *  shares (%). Ratio is percent points (87.84 = 87.84%). */
export interface ForeignHold {
  shares: number
  ratio: number
}

/** Institutional net flow in SHARES (the pipeline converts to NT$ with the
 *  session close — both exchanges publish shares, neither publishes value).
 *  There is deliberately no `total` member: the three nets SUM to it by
 *  definition, so the pipeline derives it once (`deltaRatio` and the 5-session
 *  accumulations share that one definition). */
export interface InstSrc {
  foreign: number
  trust: number
  dealer: number
}
