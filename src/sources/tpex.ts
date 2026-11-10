/**
 * TPEx readers — the OTC mirror of `twse.ts` (the same normalized row shapes,
 * so the ETL merges the two markets without branching).
 *
 * GOTCHAS this module exists to absorb (all verified live against the
 * endpoints): several column names are misspelled or inconsistently spaced
 * (`Dealers -TotalSell`, a LEADING space on some foreign-flow columns), the
 * yes/no cells are full-width `Ｙ`, dates mix ROC and Gregorian across paths,
 * and the ratios path is `/mopsfin_187ap17_O` — `mopsfin_t187ap17_O` returns
 * HTML.
 */
import {
  RISK_ALTERED,
  RISK_DISPOSAL,
  RISK_MANAGED,
  RISK_NOTICE,
  RISK_PERIODIC,
  RISK_STOP_TRADING
} from '@odf/screener-worker/screener'
import {
  changePctOf,
  dateToMs,
  extractNumber,
  getJson,
  isExchangeYes,
  num,
  rawPrice,
  ymToMs,
  type FetchLike
} from '@odf/screener-worker/httpJson'
import { dayStampOf } from '@odf/screener-worker/session'
import { readMembers, readSourcedTable, readTableRows } from './common'
import {
  sourced,
  type DailyRow,
  type InstSrc,
  type MarginSrc,
  type RatioRow,
  type RevenueRow,
  type SblSrc,
  type Sourced,
  type TpexCreditSummary,
  type UniverseRow,
  type ValuationRow
} from './types'

const BASE = 'https://www.tpex.org.tw/openapi/v1'
const WWW = 'https://www.tpex.org.tw/www'
/** 億元 → NT$. */
const YI = 1e8

/** `/tpex_mainboard_quotes` — the whole-market OTC snapshot. */
export async function tpexDaily(fetchImpl?: FetchLike): Promise<Sourced<DailyRow>> {
  return readSourcedTable<DailyRow>(
    {
      url: `${BASE}/tpex_mainboard_quotes`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v, symbol) => {
        const close = rawPrice(v('Close'))
        return {
          symbol,
          name: v('CompanyName').trim(),
          open: rawPrice(v('Open')),
          high: rawPrice(v('High')),
          low: rawPrice(v('Low')),
          close,
          changePct: changePctOf(close, num(v('Change'))),
          volume: num(v('TradingShares')),
          tradeValue: num(v('TransactionAmount')),
          transactions: num(v('TransactionNumber'))
        }
      }
    },
    fetchImpl
  )
}

/** `/mopsfin_t187ap03_O` — CompanyAbbreviation is the display name. */
export async function tpexUniverse(fetchImpl?: FetchLike): Promise<Sourced<UniverseRow>> {
  return readSourcedTable<UniverseRow>(
    {
      url: `${BASE}/mopsfin_t187ap03_O`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v, symbol) => ({
        symbol,
        name: v('CompanyAbbreviation').trim(),
        par: extractNumber(v('ParValueOfCommonStock')),
        paidIn: num(v('Paidin.Capital.NTDollars')),
        listedMs: dateToMs(v('DateOfListing'))
      })
    },
    fetchImpl
  )
}

/** `/tpex_mainboard_peratio_analysis`. */
export async function tpexValuation(fetchImpl?: FetchLike): Promise<Sourced<ValuationRow>> {
  return readSourcedTable<ValuationRow>(
    {
      url: `${BASE}/tpex_mainboard_peratio_analysis`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v) => ({
        per: num(v('PriceEarningRatio')),
        divYield: num(v('YieldRatio')),
        pbr: num(v('PriceBookRatio'))
      })
    },
    fetchImpl
  )
}

/** `/mopsfin_187ap17_O` — the `t`-less path (the `t` one answers HTML). */
export async function tpexRatios(fetchImpl?: FetchLike): Promise<Sourced<RatioRow>> {
  return readSourcedTable<RatioRow>(
    {
      url: `${BASE}/mopsfin_187ap17_O`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v) => ({
        grossMargin: num(v('毛利率')),
        opMargin: num(v('營業利益率')),
        pretaxMargin: num(v('稅前純益率')),
        netMargin: num(v('稅後純益率'))
      })
    },
    fetchImpl
  )
}

/** `/mopsfin_t187ap05_O` — revenue + MoM/YoY (the same Chinese keys as TWSE's). */
export async function tpexRevenue(fetchImpl?: FetchLike): Promise<Sourced<RevenueRow>> {
  return readSourcedTable<RevenueRow>(
    {
      url: `${BASE}/mopsfin_t187ap05_O`,
      keyField: '公司代號',
      dateField: '出表日期',
      map: (v) => ({
        industry: v('產業別').trim(),
        monthMs: ymToMs(v('資料年月')),
        revenueMonth: num(v('營業收入-當月營收')),
        revenueMom: num(v('營業收入-上月比較增減(%)')),
        revenueYoy: num(v('營業收入-去年同月增減(%)')),
        revenueCumYoy: num(v('累計營業收入-前期比較增減(%)'))
      })
    },
    fetchImpl
  )
}

/** `/tpex_mainboard_margin_balance` (carries its own Date — the ETL rejects a
 *  payload whose date is not the anchor). */
export async function tpexMargin(fetchImpl?: FetchLike): Promise<Sourced<MarginSrc>> {
  return readSourcedTable<MarginSrc>(
    {
      url: `${BASE}/tpex_mainboard_margin_balance`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v) => ({
        marginBal: num(v('MarginPurchaseBalance')),
        shortBal: num(v('ShortSaleBalance')),
        quota: num(v('MarginPurchaseQuota')),
        // 前日 · 買進 · 賣出 · 現金償還 (margin); 前日 · 賣出 · 回補 ·
        // 現券償還 (short). `ShortConvering` is the exchange's own spelling
        // of 融券買進 (buy to cover) and `StockRedemption` is 現券償還.
        marginPrev: num(v('MarginPurchaseBalancePreviousDay')),
        marginBuy: num(v('MarginPurchase')),
        marginSell: num(v('MarginSales')),
        marginCashRepay: num(v('CashRedemption')),
        shortQuota: num(v('ShortSaleQuota')),
        shortPrev: num(v('ShortSaleBalancePreviousDay')),
        shortSell: num(v('ShortSale')),
        shortCover: num(v('ShortConvering')),
        shortCashRepay: num(v('StockRedemption')),
        offset: num(v('Offsetting')),
        note: v('Note').trim()
      })
    },
    fetchImpl
  )
}

/** `/tpex_margin_sbl` — the SBL balance of the market day (its Date is the
 *  ETL's anchor gate, like the margin table). */
export async function tpexSbl(fetchImpl?: FetchLike): Promise<Sourced<SblSrc>> {
  return readSourcedTable<SblSrc>(
    {
      url: `${BASE}/tpex_margin_sbl`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v) => ({ sblBal: num(v('SecuritiesBorrowingBalanceOfTheMarketDay')) })
    },
    fetchImpl
  )
}

/** The OTC risk lists: `tpex_cmode` carries the mechanism flags (full-width
 *  `Ｙ`), the warning/disposal/spenditure tables add the event bits. (No
 *  window gating here — every OTC list is published for the current session.) */
export async function tpexRiskFlags(fetchImpl?: FetchLike): Promise<Sourced<number>> {
  const out = sourced<number>(0)
  const add = (symbol: string, bits: number): void => {
    out.data.set(symbol, (out.data.get(symbol) ?? 0) | bits)
  }
  const members = async (path: string, bits: number): Promise<void> => {
    out.rows += await readMembers(`${BASE}${path}`, fetchImpl, add, 'SecuritiesCompanyCode', bits)
  }

  const mode = await readTableRows(`${BASE}/tpex_cmode`, fetchImpl, (v) => {
    const symbol = v('SecuritiesCompanyCode').trim()
    if (!symbol) return null
    let bits = 0
    if (isExchangeYes(v('AlteredTrading'))) bits |= RISK_ALTERED
    if (isExchangeYes(v('PeriodicTrading'))) bits |= RISK_PERIODIC
    if (isExchangeYes(v('ManagedStock'))) bits |= RISK_MANAGED
    if (isExchangeYes(v('SuspensionOfTrading'))) bits |= RISK_STOP_TRADING
    return { symbol, bits }
  })
  for (const r of mode) {
    if (r.bits !== 0) add(r.symbol, r.bits)
    out.rows++
  }
  await members('/tpex_trading_warning_information', RISK_NOTICE)
  await members('/tpex_disposal_information', RISK_DISPOSAL)
  await members('/tpex_spendi_today', RISK_STOP_TRADING)
  return out
}

/** `/tpex_3insti_daily_trading` — column names are long, misspelled and
 *  inconsistently spaced (a leading space, `TotalSell` vs `Total Sell`); the
 *  shared reader's trimmed field map resolves them, and the 外陸資 + 外資自營商 /
 *  投信 / 自營商 parts are summed. Shares, like TWSE's T86.
 *
 *  A blank/`X` cell is MISSING, never zero: the exchange answers `X` for a name
 *  with no flow that day, and mapping it to 0 poisoned `instTrustStreak`, the
 *  5-session sums and `deltaRatio` (a fake zero-flow day). A part missing
 *  leaves the sum of the REMAINING parts; every part missing → NaN (stored
 *  NULL). */
export async function tpexInstitutional(fetchImpl?: FetchLike): Promise<Sourced<InstSrc>> {
  return readSourcedTable<InstSrc>(
    {
      url: `${BASE}/tpex_3insti_daily_trading`,
      keyField: 'SecuritiesCompanyCode',
      dateField: 'Date',
      map: (v) => {
        const sum = (...cells: string[]): number => {
          const parts = cells.map((c) => num(c)).filter(Number.isFinite)
          return parts.length === 0 ? NaN : parts.reduce((a, b) => a + b, 0)
        }
        const foreign = sum(
          v(
            'Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference'
          ),
          v('ForeignDealers-Difference')
        )
        const trust = num(v('SecuritiesInvestmentTrustCompanies-Difference'))
        const dealer = sum(v('Dealers-Difference'))
        return { foreign, trust, dealer }
      }
    },
    fetchImpl
  )
}

/**
 * TPEx's own market-wide 信用交易 summary (its dashboard, per date). Money
 * arrives in 億元 (×1e8 → NT$); the 融資 lot total is NOT in it (only the 融券
 * balance) — the ETL sums the per-stock balance table for that. The route
 * serves a RECENT window only (probed: 2026-10 onward), so an older date is
 * null — never a guessed total.
 */
export async function tpexCreditSummary(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<TpexCreditSummary | null> {
  const stamp = dayStampOf(dateMs)
  const url =
    `${WWW}/dashboardOtc/margin?lang=zh-tw` +
    `&date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}`
  const payload = (await getJson(url, fetchImpl, 45_000)) as {
    stat?: string
    date?: string
    marginBalance?: number
    shortBalance?: number
    creditAmount?: number
    marketCap?: number
    hasMarginData?: boolean
  } | null
  if (!payload || payload.stat !== 'ok' || payload.hasMarginData === false) return null
  if (payload.date !== undefined && payload.date !== stamp) return null
  const n = (v: unknown): number => (typeof v === 'number' ? v : num(String(v ?? '')))
  const marginBalanceNtd = n(payload.marginBalance) * YI
  const marketCapNtd = n(payload.marketCap) * YI
  if (!Number.isFinite(marginBalanceNtd) || !Number.isFinite(marketCapNtd)) return null
  return {
    dateMs,
    marginBalanceNtd,
    marketCapNtd,
    shortBalanceLots: n(payload.shortBalance),
    creditAmountNtd: n(payload.creditAmount) * YI
  }
}
