/**
 * TWSE readers — the OpenAPI "latest" mirrors plus the rwd tables that take a
 * date.
 *
 * The OpenAPI mirrors are NOT the session of record: they regenerate ~05:20
 * Taipei the NEXT morning (measured 2026-11-05 02:00: `STOCK_DAY_ALL` still
 * held T−1 while `MI_INDEX?date=T` answered), so the ETL reads the per-date
 * rwd table first and keeps the mirror as a dated fallback.
 *
 * Prices arrive as decimal strings and are stored RAW ×100; `Change` can be
 * `'X'` on an ex-rights day, which yields NaN (stored NULL — never a fake 0).
 */
import {
  RISK_ALTERED,
  RISK_DISPOSAL,
  RISK_NOTICE,
  RISK_PERIODIC,
  RISK_STOP_FUNDS,
  RISK_STOP_TRADING
} from '@odf/screener-worker/screener'
import { dayStampOf } from '@odf/screener-worker/session'
import {
  changePctOf,
  dateToMs,
  extractNumber,
  getJson,
  indexer,
  isExchangeYes,
  num,
  rawPrice,
  ymToMs,
  type FetchLike
} from '@odf/screener-worker/httpJson'
import { isScreenerSymbol } from '../universe'
import type { DividendEvent } from '@odf/screener-worker/screener'
import {
  mapTableRows,
  readMembers,
  readSourcedTable,
  readTableRows,
  TABLE_TIMEOUT_MS
} from './common'
import {
  sourced,
  type MaintenancePoint,
  type DailyRow,
  type InstSrc,
  type RatioRow,
  type RevenueRow,
  type Sourced,
  type UniverseRow,
  type ValuationRow
} from './types'

const BASE = 'https://openapi.twse.com.tw/v1'
const RWD = 'https://www.twse.com.tw/rwd/zh'

/** The whole-market daily snapshot: `/exchangeReport/STOCK_DAY_ALL`. */
export async function twseDaily(fetchImpl?: FetchLike): Promise<Sourced<DailyRow>> {
  return readSourcedTable<DailyRow>(
    {
      url: `${BASE}/exchangeReport/STOCK_DAY_ALL`,
      keyField: 'Code',
      dateField: 'Date',
      map: (v, symbol) => {
        const close = rawPrice(v('ClosingPrice'))
        return {
          symbol,
          name: v('Name').trim(),
          open: rawPrice(v('OpeningPrice')),
          high: rawPrice(v('HighestPrice')),
          low: rawPrice(v('LowestPrice')),
          close,
          changePct: changePctOf(close, num(v('Change'))),
          volume: num(v('TradeVolume')),
          tradeValue: num(v('TradeValue')),
          transactions: num(v('Transaction'))
        }
      }
    },
    fetchImpl
  )
}

/** The listed universe: `/opendata/t187ap03_L` (公司簡稱 is the display name). */
export async function twseUniverse(fetchImpl?: FetchLike): Promise<Sourced<UniverseRow>> {
  return readSourcedTable<UniverseRow>(
    {
      url: `${BASE}/opendata/t187ap03_L`,
      keyField: '公司代號',
      dateField: '出表日期',
      map: (v, symbol) => ({
        symbol,
        name: v('公司簡稱').trim(),
        par: extractNumber(v('普通股每股面額')),
        paidIn: num(v('實收資本額')),
        listedMs: dateToMs(v('上市日期'))
      })
    },
    fetchImpl
  )
}

/** `/exchangeReport/BWIBBU_ALL` — PER / 殖利率 / PBR (blank PER is normal). */
export async function twseValuation(fetchImpl?: FetchLike): Promise<Sourced<ValuationRow>> {
  return readSourcedTable<ValuationRow>(
    {
      url: `${BASE}/exchangeReport/BWIBBU_ALL`,
      keyField: 'Code',
      dateField: 'Date',
      map: (v) => ({
        per: num(v('PEratio')),
        divYield: num(v('DividendYield')),
        pbr: num(v('PBratio'))
      })
    },
    fetchImpl
  )
}

/** `/opendata/t187ap17_L` — the quarterly 營益分析 ratios (%). */
export async function twseRatios(fetchImpl?: FetchLike): Promise<Sourced<RatioRow>> {
  return readSourcedTable<RatioRow>(
    {
      url: `${BASE}/opendata/t187ap17_L`,
      keyField: '公司代號',
      dateField: '出表日期',
      map: (v) => ({
        grossMargin: num(v('毛利率(%)(營業毛利)/(營業收入)')),
        opMargin: num(v('營業利益率(%)(營業利益)/(營業收入)')),
        pretaxMargin: num(v('稅前純益率(%)(稅前純益)/(營業收入)')),
        netMargin: num(v('稅後純益率(%)(稅後純益)/(營業收入)'))
      })
    },
    fetchImpl
  )
}

/** `/opendata/t187ap05_L` — monthly revenue + MoM/YoY (%) + the industry NAME. */
export async function twseRevenue(fetchImpl?: FetchLike): Promise<Sourced<RevenueRow>> {
  return readSourcedTable<RevenueRow>(
    {
      url: `${BASE}/opendata/t187ap05_L`,
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

/** The exchange's OWN market-wide 擔保維持率 for one session
 *  (`BFIJ3U_TREND`, the margin-maintenance page's series). `keepRate` is the
 *  全市場擔保維持率 in percent; the row for the requested session is required
 *  (a lagging series answers the previous session — never stamped as this one). */
export async function twseMaintenanceRatio(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<MaintenancePoint | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${RWD}/marginTrading/BFIJ3U_TREND?response=json&days=10&date=${stamp}`,
    fetchImpl,
    45_000
  )) as { stat?: string; data?: Array<Record<string, unknown>> } | null
  return maintenanceRowOf(payload, dateMs)
}

/** Picks the session's row out of a BFIJ3U_TREND payload (pure: the shape is
 *  verified live; a stale series must answer null, never the previous day). */
export function maintenanceRowOf(raw: unknown, dateMs: number): MaintenancePoint | null {
  if (!raw || typeof raw !== 'object') return null
  const v = raw as { stat?: unknown; data?: unknown }
  if (v.stat !== 'OK' || !Array.isArray(v.data)) return null
  const stamp = dayStampOf(dateMs)
  for (const row of v.data) {
    if (!row || typeof row !== 'object') continue
    const r = row as Record<string, unknown>
    if (typeof r.date !== 'string' || r.date.trim() !== stamp) continue
    const keepRate = typeof r.keepRate === 'number' ? r.keepRate : Number(r.keepRate)
    if (!Number.isFinite(keepRate) || keepRate <= 0) return null
    return { dateMs, keepRate }
  }
  return null
}

/** The risk / method-change lists, OR-ed into one bitmask per symbol.
 *  `anchorMs` gates the window-based lists (停資停券, 暫停交易) so an expired
 *  window never keeps a name excluded. */
export async function twseRiskFlags(
  anchorMs: number,
  fetchImpl?: FetchLike
): Promise<Sourced<number>> {
  const out = sourced<number>(anchorMs)
  const add = (symbol: string, bits: number): void => {
    out.data.set(symbol, (out.data.get(symbol) ?? 0) | bits)
  }
  const member = async (
    path: string,
    codeField: string,
    bits: number,
    window?: { fromField: string; toField: string; anchorMs: number }
  ): Promise<void> => {
    out.rows += await readMembers(`${BASE}${path}`, fetchImpl, add, codeField, bits, window)
  }

  await member('/announcement/notice', 'Code', RISK_NOTICE)
  await member('/announcement/notetrans', 'Code', RISK_NOTICE)
  await member('/announcement/punish', 'Code', RISK_DISPOSAL)

  // 變更交易 / 分盤: list membership IS the 變更交易 signal; the periodic-auction
  // column adds 分盤 when it is set.
  const altered = await readTableRows(`${BASE}/exchangeReport/TWT85U`, fetchImpl, (v) => {
    const symbol = v('Code').trim()
    return symbol ? { symbol, periodic: isExchangeYes(v('PeriodicCallAuctionTrading')) } : null
  })
  for (const r of altered) {
    add(r.symbol, RISK_ALTERED | (r.periodic ? RISK_PERIODIC : 0))
    out.rows++
  }
  // 停資停券 / 暫停交易: window lists — only a window containing the anchor.
  await member('/exchangeReport/BFI84U', 'Code', RISK_STOP_FUNDS, {
    fromField: 'StartDate',
    toField: 'EndDate',
    anchorMs
  })
  await member('/exchangeReport/TWTAWU', 'Code', RISK_STOP_TRADING, {
    fromField: 'TradingHaltDate',
    toField: 'TradingResumptionDate',
    anchorMs
  })
  return out
}

/**
 * The exchange's ex-dividend calendar (`TWT48U`) — the ~3-week window STARTING
 * today (the endpoint ignores `?date=`); the ETL accumulates it into
 * `dividends.json`. The 現金股利 cell can be an HTML "待公告" notice (ETFs
 * before the distribution is fixed) — an unreadable cell is null, never 0.
 */
export async function twseUpcomingDividends(fetchImpl?: FetchLike): Promise<DividendEvent[]> {
  const payload = (await getJson(
    `https://www.twse.com.tw/rwd/zh/exRight/TWT48U?response=json`,
    fetchImpl,
    TABLE_TIMEOUT_MS
  )) as { stat?: string; fields?: string[]; data?: unknown[][] } | null
  if (!payload || payload.stat !== 'OK' || !payload.fields) return []
  const at = indexer(payload.fields.map((f) => String(f).trim()))
  const out: DividendEvent[] = []
  for (const row of payload.data ?? []) {
    const symbol = String(row[at('股票代號')] ?? '').trim()
    if (!isScreenerSymbol(symbol)) continue
    const exDate = dayStampOf(dateToMs(String(row[at('除權除息日期')] ?? '')))
    if (!exDate) continue
    const cash = num(String(row[at('現金股利')] ?? '').replace(/<[^>]+>/g, ''))
    const stock = num(String(row[at('無償配股率')] ?? ''))
    out.push({
      symbol,
      exDate,
      cash: Number.isFinite(cash) ? cash : null,
      stockRatio: Number.isFinite(stock) && stock > 0 ? stock : null,
      kind: String(row[at('除權息')] ?? '').trim()
    })
  }
  return out
}

/**
 * 當沖 (day-trade) volume per stock — TWSE `exchangeReport/TWTB4U`, one payload
 * per session: `tables[0]` is the market aggregate, `tables[1]` the per-stock
 * rows. The payload's own `date` gates the answer (the endpoint, like every
 * other TWSE one, serves the LATEST session rather than nothing on a closed
 * day — a stale answer must read as absent, never as today's).
 */
export async function twseDayTrade(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, number> | null> {
  const day = dayStampOf(dateMs)
  if (!day) return null
  // The legacy `exchangeReport` host — the rwd twin (`/rwd/zh/afterTrading/
  // TWTB4U`) answers 404, and the OpenAPI host does not carry this dataset.
  const url = `https://www.twse.com.tw/exchangeReport/TWTB4U?response=json&date=${day}&selectType=All`
  const payload = (await getJson(url, fetchImpl, TABLE_TIMEOUT_MS)) as {
    stat?: string
    date?: string
    tables?: Array<{ fields?: string[]; data?: string[][] }>
  } | null
  if (!payload || payload.stat !== 'OK' || payload.date !== day) return null
  const table = payload.tables?.[1]
  if (!table?.fields || !table.data || table.data.length === 0) return null
  const at = indexer(table.fields.map((f) => f.trim()))
  const out = new Map<string, number>()
  let sum = 0
  for (const row of table.data) {
    const shares = num(row[at('當日沖銷交易成交股數')])
    if (!Number.isFinite(shares)) continue
    sum += shares
    const symbol = (row[at('證券代號')] ?? '').trim()
    if (isScreenerSymbol(symbol)) out.set(symbol, shares)
  }
  // The per-stock rows SUM to `tables[0]`'s market total exactly (verified on
  // every probed session), so a symbol absent from the table genuinely traded
  // ZERO 當沖 that day — the caller writes 0, never `unknown`. A payload whose
  // rows do not add up is truncated: answer null instead of fabricating zeros.
  const aggregate = num(payload.tables?.[0]?.data?.[0]?.[0] ?? '')
  if (!(aggregate > 0) || Math.abs(sum - aggregate) > aggregate * 0.005) {
    console.log(`[twse] dayTrade ${day}: rows ${sum} ≠ market ${aggregate} — treated as absent`)
    return null
  }
  return out.size > 0 ? out : null
}

/** The per-stock institutional flow (rwd `fund/T86`, one request per session —
 *  shares, later valued at the session close). Values can be `'X'` on a
 *  non-trading symbol day. */
export async function twseInstitutional(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Sourced<InstSrc> | null> {
  const day = dayStampOf(dateMs)
  const out = sourced<InstSrc>(dateMs)
  if (!day) return null
  const url = `${RWD}/fund/T86?date=${day}&response=json&selectType=ALL`
  // ~3 MB of rows on a variable government host (measured 37 s under load):
  // the 10 s default times out, and even the 30 s the day walk used did. The
  // payload's OWN date is the gate: a holiday answers `stat` ≠ OK with no
  // rows, and a stale answer must never be stamped on the anchor.
  const payload = (await getJson(url, fetchImpl, 90_000)) as {
    stat?: string
    date?: string
    fields?: string[]
    data?: unknown[][]
  } | null
  // "Not this session's file yet" is a CLEAN absence (the later cron repairs);
  // only an unreadable/shape-broken payload is a failure worth a retry.
  if (payload?.stat !== 'OK') return null
  if (payload.date && payload.date !== day) return null
  const rows = mapTableRows(payload, (v) => {
    const symbol = v('證券代號').trim()
    if (!symbol) return null
    const foreign = num(v('外陸資買賣超股數(不含外資自營商)')) + num(v('外資自營商買賣超股數'))
    const trust = num(v('投信買賣超股數'))
    const dealer = num(v('自營商買賣超股數(自行買賣)')) + num(v('自營商買賣超股數(避險)'))
    return { symbol, foreign, trust, dealer }
  })
  for (const r of rows) {
    out.data.set(r.symbol, r)
    out.rows++
  }
  return out.rows > 0 ? out : null
}
