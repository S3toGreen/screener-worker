/**
 * HISTORICAL session readers — the backfill's only data source.
 *
 * The daily ETL reads each exchange's "latest" OpenAPI file; history needs the
 * same numbers for an arbitrary past date, which the exchanges serve from their
 * own per-date tables:
 *
 *   TWSE  /rwd/zh/afterTrading/MI_INDEX?date=YYYYMMDD&type=ALLBUT0999   (bars)
 *   TWSE  /rwd/zh/afterTrading/BWIBBU_d?date=YYYYMMDD&selectType=ALL    (per/pbr/yield)
 *   TWSE  /rwd/zh/fund/T86?date=YYYYMMDD                                (institutional)
 *   TWSE  /rwd/zh/fund/MI_QFIIS?date=YYYYMMDD&selectType=ALLBUT0999     (foreign holding)
 *   TWSE  /rwd/zh/marginTrading/MI_MARGN?date=YYYYMMDD&selectType=ALL   (margin/short)
 *   TWSE  /rwd/zh/marginTrading/TWT93U?date=YYYYMMDD                    (SBL balance)
 *   TPEx  /www/zh-tw/afterTrading/otc?date=YYYY/MM/DD&type=EW          (bars)
 *   TPEx  /www/zh-tw/afterTrading/peQryDate?date=YYYY/MM/DD             (per/pbr/yield)
 *   TPEx  /www/zh-tw/insti/dailyTrade?date=YYYY/MM/DD&type=Daily        (institutional)
 *   TPEx  /www/zh-tw/insti/qfii?date=YYYY/MM/DD                         (foreign holding)
 *   TPEx  /www/zh-tw/margin/balance?date=YYYY/MM/DD                    (margin/short)
 *   TPEx  /www/zh-tw/margin/sbl?date=YYYY/MM/DD                         (SBL balance)
 *
 * The date parameter is NOT uniform: the TPEx routes take `date=` (a `data=`
 * guess is silently IGNORED and answers the latest session), so every reader
 * validates the payload's own date. TWSE's SBL balance has a dated rwd route
 * (`TWT93U`, answers back to at least 2007); its old undated OpenAPI file
 * (TWT96U, 可借券賣出股數) was a DIFFERENT metric and is not used.
 * A day the archive does not answer is a null column, never a guessed value.
 *
 * Every reader returns `null` when the payload is NOT that date's session (a
 * holiday, or an exchange that has not published yet) and a symbol-keyed map
 * otherwise — the caller never mistakes "no session" for "no rows".
 */
import type { FetchLike } from '@odf/screener-worker/httpJson'
import {
  changePctOf,
  dateToMs,
  getJson,
  indexer,
  num,
  rawPrice
} from '@odf/screener-worker/httpJson'
import { dayStampOf, stampToMs } from '@odf/screener-worker/session'
import {
  sourced,
  type CreditSummary,
  type DailyRow,
  type DayBar,
  type ForeignHold,
  type InstSrc,
  type MarginSrc,
  type SblSrc,
  type Sourced,
  type ValuationRow
} from './sources/types'
import { titleIsSession } from './sources/common'
import { isScreenerSymbol } from './universe'

const TWSE_RWD = 'https://www.twse.com.tw/rwd/zh'
const TPEX_RWD = 'https://www.tpex.org.tw/www/zh-tw'
/** The per-date tables are a few MB on a variable government host (T86 was
 *  measured at 37 s under load); the 10 s default is far too tight. */
const SLOW_MS = 60_000

interface RwdTable {
  title?: string
  date?: string
  fields?: string[]
  data?: unknown[][]
}

interface RwdPayload {
  stat?: string
  date?: string
  fields?: string[]
  data?: unknown[][]
  tables?: RwdTable[]
}

/** The table body of either rwd shape — TWSE answers a flat `{fields,data}`
 *  for the fund/valuation reports, TPEx wraps every answer in `tables[0]`. */
function flatTable(
  payload: RwdPayload | null
): { fields: string[]; data: unknown[][]; date: string } | null {
  if (!payload) return null
  if (Array.isArray(payload.fields) && Array.isArray(payload.data)) {
    return { fields: payload.fields, data: payload.data, date: payload.date ?? '' }
  }
  const table = payload.tables?.[0]
  if (table && Array.isArray(table.fields) && Array.isArray(table.data)) {
    return { fields: table.fields, data: table.data, date: table.date ?? payload.date ?? '' }
  }
  return null
}

/** Does the payload's own date agree with the request? A missing date cannot
 *  be checked (the caller's `stat`/row-count guards stand); a present one that
 *  disagrees is a silently-stale answer and MUST be rejected. */
function payloadIsSession(
  date: string | undefined,
  fallback: string | undefined,
  stamp: string
): boolean {
  const reported = dateToMs(date ?? '') || dateToMs(fallback ?? '')
  return reported === 0 || reported === stampToMs(stamp)
}

function valueAt(row: unknown[], index: number): string {
  const value = row[index]
  return value === undefined || value === null ? '' : String(value)
}

/** TWSE titles carry the session ("115年10月21日", month/day zero-padded in
 *  some months: "115年03月13日") — the title is how a stale payload is caught,
 *  so the comparison is numeric, never string equality. */
function cell(row: unknown[], at: (field: string) => number, field: string): string {
  const value = row[at(field)]
  return value === undefined || value === null ? '' : String(value)
}

/** TWSE's `漲跌價差` is the magnitude only; the SIGN rides the `漲跌(+/-)` cell
 *  as HTML (`<p style= color:green>-</p>`). A blank sign on a traded row is an
 *  unchanged close (delta 0); `X` is an ex-rights day with no comparable
 *  change — NaN, never a fake 0. */
function signedChange(signCell: string, delta: number): number {
  const sign = signCell.replace(/<[^>]*>/g, '').trim()
  if (sign === '-') return -delta
  if (sign === '+' || sign === '') return delta
  return NaN
}

export async function twseDayBars(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, DayBar> | null> {
  const stamp = dayStampOf(dateMs)
  const url = `${TWSE_RWD}/afterTrading/MI_INDEX?date=${stamp}&type=ALLBUT0999&response=json`
  const payload = (await getJson(url, fetchImpl, SLOW_MS)) as RwdPayload | null
  const table = (payload?.tables ?? []).find((t) => t.fields?.includes('證券代號'))
  if (!table?.data || !table.fields || !titleIsSession(table.title, stamp)) return null
  const at = indexer(table.fields)
  const out = new Map<string, DayBar>()
  for (const row of table.data) {
    const symbol = cell(row, at, '證券代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    const close = rawPrice(cell(row, at, '收盤價'))
    if (!Number.isFinite(close) || close <= 0) continue
    out.set(symbol, {
      name: cell(row, at, '證券名稱').trim(),
      open: rawPrice(cell(row, at, '開盤價')),
      high: rawPrice(cell(row, at, '最高價')),
      low: rawPrice(cell(row, at, '最低價')),
      close,
      change: signedChange(cell(row, at, '漲跌(+/-)'), num(cell(row, at, '漲跌價差'))),
      volume: num(cell(row, at, '成交股數')),
      tickets: num(cell(row, at, '成交筆數')),
      tradeValue: num(cell(row, at, '成交金額')),
      shares: 0
    })
  }
  return out
}

/**
 * TPEx's per-date `afterTrading/otc?date=&type=EW` — "上櫃股票每日收盤行情
 * (不含定價)". `type=EW` on THIS endpoint is what strips the warrant board:
 * the payload is ~1 000 stock/ETF rows (140 KB) instead of `dailyQuotes`'
 * ~13 000 rows (2 MB, 12 000+ warrants) that used to time out the day walk.
 * The field names carry stray spaces (`收盤 `, ` 成交股數 `), which `indexer`
 * trims; 漲跌 is a SIGNED display value (`+0.30`).
 */
export async function tpexDayBars(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, DayBar> | null> {
  const stamp = dayStampOf(dateMs)
  const url = `${TPEX_RWD}/afterTrading/otc?date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json&type=EW`
  const payload = (await getJson(url, fetchImpl, SLOW_MS)) as RwdPayload | null
  if (payload?.stat !== 'ok') return null
  const table = (payload.tables ?? []).find((t) => t.fields?.some((f) => f.trim() === '代號'))
  if (!table?.data || !table.fields || !payloadIsSession(table.date, payload.date, stamp)) {
    return null
  }
  const at = indexer(table.fields)
  const out = new Map<string, DayBar>()
  for (const row of table.data) {
    const symbol = cell(row, at, '代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    const close = rawPrice(cell(row, at, '收盤'))
    if (!Number.isFinite(close) || close <= 0) continue
    out.set(symbol, {
      name: cell(row, at, '名稱').trim(),
      open: rawPrice(cell(row, at, '開盤')),
      high: rawPrice(cell(row, at, '最高')),
      low: rawPrice(cell(row, at, '最低')),
      close,
      change: num(cell(row, at, '漲跌')),
      volume: num(cell(row, at, '成交股數')),
      tickets: num(cell(row, at, '成交筆數')),
      tradeValue: num(cell(row, at, '成交金額(元)')),
      shares: num(cell(row, at, '發行股數'))
    })
  }
  // An EMPTY table IS the holiday answer (the endpoint replies 200 + 0 rows) —
  // the module's contract is `null` = not that session's payload.
  return out.size > 0 ? out : null
}

/** TWSE PER / PBR / dividend yield for ONE session. `selectType=ALL` is the
 *  variant that answers (the `ALLBUT0999` guess returns an error row), and the
 *  values are read by field NAME — 本益比 is `-` on a loss-making name. */
export async function twseDayValuation(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, ValuationRow> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TWSE_RWD}/afterTrading/BWIBBU_d?date=${stamp}&selectType=ALL&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  if (payload?.stat !== 'OK') return null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload.date, stamp)) return null
  const at = indexer(table.fields)
  const out = new Map<string, ValuationRow>()
  for (const row of table.data) {
    const symbol = cell(row, at, '證券代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    out.set(symbol, {
      per: num(cell(row, at, '本益比')),
      divYield: num(cell(row, at, '殖利率(%)')),
      pbr: num(cell(row, at, '股價淨值比'))
    })
  }
  return out.size > 0 ? out : null
}

/** TPEx PER / PBR / dividend yield for ONE session — the `date=` parameter
 *  (a `data=` guess answers the LATEST session instead; never used here). */
export async function tpexDayValuation(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, ValuationRow> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TPEX_RWD}/afterTrading/peQryDate?date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload?.date, stamp)) return null
  const at = indexer(table.fields)
  const out = new Map<string, ValuationRow>()
  for (const row of table.data) {
    const symbol = cell(row, at, '股票代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    out.set(symbol, {
      per: num(cell(row, at, '本益比')),
      divYield: num(cell(row, at, '殖利率(%)')),
      pbr: num(cell(row, at, '股價淨值比'))
    })
  }
  return out.size > 0 ? out : null
}

/**
 * ONE TWSE `MI_MARGN` response for a session — BOTH halves of that report: the
 * market-wide 信用交易統計 (the whole 上市 overview: lots AND the money rows)
 * and the per-stock 融資融券彙總 (EVERY published column, including 融券限額,
 * 資券互抵 and the raw 註記). The two consumers used to fetch this ~1.5 MB
 * payload SEPARATELY; one response now updates both.
 *
 * `summary`/`stocks` are independently null when their own table is not the
 * requested session (a holiday or a not-yet-published half); `null` = the
 * request/stat itself failed.
 */
export async function twseMarginDay(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<{ summary: CreditSummary | null; stocks: Map<string, MarginSrc> | null } | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TWSE_RWD}/marginTrading/MI_MARGN?date=${stamp}&selectType=ALL&response=json`,
    fetchImpl,
    SLOW_MS
  )) as {
    stat?: string
    tables?: Array<{ title?: string; fields?: string[]; data?: unknown[][] }>
  } | null
  if (payload?.stat !== 'OK') return null
  const tables = payload.tables ?? []

  // The summary table: 項目 | 買進 | 賣出 | 現金(券)償還 | 前日餘額 | 今日餘額.
  let summary: CreditSummary | null = null
  const summaryTable = tables.find((t) => (t.fields ?? []).includes('項目'))
  if (summaryTable?.data && summaryTable.fields && titleIsSession(summaryTable.title, stamp)) {
    const row = (label: string): string[] | null => {
      const found = summaryTable.data?.find((r) => String(r[0] ?? '').trim() === label)
      return found ? found.map((c) => String(c ?? '')) : null
    }
    const read = (label: string): CreditSummary['margin'] | null => {
      const cells = row(label)
      if (!cells) return null
      return {
        buy: num(cells[1] ?? ''),
        sell: num(cells[2] ?? ''),
        repay: num(cells[3] ?? ''),
        prev: num(cells[4] ?? ''),
        bal: num(cells[5] ?? '')
      }
    }
    const margin = read('融資(交易單位)')
    const short = read('融券(交易單位)')
    if (margin && short) {
      // 融資金額(仟元): ALL FIVE columns are kept (each ×1000); a blank cell is
      // NaN, which the caller's `summaryNum` turns into null — never a zero.
      const nt = (cells: string[] | null, pos: number): number =>
        cells ? num(cells[pos] ?? '') * 1000 : NaN
      const amount = row('融資金額(仟元)')
      summary = {
        dateMs,
        margin,
        short,
        marginAmountNtd: nt(amount, 5),
        marginAmountBuyNtd: nt(amount, 1),
        marginAmountSellNtd: nt(amount, 2),
        marginAmountRepayNtd: nt(amount, 3),
        marginAmountPrevNtd: nt(amount, 4)
      }
    }
  }

  // The per-stock table: 代號 名稱 | margin (buy/sell/repay/prev/bal/limit) |
  // short (same) | 資券互抵 | 註記. The two blocks repeat their column NAMES,
  // so they are read by POSITION: 0 code · 2-7 margin · 8-13 short · 14 offset
  // · 15 note.
  let stocks: Map<string, MarginSrc> | null = null
  const stockTable = tables.find((t) => (t.fields ?? []).includes('代號'))
  if (stockTable?.data && stockTable.fields && titleIsSession(stockTable.title, stamp)) {
    const at = indexer(stockTable.fields)
    const out = new Map<string, MarginSrc>()
    for (const row of stockTable.data) {
      const symbol = cell(row, at, '代號').trim()
      if (!isScreenerSymbol(symbol)) continue // skips the (full-width-space) 合計 row
      const n = (pos: number): number => num(String(row[pos] ?? ''))
      const today = n(6)
      if (!Number.isFinite(today)) continue
      const shortToday = n(12)
      if (!Number.isFinite(shortToday)) continue
      out.set(symbol, {
        marginBal: today,
        // A blank 融資限額 cell is UNKNOWN, never 0 — the ETL's `finiteOrNull`
        // turns NaN into a null cell (a 0 quota would read as a real limit).
        quota: n(7),
        shortBal: shortToday,
        // 2 買進 · 3 賣出 · 4 現金償還 · 5 前日 (margin); 8 買進(回補) ·
        // 9 賣出 · 10 現券償還 · 11 前日 (short); 13 券限額 · 14 資券互抵 ·
        // 15 註記.
        marginPrev: n(5),
        marginBuy: n(2),
        marginSell: n(3),
        marginCashRepay: n(4),
        shortQuota: n(13),
        shortPrev: n(11),
        shortSell: n(9),
        shortCover: n(8),
        shortCashRepay: n(10),
        offset: n(14),
        note: String(row[15] ?? '').trim()
      })
    }
    stocks = out
  }
  return summary || stocks ? { summary, stocks } : null
}

/**
 * TPEx 融資融券餘額 for ONE session (`/www/zh-tw/margin/balance?date=`). The
 * table is 交易單位 (張) only — NEITHER exchange publishes a per-stock
 * 融資金額, so the market maintenance-ratio numerator is lots × close. The
 * payload answers history back to at least 2022 (probed), and its own date is
 * the session gate. The columns repeat per side, so they are read by POSITION:
 * 0 代號 · 1 名稱 · 2 前資 · 3 資買 · 4 資賣 · 5 現償 · 6 資餘額 ·
 * 9 資限額 · 10 前券 · 11 券賣 · 12 券買 · 13 券償 · 14 券餘額 · 17 券限額 ·
 * 18 資券相抵 · 19 備註. EVERY security is kept (ETFs included) — the whole-
 * market OTC totals are summed from this map, so a stock-only filter would
 * silently undercount them (the seeds still look up stocks only).
 */
export async function tpexDayMargin(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, MarginSrc> | null> {
  const stamp = dayStampOf(dateMs)
  const url =
    `${TPEX_RWD}/margin/balance?` +
    `date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json`
  const payload = (await getJson(url, fetchImpl, SLOW_MS)) as RwdPayload | null
  if (payload?.stat !== 'ok') return null
  const table = (payload.tables ?? [])[0]
  if (!table?.data || !table.fields || !payloadIsSession(table.date, payload.date, stamp))
    return null
  const out = new Map<string, MarginSrc>()
  for (const row of table.data) {
    const symbol = String(row[0] ?? '').trim()
    // EVERY security (ETFs included): the market-wide totals are summed from
    // this map, so a stock-only filter would silently undercount them. The
    // seeds still look rows up by their own stock symbols only.
    if (!symbol) continue
    const n = (pos: number): number => num(String(row[pos] ?? ''))
    const today = n(6)
    if (!Number.isFinite(today)) continue
    const shortToday = n(14)
    if (!Number.isFinite(shortToday)) continue
    out.set(symbol, {
      marginBal: today,
      quota: n(9),
      shortBal: shortToday,
      marginPrev: n(2),
      marginBuy: n(3),
      marginSell: n(4),
      marginCashRepay: n(5),
      shortQuota: n(17),
      shortPrev: n(10),
      shortSell: n(11),
      shortCover: n(12),
      shortCashRepay: n(13),
      offset: n(18),
      note: String(row[19] ?? '').trim()
    })
  }
  return out.size > 0 ? out : null
}

/**
 * The TPEx 三大法人, positional layout (verified against the OpenAPI payload for the
 * same session): code · name · foreign-ex-dealer b/s/n · foreign dealer b/s/n ·
 * foreign total b/s/n · trust b/s/n · dealer self b/s/n · dealer hedging b/s/n ·
 * dealer total b/s/n · three-institution total n.
 */
/**
 * 當沖 (day-trade) volume per stock — TPEx `intraday/stat?type=Daily`, whose
 * `tables[1]` carries the same per-stock columns as TWSE's `TWTB4U`. The
 * payload echoes the REQUESTED date even on a holiday, with a zero-row table —
 * an empty table is the "no session" answer, so the rows gate it.
 */
export async function tpexDayTrade(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, number> | null> {
  const stamp = dayStampOf(dateMs)
  if (!stamp) return null
  const day = `${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}`
  const payload = (await getJson(
    `${TPEX_RWD}/intraday/stat?date=${day}&type=Daily&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  if (payload?.stat !== 'ok' || payload.date !== stamp) return null
  const table = (payload.tables ?? []).find((t) => t.fields?.some((f) => f.trim() === '證券代號'))
  if (!table?.fields || !table.data || table.data.length === 0) return null
  const at = indexer(table.fields)
  const out = new Map<string, number>()
  let sum = 0
  for (const row of table.data) {
    const shares = num(cell(row, at, '當日沖銷交易成交股數'))
    if (!Number.isFinite(shares)) continue
    sum += shares
    const symbol = cell(row, at, '證券代號')
    if (isScreenerSymbol(symbol)) out.set(symbol, shares)
  }
  // The rows SUM to `tables[0]`'s market total exactly (verified on every
  // probed session): a symbol absent from the table traded ZERO 當沖 that day,
  // so the caller writes 0. Rows that do not add up = a truncated payload →
  // null (unknown), never fabricated zeros.
  const aggregate = num(
    cell(
      payload.tables?.[0]?.data?.[0] ?? [],
      indexer(payload.tables?.[0]?.fields ?? []),
      '當日沖銷交易總成交股數'
    )
  )
  if (!(aggregate > 0) || Math.abs(sum - aggregate) > aggregate * 0.005) {
    console.log(`[tpex] dayTrade ${stamp}: rows ${sum} ≠ market ${aggregate} — treated as absent`)
    return null
  }
  return out.size > 0 ? out : null
}

export async function tpexDayFlows(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, InstSrc> | null> {
  const stamp = dayStampOf(dateMs)
  const url = `${TPEX_RWD}/insti/dailyTrade?date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json&type=Daily&sect=EW`
  const payload = (await getJson(url, fetchImpl, SLOW_MS)) as RwdPayload | null
  if (payload?.stat !== 'ok') return null
  const table = (payload.tables ?? [])[0]
  if (!table?.data || !payloadIsSession(table.date, payload.date, stamp)) return null
  const out = new Map<string, InstSrc>()
  for (const row of table.data) {
    const symbol = String(row[0] ?? '').trim()
    if (!isScreenerSymbol(symbol) || row.length < 23) continue
    const n = (i: number): number => num(String(row[i] ?? ''))
    const foreign = n(4) + n(7)
    if (!Number.isFinite(foreign)) continue
    out.set(symbol, { foreign, trust: n(13), dealer: n(16) + n(19) })
  }
  return out
}

/**
 * 外資持股 for ONE session on TWSE (`MI_QFIIS`): the shares held and the
 * holding ratio (%). The columns are matched by NAME — the payload's `fields`
 * list owns the exact wording (全體外資及陸資持有股數 / 全體外資及陸資持股比率)
 * and a rename must null the reader, never pick a neighbour column.
 */
export async function twseDayForeign(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, ForeignHold> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TWSE_RWD}/fund/MI_QFIIS?date=${stamp}&selectType=ALLBUT0999&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  if (payload?.stat !== 'OK') return null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload.date, stamp)) return null
  const sharesAt = table.fields.findIndex((f) => f.includes('持有股數'))
  const ratioAt = table.fields.findIndex((f) => f.includes('持股比率'))
  if (sharesAt < 0 || ratioAt < 0) return null
  const at = indexer(table.fields)
  const out = new Map<string, ForeignHold>()
  for (const row of table.data) {
    const symbol = cell(row, at, '證券代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    out.set(symbol, {
      shares: num(valueAt(row, sharesAt)),
      ratio: num(valueAt(row, ratioAt))
    })
  }
  return out.size > 0 ? out : null
}

/**
 * 外資持股 for ONE session on TPEx (`insti/qfii`, the `date=` parameter): the
 * ratio arrives as a `%` string (num strips it); the ranking column shifts
 * every position, so the code and the two values are matched by NAME.
 */
export async function tpexDayForeign(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, ForeignHold> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TPEX_RWD}/insti/qfii?date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload?.date, stamp)) return null
  const sharesAt = table.fields.findIndex((f) => f.includes('持有股數'))
  const ratioAt = table.fields.findIndex((f) => f.includes('持股比率'))
  if (sharesAt < 0 || ratioAt < 0) return null
  const at = indexer(table.fields)
  const out = new Map<string, ForeignHold>()
  for (const row of table.data) {
    const symbol = cell(row, at, '代號').trim()
    if (!isScreenerSymbol(symbol)) continue
    out.set(symbol, {
      shares: num(valueAt(row, sharesAt)),
      ratio: num(valueAt(row, ratioAt))
    })
  }
  return out.size > 0 ? out : null
}

/**
 * The TWSE 借券賣出 (SBL short-selling) balance for ONE session — the
 * `marginTrading/TWT93U` report ("信用額度總量管制餘額表", units 股). Its
 * columns repeat for the two blocks, so the SBL block is read by POSITION:
 * 8 前日餘額 · 9 當日賣出 · 10 當日還券 · 11 當日調整 · 12 當日餘額 ·
 * 13 次一營業日可限額. `當日餘額` (col 12) is the SAME metric TPEx's
 * `margin/sbl` carries and the one the contract's `sblBal` names (借券賣出,
 * DISTINCT from 融券) — the old undated `SBL/TWT96U` file answered
 * 可借券賣出股數, a different quantity, and is no longer used. The archive
 * answers back to at least 2007 (probed).
 */
export async function twseDaySblBal(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, SblSrc> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TWSE_RWD}/marginTrading/TWT93U?date=${stamp}&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  if (payload?.stat !== 'OK') return null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload.date, stamp)) return null
  const out = new Map<string, SblSrc>()
  for (const row of table.data) {
    const symbol = String(row[0] ?? '').trim()
    if (!isScreenerSymbol(symbol)) continue
    const balance = num(valueAt(row, 12))
    // A finite 0 is a REAL zero balance (the TPEx reader stores it too); only
    // a blank/'-' cell is unknown.
    if (!Number.isFinite(balance)) continue
    out.set(symbol, { sblBal: balance })
  }
  return out.size > 0 ? out : null
}

/**
 * The TPEx 借券 (SBL) balance for ONE session — the per-date `margin/sbl`
 * table's SECOND block. The block's field names repeat (`前日餘額`/`當日餘額`
 * twice), so the balance is read by POSITION: col 12 = 當日餘額 of the 借券
 * block = `SecuritiesBorrowingBalanceOfTheMarketDay`, the SAME metric the
 * daily OpenAPI reader stores.
 */
export async function tpexDaySblBal(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Map<string, SblSrc> | null> {
  const stamp = dayStampOf(dateMs)
  const payload = (await getJson(
    `${TPEX_RWD}/margin/sbl?date=${stamp.slice(0, 4)}/${stamp.slice(4, 6)}/${stamp.slice(6, 8)}&response=json`,
    fetchImpl,
    SLOW_MS
  )) as RwdPayload | null
  const table = flatTable(payload)
  if (!table || table.data.length === 0) return null
  if (!payloadIsSession(table.date, payload?.date, stamp)) return null
  const out = new Map<string, SblSrc>()
  for (const row of table.data) {
    const symbol = String(row[0] ?? '').trim()
    if (!isScreenerSymbol(symbol)) continue
    const balance = num(valueAt(row, 12))
    if (!Number.isFinite(balance)) continue
    out.set(symbol, { sblBal: balance })
  }
  return out.size > 0 ? out : null
}

/** ONE DayBar map → the ETL's daily-snapshot shape (the per-date fallback answer). */
function dayBarsToQuotes(bars: Map<string, DayBar>, dateMs: number): Sourced<DailyRow> {
  // The session is the CALENDAR DAY, but callers may pass a mid-session
  // instant (the mirrors' staleness probe passes `now`): a non-midnight stamp
  // broke every `dateMs === sessionDate` gate downstream (credit aggregate,
  // valuation freshness), so the row is stamped at Taipei midnight.
  const out = sourced<DailyRow>(stampToMs(dayStampOf(dateMs)))
  for (const [symbol, bar] of bars) {
    out.data.set(symbol, {
      symbol,
      name: bar.name,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      changePct: changePctOf(bar.close, bar.change),
      volume: bar.volume,
      tradeValue: bar.tradeValue,
      transactions: bar.tickets
    })
    out.rows++
  }
  return out
}

/**
 * The per-date table as a DAILY answer — the daily ETL's fallback when the
 * exchange's OpenAPI mirror is missing or behind the anchor (the mirrors are
 * cached separately and were observed frozen five sessions behind while the
 * per-date table answered immediately). `null` = not that session's payload
 * (holiday, not yet published) — the caller keeps its primary read.
 */
export async function twseDayQuotes(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Sourced<DailyRow> | null> {
  const bars = await twseDayBars(dateMs, fetchImpl)
  // An EMPTY table is not that session's answer (MI_INDEX answers the title
  // with zero rows before the report is generated) — the module's contract is
  // `null` = no session, so the anchor walk never treats it as a date.
  return bars && bars.size > 0 ? dayBarsToQuotes(bars, dateMs) : null
}

export async function tpexDayQuotes(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<Sourced<DailyRow> | null> {
  const bars = await tpexDayBars(dateMs, fetchImpl)
  return bars ? dayBarsToQuotes(bars, dateMs) : null
}
