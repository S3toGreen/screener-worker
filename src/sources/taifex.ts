/**
 * TAIFEX context readers — the daily derivatives aggregates the screener's
 * Market Overview consumes. The ETL now retains a rolling observation series
 * of these daily snapshots so the panel can show changes and a trend. The
 * exchange publishes one session at a time; the readers themselves stay
 * per-session and never invent a prior value.
 *
 * PAYLOAD SHAPE FLIPS: the exchange serves some of these as JSON and others as
 * CSV (the general institutional table answered CSV even with
 * `Accept: application/json`; the interest/gold/FX margining tables and the
 * contract-adjustment list are CSV too), and the CSV bodies start with a UTF-8
 * BOM. `taifexRows` reads the body ONCE as text and picks by shape, so a flip
 * can never blank a section silently.
 *
 * The exchange publishes AGGREGATES only: the three institutions' OI and the
 * large traders' top 5 / top 10 buckets — never trader-level positions. The
 * large-trader tables also carry per-settlement-month rows; the daily context
 * keeps the ALL-MONTHS aggregate row (`SettlementMonth = 999912`) alone.
 * (TWSE's own 全市場擔保維持率 lives in `twse.ts` — `BFIJ3U_TREND`.)
 */
import { getRecords, getText, gregToMs, num, type FetchLike } from '@odf/shared/httpJson'
import type {
  MarketDerivatives,
  TaifexContractPosition,
  TaifexInstitution,
  TaifexLargeTrader,
  TaifexOptionSide,
  TaifexPcr
} from '@odf/shared/screener'

const BASE = 'https://openapi.taifex.com.tw/v1'
/** The index derivatives the app's screens and the option chain cover. */
const FUTURES_CONTRACTS = new Set(['臺股期貨', '小型臺指', '微型臺指', '電子期貨', '金融期貨'])
const OPTION_CONTRACTS = new Set(['臺指選擇權'])
const LARGE_TRADER_CONTRACTS = new Set(['TX', 'MTX', 'TMF', 'TE', 'TF', 'TXO'])

/** One row's cell by column name. */
type Row = (field: string) => string

/** The TAIFEX row number: the payload's cells are strings, `-` and blank mean
 *  absent (NaN → null at the caller), never zero. */
const n = (read: Row, field: string): number | null => {
  const v = num(read(field))
  return Number.isFinite(v) ? v : null
}

/** Reads ONE endpoint through the shared shape-tolerant reader. */
const taifexRows = (path: string, fetchImpl?: FetchLike): Promise<Row[] | null> =>
  getRecords(`${BASE}${path}`, fetchImpl)

/** The session the rows report, from their own `Date` column (0 when none). */
function rowsDate(rows: Row[]): number {
  for (const r of rows) {
    const ms = gregToMs(r('Date'))
    if (ms > 0) return ms
  }
  return 0
}

/** 三大法人 — futures and options reported SEPARATELY, one row per identity. */
export async function taifexInstitutions(
  fetchImpl?: FetchLike
): Promise<{ asOf: number; rows: TaifexInstitution[] } | null> {
  const rows = await taifexRows(
    '/MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate',
    fetchImpl
  )
  if (!rows || rows.length === 0) return null
  const out: TaifexInstitution[] = []
  for (const r of rows) {
    const item = r('Item').trim()
    if (!item) continue
    out.push({
      item,
      futuresLong: n(r, 'FuturesTradingVolume(Long)'),
      futuresShort: n(r, 'FuturesTradingVolume(Short)'),
      futuresNet: n(r, 'FuturesTradingVolume(Net)'),
      futuresOiLong: n(r, 'FuturesOpenInterest(Long)'),
      futuresOiShort: n(r, 'FuturesOpenInterest(Short)'),
      futuresOiNet: n(r, 'FuturesOpenInterest(Net)'),
      optionsLong: n(r, 'OptionsTradingVolume(Long)'),
      optionsShort: n(r, 'OptionsTradingVolume(Short)'),
      optionsNet: n(r, 'OptionsTradingVolume(Net)'),
      optionsOiLong: n(r, 'OptionsOpenInterest(Long)'),
      optionsOiShort: n(r, 'OptionsOpenInterest(Short)'),
      optionsOiNet: n(r, 'OptionsOpenInterest(Net)')
    })
  }
  return out.length > 0 ? { asOf: rowsDate(rows), rows: out } : null
}

/** Institutional positions by FUTURES contract (the index roster only). */
export async function taifexFuturesPositions(
  fetchImpl?: FetchLike
): Promise<TaifexContractPosition[]> {
  const rows = await taifexRows(
    '/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate',
    fetchImpl
  )
  if (!rows) return []
  const out: TaifexContractPosition[] = []
  for (const r of rows) {
    const contract = r('ContractCode').trim()
    if (!FUTURES_CONTRACTS.has(contract)) continue
    out.push({
      contract,
      item: r('Item').trim(),
      long: n(r, 'TradingVolume(Long)'),
      short: n(r, 'TradingVolume(Short)'),
      net: n(r, 'TradingVolume(Net)'),
      oiLong: n(r, 'OpenInterest(Long)'),
      oiShort: n(r, 'OpenInterest(Short)'),
      oiNet: n(r, 'OpenInterest(Net)'),
      oiValueNtd: thousands(n(r, 'ContractValueofOpenInterest(Net)(Thousands)'))
    })
  }
  return out
}

/** Institutional CALL/PUT positions (臺指選擇權 — the chain's instrument). */
export async function taifexOptionSides(fetchImpl?: FetchLike): Promise<TaifexOptionSide[]> {
  const rows = await taifexRows(
    '/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate',
    fetchImpl
  )
  if (!rows) return []
  const out: TaifexOptionSide[] = []
  for (const r of rows) {
    const contract = r('ContractCode').trim()
    if (!OPTION_CONTRACTS.has(contract)) continue
    out.push({
      contract,
      callPut: r('CallPut').trim() === 'PUT' ? 'PUT' : 'CALL',
      item: r('Item').trim(),
      long: n(r, 'TradingVolume(Long)'),
      short: n(r, 'TradingVolume(Short)'),
      net: n(r, 'TradingVolume(Net)'),
      oiLong: n(r, 'OpenInterest(Long)'),
      oiShort: n(r, 'OpenInterest(Short)'),
      oiNet: n(r, 'OpenInterest(Net)'),
      oiValueNtd: thousands(n(r, 'ContractValueofOpenInterest(Net)(Thousands)'))
    })
  }
  return out
}

/** The exchange's ALL-MONTHS aggregate bucket in the large-trader tables. */
const AGGREGATE_MONTH = '999912'

/** Large-trader OI — top 5 / top 10 buy & sell against the market OI, for
 *  futures and options (calls and puts separate), AGGREGATE months only. */
export async function taifexLargeTraders(fetchImpl?: FetchLike): Promise<TaifexLargeTrader[]> {
  const out: TaifexLargeTrader[] = []
  const futures = await taifexRows('/OpenInterestOfLargeTradersFutures', fetchImpl)
  for (const r of futures ?? []) {
    const contract = r('Contract').trim()
    if (!LARGE_TRADER_CONTRACTS.has(contract)) continue
    if (r('SettlementMonth').trim() !== AGGREGATE_MONTH) continue
    out.push({
      contract,
      name: r('ContractName').trim(),
      product: 'futures',
      callPut: null,
      traderType: r('TypeOfTraders').trim(),
      top5Buy: n(r, 'Top5Buy'),
      top5Sell: n(r, 'Top5Sell'),
      top10Buy: n(r, 'Top10Buy'),
      top10Sell: n(r, 'Top10Sell'),
      marketOi: n(r, 'OIOfMarket')
    })
  }
  const options = await taifexRows('/OpenInterestOfLargeTradersOptions', fetchImpl)
  for (const r of options ?? []) {
    const contract = r('Contract').trim()
    if (!LARGE_TRADER_CONTRACTS.has(contract)) continue
    if (r('SettlementMonth').trim() !== AGGREGATE_MONTH) continue
    const callPut = r('CallPut').trim()
    out.push({
      contract,
      name: r('ContractName').trim(),
      product: 'options',
      callPut: callPut === 'PUT' ? 'PUT' : callPut === 'CALL' ? 'CALL' : null,
      traderType: r('TypeOfTraders').trim(),
      top5Buy: n(r, 'Top5Buy'),
      top5Sell: n(r, 'Top5Sell'),
      top10Buy: n(r, 'Top10Buy'),
      top10Sell: n(r, 'Top10Sell'),
      marketOi: n(r, 'OiOfMarket')
    })
  }
  return out
}

/** 臺指選擇權 Put/Call statistics — the endpoint answers ~19 recent sessions;
 *  the VOLUME and OI ratios are the exchange's own, kept distinct. */
export async function taifexPcr(fetchImpl?: FetchLike): Promise<TaifexPcr[]> {
  const rows = await taifexRows('/PutCallRatio', fetchImpl)
  if (!rows) return []
  const out: TaifexPcr[] = []
  for (const r of rows) {
    const dateMs = gregToMs(r('Date'))
    if (dateMs === 0) continue
    const ratio = (field: string): number | null => {
      const v = r(field).trim().replace(/%$/, '')
      const parsed = num(v)
      return Number.isFinite(parsed) ? parsed : null
    }
    out.push({
      dateMs,
      putVolume: n(r, 'PutVolume'),
      callVolume: n(r, 'CallVolume'),
      volumeRatio: ratio('PutCallVolumeRatio%'),
      putOi: n(r, 'PutOI'),
      callOi: n(r, 'CallOI'),
      oiRatio: ratio('PutCallOIRatio%')
    })
  }
  return out.sort((a, b) => b.dateMs - a.dateMs)
}

/** `仟元` → NT$ (the report's unit, kept explicit at the boundary). */
function thousands(v: number | null): number | null {
  return v === null ? null : v * 1000
}

/**
 * The TAIWAN VIX daily close — the exchange's own monthly file
 * (`log2data/YYYYMMnew.txt`: date, time, index, 1-minute-average), latest row
 * of the newest month that has data. Big5 encoding; the numbers are ASCII.
 */
export async function taifexVix(
  fetchImpl?: FetchLike
): Promise<{ dateMs: number; close: number } | null> {
  const now = new Date()
  for (let back = 0; back < 3; back++) {
    const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - back, 1))
    const ym = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}`
    let text: string
    try {
      text = await getText(
        `https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/${ym}new.txt`,
        fetchImpl,
        20_000,
        'big5'
      )
    } catch {
      continue
    }
    const lines = text.split(/\r?\n/).filter((line) => /^\d{8}\t/.test(line))
    const last = lines[lines.length - 1]
    if (!last) continue
    // The row is TAB-padded (`date \t time \t \t \t index \t \t avg`), so
    // the EMPTY cells are dropped before indexing.
    const cells = last
      .split('\t')
      .map((c) => c.trim())
      .filter((c) => c !== '')
    const dateMs = gregToMs(cells[0] ?? '')
    const close = num(cells[2] ?? '')
    if (dateMs > 0 && Number.isFinite(close) && close > 0) return { dateMs, close }
  }
  return null
}

/**
 * The whole latest-only derivatives context. Every section is optional: a
 * failed read yields an empty list / null (the snapshot's section renders as
 * absent), never a zero. Failures are logged by the caller's `readSafe`.
 */
export async function taifexDerivatives(fetchImpl?: FetchLike): Promise<MarketDerivatives> {
  const [inst, futures, options, largeTraders, pcr, vix] = await Promise.all([
    taifexInstitutions(fetchImpl),
    taifexFuturesPositions(fetchImpl),
    taifexOptionSides(fetchImpl),
    taifexLargeTraders(fetchImpl),
    taifexPcr(fetchImpl),
    taifexVix(fetchImpl)
  ])
  const asOf = Math.max(inst?.asOf ?? 0, pcr[0]?.dateMs ?? 0, vix?.dateMs ?? 0)
  return {
    asOf,
    institutions: inst?.rows ?? [],
    futures,
    options,
    largeTraders,
    pcr,
    vix
  }
}
