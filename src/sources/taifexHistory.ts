/**
 * TAIFEX 三大法人 historical context — the exchange's dated HTML tables.
 *
 * The daily context reads the OpenAPI endpoints (latest session only). The
 * exchange's own `/cht/3/futAndOptDate` page accepts a POST date and splits
 * FUTURES and OPTIONS per identity — exactly the `TaifexInstitution` shape the
 * market history needs. `/cht/3/totalTableDate` is the COMBINED table and must
 * not be used for the split fields.
 *
 * This module is OPERATOR-ONLY (`seedMarketHistory.ts --taifex N`); the daily
 * ETL never calls it.
 */
import { postText, gregToMs, num, type FetchLike } from '@odf/screener-worker/httpJson'
import type {
  MarketDerivatives,
  MarketHistoryPoint,
  TaifexInstitution
} from '@odf/screener-worker/screener'

export const TAIFEX_FUT_OPT_URL = 'https://www.taifex.com.tw/cht/3/futAndOptDate'

const IDENTITY: Record<string, string> = {
  自營商: '自營商',
  投信: '投信',
  外資: '外資及陸資',
  外資及陸資: '外資及陸資'
}

const cellText = (html: string): string =>
  html
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/\s+/g, ' ')
    .trim()

const numOrNull = (value: string | undefined): number | null => {
  const n = num(value ?? '')
  return Number.isFinite(n) ? n : null
}

/** Parses the dated split table. Exported for the fixture test. */
export function parseTaifexInstitutionHistory(html: string): MarketHistoryPoint | null {
  if (html.includes('查無資料')) return null
  const dateMatch = html.match(/日期\s*(\d{4})\/(\d{1,2})\/(\d{1,2})/)
  if (!dateMatch) return null
  const dateMs = gregToMs(`${dateMatch[1]}/${dateMatch[2]}/${dateMatch[3]}`)
  if (dateMs <= 0) return null

  const rows: string[][] = []
  for (const match of html.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...match[1]!.matchAll(/<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((c) =>
      cellText(c[1]!)
    )
    const identity = cells[0] ?? ''
    if (cells.length < 13 || !(identity in IDENTITY || identity === '合計')) continue
    rows.push(cells)
  }
  // Two sections, four identities each: trading volume then open interest.
  if (rows.length < 8) return null
  const institutions: TaifexInstitution[] = []
  for (let i = 0; i < 4; i++) {
    const trading = rows[i]
    const oi = rows[i + 4]
    if (!trading || !oi) continue
    const item = IDENTITY[trading[0]!]
    if (!item) continue // the 合計 row sums the three identities; UI sums them too
    institutions.push({
      item,
      futuresLong: numOrNull(trading[0 + 1]),
      optionsLong: numOrNull(trading[0 + 2]),
      futuresShort: numOrNull(trading[4 + 1]),
      optionsShort: numOrNull(trading[4 + 2]),
      futuresNet: numOrNull(trading[8 + 1]),
      optionsNet: numOrNull(trading[8 + 2]),
      futuresOiLong: numOrNull(oi[0 + 1]),
      optionsOiLong: numOrNull(oi[0 + 2]),
      futuresOiShort: numOrNull(oi[4 + 1]),
      optionsOiShort: numOrNull(oi[4 + 2]),
      futuresOiNet: numOrNull(oi[8 + 1]),
      optionsOiNet: numOrNull(oi[8 + 2])
    })
  }
  return institutions.length > 0
    ? {
        sessionDate: dateMs,
        credit: null,
        derivatives: {
          asOf: dateMs,
          institutions,
          futures: [],
          options: [],
          largeTraders: [],
          pcr: [],
          vix: null
        } satisfies MarketDerivatives
      }
    : null
}

/** Fetches ONE historical session; null when the exchange has no table. */
export async function taifexInstitutionHistory(
  dateMs: number,
  fetchImpl?: FetchLike
): Promise<MarketHistoryPoint | null> {
  const d = new Date(dateMs + 8 * 3_600_000)
  const queryDate = `${d.getUTCFullYear()}/${d.getUTCMonth() + 1}/${d.getUTCDate()}`
  const body = new URLSearchParams({
    queryDate,
    queryType: '1',
    doQuery: '1',
    dateaddcnt: ''
  }).toString()
  const html = await postText(TAIFEX_FUT_OPT_URL, body, fetchImpl)
  return parseTaifexInstitutionHistory(html)
}
