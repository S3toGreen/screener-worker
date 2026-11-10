/**
 * MOPS readers — the two fundamental series the exchanges' OpenAPI files do
 * NOT provide per past session:
 *
 *   營益分析 (quarterly 三率)  POST /mops/web/ajax_t163sb06   (UTF-8 HTML)
 *   月營收 (MoM/YoY)          GET  /nas/t21/{sii|otc}/t21sc03_{ROC year}_{month}_{0|1}.html
 *                                                                (Big5 HTML)
 *
 * Both are market-wide for ONE quarter/month, so a 3-year backfill is ~40
 * requests instead of ~2 000 per-symbol pulls. The published values are the
 * same ones the daily `t187ap17`/`t187ap05` mirrors carry — MINUS the per-symbol
 * query cost.
 *
 * NOT called by the daily ETL or the backfill (their data contract is the
 * exchanges' DAILY RWD files only). This reader is retained for a low-frequency
 * manual fundamental refresh / a future monthly job.
 *
 * Layout facts (verified live):
 *   · the quarterly table is ONE `hasBorder` table; a row is
 *     公司代號 · 公司名稱 · 營業收入(百萬) · 毛利率 · 營業利益率 · 稅前純益率 · 稅後純益率
 *     and the trailing 合計 row has a single cell (filtered by isScreenerSymbol).
 *   · the monthly archive holds one nested table PER INDUSTRY; a row is
 *     代號 · 名稱 · 當月 · 上月 · 去年當月 · MoM% · YoY% · 當月累計 · 去年累計 · 前期% · 備註
 *     with the month UNPADDED in the filename (`_9_0`, `_1_1`) — a padded `_09`
 *     is a 404.
 *   · some cells use an upper-case `<Td …>` tag, so the cell scan is
 *     case-insensitive (a case-sensitive one silently drops that column).
 */
import { getText, num, postText, type FetchLike } from '@odf/screener-worker/httpJson'
import { isScreenerSymbol } from '../universe'

const MOPS = 'https://mopsov.twse.com.tw'

/** The quarterly 營益分析 ratios (%), null when the cell is empty/'-'. */
export interface QuarterMargins {
  gross: number | null
  op: number | null
  pretax: number | null
  net: number | null
}

/** The monthly revenue deltas (%) — the archive publishes them directly. */
export interface RevenueDelta {
  mom: number | null
  yoy: number | null
  /** 前期比較增減 (%) — the CUMULATIVE year-to-date growth the report states. */
  cumYoy: number | null
  /** 當月營收 (NT$ thousands) and the report's month (`YYYYMM`). */
  revenueMonth: number | null
  revenueYm: number | null
}

/** A MOPS report is a whole market's page — give it a minute. */
const MOPS_MS = 60_000

/** Run time, not startup: a broken MOPS layout must fail the run loudly
 *  instead of null-filling half the history (the plan's louder-failure rule). */
export function expectRows<T>(
  rows: Map<string, T>,
  minCount: number,
  what: string
): Map<string, T> {
  if (rows.size < minCount) {
    throw new Error(`${what}: parsed ${rows.size} rows, expected >= ${minCount}`)
  }
  return rows
}

/** The text of a cell, tags stripped and `&nbsp;` unwrapped. */
function cellText(html: string): string {
  return html
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/gi, ' ')
    .trim()
}

function nullable(value: number): number | null {
  return Number.isFinite(value) ? value : null
}

/**
 * `yearCn` is the ROC year (115 = 2026), `season` '01'..'04' with 04 = the
 * annual report. The market is `sii` (上市) or `otc` (上櫃); the MOPS POST
 * body repeats the site's own form (including its `firstin=ture` typo).
 */
export async function mopsQuarterMargins(
  yearCn: number,
  season: '01' | '02' | '03' | '04',
  market: 'sii' | 'otc',
  fetchImpl?: FetchLike
): Promise<Map<string, QuarterMargins>> {
  const body = new URLSearchParams({
    encodeURIComponent: '1',
    step: '1',
    firstin: 'ture',
    off: '1',
    isQuery: 'Y',
    TYPEK: market,
    year: String(yearCn),
    season
  }).toString()
  const html = await postText(`${MOPS}/mops/web/ajax_t163sb06`, body, fetchImpl, MOPS_MS)
  const out = new Map<string, QuarterMargins>()
  for (const match of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...match[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((m) => cellText(m[1]))
    if (cells.length < 7) continue
    const symbol = cells[0]
    if (!isScreenerSymbol(symbol)) continue
    const margins: QuarterMargins = {
      gross: nullable(num(cells[3])),
      op: nullable(num(cells[4])),
      pretax: nullable(num(cells[5])),
      net: nullable(num(cells[6]))
    }
    if (
      margins.gross === null &&
      margins.op === null &&
      margins.pretax === null &&
      margins.net === null
    ) {
      continue
    }
    out.set(symbol, margins)
  }
  return out
}

/**
 * The monthly revenue archive for ONE month. `foreign` selects the 國外 (KY)
 * file — a company files under exactly one of the two, so the caller unions
 * them. The month is UNPADDED in the filename (`t21sc03_115_9_0.html`); the
 * file is Big5.
 */
export async function mopsMonthRevenue(
  yearCn: number,
  month: number,
  market: 'sii' | 'otc',
  foreign: boolean,
  fetchImpl?: FetchLike
): Promise<Map<string, RevenueDelta>> {
  const url = `${MOPS}/nas/t21/${market}/t21sc03_${yearCn}_${month}_${foreign ? 1 : 0}.html`
  const out = new Map<string, RevenueDelta>()
  let html: string
  try {
    html = await getText(url, fetchImpl, MOPS_MS, 'big5')
  } catch (err) {
    // A 404 = that file does not exist for this month (an archive edge, or a
    // KY file with no filers). Absent rows, never a failed backfill — the
    // publish-day gate already excludes months that are simply not out yet.
    if (String(err).includes('HTTP 404')) return out
    throw err
  }
  // One nested table PER INDUSTRY — walk every row of the document.
  for (const match of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...match[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((m) => cellText(m[1]))
    if (cells.length < 7) continue
    const symbol = cells[0]
    if (!isScreenerSymbol(symbol)) continue
    out.set(symbol, {
      mom: nullable(num(cells[5])),
      yoy: nullable(num(cells[6])),
      cumYoy: nullable(num(cells[9])),
      revenueMonth: nullable(num(cells[2])),
      revenueYm: (yearCn + 1911) * 100 + month
    })
  }
  return out
}
