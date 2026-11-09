/**
 * 集保戶股權分散 (TDCC) — the WEEKLY 千張大戶 / 散戶 split, collected from the
 * TDCC portal and stored as ONE small object (`holdings.json`):
 *
 *   { updatedAt, weeks: [ { week: 'YYYYMMDD',
 *                           symbols: { '2330': [big%, retail%, holders] } }, … ] }
 *
 * Newest week first, up to `HOLDINGS_WEEKS` (26) — the ETL joins the latest
 * week plus a 4-week Δ, so the series lives here rather than in the 780-session
 * history (a weekly datum repeated per session would be noise there).
 *
 * `big`   = the `1,000,001 股以上` bracket's share (%) of the 集保 CUSTODY
 *           balance — TDCC's own denominator, not the issued shares.
 * `retail`= the brackets at or below 50,000 shares (`≤ 50 張`, the usual 散戶
 *           line), same denominator.
 * `holders` = the total 集保 account count (戶數).
 *
 * WHY a separate job: the portal answers ONE stock per POST behind a one-time
 * Struts synchronizer token (a fresh page fetch per symbol), and the data is
 * weekly — a daily ETL has no business making 2 × N requests. The weekly
 * workflow runs `holdings.ts`; a failed symbol is simply absent for that week
 * (the snapshot cell reads `–`), and a run under 50 % coverage does NOT
 * overwrite the stored series (a throttled scrape must never erase it).
 */
import {
  HOLDINGS_WEEKS,
  type HoldingsObject,
  type HoldingsRow,
  type HoldingsWeek
} from '@odf/shared/screener'
import type { ScreenerEnv } from './env'
import { HOLDINGS_KEY } from './keys'

/** The v1 shape (`{week, symbols: {sym: {big, holders}}}`) is still readable:
 *  it is folded into a one-week series with no retail split. */
function normalize(raw: unknown): HoldingsObject | null {
  if (!raw || typeof raw !== 'object') return null
  const o = raw as Record<string, unknown>
  if (Array.isArray(o.weeks)) {
    const weeks: HoldingsWeek[] = []
    for (const w of o.weeks) {
      if (!w || typeof w !== 'object') continue
      const wk = w as HoldingsWeek
      if (typeof wk.week !== 'string' || !/^\d{8}$/.test(wk.week)) continue
      if (!wk.symbols || typeof wk.symbols !== 'object') continue
      weeks.push(wk)
    }
    weeks.sort((a, b) => b.week.localeCompare(a.week))
    return weeks.length > 0 ? { updatedAt: Number(o.updatedAt) || 0, weeks } : null
  }
  if (typeof o.week === 'string' && o.symbols && typeof o.symbols === 'object') {
    const symbols: Record<string, HoldingsRow> = {}
    for (const [symbol, v] of Object.entries(
      o.symbols as Record<string, { big?: unknown; holders?: unknown }>
    )) {
      const big = Number(v?.big)
      const holders = Number(v?.holders)
      if (Number.isFinite(big))
        symbols[symbol] = [big, null, Number.isFinite(holders) ? holders : 0]
    }
    return { updatedAt: Number(o.updatedAt) || 0, weeks: [{ week: o.week, symbols }] }
  }
  return null
}

/** Folds one collected week into the stored series (the same week is
 *  replaced, never duplicated) and keeps the newest `HOLDINGS_WEEKS`. */
export function mergeHoldings(
  raw: unknown,
  week: string,
  symbols: Record<string, HoldingsRow>
): HoldingsObject {
  const prior = normalize(raw)?.weeks ?? []
  const weeks = [{ week, symbols }, ...prior.filter((w) => w.week !== week)]
  weeks.sort((a, b) => b.week.localeCompare(a.week))
  return { updatedAt: Date.now(), weeks: weeks.slice(0, HOLDINGS_WEEKS) }
}

/** The stored series (v1 or v2), or null (absent / unreadable). */
export async function readHoldings(env: ScreenerEnv): Promise<HoldingsObject | null> {
  const obj = await env.R2_BUCKET.get(HOLDINGS_KEY)
  if (!obj) return null
  try {
    return normalize(await obj.json())
  } catch {
    return null
  }
}

const TDCC = 'https://www.tdcc.com.tw/portal/zh/smWeb/qryStock'
const UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36'
/** The portal's politeness budget: one GET+POST pair per paper, serialized —
 *  ~30 min for the whole market, once a week. */
const TDCC_DELAY_MS = 400

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

interface TdccSymbol {
  value: HoldingsRow
  week: string
}

interface TdccSession {
  token: string
  uri: string
  cookie: string
  /** The newest week the portal offers (`YYYYMMDD`). */
  week: string
}

/** A fresh form session: the synchronizer token is ONE-TIME, so every stock
 *  needs its own page fetch (and its own JSESSIONID cookie). */
async function tdccSession(): Promise<TdccSession> {
  const res = await fetch(TDCC, { headers: { 'User-Agent': UA } })
  if (!res.ok) throw new Error(`tdcc page HTTP ${res.status}`)
  const html = await res.text()
  const cookie = (res.headers.getSetCookie?.() ?? []).map((c) => c.split(';')[0]).join('; ')
  const token = html.match(/name="SYNCHRONIZER_TOKEN"[^>]*value="([^"]*)"/)?.[1] ?? ''
  const uri = html.match(/name="SYNCHRONIZER_URI"[^>]*value="([^"]*)"/)?.[1] ?? ''
  const weeks = [...html.matchAll(/<option[^>]*value="(\d{8})"/g)].map((m) => m[1])
  const dates = weeks.length > 0 ? weeks : [...html.matchAll(/value="(\d{8})"/g)].map((m) => m[1])
  const week = dates.slice().sort().at(-1)
  if (!token || !week) throw new Error('tdcc page lost its form token / week list')
  return { token, uri, cookie, week }
}

/** `"1,000,001以上"` → 1000001; `"1-999"` → 1; anything else → null. */
function bracketStart(label: string): number | null {
  const m = /^([\d,]+)(?:-|以上)/.exec(label)
  if (!m) return null
  const n = Number(m[1].replace(/,/g, ''))
  return Number.isFinite(n) ? n : null
}

/** The 散戶 line: brackets at or below 50,000 shares (`≤ 50 張`). */
const RETAIL_MAX_START = 50_000

/** One stock's table → [top bracket %, ≤50張 %, total holders]. */
function parseTdcc(html: string): HoldingsRow | null {
  let bestStart = -1
  let best: number | null = null
  let retail = 0
  let holders: number | null = null
  for (const tr of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)) {
    const cells = [...tr[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map((c) =>
      c[1]
        .replace(/<[^>]+>/g, '')
        .replace(/&nbsp;/g, ' ')
        .trim()
    )
    if (cells.length < 5) continue
    const label = cells[1]
    if (label.includes('合') && label.includes('計')) {
      const n = Number(cells[2].replace(/,/g, ''))
      if (Number.isFinite(n)) holders = n
      continue
    }
    const start = bracketStart(label)
    if (start === null) continue
    const share = Number(cells[4].replace(/,/g, ''))
    if (!Number.isFinite(share)) continue
    if (start < RETAIL_MAX_START) retail += share
    if (start > bestStart) {
      bestStart = start
      best = share
    }
  }
  if (best === null || bestStart < 1_000_000) return null
  return [best, Math.round(retail * 100) / 100, holders ?? 0]
}

async function tdccSymbol(symbol: string): Promise<TdccSymbol | null> {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const session = await tdccSession()
      await delay(TDCC_DELAY_MS)
      const body = new URLSearchParams({
        SYNCHRONIZER_TOKEN: session.token,
        SYNCHRONIZER_URI: session.uri,
        method: 'submit',
        firDate: session.week,
        scaDate: session.week,
        sqlMethod: 'StockNo',
        stockNo: symbol
      })
      const res = await fetch(TDCC, {
        method: 'POST',
        headers: {
          'User-Agent': UA,
          'Content-Type': 'application/x-www-form-urlencoded',
          Cookie: session.cookie,
          Referer: TDCC
        },
        body
      })
      if (!res.ok) throw new Error(`tdcc POST HTTP ${res.status}`)
      const parsed = parseTdcc(await res.text())
      if (parsed) return { value: parsed, week: session.week }
    } catch (err) {
      if (attempt === 2) console.warn(`[holdings] ${symbol} failed: ${String(err).slice(0, 160)}`)
    }
    await delay(TDCC_DELAY_MS * 2)
  }
  return null
}

export interface CollectResult {
  /** The newest week the portal served. */
  week: string
  symbols: Record<string, HoldingsRow>
  /** Symbols the portal answered. */
  found: number
  /** Symbols asked. */
  asked: number
}

/** Walks the symbols serially (the portal is a government host with an
 *  anti-bot token — never a burst). */
export async function collectHoldings(symbols: readonly string[]): Promise<CollectResult> {
  const out: Record<string, HoldingsRow> = {}
  let week = ''
  let found = 0
  for (const [i, symbol] of symbols.entries()) {
    const parsed = await tdccSymbol(symbol)
    if (parsed) {
      out[symbol] = parsed.value
      // Every session answers the same newest week; the first one wins.
      week ||= parsed.week
      found++
    }
    if ((i + 1) % 250 === 0) {
      console.log(`[holdings] ${i + 1}/${symbols.length} (found ${found})`)
    }
    // A short gap before the next symbol's page fetch (the pre-POST delay is
    // the main pacing; both requests still stay well under a burst).
    await delay(100)
  }
  if (!week) {
    const stamp = new Date(Date.now() + 8 * 3_600_000).toISOString().slice(0, 10).replace(/-/g, '')
    week = stamp
  }
  return { week, symbols: out, found, asked: symbols.length }
}
