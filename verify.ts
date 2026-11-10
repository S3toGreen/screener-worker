/**
 * The post-publish smoke check — reads the PUBLIC API (never R2) and proves the
 * five things a broken deploy silently loses:
 *
 *   1. the grid is the LATEST session, at the full column width, and carries
 *      NOTHING market-wide (the context is its own object);
 *   2. its TAIEX benchmark close IS the index's official close for that
 *      session, and `ret1d` derives from the stored series;
 *   3. `/api/market` answers a valid context (credit and/or derivatives);
 *   4. a sample of histories (a TWSE stock, a TPEx stock, an ETF, a six-digit
 *      ETF) is aligned, ascending, ends on the session and agrees with the row;
 *   5. the API actually SERVES those histories — a Pages deploy that predates
 *      the fund universe 404s every ETF while the bucket holds them.
 *
 *   bun --env-file=.dev.vars verify.ts
 *   bun --env-file=.dev.vars verify.ts --api https://odf-trading.pages.dev --symbols 2330,0050
 *
 * Exit 0 = every check passed; exit 1 = at least one failed (printed).
 */
import {
  HOLDINGS_WEEKS,
  SC,
  SCREENER_BASE_FIELDS,
  SCREENER_FIELDS,
  SCREENER_HISTORY_SESSIONS,
  holdingsSeriesOf,
  isDividendsObject,
  isHoldingsObject,
  isScreenerHistory,
  readMarketContext,
  screenNum,
  screenerDividendsUrl,
  screenerHistoryUrl,
  screenerHoldingsUrl,
  screenerMarketUrl,
  screenerSnapshotUrl
} from '@odf/screener-worker/screener'
import { SCREENER_CDN_DEFAULT } from '@odf/screener-worker/config'

const arg = (name: string, fallback: string): string => {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback
}
const api = arg('api', SCREENER_CDN_DEFAULT).replace(/\/+$/, '')
const symbols = arg('symbols', '2330,0050,6488,009800').split(',')

const failures: string[] = []
function check(ok: boolean, what: string, detail = ''): void {
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}${detail ? ` — ${detail}` : ''}`)
  if (!ok) failures.push(what)
}

/** The session stamp (YYYYMMDD) of an epoch, in the exchange's own calendar. */
function stampOf(ms: number): string {
  return new Date(ms + 8 * 3_600_000).toISOString().slice(0, 10).replace(/-/g, '')
}

/** `20261030` → the ROC form the rwd endpoints take (`115/10/30`). */
function rocDate(stamp: string): string {
  return `${Number(stamp.slice(0, 4)) - 1911}/${stamp.slice(4, 6)}/${stamp.slice(6)}`
}

async function getJson(url: string): Promise<unknown> {
  // A hung edge/R2 response must fail THIS check, not burn the step's 5-minute
  // budget (the workflow's timeout is the outer bound, this the inner one).
  const res = await fetch(url, {
    headers: { 'User-Agent': 'odf-screener-verify' },
    signal: AbortSignal.timeout(30_000)
  })
  if (!res.ok) throw new Error(`${url} → HTTP ${res.status}`)
  return res.json()
}

const bust = `?t=${Date.now()}`
const snapshot = (await getJson(`${screenerSnapshotUrl(api)}${bust}`)) as {
  sessionDate: number
  rows: Array<Array<string | number | null>>
  benchmark: { close: number | null; ret1d: number | null } | null
  market?: unknown
}
const stamp = stampOf(snapshot.sessionDate)
check(snapshot.rows.length > 500, 'snapshot rows', String(snapshot.rows.length))
// The row contract is APPEND-ONLY: a run that SKIPPED (the session was
// already stored) leaves the previous width in place, and every client pads an
// older row to the current contract. Demand the BASE width — a truncated
// payload still fails — and report which contract the served object is on.
const width = snapshot.rows[0]?.length ?? 0
check(
  width >= SCREENER_BASE_FIELDS,
  `snapshot width ≥ base contract`,
  `${width}/${SCREENER_BASE_FIELDS}`
)
if (width < SCREENER_FIELDS.length) {
  console.log(
    `  · snapshot is on the ${width}-column contract (current ${SCREENER_FIELDS.length}) — the next ETL publish widens it`
  )
}
// The stock grid carries per-stock rows ONLY: a market-wide block riding it
// means the two products were mixed again.
check(snapshot.market === undefined, 'snapshot carries no market-wide block')

// 3. The market context is its OWN object/route (regularly updated, and its
// absence must be a missing ROUTE — never a silently empty section).
try {
  const raw: unknown = await getJson(`${screenerMarketUrl(api)}${bust}`)
  const ctx = readMarketContext(raw)
  check(ctx !== null, 'market context shape')
  if (ctx) {
    check(
      ctx.credit !== null || ctx.derivatives !== null,
      'market context has credit and/or derivatives',
      `credit=${ctx.credit !== null} derivatives=${ctx.derivatives !== null}`
    )
    if (ctx.credit) {
      check(
        ctx.credit.twseMarginBalanceLots !== null &&
          (ctx.credit.maintenanceRatio !== null || ctx.credit.officialMaintenanceRatio !== null),
        'credit balance present; the estimate and the official ratio are separate slots',
        `estimate=${ctx.credit.maintenanceRatio} official=${ctx.credit.officialMaintenanceRatio} scope=${ctx.credit.scope}`
      )
    }
    if (ctx.derivatives) {
      check(
        ctx.derivatives.institutions.length > 0 || ctx.derivatives.pcr.length > 0,
        'derivatives context rows',
        `${ctx.derivatives.institutions.length} institutions, ${ctx.derivatives.pcr.length} PCR`
      )
    }
  }
} catch (err) {
  check(false, 'market context served', String(err))
}

// 2. The TAIEX benchmark against the exchange's own monthly index table.
const fmtqik = (await getJson(
  `https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=${stamp}&response=json`
)) as { stat?: string; data?: Array<Array<string>> }
const rows = fmtqik.data ?? []
const at = rows.findIndex((r) => String(r[0]).trim() === rocDate(stamp))
const official = at >= 0 ? Number(String(rows[at][4]).replace(/,/g, '')) : NaN
const bench = snapshot.benchmark
check(Number.isFinite(official), `TAIEX close published for ${stamp}`, rocDate(stamp))
check(
  bench !== null && bench.close !== null && Math.abs(bench.close - official) < 0.01,
  'benchmark.close = the official index close',
  `snapshot ${bench?.close} vs exchange ${official}`
)
if (at >= 1 && bench?.ret1d != null) {
  const prev = Number(String(rows[at - 1][4]).replace(/,/g, ''))
  const ret1d = (official / prev - 1) * 100
  check(
    Math.abs(bench.ret1d - ret1d) < 0.01,
    'benchmark.ret1d derives from the series',
    `snapshot ${bench.ret1d.toFixed(4)} vs computed ${ret1d.toFixed(4)}`
  )
}

// 3+4. The histories: served, aligned, ascending, ending on the session, and
// agreeing with the grid's own close for that symbol.
const bySymbol = new Map(snapshot.rows.map((r) => [String(r[0]), r]))

// The two sourced columns must be present on a TWSE row once their feeds ran:
// 當沖 comes from the daily ETL, 千張大戶 from the weekly TDCC job (absent until
// the first run — reported, not failed).
const r2330 = bySymbol.get('2330')
const dayTrade = r2330 ? screenNum(r2330, 'dayTradeRatio') : null
check(
  dayTrade !== null && dayTrade > 0 && dayTrade <= 100,
  '2330 dayTradeRatio in (0, 100]',
  `${dayTrade}`
)
const retail = r2330 ? screenNum(r2330, 'retailPct') : null
check(retail !== null && retail > 0 && retail < 100, '2330 retailPct in (0, 100)', `${retail}`)
const cumYoy = r2330 ? screenNum(r2330, 'revenueCumYoy') : null
check(cumYoy !== null, '2330 revenueCumYoy present', `${cumYoy}`)
const ttm = r2330 ? screenNum(r2330, 'revenueTtm') : null
check(ttm !== null && ttm > 0, '2330 revenueTtm present (12 months)', `${ttm}`)
const listed = r2330 ? String(r2330[SC.listedDate] ?? '') : ''
check(/^\d{8}$/.test(listed), '2330 listedDate stamp', listed)
// The quarterly tables publish in WAVES, so a mid-season snapshot must carry
// the last KNOWN quarter for every company — a batch-only read nulls them.
const gm2330 = r2330 ? screenNum(r2330, 'grossMargin') : null
check(
  gm2330 !== null && gm2330 > 0 && gm2330 < 100,
  '2330 grossMargin (carried quarter)',
  `${gm2330}`
)
const marginRows = snapshot.rows.filter((r) => r[SC.grossMargin] !== null).length
check(
  marginRows > 1500,
  'margin coverage (stocks, not just the newest filing batch)',
  `${marginRows}/${snapshot.rows.length}`
)
const big = r2330 ? screenNum(r2330, 'bigHolderPct') : null
console.log(`${big === null ? 'note' : 'ok  '} 2330 bigHolderPct — ${big ?? 'no TDCC object yet'}`)
// A TPEx row carries its own 當沖 series (the OTC exchange publishes one too).
const r6488 = bySymbol.get('6488')
const tpDay = r6488 ? screenNum(r6488, 'dayTradeRatio') : null
check(
  tpDay !== null && tpDay > 0 && tpDay <= 100,
  '6488 (TPEx) dayTradeRatio in (0, 100]',
  `${tpDay}`
)

for (const symbol of symbols) {
  const row = bySymbol.get(symbol)
  if (!row) {
    check(false, `${symbol} in the snapshot`, 'absent')
    continue
  }
  let body: unknown
  try {
    body = await getJson(`${screenerHistoryUrl(api, symbol)}${bust}`)
  } catch (err) {
    check(false, `${symbol} history served`, String(err))
    continue
  }
  check(isScreenerHistory(body, symbol), `${symbol} history shape`, `${typeof body}`)
  if (!isScreenerHistory(body, symbol)) continue
  const h = body
  const n = h.dates.length
  const ascending = h.dates.every((d, i) => i === 0 || h.dates[i - 1] < d)
  // `symbol` is the one string member; every other key is an aligned column.
  const aligned = Object.entries(h).every(
    ([key, v]) => key === 'symbol' || (Array.isArray(v) && v.length === n)
  )
  check(ascending && aligned, `${symbol} aligned + ascending`, `${n} sessions`)
  check(
    n <= SCREENER_HISTORY_SESSIONS,
    `${symbol} within the retention`,
    `${n} ≤ ${SCREENER_HISTORY_SESSIONS}`
  )
  check(h.dates[n - 1] === stamp, `${symbol} ends on the session`, `${h.dates[n - 1]} vs ${stamp}`)
  const lastClose = h.close[n - 1]
  const rowClose = screenNum(row, 'close')
  check(
    lastClose != null && rowClose != null && lastClose === rowClose,
    `${symbol} history close = snapshot close`,
    `${lastClose} vs ${rowClose}`
  )
}

// The two REFERENCE objects ride their own routes (they are market-wide, not
// per symbol) — a Pages deploy that predates them 404s the preview's weekly
// TDCC pane and its dividend row while the bucket holds both, so prove each
// one is SERVED through the deployed function.
try {
  const holdings: unknown = await getJson(`${screenerHoldingsUrl(api)}${bust}`)
  check(isHoldingsObject(holdings), 'holdings object served', typeof holdings)
  if (isHoldingsObject(holdings)) {
    const weeks = holdings.weeks.length
    check(weeks > 0 && weeks <= HOLDINGS_WEEKS, 'holdings week count', `${weeks}`)
    const newest = holdings.weeks[0].week
    check(newest <= stamp, 'holdings newest week not ahead of the session', `${newest} vs ${stamp}`)
    const covered = Object.keys(holdings.weeks[0].symbols).length
    check(covered >= 500, 'holdings coverage (newest week)', `${covered} symbols`)
    const known = holdingsSeriesOf(holdings, '2330')
    check(known.length > 0, 'holdings carry 2330', `${known.length} weeks`)
  }
} catch (err) {
  check(false, 'holdings object served', String(err))
}
try {
  const dividends: unknown = await getJson(`${screenerDividendsUrl(api)}${bust}`)
  // Shape only: the calendar legitimately empties outside a dividend season,
  // so an empty list is a valid answer — a missing ROUTE is not.
  check(isDividendsObject(dividends), 'dividends object served', typeof dividends)
} catch (err) {
  check(false, 'dividends object served', String(err))
}

if (failures.length > 0) {
  console.error(`\n${failures.length} check(s) FAILED:\n  ${failures.join('\n  ')}`)
  process.exit(1)
}
console.log(`\nall checks passed — ${api} serves session ${stamp} (${snapshot.rows.length} rows)`)
