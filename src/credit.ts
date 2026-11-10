/**
 * 信用交易 (margin / short) analysis shared by the screener ETL — which
 * produces the per-stock estimates — and the renderer, which displays them.
 *
 * TERMINOLOGY (the two short mechanisms are NEVER merged):
 * - 融資融券 = margin purchase / SHORT SALE (our `margin*` / `short*` fields).
 * - 借券賣出 = SECURITIES LENDING short sales (`sblBal` on the row) — a
 *   different mechanism with its own balance and its own history column.
 *
 * UNITS: balances are the exchange's own margin units — 交易單位 (lots) for
 * stocks and ETFs on both TWSE and TPEx. `close` and `cost` are NT$ per SHARE.
 * The rolling formula is unit-agnostic (lots × NT$/share cancels), so the lot
 * balance is used as-is.
 *
 * WHAT IS AN ESTIMATE (and always labelled as one):
 * - `cost` — no official per-investor margin cost exists. It is a rolling
 *   volume-weighted approximation from the published daily flows
 *   (`rollAverageMarginCost`). A COLD START (no prior state) seeds it from the
 *   symbol's trailing N-session VWAP (Σ成交金額 ÷ Σ成交股數), not from today's
 *   close: a close-seeded book makes every symbol read exactly 100/0.6 = 166.7 %
 *   and the column useless. With no warm seed the session publishes NULL —
 *   never a fabricated constant — while the state still bootstraps.
 * - `financingRatio` — the general maximum is 60 % for TWSE/TPEx listed
 *   securities. Per-security overrides (notice/disposal names) have no
 *   machine-readable per-date route, so the default is used and marked.
 * - The resulting maintenance ratio is a STOCK-LEVEL RISK INDICATOR, not any
 *   brokerage account's actual 整戶擔保維持率.
 */

import { dayStampOf } from './session'

/** The general maximum margin-purchase ratio, in PERCENT (TWSE and TPEx). */
export const MARGIN_FINANCING_RATIO_DEFAULT = 60

/**
 * A one-session price gap at or above this ratio re-initializes the rolling
 * cost at the current close. A split / 減資 moves the price this far while the
 * lot balance stays (a corporate action, not a loss); a genuine one-day move
 * of this size is rare enough that resetting the estimate is the safe read.
 */
export const MARGIN_COST_RESET_JUMP = 0.5

/** Entries not refreshed for this long are dropped from the state file. */
export const MARGIN_COST_MAX_AGE_DAYS = 420

/** The state object's shape version. Bumping it re-initializes every symbol
 *  (v2 = the warm VWAP cold start; a v1 file was close-seeded). */
export const MARGIN_COST_STATE_VERSION = 2

/** One symbol's rolling estimate: NT$ per share and the session it describes. */
export interface MarginCostEntry {
  cost: number
  /** `YYYYMMDD` session stamp. */
  asOf: string
  /** False when the cost was initialized at the CLOSE (no warm prior / a
   *  corporate-action jump) — such a cost carries no information and its
   *  ratio (the constant 100/0.6) must stay unpublished, including on a
   *  same-session re-run. Absent = warm (an older v2 entry). */
  warm?: boolean
}

/** `symbol → entry` — the whole persisted estimate state. */
export type MarginCostState = Record<string, MarginCostEntry>

/** The persisted object `credit-state.json`. */
export interface CreditStateFile {
  version: number
  /** The newest session the state was rolled for. */
  asOf: string
  symbols: MarginCostState
}

/** One session's published margin flows, in the exchange's units. */
export interface CreditFlows {
  prevBal: number
  buy: number
  sell: number
  cashRepay: number
  /** Today's balance (the exchange's own, authoritative). */
  bal: number
  /** NT$ per share. */
  close: number
}

export interface RolledMarginCost {
  cost: number
  /** True when the estimate restarted (no usable prior state / price jump). */
  reset: boolean
  /** True when the cost carries information beyond today's close (rolled from
   *  a prior book, or seeded from the warm VWAP). False = initialized at the
   *  close, where the ratio is the constant 100/0.6 — publish NOTHING. */
  warm: boolean
}

/**
 * The rolling average margin cost:
 *
 *   remaining = prevBal − cashRepay − sell
 *   cost = (remaining × priorCost + buy × close) / bal
 *
 * Null when the balance is zero or the price is unusable. A missing/zero
 * prior balance, a missing prior state, or a corporate-action-sized price jump
 * initializes at today's close instead (the caller can log `reset`).
 */
export function rollAverageMarginCost(
  prior: MarginCostEntry | null,
  flows: CreditFlows,
  seedCost?: number | null
): RolledMarginCost | null {
  const { prevBal, buy, sell, cashRepay, bal, close } = flows
  if (!(bal > 0) || !(close > 0) || !Number.isFinite(close)) return null
  const remaining = Math.max(0, prevBal - cashRepay - sell)
  const fresh = Math.max(0, buy)
  if (!prior || !(prior.cost > 0) || !(prevBal > 0)) {
    // Cold start: the trailing VWAP is the best available cost basis. Guard it
    // against a corporate action's price jump — there the close IS the right
    // post-split anchor and the ratio stays unpublishable for one session.
    const warm =
      seedCost !== null && seedCost !== undefined && seedCost > 0 && Number.isFinite(seedCost)
    if (warm && Math.abs(close / seedCost! - 1) < MARGIN_COST_RESET_JUMP) {
      return { cost: seedCost!, reset: true, warm: true }
    }
    return { cost: close, reset: true, warm: false }
  }
  if (Math.abs(close / prior.cost - 1) >= MARGIN_COST_RESET_JUMP)
    return { cost: close, reset: true, warm: false }
  if (remaining + fresh <= 0) return { cost: prior.cost, reset: false, warm: true }
  const cost = (remaining * prior.cost + fresh * close) / bal
  return Number.isFinite(cost) && cost > 0
    ? { cost, reset: false, warm: true }
    : { cost: close, reset: true, warm: false }
}

/**
 * `close / (cost × ratio) × 100` (the ratio as a FRACTION) — the stock-level
 * estimated maintenance ratio in percent. Null without a usable estimate.
 */
export function estimatedMaintenanceRatio(
  closeNtd: number,
  cost: number | null,
  ratioPercent: number = MARGIN_FINANCING_RATIO_DEFAULT
): number | null {
  if (cost === null || !(cost > 0) || !(closeNtd > 0) || !(ratioPercent > 0)) return null
  return (closeNtd / (cost * (ratioPercent / 100))) * 100
}

/**
 * Reads the persisted state defensively: unknown shapes, non-finite costs and
 * stale entries are dropped (a stale symbol simply re-initializes at its next
 * session). `nowMs` (epoch ms) bounds the age; absent = no age prune.
 */
export function sanitizeCreditState(raw: unknown, nowMs?: number): CreditStateFile {
  const out: CreditStateFile = { version: MARGIN_COST_STATE_VERSION, asOf: '', symbols: {} }
  if (!raw || typeof raw !== 'object') return out
  const file = raw as { version?: unknown; asOf?: unknown; symbols?: unknown }
  // A file from another shape version re-initializes (v1 was close-seeded).
  if (file.version !== MARGIN_COST_STATE_VERSION) return out
  if (typeof file.asOf === 'string' && /^\d{8}$/.test(file.asOf)) out.asOf = file.asOf
  if (!file.symbols || typeof file.symbols !== 'object') return out
  const minStamp =
    nowMs === undefined ? '' : dayStampOf(nowMs - MARGIN_COST_MAX_AGE_DAYS * 86_400_000)
  for (const [symbol, entry] of Object.entries(file.symbols as Record<string, unknown>)) {
    if (!entry || typeof entry !== 'object') continue
    const e = entry as { cost?: unknown; asOf?: unknown; warm?: unknown }
    const cost = typeof e.cost === 'number' && Number.isFinite(e.cost) ? e.cost : NaN
    const asOf = typeof e.asOf === 'string' && /^\d{8}$/.test(e.asOf) ? e.asOf : ''
    if (!(cost > 0) || asOf === '') continue
    if (minStamp !== '' && asOf < minStamp) continue
    out.symbols[symbol] = { cost, asOf, warm: e.warm !== false }
  }
  return out
}
