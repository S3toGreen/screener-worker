/**
 * The screenable-universe rule — the user's contract: **上市 + 上櫃** (TWSE and
 * TPEx mainboard) STOCKS plus the listed FUNDS (ETFs / ETNs / REITs).
 * Everything else the exchanges publish is excluded.
 *
 *   stock  `^[1-9][0-9]{3}$`          — 1101, 2330, 6488 (4 digits, no leading 0)
 *   fund   `^0[0-2][0-9]{2,4}[A-Z]?$` — 0050, 006203, 00679B, 00631L (ETF),
 *                                      02001L (ETN), 01001T (REIT), 009800
 *
 * What the two patterns exclude BY SHAPE (and why that is exactly the
 * categories the user wants stripped):
 *   - warrants   TWSE `03`–`08` + 6 digits, TPEx `7xxxxx` — the `0[0-2]` fund
 *                prefix is deliberately narrower; 12 000+ TPEx warrant rows
 *                ride the SAME quote payload as the stocks.
 *   - 特別股      `2881A`, `2887Z1` — a stock code with a suffix character;
 *                the stock pattern is anchored to 4 digits, so every class of
 *                preferred share (A/B/C… and the two-char Z1 form) is out.
 *   - TDR/DR     `91xxxx`, `910322` — 6 digits that do not start `0[0-2]`.
 *   - indices    `TAIEX`, `TX00` — no digits.
 *   - 興櫃       never reaches these tables at all: the daily sources are
 *                `STOCK_DAY_ALL`/`twse_mainboard_quotes` (上市) and
 *                `tpex_mainboard_quotes`/`afterTrading/otc` (上櫃); the
 *                emerging market has its own `tpex_esb_*` endpoints.
 *
 * A fund has NO company record, so the ETL's listing-universe gate stands
 * down for it (`isFundSymbol`) and its marketCap/turnoverRate stay null —
 * which is why the fundamental rules treat an absent value as FAILING: a
 * user filtering on 三率/PER/營收 culls ETFs/ETNs without any special case.
 */

import { isFundSymbol } from '@odf/screener-worker/screener'

const STOCK_CODE = /^[1-9][0-9]{3}$/

export function isScreenerSymbol(symbol: string): boolean {
  return STOCK_CODE.test(symbol) || isFundSymbol(symbol)
}
