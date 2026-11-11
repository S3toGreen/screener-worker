import { describe, expect, test } from 'bun:test'
import { stampToMs } from '@odf/screener-worker/session'
import type { FetchLike, HttpResult } from '@odf/screener-worker/httpJson'
import { twseDayQuotes, twseDaySblBal } from './history'
import { twseOfficialTrend } from './sources/twse'

/** A fetch double serving one fixed JSON payload. */
function stub(payload: unknown): FetchLike {
  const text = JSON.stringify(payload)
  return async (): Promise<HttpResult> => ({
    ok: true,
    status: 200,
    json: async () => payload,
    text: async () => text,
    arrayBuffer: async () => new ArrayBuffer(0)
  })
}

const T93_FIELDS = [
  '代號',
  '名稱',
  '前日餘額',
  '賣出',
  '買進',
  '現券',
  '今日餘額',
  '次一營業日限額',
  '前日餘額',
  '當日賣出',
  '當日還券',
  '當日調整',
  '當日餘額',
  '次一營業日可限額',
  '備註'
]
const T93_ROW = [
  '2330',
  '台積電',
  '111,000',
  '6,000',
  '101,000',
  '0',
  '16,000',
  '6,483,092,516',
  '16,617,514',
  '892,000',
  '0',
  '0',
  '17,509,514',
  '8,770,320',
  ' '
]
const t93 = (date = '20261104'): unknown => ({
  stat: 'OK',
  date,
  title: '115年11月04日 信用額度總量管制餘額表',
  fields: T93_FIELDS,
  data: [T93_ROW]
})

describe('twseDaySblBal — the dated SBL (借券賣出) balance', () => {
  test("maps block B's 當日餘額 (shares), NOT 可借券賣出股數", async () => {
    const map = await twseDaySblBal(stampToMs('20261104'), stub(t93()))
    expect(map?.get('2330')?.sblBal).toBe(17_509_514)
  })

  test('a payload from another session is null (never stamped)', async () => {
    expect(await twseDaySblBal(stampToMs('20261105'), stub(t93('20261104')))).toBeNull()
  })

  test('a not-published answer (stat ≠ OK) is a clean null', async () => {
    const map = await twseDaySblBal(stampToMs('20261107'), stub({ stat: '很抱歉，沒有資料' }))
    expect(map).toBeNull()
  })
})

describe('twseDayQuotes — an empty table is no session', () => {
  test('the title alone does not make a session', async () => {
    const payload = {
      stat: 'OK',
      tables: [
        {
          title: '115年11月04日 每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))',
          fields: ['證券代號', '證券名稱', '收盤價'],
          data: []
        }
      ]
    }
    expect(await twseDayQuotes(stampToMs('20261104'), stub(payload))).toBeNull()
  })
})

describe('twseOfficialTrend — an empty crdAmt is absent, not zero', () => {
  test('a blank crdAmt stays null', async () => {
    const dateMs = stampToMs('20261110')
    const map = await twseOfficialTrend(
      1,
      dateMs,
      stub({
        stat: 'OK',
        date: '20261110',
        data: [{ date: '20261110', keepRate: 180, crdAmt: '' }]
      })
    )
    expect(map.get(dateMs)?.keepRate).toBe(180)
    expect(map.get(dateMs)?.creditAmountNtd).toBeNull()
  })
})
