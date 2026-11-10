import { describe, expect, test } from 'bun:test'
import { parseTaifexInstitutionHistory } from './taifexHistory'

/** Trimmed shape of /cht/3/futAndOptDate (identity, 12 numeric cells, twice). */
const html = `
<p>日期2026/09/01</p>
<table><tbody>
<tr><th>身份別</th><th>期貨</th><th>選擇權</th></tr>
<tr><td>自營商</td><td>100,909</td><td>103,037</td><td>1</td><td>2</td><td>86,729</td><td>96,351</td><td>3</td><td>4</td><td>14,180</td><td>6,686</td><td>5</td><td>6</td></tr>
<tr><td>投信</td><td>702</td><td>0</td><td>1</td><td>2</td><td>1,010</td><td>41</td><td>3</td><td>4</td><td>-308</td><td>-41</td><td>5</td><td>6</td></tr>
<tr><td>外資</td><td>586,328</td><td>119,322</td><td>1</td><td>2</td><td>570,209</td><td>119,871</td><td>3</td><td>4</td><td>16,119</td><td>-549</td><td>5</td><td>6</td></tr>
<tr><td>合計</td><td>687,939</td><td>222,359</td><td>1</td><td>2</td><td>657,948</td><td>216,263</td><td>3</td><td>4</td><td>29,991</td><td>6,096</td><td>5</td><td>6</td></tr>
<tr><td>自營商</td><td>112,990</td><td>59,149</td><td>1</td><td>2</td><td>361,140</td><td>47,347</td><td>3</td><td>4</td><td>-248,150</td><td>11,802</td><td>5</td><td>6</td></tr>
<tr><td>投信</td><td>94,763</td><td>4</td><td>1</td><td>2</td><td>3,559</td><td>4,523</td><td>3</td><td>4</td><td>91,204</td><td>-4,519</td><td>5</td><td>6</td></tr>
<tr><td>外資</td><td>239,411</td><td>13,795</td><td>1</td><td>2</td><td>710,705</td><td>18,914</td><td>3</td><td>4</td><td>-471,294</td><td>-5,119</td><td>5</td><td>6</td></tr>
<tr><td>合計</td><td>447,164</td><td>72,948</td><td>1</td><td>2</td><td>1,075,404</td><td>70,784</td><td>3</td><td>4</td><td>-628,240</td><td>2,164</td><td>5</td><td>6</td></tr>
</tbody></table>`

describe('TAIFEX split history', () => {
  test('parses the futures/options split per identity', () => {
    const point = parseTaifexInstitutionHistory(html)
    expect(point?.sessionDate).toBe(Date.UTC(2026, 8, 1) - 8 * 3_600_000)
    expect(point?.derivatives?.institutions).toEqual([
      {
        item: '自營商',
        futuresLong: 100_909,
        optionsLong: 103_037,
        futuresShort: 86_729,
        optionsShort: 96_351,
        futuresNet: 14_180,
        optionsNet: 6_686,
        futuresOiLong: 112_990,
        optionsOiLong: 59_149,
        futuresOiShort: 361_140,
        optionsOiShort: 47_347,
        futuresOiNet: -248_150,
        optionsOiNet: 11_802
      },
      {
        item: '投信',
        futuresLong: 702,
        optionsLong: 0,
        futuresShort: 1_010,
        optionsShort: 41,
        futuresNet: -308,
        optionsNet: -41,
        futuresOiLong: 94_763,
        optionsOiLong: 4,
        futuresOiShort: 3_559,
        optionsOiShort: 4_523,
        futuresOiNet: 91_204,
        optionsOiNet: -4_519
      },
      {
        item: '外資及陸資',
        futuresLong: 586_328,
        optionsLong: 119_322,
        futuresShort: 570_209,
        optionsShort: 119_871,
        futuresNet: 16_119,
        optionsNet: -549,
        futuresOiLong: 239_411,
        optionsOiLong: 13_795,
        futuresOiShort: 710_705,
        optionsOiShort: 18_914,
        futuresOiNet: -471_294,
        optionsOiNet: -5_119
      }
    ])
  })

  test('an exchange no-data page is not a zero observation', () => {
    expect(parseTaifexInstitutionHistory('<script>alert("查無資料")</script>')).toBeNull()
  })
})
