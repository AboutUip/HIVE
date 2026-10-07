import assert from 'node:assert/strict'
import test from 'node:test'
import { HybridClock, parseStamp, stampOf } from '../dist/index.js'

test('发号单调:同一毫秒里逻辑计数递增', () => {
  const clock = new HybridClock({ now: () => 1_000 })
  const first = clock.issue()
  const second = clock.issue()
  const third = clock.issue()
  assert.equal(first, '1970-01-01T00:00:01.000Z-000000')
  assert.equal(second, '1970-01-01T00:00:01.000Z-000001')
  assert.equal(third, '1970-01-01T00:00:01.000Z-000002')
  assert.ok(first < second && second < third, '字符串比较即时间顺序')
})

test('物理时间前进时逻辑计数归零', () => {
  let pt = 1_000
  const clock = new HybridClock({ now: () => pt })
  clock.issue()
  clock.issue()
  pt = 1_500
  assert.equal(clock.issue(), '1970-01-01T00:00:01.500Z-000000')
})

test('时钟回拨也不倒退:以物理上限为准', () => {
  let pt = 2_000
  const clock = new HybridClock({ now: () => pt })
  clock.issue()
  pt = 1_000
  const next = clock.issue()
  assert.ok(next > '1970-01-01T00:00:02.000Z-000000')
})

test('收到更晚的时间戳后,下一次发号比收到的更晚', () => {
  const clock = new HybridClock({ now: () => 1_000 })
  const before = clock.issue()
  clock.observe('2026-01-01T00:00:00.000Z-000007')
  const after = clock.issue()
  assert.ok(after > '2026-01-01T00:00:00.000Z-000007')
  assert.ok(after > before)
})

test('收到相同物理时间时取逻辑计数更大者加一', () => {
  const pt = Date.parse('2026-04-01T00:00:00.000Z')
  const clock = new HybridClock({ now: () => pt })
  clock.issue()
  clock.issue()
  clock.observe('2026-04-01T00:00:00.000Z-000005')
  assert.equal(clock.issue(), '2026-04-01T00:00:00.000Z-000007')
})

test('老格式的 UTC 时间戳解析成逻辑位 0', () => {
  assert.deepEqual(parseStamp('2026-04-01T00:00:00.000Z'), {
    l: Date.parse('2026-04-01T00:00:00.000Z'),
    c: 0
  })
})

test('老格式排在新格式同毫秒之前', () => {
  const pt = Date.parse('2026-04-01T00:00:00.000Z')
  const clock = new HybridClock({ now: () => pt })
  clock.observe('2026-04-01T00:00:00.000Z')
  assert.equal(clock.issue(), '2026-04-01T00:00:00.000Z-000002')
})

test('从库里的最新时间戳恢复,重启后发号仍单调', () => {
  const dir = '2026-04-01T00:00:00.000Z-000009'
  const restarted = new HybridClock({ now: () => 1_773_407_999_000, stamp: dir })
  assert.ok(restarted.issue() > dir)
})

test('stampOf 与 parseStamp 互逆', () => {
  const stamp = stampOf(1_773_408_000_123, 42)
  assert.deepEqual(parseStamp(stamp), { l: 1_773_408_000_123, c: 42 })
})

test('解析不了的时间戳抛出', () => {
  assert.throws(() => parseStamp('不是时间'), /无法解析/)
  assert.throws(() => parseStamp(''), /不能是空的/)
})
