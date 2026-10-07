const LOGICAL_WIDTH = 6
const MAX_LOGICAL = 10 ** LOGICAL_WIDTH - 1

export function stampOf(l, c) {
  return `${new Date(l).toISOString()}-${String(c).padStart(LOGICAL_WIDTH, '0')}`
}

export function parseStamp(stamp) {
  const text = String(stamp ?? '').trim()
  if (!text) throw new Error('时间戳不能是空的')
  const match = /^(.*)-(\d+)$/.exec(text)
  const physical = match ? match[1] : text
  const ms = Date.parse(physical)
  if (!Number.isFinite(ms)) throw new Error(`无法解析时间戳：${text}`)
  const logical = match ? Number(match[2]) : 0
  if (!Number.isInteger(logical) || logical < 0 || logical > MAX_LOGICAL) {
    throw new Error(`无法解析时间戳：${text}`)
  }
  return { l: ms, c: logical }
}

export class HybridClock {
  constructor({ now = () => Date.now(), stamp = null } = {}) {
    this.now = now
    if (stamp) {
      const parsed = parseStamp(stamp)
      this.l = parsed.l
      this.c = parsed.c
    } else {
      this.l = 0
      this.c = 0
    }
  }

  physical() {
    const raw = this.now()
    if (raw instanceof Date) return raw.getTime()
    const ms = Number(raw)
    if (Number.isFinite(ms)) return ms
    const parsed = Date.parse(String(raw))
    return Number.isFinite(parsed) ? parsed : Date.now()
  }

  issue() {
    const pt = this.physical()
    let l = Math.max(this.l, pt)
    let c = this.c + 1
    if (l > this.l) c = 0
    if (c > MAX_LOGICAL) {
      l += 1
      c = 0
    }
    this.l = l
    this.c = c
    return stampOf(l, c)
  }

  observe(stamp) {
    const remote = parseStamp(stamp)
    const pt = this.physical()
    const l = Math.max(this.l, remote.l, pt)
    let c = 0
    if (l === this.l && l === remote.l) c = Math.max(this.c, remote.c) + 1
    else if (l === this.l) c = this.c + 1
    else if (l === remote.l) c = remote.c + 1
    if (c > MAX_LOGICAL) {
      this.l = l + 1
      this.c = 0
      return
    }
    this.l = l
    this.c = c
  }
}
