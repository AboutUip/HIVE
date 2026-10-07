import path from 'node:path'
import { defaultConfigPath, loadConfig } from './config.js'
import { FileStore } from './file-store.js'
import { HybridClock } from './hlc.js'
import { appendRetained } from './retain.js'
import { assertStore } from './store.js'

function fail(message) {
  const error = new Error(message)
  error.expose = true
  return error
}

export function createHive(options = {}) {
  if (!options.dataDir) throw fail('要指定数据目录')
  return new Hive(options.dataDir, options)
}

export class Hive {
  constructor(dataDir, options = {}) {
    this.dataDir = dataDir
    this.now = options.now || (() => new Date())
    this.retentionFile = options.retentionFile || path.join(dataDir, 'retained.log')
    this.retentionMaxBytes = options.retentionMaxBytes || 10 * 1024 * 1024
    this.store = options.store || new FileStore(dataDir)
    assertStore(this.store)
    this.clocks = new Map()
    this.unreachable = new Set()
    this.indexDeaf = new Set()
    this.dbBroken = new Set()
    this.aliasLookups = 0
    this.configSource = options.config
      ? { kind: 'object', value: options.config }
      : { kind: 'file', path: options.configPath || defaultConfigPath }
    this.store.open()
    this.captureClock()
  }

  currentConfig() {
    if (this.configSource.kind === 'object') return loadConfig(this.configSource.value)
    return loadConfig(this.configSource.path)
  }

  requireReady() {
    if (!this.store.isOpen()) throw fail('库已关闭')
    if (!this.store.hasLayout()) throw fail('这个目录还没有局面，先 reset()')
  }

  captureClock() {
    if (!this.store.isOpen() || !this.store.hasLayout()) return
    for (const node of this.store.listNodes()) {
      if (node.role !== 'service') continue
      const stamp = this.store.latestStamp(node.id)
      if (!stamp) continue
      this.clocks.set(node.id, new HybridClock({ now: this.now, stamp }))
    }
  }

  clockFor(nodeId) {
    let clock = this.clocks.get(nodeId)
    if (!clock) {
      clock = new HybridClock({ now: this.now })
      this.clocks.set(nodeId, clock)
    }
    return clock
  }

  observeInto(nodeId, stamp) {
    this.clockFor(nodeId).observe(stamp)
  }

  finish(outcome) {
    return this.reply(outcome.message, outcome.extra || {})
  }

  reset() {
    const config = this.currentConfig()
    this.clocks.clear()
    this.unreachable.clear()
    this.indexDeaf.clear()
    this.dbBroken.clear()
    this.aliasLookups = 0
    this.store.reset()
    const outcome = this.store.transaction(() => this.seed(config))
    this.captureClock()
    return this.reply(outcome.message)
  }

  seed(config) {
    const actorNode = config.nodes.find((node) => node.id === config.actor.nodeId)
    this.setMeta('tick', '0')
    this.setMeta('sync_period', config.syncPeriod)
    this.setMeta('dr_period', config.drPeriod)
    this.setMeta('report_tolerance', config.reportTolerance)
    this.setMeta('actor_user', config.actor.userId)
    this.setMeta('actor_node', config.actor.nodeId)
    this.setMeta('node_seq', config.nodeSeq)
    this.setMeta('retention', '0')
    this.setMeta('new_node_id', config.newNodeId)
    this.setMeta('new_node_ip', config.newNodeIp)
    for (const node of config.nodes) this.insertNode(node)
    this.event(
      0,
      'info',
      `已重置。人在${actorNode.region}。上报每 ${config.syncPeriod} 拍，灾备每 ${config.drPeriod} 拍。数据时间用各节点自己的 UTC。`
    )
    return { message: '已重置到起始局面' }
  }

  close() {
    this.store.close()
  }

  reply(message, extra = {}) {
    return { message, state: this.getState(), ...extra }
  }

  setMeta(key, value) {
    this.store.setMeta(key, String(value))
  }

  meta(key) {
    return this.store.getMeta(key)
  }

  tick() {
    this.requireReady()
    return Number(this.meta('tick'))
  }

  syncPeriod() {
    return Number(this.meta('sync_period'))
  }

  drPeriod() {
    return Number(this.meta('dr_period'))
  }

  tolerance() {
    return Number(this.meta('report_tolerance') || 1)
  }

  nodePattern() {
    const newNodeId = this.meta('new_node_id')
    const newNodeIp = this.meta('new_node_ip')
    if (!newNodeId || !newNodeIp) throw fail('局面里没有新节点的编号样式')
    return { newNodeId, newNodeIp }
  }

  issuedAt(nodeId) {
    return this.clockFor(nodeId).issue()
  }

  event(tick, kind, message) {
    this.store.addEvent({ tick, kind, message })
  }

  insertNode(node, status = 'up', lastReport = 0) {
    this.store.insertNode({
      id: node.id,
      name: node.name,
      role: node.role,
      region: node.region,
      ip: node.ip,
      alias: node.alias || node.id,
      status,
      covers: node.covers || [],
      report_period: this.syncPeriod() || 2,
      last_report: lastReport,
      retries: 0,
      window_notice: null
    })
  }

  getNode(id) {
    const row = this.store.getNode(id)
    if (!row) throw fail('没有这台节点')
    return row
  }

  nodes() {
    return this.store.listNodes()
  }

  serviceNodes() {
    return this.nodes().filter((node) => node.role === 'service')
  }

  mergeJob() {
    return this.store.getMergeJob()
  }

  canReach(nodeId) {
    return !this.unreachable.has(nodeId)
  }

  fragmentCount(nodeId) {
    if (this.dbBroken.has(nodeId)) return Number.POSITIVE_INFINITY
    return this.store.countFragments(nodeId)
  }

  lowestLoad(candidates = this.serviceNodes().filter((node) => node.status === 'up')) {
    const usable = candidates.filter((node) => this.canReach(node.id) && !this.dbBroken.has(node.id))
    if (usable.length === 0) return null
    return [...usable].sort((a, b) => {
      const load = this.fragmentCount(a.id) - this.fragmentCount(b.id)
      if (load !== 0) return load
      return a.id < b.id ? -1 : 1
    })[0]
  }

  setActor(nodeId, userId) {
    this.requireReady()
    return this.finish(
      this.store.transaction(() => {
        const node = this.getNode(nodeId)
        if (node.role !== 'service') throw fail('人只能站在服务节点上')
        const user = String(userId || '').trim()
        if (!user) throw fail('先写上用户是谁')
        this.setMeta('actor_node', node.id)
        this.setMeta('actor_user', user)
        return { message: `${user} 现在从${node.region}进入，直达${node.name}` }
      })
    )
  }

  setWindows(syncPeriod, drPeriod) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performSetWindows(syncPeriod, drPeriod)))
  }

  performSetWindows(syncPeriod, drPeriod) {
    const sync = Number(syncPeriod)
    const dr = Number(drPeriod)
    if (!Number.isInteger(sync) || sync < 1) throw fail('上报窗口至少是 1 拍')
    if (!Number.isInteger(dr) || dr < 1) throw fail('灾备窗口至少是 1 拍')
    const tick = this.tick()
    this.setMeta('sync_period', sync)
    this.setMeta('dr_period', dr)
    for (const node of this.serviceNodes()) {
      this.store.updateNode(node.id, {
        report_period: sync,
        window_notice: tick,
        last_report: tick,
        retries: 0
      })
    }
    this.event(tick, 'info', `已通知中央：上报改为每 ${sync} 拍，灾备每 ${dr} 拍。这段空档不算漏报。`)
    return { message: `上报每 ${sync} 拍，灾备每 ${dr} 拍` }
  }

  setReportPeriod(nodeId, period) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performSetReportPeriod(nodeId, period)))
  }

  performSetReportPeriod(nodeId, period) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只有服务节点有上报周期')
    const next = Number(period)
    if (!Number.isInteger(next) || next < 1) throw fail('上报窗口至少是 1 拍')
    const tick = this.tick()
    this.store.updateNode(node.id, {
      report_period: next,
      window_notice: tick,
      last_report: tick,
      retries: 0
    })
    this.event(tick, 'info', `${node.name}通知中央：上报改为每 ${next} 拍。`)
    return { message: `${node.name}之后每 ${next} 拍上报` }
  }

  setRetention(enabled) {
    this.requireReady()
    return this.finish(
      this.store.transaction(() => {
        this.setMeta('retention', enabled ? '1' : '0')
        return { message: enabled ? '用完的日志会另写成 log 文件' : '用完的日志直接删除' }
      })
    )
  }

  setReachable(nodeId, reachable) {
    this.requireReady()
    this.getNode(nodeId)
    if (reachable) this.unreachable.delete(nodeId)
    else this.unreachable.add(nodeId)
    return this.reply(reachable ? `${nodeId} 可以访问` : `${nodeId} 无法访问`)
  }

  setIndexLink(nodeId, up) {
    this.requireReady()
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('索引只派发给服务节点')
    if (up) this.indexDeaf.delete(nodeId)
    else this.indexDeaf.add(nodeId)
    return this.reply(up ? `${node.name}重新接收索引` : `${node.name}这一轮收不到索引，留到下次联系再补`)
  }

  breakDatabase(nodeId) {
    this.requireReady()
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('这里只标记服务节点的库')
    this.dbBroken.add(nodeId)
    return this.reply(`${node.name}的数据库不可达，等它自己申请挂失`)
  }

  reportRuntime(nodeId, reason = '运行报错') {
    return this.applyLoss(nodeId, String(reason || '运行报错'))
  }

  write({ nodeId, userId, entries }) {
    this.requireReady()
    const node = this.getNode(nodeId)
    this.assertWritable(node)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(entries) || entries.length === 0) throw fail('这次没有要写入的内容')
    const written = []
    for (const entry of entries) {
      const key = String(entry.key || '').trim()
      const value = String(entry.value ?? '')
      if (!key) throw fail('每一份数据都要有名字')
      if (!value) throw fail(`${key} 还没有内容`)
      this.store.transaction(() => {
        const updatedAt = this.issuedAt(node.id)
        this.store.putFragment(node.id, { user_id: user, data_key: key, value, updated_at: updatedAt })
        this.store.insertOutbox(node.id, {
          tick: this.tick(),
          user_id: user,
          data_key: key,
          value,
          updated_at: updatedAt,
          op: 'put'
        })
      })
      written.push(`${key}=${value}`)
    }
    const message = `在${node.name}写下 ${user} 的 ${written.join('、')}。数据留在这台节点，日志在中央确认前留在待上报。`
    this.store.transaction(() => {
      this.setMeta('actor_node', node.id)
      this.setMeta('actor_user', user)
      this.event(this.tick(), 'write', message)
    })
    return this.reply(message)
  }

  remove({ nodeId, userId, keys }) {
    this.requireReady()
    const node = this.getNode(nodeId)
    this.assertWritable(node)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要删除的键')
    const removed = []
    for (const raw of keys) {
      const key = String(raw || '').trim()
      if (!key) throw fail('每一份数据都要有名字')
      const existing = this.store.getFragment(node.id, user, key)
      if (!existing) throw fail(`${node.name}上没有 ${user} 的 ${key}，不能在这里删除`)
      this.store.transaction(() => {
        const updatedAt = this.issuedAt(node.id)
        this.store.deleteFragment(node.id, user, key)
        this.store.deleteIndex(node.id, user, key)
        this.store.insertOutbox(node.id, {
          tick: this.tick(),
          user_id: user,
          data_key: key,
          value: '',
          updated_at: updatedAt,
          op: 'delete'
        })
      })
      removed.push(key)
    }
    const message = `在${node.name}删除 ${user} 的 ${removed.join('、')}。删除日志在中央确认前留在待上报。`
    this.store.transaction(() => {
      this.event(this.tick(), 'delete', message)
    })
    return this.reply(message)
  }

  assertWritable(node) {
    if (node.role !== 'service') throw fail('只能往服务节点写')
    if (node.status === 'lost') throw fail(`${node.name}已挂失，不能在这里写入`)
    if (node.status === 'joining') throw fail(`${node.name}还在对齐索引，还不能写入`)
    if (node.status !== 'up' && node.status !== 'merging') throw fail(`${node.name}现在不能写入`)
  }

  guardNode(node) {
    if (!this.dbBroken.has(node.id)) return
    if (node.status !== 'lost' && node.status !== 'joining') {
      this.store.transaction(() => this.performApplyLoss(node.id, '数据库不可达'))
    }
    throw fail(`${node.name}的数据库不可达，已申请挂失`)
  }

  read({ nodeId, userId, keys }) {
    this.requireReady()
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能向服务节点读取')
    if (node.status === 'lost') throw fail(`${node.name}已挂失，请求停在原地`)
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务`)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要读取的键')
    return this.finish(
      this.store.transaction(() => {
        const results = keys.map((raw) => this.readOne(node, user, String(raw || '').trim()))
        this.setMeta('actor_node', node.id)
        this.setMeta('actor_user', user)
        const message = this.describeRead(node, results)
        this.event(this.tick(), results.some((item) => item.source === 'rejected') ? 'reject' : 'read', message)
        return { message, extra: { results } }
      })
    )
  }

  readOne(node, user, key) {
    if (!key) return { key, source: 'rejected', message: '没有键名' }
    const local = this.store.getFragment(node.id, user, key)
    if (local) {
      return {
        key,
        value: local.value,
        source: 'local',
        storedLocally: true,
        fromNodeId: node.id,
        fromIp: node.ip,
        via: 'local',
        message: '就在这台节点上'
      }
    }
    const index = this.store.getIndex(node.id, user, key)
    if (!index) return { key, source: 'rejected', message: '本地没有，索引里也没有，拒绝' }
    if (index.suspended) return { key, source: 'suspended', fromIp: index.ip, message: '索引已挂起，请求未发出' }
    let home = this.nodes().find((item) => item.id === index.node_id)
    if (!home || home.status === 'lost') {
      return { key, source: 'suspended', fromIp: index.ip, message: '目标节点不可用，请求未发出' }
    }
    const direct = home.ip === index.ip && this.canReach(home.id) && !this.dbBroken.has(home.id)
    let via = 'ip'
    if (!direct) {
      this.aliasLookups += 1
      const resolved = this.nodes().find((item) => item.alias === index.alias && item.role === 'service')
      if (
        !resolved ||
        resolved.status === 'lost' ||
        resolved.status === 'joining' ||
        !this.canReach(resolved.id) ||
        this.dbBroken.has(resolved.id)
      ) {
        return { key, source: 'rejected', fromIp: index.ip, message: '地址不可达，向中央核对别名后仍不可达' }
      }
      this.store.updateIndexIpByAlias(node.id, index.alias, resolved.ip)
      home = resolved
      via = 'alias'
    }
    const remote = this.store.getFragment(home.id, user, key)
    if (!remote) {
      return {
        key,
        source: 'rejected',
        fromNodeId: home.id,
        fromIp: home.ip,
        message: `索引指向 ${home.ip}，但那里已经没有这份数据`
      }
    }
    const hop = via === 'alias' ? `原地址不通，按别名 ${home.alias} 更新为 ${home.ip} 后取回` : `从 ${home.ip} 取回`
    return {
      key,
      value: remote.value,
      source: 'remote',
      storedLocally: false,
      fromNodeId: home.id,
      fromIp: home.ip,
      via,
      message: `${hop}，未写入本机`
    }
  }

  describeRead(node, results) {
    return results
      .map((item) => {
        if (item.source === 'local') return `${node.name}本地有 ${item.key}=${item.value}`
        if (item.source === 'remote') return `${node.name}${item.message} ${item.key}=${item.value}`
        return `${node.name}读取 ${item.key}：${item.message}`
      })
      .join('；')
  }

  list({ nodeId, userId }) {
    this.requireReady()
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能向服务节点要索引')
    if (node.status === 'lost') throw fail(`${node.name}已挂失`)
    this.guardNode(node)
    const user = String(userId || '').trim()
    const temporary = this.temporaryIndex(node)
    const rows = temporary.rows.filter((row) => !user || row.user_id === user)
    const message = `${node.name}临时列出 ${rows.length} 条地址，自身数据也写进这张表。用完即弃，不写回持久索引。`
    return this.reply(message, { rows })
  }

  tickOnce() {
    this.requireReady()
    if (this.store.inTransaction()) return this.performTick()
    return this.store.transaction(() => this.performTick())
  }

  performTick() {
    const tick = this.tick() + 1
    this.setMeta('tick', tick)
    const due = this.serviceNodes().filter((node) => node.status === 'up' || node.status === 'merging')
    let synced = false
    for (const node of due) {
      const fresh = this.getNode(node.id)
      if (fresh.status !== 'up' && fresh.status !== 'merging') continue
      if (this.expectReport(fresh, tick)) synced = true
    }
    let organized = false
    if (tick % this.drPeriod() === 0) {
      this.organize(tick)
      organized = true
    }
    if (!synced && !organized && tick % this.syncPeriod() !== 0) {
      this.event(tick, 'info', `第 ${tick} 拍。上报和灾备都还没到窗口。`)
    }
    return { synced, organized }
  }

  advance(mode) {
    this.requireReady()
    if (mode === 'one') {
      this.tickOnce()
      return this.reply(`已推进到第 ${this.tick()} 拍`)
    }
    if (mode !== 'sync' && mode !== 'dr') throw fail('未知的推进方式')
    const limit = mode === 'sync' ? this.syncPeriod() + 1 : this.drPeriod() + 1
    let happened = false
    for (let i = 0; i < limit; i += 1) {
      const result = this.tickOnce()
      if (mode === 'sync' && result.synced) {
        happened = true
        break
      }
      if (mode === 'dr' && result.organized) {
        happened = true
        break
      }
    }
    if (!happened) throw fail('这一轮没有走到目标窗口')
    const label = mode === 'sync' ? '上报' : '灾备整理'
    return this.reply(`已推进到第 ${this.tick()} 拍，并完成${label}`)
  }

  expectReport(node, tick) {
    if (this.dbBroken.has(node.id)) {
      try {
        this.store.transaction(() => this.performApplyLoss(node.id, '数据库不可达'))
      } catch (error) {
        this.event(tick, 'lose', error.message)
      }
      return false
    }
    const period = Number(node.report_period)
    const last = Number(node.last_report)
    if (tick - last < period) return false
    const deadline = last + period + this.tolerance()
    if (!this.canReach(node.id)) {
      if (tick >= deadline) {
        const fails = Number(node.retries) + 1
        this.store.updateNode(node.id, { retries: fails })
        if (fails >= 2) {
          try {
            this.store.transaction(() => this.performForceLoss(node.id, '未按预期上报'))
          } catch (error) {
            this.event(tick, 'lose', error.message)
          }
          return false
        }
        this.event(tick, 'sync', `超过上报时间，向${node.name}索取日志失败，准备重试。`)
        return false
      }
      this.event(tick, 'sync', `第 ${tick} 拍没有收到${node.name}的上报。`)
      return false
    }
    this.acceptReport(node, tick)
    return true
  }

  acceptReport(node, tick) {
    const rows = this.store.listOutbox(node.id)
    const accepted = []
    let applied = 0
    let filtered = 0
    for (const row of rows) {
      const decision = this.ingest(tick, { ...row, source: node.id })
      if (decision === 'applied') applied += 1
      if (decision === 'duplicate') filtered += 1
      if (decision === 'applied' || decision === 'duplicate') accepted.push(row.id)
    }
    if (accepted.length > 0) this.store.deleteOutbox(node.id, accepted)
    this.store.updateNode(node.id, { last_report: tick, retries: 0 })
    const delivered = this.deliverPending(node.id, tick)
    if (applied > 0 || filtered > 0 || delivered > 0) {
      this.event(
        tick,
        'sync',
        `第 ${tick} 拍，中央收下${node.name}的 ${applied} 条日志，过滤 ${filtered} 条。确认之后才从节点删掉。漏收的地址补送了 ${delivered} 条。`
      )
    }
    return { applied, filtered, delivered }
  }

  ingest(tick, row) {
    const op = row.op || 'put'
    const receipt = {
      source_node: row.source,
      user_id: row.user_id,
      data_key: row.data_key,
      updated_at: row.updated_at,
      op
    }
    if (this.store.hasReceipt(receipt)) return 'duplicate'
    this.store.insertReceipt(receipt)
    this.store.insertLog({
      tick,
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value ?? '',
      updated_at: row.updated_at,
      source_node: row.source,
      op
    })
    const source = this.getNode(row.source)
    const entry = {
      user_id: row.user_id,
      data_key: row.data_key,
      node_id: source.id,
      ip: source.ip,
      alias: source.alias,
      updated_at: row.updated_at,
      suspended: 0,
      op
    }
    if (op === 'delete') {
      if (this.deleteMaster(entry)) this.fanout(entry, source.id, tick)
      return 'applied'
    }
    if (this.upsertMaster(entry)) this.fanout(entry, source.id, tick)
    return 'applied'
  }

  upsertMaster(entry) {
    const current = this.store.getMaster(entry.user_id, entry.data_key)
    if (current && current.updated_at > entry.updated_at) return false
    this.store.putMaster({
      user_id: entry.user_id,
      data_key: entry.data_key,
      node_id: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended
    })
    return true
  }

  deleteMaster(entry) {
    const current = this.store.getMaster(entry.user_id, entry.data_key)
    if (current && current.updated_at > entry.updated_at) return false
    this.store.deleteMaster(entry.user_id, entry.data_key)
    return true
  }

  fanout(entry, excludeId, tick) {
    for (const node of this.serviceNodes()) {
      if (excludeId && node.id === excludeId) continue
      if (node.status === 'lost') continue
      if (node.status === 'joining') {
        this.bufferIndex(node.id, entry)
        continue
      }
      if (node.status !== 'up' && node.status !== 'merging') continue
      if (this.indexDeaf.has(node.id) || this.dbBroken.has(node.id) || !this.canReach(node.id)) {
        this.enqueuePending(node.id, entry)
        continue
      }
      if (entry.op === 'delete') this.applyDelete(node.id, entry, tick)
      else this.applyIndex(node.id, entry, tick)
    }
  }

  bufferIndex(nodeId, entry) {
    const existing = this.store.getBuffer(nodeId, entry.user_id, entry.data_key)
    if (existing && existing.updated_at > entry.updated_at) return
    const next = {
      target_node: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended,
      op: entry.op || 'put'
    }
    if (existing) {
      this.store.updateBuffer(existing.id, next)
      return
    }
    this.store.insertBuffer({
      node_id: nodeId,
      user_id: entry.user_id,
      data_key: entry.data_key,
      ...next
    })
  }

  enqueuePending(nodeId, entry) {
    const existing = this.store.getPending(nodeId, entry.user_id, entry.data_key)
    if (existing && existing.updated_at > entry.updated_at) return
    this.store.putPending({
      node_id: nodeId,
      user_id: entry.user_id,
      data_key: entry.data_key,
      target_node: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended,
      op: entry.op || 'put'
    })
  }

  deliverPending(nodeId, tick) {
    if (this.indexDeaf.has(nodeId) || this.dbBroken.has(nodeId) || !this.canReach(nodeId)) return 0
    const rows = this.store.listPending(nodeId)
    for (const row of rows) {
      const target = this.nodes().find((item) => item.id === row.target_node)
      const entry = {
        user_id: row.user_id,
        data_key: row.data_key,
        node_id: row.target_node,
        ip: target && target.status !== 'lost' ? target.ip : row.ip,
        alias: target?.alias || row.alias,
        updated_at: row.updated_at,
        suspended: row.suspended,
        op: row.op
      }
      if (entry.op === 'delete') this.applyDelete(nodeId, entry, tick)
      else this.applyIndex(nodeId, entry, tick)
    }
    if (rows.length > 0) this.store.deletePending(nodeId)
    return rows.length
  }

  applyIndex(nodeId, entry, tick) {
    this.observeInto(nodeId, entry.updated_at)
    const current = this.store.getIndex(nodeId, entry.user_id, entry.data_key)
    if (current && current.updated_at > entry.updated_at) return
    this.store.putIndex(nodeId, {
      user_id: entry.user_id,
      data_key: entry.data_key,
      node_id: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended
    })
    if (entry.node_id === nodeId) return
    const fragment = this.store.getFragment(nodeId, entry.user_id, entry.data_key)
    if (!fragment || entry.updated_at < fragment.updated_at) return
    this.store.deleteFragment(nodeId, entry.user_id, entry.data_key)
    const node = this.getNode(nodeId)
    this.event(
      tick,
      'drop',
      `${node.name}按时间去掉过时的 ${entry.data_key}（旧值 ${fragment.value}）。这份数据的地址改为 ${entry.ip}。`
    )
  }

  applyDelete(nodeId, entry, tick) {
    this.observeInto(nodeId, entry.updated_at)
    const current = this.store.getIndex(nodeId, entry.user_id, entry.data_key)
    if (!current || current.updated_at <= entry.updated_at) {
      this.store.deleteIndex(nodeId, entry.user_id, entry.data_key)
    }
    const fragment = this.store.getFragment(nodeId, entry.user_id, entry.data_key)
    if (!fragment || entry.updated_at < fragment.updated_at) return
    this.store.deleteFragment(nodeId, entry.user_id, entry.data_key)
    const node = this.getNode(nodeId)
    this.event(tick, 'drop', `${node.name}按时间删掉 ${entry.data_key}（旧值 ${fragment.value}）。`)
  }

  organize(tick) {
    const pending = this.store.countLogs()
    if (pending === 0) {
      this.event(tick, 'dr', `第 ${tick} 拍到了灾备窗口，没有新的完整日志需要整理。`)
      return
    }
    const stored = this.foldLogs(tick)
    const kept = this.meta('retention') === '1' ? '另写了 log 文件。' : '用过的日志已从中央删除。'
    this.event(tick, 'dr', `第 ${tick} 拍，${pending} 条完整日志收成稳定数据，交给灾备 ${stored} 份。${kept}`)
  }

  foldLogs(tick) {
    const logs = this.store.listLogs()
    if (logs.length === 0) return 0
    const collapsed = new Map()
    for (const log of logs) {
      const id = `${log.user_id}\0${log.data_key}`
      const prev = collapsed.get(id)
      if (!prev || log.updated_at >= prev.updated_at) collapsed.set(id, log)
    }
    let stored = 0
    for (const log of collapsed.values()) {
      const home = this.store.getMaster(log.user_id, log.data_key)
      const homeId = home ? home.node_id : log.source_node
      for (const dr of this.nodes().filter((node) => node.role === 'dr')) {
        const covered = homeId && dr.covers.includes(homeId)
        if (!covered || log.op === 'delete') {
          if (!covered) {
            this.store.deleteRecord(dr.id, log.user_id, log.data_key)
            continue
          }
          const existing = this.store.getRecord(dr.id, log.user_id, log.data_key)
          if (!existing || existing.updated_at <= log.updated_at) {
            this.store.deleteRecord(dr.id, log.user_id, log.data_key)
          }
          continue
        }
        const existing = this.store.getRecord(dr.id, log.user_id, log.data_key)
        if (existing && existing.updated_at > log.updated_at) continue
        this.store.putRecord(dr.id, {
          user_id: log.user_id,
          data_key: log.data_key,
          value: log.value,
          updated_at: log.updated_at,
          home_node: homeId,
          organized_tick: tick
        })
        stored += 1
      }
    }
    if (this.meta('retention') === '1') {
      const lines = logs
        .map((log) =>
          JSON.stringify({
            userId: log.user_id,
            key: log.data_key,
            value: log.value,
            op: log.op,
            updatedAt: log.updated_at,
            source: log.source_node
          })
        )
      appendRetained(this.retentionFile, lines, this.retentionMaxBytes)
    }
    this.store.deleteLogs(logs.map((log) => log.id))
    return stored
  }

  temporaryIndex(source) {
    const table = new Map()
    for (const row of this.store.listIndexes(source.id)) {
      table.set(`${row.user_id}\0${row.data_key}`, { ...row, alias: row.alias || row.node_id })
    }
    const own = this.store.listFragments(source.id)
    for (const fragment of own) {
      table.set(`${fragment.user_id}\0${fragment.data_key}`, {
        user_id: fragment.user_id,
        data_key: fragment.data_key,
        node_id: source.id,
        ip: source.ip,
        alias: source.alias,
        updated_at: fragment.updated_at,
        suspended: 0
      })
    }
    return { rows: [...table.values()], ownCount: own.length }
  }

  join({ region, name }) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performJoin({ region, name })))
  }

  performJoin({ region, name }) {
    const regionName = String(region || '').trim()
    if (!regionName) throw fail('新节点要落在一个区域')
    const source = this.lowestLoad()
    if (!source) throw fail('现在没有正在服务的节点可以提供索引')
    const seq = Number(this.meta('node_seq'))
    this.setMeta('node_seq', seq + 1)
    const pattern = this.nodePattern()
    const id = pattern.newNodeId.replaceAll('{seq}', String(seq))
    const node = {
      id,
      name: String(name || '').trim() || `${regionName}节点`,
      role: 'service',
      region: regionName,
      ip: pattern.newNodeIp.replaceAll('{seq}', String(seq)),
      alias: id,
      covers: []
    }
    this.insertNode(node, 'joining', this.tick())
    const temporary = this.temporaryIndex(source)
    for (const row of temporary.rows) {
      this.observeInto(node.id, row.updated_at)
      this.store.putIndex(node.id, {
        user_id: row.user_id,
        data_key: row.data_key,
        node_id: row.node_id,
        ip: row.ip,
        alias: row.alias || row.node_id,
        updated_at: row.updated_at,
        suspended: row.suspended
      })
    }
    const message = `${node.name}已连上中央。索引从当前负载最低的${source.name}拉来，并临时补上它自己的 ${temporary.ownCount} 份数据，共 ${temporary.rows.length} 条。这张临时表不写回${source.name}。中央为它建了缓冲区，就绪前不投入服务。它还没有被灾备声明负责。`
    this.event(this.tick(), 'join', message)
    return { message, extra: { nodeId: node.id } }
  }

  ready(nodeId) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performReady(nodeId)))
  }

  performReady(nodeId) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'joining') throw fail(`${node.name}不在对齐过程中`)
    const rows = this.store.listBuffers(nodeId)
    for (const row of rows) {
      const target = this.nodes().find((item) => item.id === row.target_node)
      const entry = {
        user_id: row.user_id,
        data_key: row.data_key,
        node_id: row.target_node,
        ip: target && target.status !== 'lost' ? target.ip : row.ip,
        alias: target?.alias || row.alias,
        updated_at: row.updated_at,
        suspended: row.suspended,
        op: row.op
      }
      if (entry.op === 'delete') this.applyDelete(nodeId, entry, this.tick())
      else this.applyIndex(nodeId, entry, this.tick())
    }
    this.store.deleteBuffers(nodeId)
    this.store.updateNode(nodeId, { status: 'up', last_report: this.tick() })
    const message = `${node.name}已就绪，缓冲区里的 ${rows.length} 条索引已补上，开始服务。`
    this.event(this.tick(), 'ready', message)
    return { message }
  }

  setCovers(drId, nodeIds) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performSetCovers(drId, nodeIds)))
  }

  performSetCovers(drId, nodeIds) {
    const dr = this.getNode(drId)
    if (dr.role !== 'dr') throw fail('只有灾备节点需要声明负责范围')
    const services = new Set(this.serviceNodes().map((node) => node.id))
    const covers = [...new Set(nodeIds.map((id) => String(id)))].filter((id) => services.has(id))
    this.store.updateNode(dr.id, { covers })
    const names = covers.map((id) => this.getNode(id).name)
    const message = names.length ? `${dr.name}现在负责${names.join('、')}` : `${dr.name}现在不负责任何服务节点`
    this.event(this.tick(), 'info', message)
    return { message }
  }

  lose(nodeId, { cause = '中央强制挂失' } = {}) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performLose(nodeId, { cause })))
  }

  performLose(nodeId, { cause = '中央强制挂失' } = {}) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能挂失服务节点')
    if (node.status === 'lost') throw fail(`${node.name}已经挂失`)
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务，不用走挂失`)
    const job = this.mergeJob()
    if (job && (job.target_id === node.id || node.status === 'merging')) return this.performRollback(node.id, cause)
    if (job) throw fail('已经有节点在融合，先完成这次再挂失下一台')
    const target = this.lowestLoad(this.serviceNodes().filter((item) => item.status === 'up' && item.id !== node.id))
    if (!target) throw fail('没有还能接替的服务节点')
    this.store.updateNode(node.id, { status: 'lost' })
    this.store.updateNode(target.id, { status: 'merging' })
    this.store.insertMergeJob({ lost_id: node.id, target_id: target.id, started_tick: this.tick() })
    this.suspendLost(node.id)
    let pending = 0
    if (!this.dbBroken.has(node.id)) pending = this.store.countOutbox(node.id)
    const load = this.fragmentCount(target.id)
    let message = `${cause}：${node.name}（${node.ip}）挂失。指向它的索引挂起，相关请求停住、不发出。真实数据将装入负载较低的${target.name}（当前碎片 ${load}），不按地理远近。装入完成前不接受针对这台丢失节点的请求。`
    if (pending > 0) message += `挂失时还有 ${pending} 条日志没上报，恢复只用已经到过中央或灾备的数据。`
    this.event(this.tick(), 'lose', message)
    return { message }
  }

  applyLoss(nodeId, cause = '节点申请挂失') {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performApplyLoss(nodeId, cause)))
  }

  performApplyLoss(nodeId, cause = '节点申请挂失') {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能挂失服务节点')
    if (node.status === 'lost') return { message: `${node.name}已经挂失` }
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务，不用走挂失`)
    return this.performLose(nodeId, { cause })
  }

  performForceLoss(nodeId, cause) {
    const node = this.getNode(nodeId)
    if (node.status === 'lost') return { message: `${node.name}已经挂失` }
    const job = this.mergeJob()
    if (node.status === 'merging' || (job && job.target_id === nodeId)) return this.performRollback(nodeId, cause)
    return this.performLose(nodeId, { cause })
  }

  suspendLost(lostId) {
    this.store.suspendMasters(lostId)
    this.store.suspendBuffers(lostId)
    this.store.suspendPending(lostId)
    for (const node of this.serviceNodes()) {
      if (node.status === 'lost' || node.id === lostId) continue
      if (this.dbBroken.has(node.id)) continue
      this.store.suspendIndexes(node.id, lostId)
    }
  }

  recover(nodeId) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performRecover(nodeId)))
  }

  performRecover(nodeId) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'lost') throw fail('只能对已经挂失的服务节点继续恢复')
    if (this.mergeJob()) throw fail('已经有节点在融合')
    const target = this.lowestLoad(this.serviceNodes().filter((item) => item.status === 'up' && item.id !== node.id))
    if (!target) throw fail('没有还能接替的服务节点')
    this.store.updateNode(target.id, { status: 'merging' })
    this.store.insertMergeJob({ lost_id: node.id, target_id: target.id, started_tick: this.tick() })
    const message = `继续把${node.name}的数据装入${target.name}。这是所有权转移。`
    this.event(this.tick(), 'lose', message)
    return { message }
  }

  performRollback(targetId, cause) {
    const job = this.mergeJob()
    const target = this.getNode(targetId)
    if (target.status !== 'lost') this.store.updateNode(targetId, { status: 'lost' })
    if (job && job.target_id === targetId) this.store.deleteMergeJob()
    this.suspendLost(targetId)
    const message = `立刻回退尚未完成的装入，并将${target.name}标记挂失。数据仍留在灾备。原因：${cause}。`
    this.event(this.tick(), 'lose', message)
    return { message }
  }

  finishMerge() {
    this.requireReady()
    const prepared = this.store.transaction(() => this.prepareFinish())
    if (prepared.kind === 'rollback') {
      return this.finish(this.store.transaction(() => this.performRollback(prepared.targetId, prepared.cause)))
    }
    try {
      return this.finish(this.store.transaction(() => this.applyFinish(prepared.lostId, prepared.targetId)))
    } catch (error) {
      return this.finish(
        this.store.transaction(() => this.performRollback(prepared.targetId, error.message || '接替节点无法访问'))
      )
    }
  }

  prepareFinish() {
    const job = this.mergeJob()
    if (!job) throw fail('现在没有正在融合的节点')
    const pendingLogs = this.store.countLogs()
    if (pendingLogs > 0) {
      this.foldLogs(this.tick())
      this.event(this.tick(), 'dr', `灾备数据取到之后，中央还有 ${pendingLogs} 条日志，已再次更新灾备。`)
    }
    const target = this.getNode(job.target_id)
    if (!this.canReach(target.id) || this.dbBroken.has(target.id)) {
      return { kind: 'rollback', lostId: job.lost_id, targetId: target.id, cause: '接替节点无法访问' }
    }
    return { kind: 'apply', lostId: job.lost_id, targetId: job.target_id }
  }

  applyFinish(lostId, targetId) {
    const lost = this.getNode(lostId)
    const target = this.getNode(targetId)
    const records = this.drRecordsFor(lost.id)
    if (!this.canReach(target.id) || this.dbBroken.has(target.id)) throw fail('接替节点无法访问')
    const unique = []
    const seen = new Set()
    for (const record of records) {
      const id = `${record.user_id}\0${record.data_key}`
      if (seen.has(id)) continue
      seen.add(id)
      unique.push(record)
    }
    let keptLocal = 0
    let loaded = 0
    for (const record of unique) {
      if (!this.canReach(target.id) || this.dbBroken.has(target.id)) throw fail('接替节点无法访问')
      this.observeInto(target.id, record.updated_at)
      const local = this.store.getFragment(target.id, record.user_id, record.data_key)
      let winning = record
      if (local && local.updated_at > record.updated_at) {
        winning = {
          user_id: local.user_id,
          data_key: local.data_key,
          value: local.value,
          updated_at: local.updated_at
        }
        keptLocal += 1
      } else {
        this.store.putFragment(target.id, {
          user_id: record.user_id,
          data_key: record.data_key,
          value: record.value,
          updated_at: record.updated_at
        })
        loaded += 1
      }
      const freshTarget = this.getNode(target.id)
      const entry = {
        user_id: winning.user_id,
        data_key: winning.data_key,
        node_id: freshTarget.id,
        ip: freshTarget.ip,
        alias: freshTarget.alias,
        updated_at: winning.updated_at,
        suspended: 0,
        op: 'put'
      }
      this.upsertMaster(entry)
      this.fanout(entry, null, this.tick())
      this.retargetDrRecord(lost.id, freshTarget.id, winning)
    }
    this.store.updateNode(target.id, { status: 'up' })
    this.store.deleteMergeJob()
    const still = this.store.countSuspendedMasters(lost.id)
    let message = `灾备数据已按时间并入${target.name}：装入 ${loaded} 份，本机较新的 ${keptLocal} 份保留。原 ${lost.ip} 丢失，新地址为 ${target.ip}。各节点只更新了这些索引。`
    const uncovered = this.nodes().filter((node) => node.role === 'dr' && !node.covers.includes(target.id))
    if (records.length > 0 && uncovered.length > 0) {
      message += `${uncovered.map((node) => node.name).join('、')}没有声明负责${target.name}，丢失节点留在这些灾备上的对应记录已移出。`
    }
    if (still > 0) message += `还有 ${still} 条索引在灾备里没有对应数据，继续挂起。`
    this.event(this.tick(), 'merge', message)
    return { message }
  }

  retargetDrRecord(lostId, targetId, winning) {
    for (const dr of this.nodes().filter((node) => node.role === 'dr')) {
      if (dr.covers.includes(targetId)) {
        this.store.updateRecordHome(dr.id, {
          home_node: targetId,
          value: winning.value,
          updated_at: winning.updated_at,
          organized_tick: this.tick(),
          user_id: winning.user_id,
          data_key: winning.data_key,
          from_home: lostId
        })
        continue
      }
      this.store.deleteRecordHome(dr.id, winning.user_id, winning.data_key, lostId)
    }
  }

  drRecordsFor(homeId) {
    const records = []
    for (const dr of this.nodes().filter((node) => node.role === 'dr' && node.covers.includes(homeId))) {
      records.push(...this.store.listRecordsByHome(dr.id, homeId))
    }
    records.sort((a, b) => {
      if (a.updated_at === b.updated_at) return b.organized_tick - a.organized_tick
      return a.updated_at < b.updated_at ? 1 : -1
    })
    return records
  }

  rejoin(nodeId, { region, name } = {}) {
    this.requireReady()
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'lost') throw fail('只有挂失节点回来时才清空并作为新节点')
    const job = this.mergeJob()
    if (job && (job.lost_id === nodeId || job.target_id === nodeId)) throw fail('融合尚未结束，先处理完这次装入')
    return this.finish(
      this.store.transaction(() => {
        this.dbBroken.delete(nodeId)
        this.unreachable.delete(nodeId)
        this.indexDeaf.delete(nodeId)
        this.store.clearHolder(nodeId)
        return this.performJoin({ region: region || node.region, name: name || `${node.region}新节点` })
      })
    )
  }

  changeIp(nodeId, ip) {
    this.requireReady()
    return this.finish(this.store.transaction(() => this.performChangeIp(nodeId, ip)))
  }

  performChangeIp(nodeId, ip) {
    const node = this.getNode(nodeId)
    const next = String(ip || '').trim()
    if (!next) throw fail('要写上新的地址')
    this.store.updateNode(node.id, { ip: next })
    this.store.updateMasterIp(node.id, next)
    this.store.updateBufferIp(node.id, next)
    this.store.updatePendingIp(node.id, next)
    return { message: `${node.name}的地址改为 ${next}。别名仍是 ${node.alias}。各节点继续用旧地址，不通时再向中央要一次。` }
  }

  getState() {
    this.requireReady()
    const tick = this.tick()
    const syncPeriod = this.syncPeriod()
    const drPeriod = this.drPeriod()
    const nodes = this.nodes().map((node) => this.presentNode(node))
    const job = this.mergeJob()
    return {
      tick,
      syncPeriod,
      drPeriod,
      tolerance: this.tolerance(),
      retention: this.meta('retention') === '1',
      nextSyncIn: (syncPeriod - (tick % syncPeriod)) % syncPeriod || syncPeriod,
      nextDrIn: (drPeriod - (tick % drPeriod)) % drPeriod || drPeriod,
      actor: { userId: this.meta('actor_user'), nodeId: this.meta('actor_node') },
      nodes,
      indexMaster: this.store.listMasters(),
      logs: this.store.listRecentLogs(12),
      logCount: this.store.countLogs(),
      unorganizedLogCount: this.store.countLogs(),
      mergeJob: job ? { lostId: job.lost_id, targetId: job.target_id, startedTick: job.started_tick } : null,
      events: this.store.listEvents(60)
    }
  }

  presentNode(node) {
    const base = {
      id: node.id,
      name: node.name,
      role: node.role,
      region: node.region,
      ip: node.ip,
      alias: node.alias,
      status: node.status,
      covers: node.covers,
      reportPeriod: node.report_period,
      lastReport: node.last_report,
      reachable: this.canReach(node.id)
    }
    if (node.role === 'service' && this.dbBroken.has(node.id)) {
      return {
        ...base,
        dbBroken: true,
        fragmentCount: null,
        outbox: [],
        fragments: [],
        indexEntries: [],
        buffer: []
      }
    }
    if (node.role === 'service') {
      return {
        ...base,
        dbBroken: false,
        fragmentCount: this.store.countFragments(node.id),
        outbox: this.store.listOutbox(node.id).map((row) => ({
          id: row.id,
          user_id: row.user_id,
          data_key: row.data_key,
          value: row.value,
          updated_at: row.updated_at,
          op: row.op
        })),
        fragments: this.store.listFragments(node.id),
        indexEntries: this.store.listIndexes(node.id),
        buffer: this.store.listBuffers(node.id)
      }
    }
    if (node.role === 'dr') {
      return { ...base, records: this.store.listRecords(node.id) }
    }
    return base
  }
}
