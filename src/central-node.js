import fs from 'node:fs'
import path from 'node:path'
import { FileStore } from './file-store.js'
import { call, fail, parseAddress, serve } from './rpc.js'
import { collapseLogs, putMaster, rejectsOlder, removeMaster } from './rules.js'

function addressOf(node) {
  return node?.ip || ''
}

export async function startCentral(options = {}) {
  const central = new Central(options)
  await central.start()
  return central
}

class Central {
  constructor(options) {
    if (!options.dataDir) throw fail('要指定数据目录')
    if (!options.token) throw fail('集群令牌不能是空的')
    if (!Array.isArray(options.nodes) || options.nodes.length === 0) throw fail('配置里至少要有一台节点')
    this.token = options.token
    this.host = options.host || '127.0.0.1'
    this.port = options.port ?? 0
    this.clock = options.clock || (() => Date.now())
    this.reportIntervalMs = positive(options.reportIntervalMs ?? 2000, '上报间隔')
    this.drIntervalMs = positive(options.drIntervalMs ?? 6000, '灾备间隔')
    this.toleranceMs = positive(options.toleranceMs ?? 1000, '上报容错')
    this.checkIntervalMs = positive(options.checkIntervalMs ?? 1000, '检查间隔')
    this.newNodeId = options.newNodeId || 'svc-{seq}'
    this.nodeSeq = options.nodeSeq || 1
    this.retentionFile = options.retentionFile || path.join(options.dataDir, 'retained.log')
    this.retention = Boolean(options.retention)
    this.configured = options.nodes.map((node, index) => normalize(node, index))
    this.store = options.store || new FileStore(options.dataDir)
    this.http = null
    this.timer = null
    this.working = false
  }

  async start() {
    this.store.open()
    if (!this.store.hasLayout()) this.store.transaction(() => this.seed())
    this.http = await serve({
      host: this.host,
      port: this.port,
      token: this.token,
      handler: (request) => this.route(request)
    })
    this.timer = setInterval(() => {
      this.tick().catch(() => {})
    }, this.checkIntervalMs)
    if (this.store.getMergeJob()) await this.finishMerge().catch(() => {})
    return this
  }

  seed() {
    const services = this.configured.filter((node) => node.role === 'service')
    if (services.length === 0) throw fail('配置里至少要有一台服务节点')
    this.store.setMeta('tick', '0')
    this.store.setMeta('report_interval', String(this.reportIntervalMs))
    this.store.setMeta('dr_interval', String(this.drIntervalMs))
    this.store.setMeta('tolerance', String(this.toleranceMs))
    this.store.setMeta('node_seq', String(this.nodeSeq))
    this.store.setMeta('new_node_id', this.newNodeId)
    this.store.setMeta('last_dr', '0')
    this.store.setMeta('retention', this.retention ? '1' : '0')
    for (const node of this.configured) {
      this.store.insertNode({
        ...node,
        ip: node.address || '',
        status: 'up',
        report_period: this.reportIntervalMs,
        last_report: 0,
        retries: 0,
        window_notice: null
      })
    }
  }

  async tick() {
    if (this.working) return
    this.working = true
    try {
      await this.checkLiveness()
      await this.organize()
      if (this.store.getMergeJob()) await this.finishMerge()
    } finally {
      this.working = false
    }
  }

  async route({ method, path, query, body }) {
    if (method === 'POST' && path === '/v1/hello') return { body: this.hello(body || {}) }
    if (method === 'POST' && path === '/v1/report') return { body: await this.report(body || {}) }
    if (method === 'GET' && path === '/v1/alias') return { body: this.alias(query.get('alias')) }
    if (method === 'POST' && path === '/v1/join') return { body: await this.join(body || {}) }
    if (method === 'POST' && path === '/v1/ready') return { body: this.ready(body || {}) }
    if (method === 'POST' && path === '/v1/loss') return { body: await this.lose(body?.nodeId, body?.cause || '节点申请挂失') }
    if (method === 'POST' && path === '/v1/window') return { body: this.window(body || {}) }
    throw fail('没有这个接口', 404)
  }

  node(id) {
    const row = this.store.getNode(id)
    if (!row) throw fail('没有这台节点')
    return row
  }

  services() {
    return this.store.listNodes().filter((node) => node.role === 'service')
  }

  hello(body) {
    const node = this.node(body.id)
    if (node.status === 'lost') return { status: 'lost', address: addressOf(node) }
    const address = String(body.address || '').trim()
    if (!address) throw fail('要带上可以连接的地址')
    parseAddress(address)
    this.store.transaction(() => {
      this.store.updateNode(node.id, { ip: address, last_report: this.clock(), retries: 0 })
      if (node.role === 'dr' && Array.isArray(body.covers)) {
        this.store.updateNode(node.id, { covers: body.covers.map(String) })
      }
    })
    return { status: node.status, address }
  }

  async report(body) {
    const done = await this.accept(body.nodeId, body.entries || [])
    return { done }
  }

  async accept(nodeId, entries) {
    const node = this.node(nodeId)
    if (node.role !== 'service') throw fail('只有服务节点上报')
    if (node.status !== 'up' && node.status !== 'merging') throw fail(`${node.name}现在不能上报`)
    const done = []
    const fanout = []
    this.store.transaction(() => {
      for (const entry of entries) {
        const op = entry.op || 'put'
        const receipt = {
          source_node: node.id,
          user_id: entry.user_id,
          data_key: entry.data_key,
          updated_at: entry.updated_at,
          op
        }
        const duplicate = this.store.hasReceipt(receipt)
        if (!duplicate) {
          this.store.insertReceipt(receipt)
          this.store.insertLog({
            tick: this.clock(),
            user_id: entry.user_id,
            data_key: entry.data_key,
            value: entry.value ?? '',
            updated_at: entry.updated_at,
            source_node: node.id,
            op
          })
          const next = {
            user_id: entry.user_id,
            data_key: entry.data_key,
            node_id: node.id,
            ip: addressOf(this.node(node.id)),
            alias: node.alias,
            updated_at: entry.updated_at,
            suspended: 0,
            op,
            source: node.id
          }
          const changed = op === 'delete' ? removeMaster(this.store, next) : putMaster(this.store, next)
          if (changed) fanout.push(next)
        }
        done.push({ user_id: entry.user_id, data_key: entry.data_key, updated_at: entry.updated_at, op })
      }
      this.queueFanout(fanout)
      this.store.updateNode(node.id, { last_report: this.clock(), retries: 0 })
    })
    await this.flushPending()
    return done
  }

  queueFanout(entries) {
    for (const entry of entries) {
      for (const target of this.services()) {
        if (target.id === entry.source) continue
        if (target.status === 'lost') continue
        if (target.status === 'joining') {
          this.buffer(target.id, entry)
          continue
        }
        if (target.status !== 'up' && target.status !== 'merging') continue
        this.remember(target.id, entry)
      }
    }
  }

  buffer(nodeId, entry) {
    const existing = this.store.getBuffer(nodeId, entry.user_id, entry.data_key)
    if (rejectsOlder(existing, entry.updated_at)) return
    const next = {
      target_node: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended ? 1 : 0,
      op: entry.op || 'put'
    }
    if (existing) {
      this.store.updateBuffer(existing.id, next)
      return
    }
    this.store.insertBuffer({ node_id: nodeId, user_id: entry.user_id, data_key: entry.data_key, ...next })
  }

  remember(nodeId, entry) {
    const existing = this.store.getPending(nodeId, entry.user_id, entry.data_key)
    if (rejectsOlder(existing, entry.updated_at)) return
    this.store.putPending({
      node_id: nodeId,
      user_id: entry.user_id,
      data_key: entry.data_key,
      target_node: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended ? 1 : 0,
      op: entry.op || 'put'
    })
  }

  pendingEntries(nodeId) {
    return this.store.listPending(nodeId).map((row) => ({
      user_id: row.user_id,
      data_key: row.data_key,
      node_id: row.target_node,
      ip: row.ip,
      alias: row.alias,
      updated_at: row.updated_at,
      suspended: row.suspended,
      op: row.op || 'put'
    }))
  }

  async flushPending() {
    for (const node of this.services()) {
      if ((node.status !== 'up' && node.status !== 'merging') || !node.ip) continue
      const entries = this.pendingEntries(node.id)
      if (entries.length === 0) continue
      try {
        const { host, port } = parseAddress(node.ip)
        await call({ host, port, method: 'POST', path: '/v1/index', body: { entries }, token: this.token })
        this.store.transaction(() => this.store.deletePending(node.id))
      } catch {
        // 留到下次联系再送。
      }
    }
  }

  alias(alias) {
    const node = this.store.listNodes().find((item) => item.alias === alias && item.role === 'service')
    if (!node) throw fail('没有这个别名')
    return { id: node.id, ip: node.ip, alias: node.alias, status: node.status }
  }

  async checkLiveness() {
    const now = this.clock()
    const tolerance = Number(this.store.getMeta('tolerance') || this.toleranceMs)
    for (const node of this.services()) {
      if (node.status !== 'up' && node.status !== 'merging') continue
      const period = Number(node.report_period)
      const last = Number(node.last_report)
      if (!last || now - last < period) continue
      if (now < last + period + tolerance) continue
      try {
        if (!node.ip) throw fail('无法连接')
        const { host, port } = parseAddress(node.ip)
        const listed = await call({ host, port, method: 'GET', path: '/v1/outbox', token: this.token })
        await this.accept(node.id, listed.entries || [])
      } catch {
        const fresh = this.node(node.id)
        if (fresh.status !== 'up' && fresh.status !== 'merging') continue
        const fails = Number(fresh.retries) + 1
        this.store.transaction(() => this.store.updateNode(node.id, { retries: fails }))
        if (fails >= 2) await this.lose(node.id, '未按预期上报')
      }
    }
  }

  async lose(nodeId, cause = '中央强制挂失') {
    const node = this.node(nodeId)
    if (node.role !== 'service') throw fail('只能挂失服务节点')
    if (node.status === 'lost') return { message: `${node.name}已经挂失` }
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务，不用走挂失`)
    const job = this.store.getMergeJob()
    if (job && (job.target_id === node.id || node.status === 'merging')) return this.rollback(node.id, cause)
    if (job) throw fail('已经有节点在融合，先完成这次再挂失下一台')
    const target = await this.lowestLoad(node.id)
    if (!target) throw fail('没有还能接替的服务节点')
    this.store.transaction(() => {
      this.store.updateNode(node.id, { status: 'lost' })
      this.store.updateNode(target.id, { status: 'merging' })
      this.store.insertMergeJob({ lost_id: node.id, target_id: target.id, started_tick: this.clock() })
      this.store.suspendMasters(node.id)
    })
    await this.pushSuspend(node.id)
    const finished = await this.finishMerge()
    return { message: `${cause}：${node.name}挂失，数据装入${target.name}。${finished.message}` }
  }

  async pushSuspend(lostId) {
    const entry = { nodeId: lostId }
    for (const node of this.services()) {
      if (node.id === lostId || node.status === 'lost' || !node.ip) continue
      try {
        const { host, port } = parseAddress(node.ip)
        await call({ host, port, method: 'POST', path: '/v1/suspend', body: entry, token: this.token })
      } catch {
        // 下次上报时索引仍以中央为准，读路径看到挂起后不会发出。
      }
    }
  }

  async lowestLoad(excludeId) {
    const ranked = []
    for (const node of this.services()) {
      if (node.id === excludeId || node.status !== 'up' || !node.ip) continue
      try {
        const { host, port } = parseAddress(node.ip)
        const body = await call({ host, port, method: 'GET', path: '/v1/count', token: this.token })
        ranked.push({ node, count: Number(body.count) || 0 })
      } catch {
        // 连不上的节点不参与接替。
      }
    }
    ranked.sort((a, b) => a.count - b.count || (a.node.id < b.node.id ? -1 : 1))
    return ranked[0]?.node || null
  }

  async finishMerge() {
    const job = this.store.getMergeJob()
    if (!job) throw fail('现在没有正在融合的节点')
    if (this.store.countLogs() > 0) await this.organize({ force: true })
    const lost = this.node(job.lost_id)
    const target = this.node(job.target_id)
    let winners = []
    try {
      if (!target.ip) throw fail('接替节点无法访问')
      const records = await this.recordsFor(lost.id)
      const { host, port } = parseAddress(target.ip)
      const merged = await call({
        host,
        port,
        method: 'POST',
        path: '/v1/merge',
        body: { records },
        token: this.token
      })
      winners = dedupe(merged.winners || [])
    } catch (error) {
      const rolled = this.rollback(target.id, error.message || '接替节点无法访问')
      await this.pushSuspend(target.id)
      return rolled
    }
    await this.retarget(lost.id, target, winners)
    this.store.transaction(() => {
      for (const winning of winners) {
        putMaster(this.store, {
          user_id: winning.user_id,
          data_key: winning.data_key,
          node_id: target.id,
          ip: target.ip,
          alias: target.alias,
          updated_at: winning.updated_at,
          suspended: 0
        })
      }
      this.store.updateNode(target.id, { status: 'up' })
      this.store.deleteMergeJob()
      this.queueFanout(
        winners.map((winning) => ({
          user_id: winning.user_id,
          data_key: winning.data_key,
          node_id: target.id,
          ip: target.ip,
          alias: target.alias,
          updated_at: winning.updated_at,
          suspended: 0,
          op: 'put',
          source: ''
        }))
      )
    })
    await this.flushPending()
    return { message: `已装入 ${winners.length} 份` }
  }

  async recordsFor(homeId) {
    const records = []
    for (const dr of this.store.listNodes()) {
      if (dr.role !== 'dr' || !dr.covers.includes(homeId) || !dr.ip) continue
      const { host, port } = parseAddress(dr.ip)
      const body = await call({ host, port, method: 'GET', path: `/v1/records?home=${encodeURIComponent(homeId)}`, token: this.token })
      records.push(...(body.records || []))
    }
    return records
  }

  async retarget(lostId, target, winners) {
    for (const dr of this.store.listNodes()) {
      if (dr.role !== 'dr' || !dr.ip) continue
      const keep = dr.covers.includes(target.id)
      const { host, port } = parseAddress(dr.ip)
      await call({
        host,
        port,
        method: 'POST',
        path: '/v1/retarget',
        body: { lostId, targetId: target.id, keep, records: winners, organizedTick: this.clock() },
        token: this.token
      })
    }
  }

  rollback(targetId, cause) {
    const job = this.store.getMergeJob()
    const target = this.node(targetId)
    this.store.transaction(() => {
      if (target.status !== 'lost') this.store.updateNode(targetId, { status: 'lost' })
      if (job && job.target_id === targetId) this.store.deleteMergeJob()
      this.store.suspendMasters(targetId)
    })
    return { message: `立刻回退尚未完成的装入，并将${target.name}标记挂失。数据仍留在灾备。原因：${cause}。` }
  }

  async recover(nodeId) {
    const node = this.node(nodeId)
    if (node.role !== 'service' || node.status !== 'lost') throw fail('只能对已经挂失的服务节点继续恢复')
    if (this.store.getMergeJob()) throw fail('已经有节点在融合')
    const target = await this.lowestLoad(node.id)
    if (!target) throw fail('没有还能接替的服务节点')
    this.store.transaction(() => {
      this.store.updateNode(target.id, { status: 'merging' })
      this.store.insertMergeJob({ lost_id: node.id, target_id: target.id, started_tick: this.clock() })
    })
    return this.finishMerge()
  }

  async organize({ force = false } = {}) {
    const now = this.clock()
    const last = Number(this.store.getMeta('last_dr') || 0)
    if (!force && now - last < this.drIntervalMs) return { stored: 0 }
    const logs = this.store.listLogs()
    if (logs.length === 0) {
      this.store.transaction(() => this.store.setMeta('last_dr', String(now)))
      return { stored: 0 }
    }
    const collapsed = collapseLogs(logs)
    const batches = new Map()
    for (const log of collapsed) {
      const home = this.store.getMaster(log.user_id, log.data_key)
      const homeId = home ? home.node_id : log.source_node
      for (const dr of this.store.listNodes().filter((node) => node.role === 'dr')) {
        const batch = batches.get(dr.id) || { upserts: [], deletes: [], drops: [] }
        const covered = Boolean(homeId && dr.covers.includes(homeId))
        if (!covered) batch.drops.push({ user_id: log.user_id, data_key: log.data_key })
        else if (log.op === 'delete') {
          batch.deletes.push({ user_id: log.user_id, data_key: log.data_key, updated_at: log.updated_at })
        } else {
          batch.upserts.push({
            user_id: log.user_id,
            data_key: log.data_key,
            value: log.value,
            updated_at: log.updated_at,
            home_node: homeId,
            organized_tick: now
          })
        }
        batches.set(dr.id, batch)
      }
    }
    let stored = 0
    for (const [drId, batch] of batches) {
      const dr = this.node(drId)
      if (!dr.ip) throw fail(`${dr.name}还没有连上`)
      const { host, port } = parseAddress(dr.ip)
      await call({ host, port, method: 'POST', path: '/v1/apply', body: batch, token: this.token })
      stored += batch.upserts.length
    }
    if (this.store.getMeta('retention') === '1') this.keep(logs)
    this.store.transaction(() => {
      this.store.deleteLogs(logs.map((log) => log.id))
      this.store.setMeta('last_dr', String(now))
    })
    return { stored }
  }

  keep(logs) {
    fs.mkdirSync(path.dirname(this.retentionFile), { recursive: true })
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
      .join('\n')
    fs.appendFileSync(this.retentionFile, `${lines}\n`)
  }

  async join(body) {
    const region = String(body.region || '').trim()
    if (!region) throw fail('新节点要落在一个区域')
    const address = String(body.address || '').trim()
    parseAddress(address)
    const source = await this.lowestLoad('')
    if (!source) throw fail('现在没有正在服务的节点可以提供索引')
    const { host, port } = parseAddress(source.ip)
    const temporary = await call({ host, port, method: 'GET', path: '/v1/temporary-index', token: this.token })
    const seq = Number(this.store.getMeta('node_seq'))
    const id = String(this.store.getMeta('new_node_id')).replaceAll('{seq}', String(seq))
    const name = String(body.name || '').trim() || `${region}节点`
    this.store.transaction(() => {
      this.store.setMeta('node_seq', String(seq + 1))
      this.store.insertNode({
        id,
        name,
        role: 'service',
        region,
        ip: address,
        alias: id,
        status: 'joining',
        covers: [],
        report_period: this.reportIntervalMs,
        last_report: this.clock(),
        retries: 0,
        window_notice: null
      })
    })
    return { nodeId: id, name, ip: address, index: temporary.rows || [] }
  }

  ready(body) {
    const node = this.node(body.nodeId)
    if (node.role !== 'service' || node.status !== 'joining') throw fail(`${node.name}不在对齐过程中`)
    const indexes = this.store.listBuffers(node.id).map((row) => ({
      user_id: row.user_id,
      data_key: row.data_key,
      node_id: row.target_node,
      ip: row.ip,
      alias: row.alias,
      updated_at: row.updated_at,
      suspended: row.suspended,
      op: row.op || 'put'
    }))
    this.store.transaction(() => {
      this.store.deleteBuffers(node.id)
      this.store.updateNode(node.id, { status: 'up', last_report: this.clock(), retries: 0 })
    })
    return { indexes }
  }

  window(body) {
    const node = this.node(body.nodeId)
    if (node.role !== 'service') throw fail('只有服务节点有上报周期')
    const interval = positive(body.reportIntervalMs, '上报间隔')
    const now = this.clock()
    this.store.transaction(() => {
      this.store.updateNode(node.id, { report_period: interval, last_report: now, retries: 0, window_notice: now })
    })
    return { reportIntervalMs: interval }
  }

  async close() {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    if (this.http) await this.http.close()
    this.http = null
    this.store.close()
  }

  get address() {
    return `${this.host}:${this.http?.port ?? this.port}`
  }
}

function dedupe(records) {
  const chosen = new Map()
  for (const record of records) {
    const id = `${record.user_id}\0${record.data_key}`
    const prev = chosen.get(id)
    if (!prev || record.updated_at >= prev.updated_at) chosen.set(id, record)
  }
  return [...chosen.values()]
}

function positive(value, label) {
  const number = Number(value)
  if (!Number.isInteger(number) || number < 1) throw fail(`${label}必须是不小于 1 的整数`)
  return number
}

function normalize(node, index) {
  if (!node || typeof node !== 'object') throw fail(`第 ${index + 1} 台节点不是对象`)
  const id = String(node.id || '').trim()
  const name = String(node.name || '').trim()
  const role = String(node.role || '').trim()
  const region = String(node.region || '').trim()
  const alias = String(node.alias || id).trim()
  if (!id || !name || !region || !alias) throw fail(`${name || id || '有一台节点'}缺少编号、名称、区域或别名`)
  if (role !== 'service' && role !== 'dr') throw fail(`${name}的角色只能是 service 或 dr`)
  const covers = role === 'dr' ? [...new Set((node.covers || []).map((item) => String(item)))] : []
  return { id, name, role, region, alias, covers, address: String(node.address || '').trim() }
}
