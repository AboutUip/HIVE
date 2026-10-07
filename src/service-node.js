import { FileStore } from './file-store.js'
import { call, delay, fail, parseAddress, serve } from './rpc.js'
import { applyLocal } from './rules.js'

export async function startService(options = {}) {
  const service = new Service(options)
  await service.start()
  return service
}

class Service {
  constructor(options) {
    if (!options.dataDir) throw fail('要指定数据目录')
    if (!options.token) throw fail('集群令牌不能是空的')
    if (!options.central?.host || !options.central?.port) throw fail('要写上中央的地址')
    this.token = options.token
    this.dataDir = options.dataDir
    this.host = options.host || '127.0.0.1'
    this.port = options.port ?? 0
    this.central = options.central
    this.now = options.now || (() => new Date())
    this.reportIntervalMs = positive(options.reportIntervalMs ?? 2000, '上报间隔')
    this.helloTimeoutMs = options.helloTimeoutMs ?? 5000
    this.join = Boolean(options.join)
    this.id = options.id
    this.name = options.name
    this.region = options.region
    this.alias = options.alias || options.id
    this.store = options.store || new FileStore(options.dataDir)
    this.lastAt = 0
    this.joining = false
    this.http = null
    this.timer = null
  }

  async start() {
    this.store.open()
    const stamp = this.id ? this.store.latestStamp(this.id) : null
    if (stamp) {
      const ms = Date.parse(stamp)
      if (Number.isFinite(ms)) this.lastAt = ms
    }
    this.http = await serve({
      host: this.host,
      port: this.port,
      token: this.token,
      handler: (request) => this.route(request)
    })
    this.address = `${this.host}:${this.http.port}`
    if (this.join) await this.joinCluster()
    else await this.announce()
    this.timer = setInterval(() => {
      if (!this.joining) this.report().catch(() => {})
    }, this.reportIntervalMs)
    return this
  }

  async announce() {
    if (!this.id || !this.name || !this.region) throw fail('服务节点要有编号、名称和区域')
    const deadline = Date.now() + this.helloTimeoutMs
    let last = fail('中央还没有连上')
    while (Date.now() < deadline) {
      try {
        const body = await this.centralCall('POST', '/v1/hello', {
          id: this.id,
          name: this.name,
          role: 'service',
          region: this.region,
          alias: this.alias,
          address: this.address
        })
        if (body.status === 'lost') throw fail('这台节点已挂失，清空数据目录后用 join 重新加入')
        this.joining = body.status === 'joining'
        return
      } catch (error) {
        if (error.message.includes('已挂失')) throw error
        last = error
        await delay(50)
      }
    }
    throw last
  }

  async joinCluster() {
    const region = String(this.region || '').trim()
    if (!region) throw fail('新节点要落在一个区域')
    const created = await this.centralCall('POST', '/v1/join', {
      region,
      name: this.name,
      address: this.address
    })
    this.id = created.nodeId
    this.name = created.name
    this.alias = created.nodeId
    this.joining = true
    this.store.transaction(() => {
      for (const row of created.index || []) {
        this.store.putIndex(this.id, {
          user_id: row.user_id,
          data_key: row.data_key,
          node_id: row.node_id,
          ip: row.ip,
          alias: row.alias || row.node_id,
          updated_at: row.updated_at,
          suspended: row.suspended ? 1 : 0
        })
      }
    })
  }

  async ready() {
    if (!this.joining) throw fail(`${this.name}不在对齐过程中`)
    const body = await this.centralCall('POST', '/v1/ready', { nodeId: this.id })
    this.store.transaction(() => {
      for (const entry of body.indexes || []) applyLocal(this.store, this.id, entry)
    })
    this.joining = false
    return { message: `${this.name}已就绪` }
  }

  async route({ method, path, query, body }) {
    if (method === 'GET' && path === '/v1/data') return { body: this.readLocal(query.get('userId'), query.get('key')) }
    if (method === 'GET' && path === '/v1/count') return { body: { count: this.store.countFragments(this.id) } }
    if (method === 'GET' && path === '/v1/outbox') return { body: { entries: this.outboxEntries() } }
    if (method === 'GET' && path === '/v1/temporary-index') return { body: { rows: this.temporaryIndex() } }
    if (method === 'POST' && path === '/v1/index') return { body: this.takeIndex(body || {}) }
    if (method === 'POST' && path === '/v1/suspend') return { body: this.suspend(body || {}) }
    if (method === 'POST' && path === '/v1/merge') return { body: this.merge(body || {}) }
    throw fail('没有这个接口', 404)
  }

  readLocal(userId, key) {
    const row = this.store.getFragment(this.id, userId, key)
    if (!row) return { found: false }
    return { found: true, value: row.value, updated_at: row.updated_at }
  }

  outboxEntries() {
    return this.store.listOutbox(this.id).map((row) => ({
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value,
      updated_at: row.updated_at,
      op: row.op
    }))
  }

  temporaryIndex() {
    const table = new Map()
    for (const row of this.store.listIndexes(this.id)) {
      table.set(`${row.user_id}\0${row.data_key}`, { ...row, alias: row.alias || row.node_id })
    }
    for (const fragment of this.store.listFragments(this.id)) {
      table.set(`${fragment.user_id}\0${fragment.data_key}`, {
        user_id: fragment.user_id,
        data_key: fragment.data_key,
        node_id: this.id,
        ip: this.address,
        alias: this.alias,
        updated_at: fragment.updated_at,
        suspended: 0
      })
    }
    return [...table.values()]
  }

  takeIndex(body) {
    this.store.transaction(() => {
      for (const entry of body.entries || []) applyLocal(this.store, this.id, entry)
    })
    return { ok: true }
  }

  suspend(body) {
    this.store.transaction(() => this.store.suspendIndexes(this.id, body.nodeId))
    return { ok: true }
  }

  merge(body) {
    const winners = []
    this.store.transaction(() => {
      for (const record of body.records || []) {
        const local = this.store.getFragment(this.id, record.user_id, record.data_key)
        if (local && local.updated_at > record.updated_at) {
          winners.push({
            user_id: local.user_id,
            data_key: local.data_key,
            value: local.value,
            updated_at: local.updated_at
          })
          continue
        }
        this.store.putFragment(this.id, {
          user_id: record.user_id,
          data_key: record.data_key,
          value: record.value,
          updated_at: record.updated_at
        })
        winners.push({
          user_id: record.user_id,
          data_key: record.data_key,
          value: record.value,
          updated_at: record.updated_at
        })
      }
    })
    return { winners }
  }

  async write({ userId, entries }) {
    try {
      return await this.writeEntries({ userId, entries })
    } catch (error) {
      await this.reportStoreFailure(error)
      throw error
    }
  }

  async writeEntries({ userId, entries }) {
    this.assertServing()
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
        const updatedAt = this.issuedAt()
        this.store.putFragment(this.id, { user_id: user, data_key: key, value, updated_at: updatedAt })
        this.store.insertOutbox(this.id, {
          tick: 0,
          user_id: user,
          data_key: key,
          value,
          updated_at: updatedAt,
          op: 'put'
        })
      })
      written.push(key)
    }
    return { written }
  }

  async remove({ userId, keys }) {
    try {
      return await this.removeKeys({ userId, keys })
    } catch (error) {
      await this.reportStoreFailure(error)
      throw error
    }
  }

  async removeKeys({ userId, keys }) {
    this.assertServing()
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要删除的键')
    const removed = []
    for (const raw of keys) {
      const key = String(raw || '').trim()
      if (!key) throw fail('每一份数据都要有名字')
      if (!this.store.getFragment(this.id, user, key)) throw fail(`${this.name}上没有 ${user} 的 ${key}，不能在这里删除`)
      this.store.transaction(() => {
        const updatedAt = this.issuedAt()
        this.store.deleteFragment(this.id, user, key)
        this.store.deleteIndex(this.id, user, key)
        this.store.insertOutbox(this.id, {
          tick: 0,
          user_id: user,
          data_key: key,
          value: '',
          updated_at: updatedAt,
          op: 'delete'
        })
      })
      removed.push(key)
    }
    return { removed }
  }

  async read({ userId, keys }) {
    try {
      return await this.readKeys({ userId, keys })
    } catch (error) {
      await this.reportStoreFailure(error)
      throw error
    }
  }

  async readKeys({ userId, keys }) {
    this.assertServing()
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要读取的键')
    const results = []
    for (const raw of keys) results.push(await this.readOne(user, String(raw || '').trim()))
    return { results }
  }

  async readOne(user, key) {
    if (!key) return { key, source: 'rejected', message: '没有键名' }
    const local = this.store.getFragment(this.id, user, key)
    if (local) {
      return { key, value: local.value, source: 'local', storedLocally: true, fromNodeId: this.id, fromIp: this.address, via: 'local', message: '就在这台节点上' }
    }
    const index = this.store.getIndex(this.id, user, key)
    if (!index) return { key, source: 'rejected', message: '本地没有，索引里也没有，拒绝' }
    if (index.suspended) return { key, source: 'suspended', fromIp: index.ip, message: '索引已挂起，请求未发出' }
    const direct = await this.fetch(index.ip, user, key)
    if (direct.status === 'ok') {
      return { key, value: direct.value, source: 'remote', storedLocally: false, fromIp: index.ip, via: 'ip', message: `从 ${index.ip} 取回，未写入本机` }
    }
    if (direct.status === 'missing') {
      return { key, source: 'rejected', fromIp: index.ip, message: `索引指向 ${index.ip}，但那里已经没有这份数据` }
    }
    let resolved
    try {
      resolved = await this.centralCall('GET', `/v1/alias?alias=${encodeURIComponent(index.alias)}`)
    } catch {
      return { key, source: 'rejected', fromIp: index.ip, message: '地址不可达，向中央核对别名后仍不可达' }
    }
    if (!resolved || resolved.status === 'lost' || resolved.status === 'joining') {
      return { key, source: 'suspended', fromIp: index.ip, message: '目标节点不可用，请求未发出' }
    }
    this.store.transaction(() => this.store.updateIndexIpByAlias(this.id, index.alias, resolved.ip))
    const again = await this.fetch(resolved.ip, user, key)
    if (again.status === 'ok') {
      return {
        key,
        value: again.value,
        source: 'remote',
        storedLocally: false,
        fromNodeId: resolved.id,
        fromIp: resolved.ip,
        via: 'alias',
        message: `原地址不通，按别名 ${index.alias} 更新为 ${resolved.ip} 后取回，未写入本机`
      }
    }
    if (again.status === 'missing') {
      return { key, source: 'rejected', fromIp: resolved.ip, message: `索引指向 ${resolved.ip}，但那里已经没有这份数据` }
    }
    return { key, source: 'rejected', fromIp: index.ip, message: '地址不可达，向中央核对别名后仍不可达' }
  }

  async fetch(address, user, key) {
    try {
      const { host, port } = parseAddress(address)
      const body = await call({
        host,
        port,
        method: 'GET',
        path: `/v1/data?userId=${encodeURIComponent(user)}&key=${encodeURIComponent(key)}`,
        token: this.token
      })
      if (!body.found) return { status: 'missing' }
      return { status: 'ok', value: body.value }
    } catch {
      return { status: 'down' }
    }
  }

  list({ userId } = {}) {
    try {
      const user = String(userId || '').trim()
      const rows = this.temporaryIndex().filter((row) => !user || row.user_id === user)
      return { rows }
    } catch (error) {
      if (!error.expose) void this.reportStoreFailure(error)
      throw error
    }
  }

  async report() {
    try {
      return await this.sendReport()
    } catch (error) {
      await this.reportStoreFailure(error)
      throw error
    }
  }

  async sendReport() {
    if (this.joining) return { reported: 0 }
    const entries = this.outboxEntries()
    const body = await this.centralCall('POST', '/v1/report', { nodeId: this.id, entries })
    const done = new Set((body.done || []).map((row) => `${row.user_id}\0${row.data_key}\0${row.updated_at}\0${row.op}`))
    this.store.transaction(() => {
      const remove = this.store
        .listOutbox(this.id)
        .filter((row) => done.has(`${row.user_id}\0${row.data_key}\0${row.updated_at}\0${row.op}`))
      if (remove.length > 0) this.store.deleteOutbox(this.id, remove.map((row) => row.id))
    })
    return { reported: entries.length }
  }

  async setReportInterval(reportIntervalMs) {
    const interval = positive(reportIntervalMs, '上报间隔')
    await this.centralCall('POST', '/v1/window', { nodeId: this.id, reportIntervalMs: interval })
    this.reportIntervalMs = interval
    if (this.timer) clearInterval(this.timer)
    this.timer = setInterval(() => {
      if (!this.joining) this.report().catch(() => {})
    }, interval)
    return { reportIntervalMs: interval }
  }

  async reportStoreFailure(error) {
    if (error?.expose || this.reportingLoss || !this.id) return
    this.reportingLoss = true
    try {
      await this.centralCall('POST', '/v1/loss', { nodeId: this.id, cause: '数据库不可用' })
    } catch {
      // 中央联系不上时，原来的错误仍然交给调用方。
    } finally {
      this.reportingLoss = false
    }
  }

  assertServing() {
    if (this.joining) throw fail(`${this.name}还没投入服务`)
  }

  issuedAt() {
    const raw = this.now()
    const parsed = raw instanceof Date ? raw.getTime() : Date.parse(String(raw))
    let ms = Number.isFinite(parsed) ? parsed : Date.now()
    if (ms <= this.lastAt) ms = this.lastAt + 1
    this.lastAt = ms
    return new Date(ms).toISOString()
  }

  centralCall(method, path, body) {
    return call({ host: this.central.host, port: this.central.port, method, path, body, token: this.token })
  }

  async close() {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    if (this.http) await this.http.close()
    this.http = null
    this.store.close()
  }
}

function positive(value, label) {
  const number = Number(value)
  if (!Number.isInteger(number) || number < 1) throw fail(`${label}必须是不小于 1 的整数`)
  return number
}
