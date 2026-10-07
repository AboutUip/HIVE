import { FileStore } from './file-store.js'
import { call, delay, fail, serve } from './rpc.js'

export async function startDr(options = {}) {
  const dr = new Dr(options)
  await dr.start()
  return dr
}

class Dr {
  constructor(options) {
    if (!options.dataDir) throw fail('要指定数据目录')
    if (!options.token) throw fail('集群令牌不能是空的')
    if (!options.central?.host || !options.central?.port) throw fail('要写上中央的地址')
    if (!options.id || !options.name || !options.region) throw fail('灾备要有编号、名称和区域')
    this.token = options.token
    this.id = options.id
    this.name = options.name
    this.region = options.region
    this.alias = options.alias || options.id
    this.covers = [...new Set((options.covers || []).map((item) => String(item)))]
    this.host = options.host || '127.0.0.1'
    this.port = options.port ?? 0
    this.central = options.central
    this.helloTimeoutMs = options.helloTimeoutMs ?? 5000
    this.store = options.store || new FileStore(options.dataDir)
    this.http = null
  }

  async start() {
    this.store.open()
    this.http = await serve({
      host: this.host,
      port: this.port,
      token: this.token,
      handler: (request) => this.route(request)
    })
    this.address = `${this.host}:${this.http.port}`
    await this.announce()
    return this
  }

  async announce() {
    const deadline = Date.now() + this.helloTimeoutMs
    let last = fail('中央还没有连上')
    while (Date.now() < deadline) {
      try {
        await call({
          host: this.central.host,
          port: this.central.port,
          method: 'POST',
          path: '/v1/hello',
          token: this.token,
          body: {
            id: this.id,
            name: this.name,
            role: 'dr',
            region: this.region,
            alias: this.alias,
            address: this.address,
            covers: this.covers
          }
        })
        return
      } catch (error) {
        last = error
        await delay(50)
      }
    }
    throw last
  }

  route({ method, path, query, body }) {
    if (method === 'POST' && path === '/v1/apply') return { body: this.apply(body || {}) }
    if (method === 'GET' && path === '/v1/records') return { body: { records: this.records(query.get('home')) } }
    if (method === 'POST' && path === '/v1/retarget') return { body: this.retarget(body || {}) }
    throw fail('没有这个接口', 404)
  }

  apply(body) {
    this.store.transaction(() => {
      for (const item of body.drops || []) this.store.deleteRecord(this.id, item.user_id, item.data_key)
      for (const item of body.deletes || []) {
        const existing = this.store.getRecord(this.id, item.user_id, item.data_key)
        if (!existing || existing.updated_at <= item.updated_at) this.store.deleteRecord(this.id, item.user_id, item.data_key)
      }
      for (const item of body.upserts || []) {
        const existing = this.store.getRecord(this.id, item.user_id, item.data_key)
        if (existing && existing.updated_at > item.updated_at) continue
        this.store.putRecord(this.id, item)
      }
    })
    return { ok: true }
  }

  records(home) {
    return this.store.listRecordsByHome(this.id, home)
  }

  retarget(body) {
    this.store.transaction(() => {
      for (const record of body.records || []) {
        if (body.keep) {
          this.store.updateRecordHome(this.id, {
            home_node: body.targetId,
            value: record.value,
            updated_at: record.updated_at,
            organized_tick: body.organizedTick,
            user_id: record.user_id,
            data_key: record.data_key,
            from_home: body.lostId
          })
          continue
        }
        this.store.deleteRecordHome(this.id, record.user_id, record.data_key, body.lostId)
      }
    })
    return { ok: true }
  }

  async close() {
    if (this.http) await this.http.close()
    this.http = null
    this.store.close()
  }
}
