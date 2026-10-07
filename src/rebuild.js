import { FileStore } from './file-store.js'
import { call, fail, parseAddress } from './rpc.js'

function keyOf(row) {
  return `${row.user_id}\0${row.data_key}`
}

export async function rebuildCentralIndexes(options = {}) {
  const { dataDir, token, nodes = null, timeoutMs = 2000, retries = 1, tls = null } = options
  const store = options.store || new FileStore(dataDir)
  store.open()
  try {
    if (!store.hasLayout()) {
      if (!Array.isArray(nodes) || nodes.length === 0) throw fail('中央的库是空的，要带上节点清单')
      store.transaction(() => {
        store.setMeta('tick', '0')
        for (const node of nodes) {
          store.insertNode({
            id: String(node.id),
            name: String(node.name || node.id),
            role: node.role,
            region: String(node.region || ''),
            ip: String(node.ip || ''),
            alias: String(node.alias || node.id),
            status: 'up',
            covers: [...(node.covers || [])],
            report_period: 2000,
            last_report: 0,
            retries: 0,
            window_notice: null
          })
        }
      })
    }
    const all = store.listNodes()
    const byId = new Map(all.map((node) => [node.id, node]))
    const services = all.filter((node) => node.role === 'service')
    const drs = all.filter((node) => node.role === 'dr' && node.ip)

    const candidates = new Map()

    for (const node of services) {
      if (!node.ip || node.status === 'lost' || node.status === 'joining') continue
      let body
      try {
        const { host, port } = parseAddress(node.ip)
        body = await call({ host, port, method: 'GET', path: '/v1/fragments', token, timeoutMs, retries, tls })
      } catch {
        continue
      }
      for (const row of body.rows || []) {
        const id = keyOf(row)
        const prev = candidates.get(id)
        if (prev && prev.updated_at >= row.updated_at) continue
        candidates.set(id, {
          user_id: row.user_id,
          data_key: row.data_key,
          value: row.value,
          updated_at: row.updated_at,
          node_id: node.id,
          ip: node.ip,
          alias: node.alias
        })
      }
    }

    for (const dr of drs) {
      let body
      try {
        const { host, port } = parseAddress(dr.ip)
        body = await call({ host, port, method: 'GET', path: '/v1/records', token, timeoutMs, retries, tls })
      } catch {
        continue
      }
      for (const row of body.records || []) {
        const id = keyOf(row)
        const prev = candidates.get(id)
        if (prev && prev.updated_at >= row.updated_at) continue
        const home = byId.get(row.home_node)
        candidates.set(id, {
          user_id: row.user_id,
          data_key: row.data_key,
          value: row.value,
          updated_at: row.updated_at,
          node_id: row.home_node,
          ip: home?.ip || '',
          alias: home?.alias || row.home_node
        })
      }
    }

    let masters = 0
    store.transaction(() => {
      for (const entry of candidates.values()) {
        const home = byId.get(entry.node_id)
        if (!home) continue
        store.putMaster({
          user_id: entry.user_id,
          data_key: entry.data_key,
          node_id: entry.node_id,
          ip: entry.ip,
          alias: entry.alias,
          updated_at: entry.updated_at,
          suspended: home.status === 'lost' ? 1 : 0
        })
        masters += 1
      }
    })

    const entries = [...candidates.values()].map((entry) => ({
      ...entry,
      suspended: byId.get(entry.node_id)?.status === 'lost' ? 1 : 0,
      op: 'put'
    }))
    let pushed = 0
    for (const node of services) {
      if (!node.ip || node.status === 'lost' || node.status === 'joining') continue
      try {
        const { host, port } = parseAddress(node.ip)
        await call({ host, port, method: 'POST', path: '/v1/index', body: { entries }, token, timeoutMs, retries, tls })
        pushed += 1
      } catch {
        // 留到下次联系再送。
      }
    }
    return { masters, pushed, collected: candidates.size }
  } finally {
    if (!options.store) store.close()
  }
}
