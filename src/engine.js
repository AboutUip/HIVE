import fs from 'node:fs'
import path from 'node:path'
import { defaultConfigPath, loadConfig } from './config.js'
import { dbPath, initCentral, initDr, initService, openDatabase } from './db.js'

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
    this.central = null
    this.files = new Map()
    this.lastAt = new Map()
    this.unreachable = new Set()
    this.indexDeaf = new Set()
    this.dbBroken = new Set()
    this.aliasLookups = 0
    this.configSource = options.config
      ? { kind: 'object', value: options.config }
      : { kind: 'file', path: options.configPath || defaultConfigPath }
    this.layout = null
  }

  currentConfig() {
    if (this.configSource.kind === 'object') return loadConfig(this.configSource.value)
    return loadConfig(this.configSource.path)
  }

  reset() {
    this.close()
    this.lastAt.clear()
    this.unreachable.clear()
    this.indexDeaf.clear()
    this.dbBroken.clear()
    this.aliasLookups = 0
    fs.mkdirSync(this.dataDir, { recursive: true })
    fs.rmSync(this.dataDir, { recursive: true, force: true })
    fs.mkdirSync(this.dataDir, { recursive: true })
    this.central = openDatabase(dbPath(this.dataDir, 'central'))
    initCentral(this.central)
    this.central.exec(`
      DELETE FROM logs;
      DELETE FROM receipts;
      DELETE FROM index_master;
      DELETE FROM buffers;
      DELETE FROM pending_index;
      DELETE FROM events;
      DELETE FROM merge_job;
      DELETE FROM nodes;
      DELETE FROM meta;
    `)
    const config = this.currentConfig()
    this.layout = config
    const actorNode = config.nodes.find((node) => node.id === config.actor.nodeId)
    this.setMeta('tick', '0')
    this.setMeta('sync_period', config.syncPeriod)
    this.setMeta('dr_period', config.drPeriod)
    this.setMeta('report_tolerance', config.reportTolerance)
    this.setMeta('actor_user', config.actor.userId)
    this.setMeta('actor_node', config.actor.nodeId)
    this.setMeta('node_seq', config.nodeSeq)
    this.setMeta('retention', '0')
    for (const node of config.nodes) {
      this.insertNode(node)
      this.openNodeFile(node)
    }
    this.event(
      0,
      'info',
      `已重置。人在${actorNode.region}。上报每 ${config.syncPeriod} 拍，灾备每 ${config.drPeriod} 拍。数据时间用各节点自己的 UTC。`
    )
    return this.reply('已重置到起始局面')
  }

  close() {
    for (const db of this.files.values()) db.close()
    this.files.clear()
    if (this.central) {
      this.central.close()
      this.central = null
    }
  }

  reply(message, extra = {}) {
    return { message, state: this.getState(), ...extra }
  }

  setMeta(key, value) {
    this.central
      .prepare(
        `INSERT INTO meta (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`
      )
      .run(key, String(value))
  }

  meta(key) {
    const row = this.central.prepare('SELECT value FROM meta WHERE key = ?').get(key)
    return row ? row.value : null
  }

  tick() {
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

  issuedAt(nodeId) {
    const raw = this.now()
    const parsed = raw instanceof Date ? raw.getTime() : Date.parse(String(raw))
    let ms = Number.isFinite(parsed) ? parsed : Date.now()
    const last = this.lastAt.get(nodeId) ?? 0
    if (ms <= last) ms = last + 1
    this.lastAt.set(nodeId, ms)
    return new Date(ms).toISOString()
  }

  event(tick, kind, message) {
    this.central.prepare('INSERT INTO events (tick, kind, message) VALUES (?, ?, ?)').run(tick, kind, message)
  }

  insertNode(node) {
    this.central
      .prepare(
        `INSERT INTO nodes
           (id, name, role, region, ip, alias, status, covers, report_period, last_report, retries)
         VALUES (?, ?, ?, ?, ?, ?, 'up', ?, ?, 0, 0)`
      )
      .run(
        node.id,
        node.name,
        node.role,
        node.region,
        node.ip,
        node.alias || node.id,
        JSON.stringify(node.covers || []),
        this.syncPeriod() || 2
      )
  }

  openNodeFile(node) {
    const db = openDatabase(dbPath(this.dataDir, node.id))
    if (node.role === 'service') {
      initService(db)
      db.exec('DELETE FROM fragments; DELETE FROM index_entries; DELETE FROM outbox;')
    }
    if (node.role === 'dr') {
      initDr(db)
      db.exec('DELETE FROM records;')
    }
    this.files.set(node.id, db)
    return db
  }

  nodeDb(id) {
    if (this.dbBroken.has(id)) throw fail('数据库不可达')
    const db = this.files.get(id)
    if (!db) throw fail('这台节点的库还没有打开')
    return db
  }

  getNode(id) {
    const row = this.central.prepare('SELECT * FROM nodes WHERE id = ?').get(id)
    if (!row) throw fail('没有这台节点')
    return { ...row, covers: JSON.parse(row.covers || '[]') }
  }

  nodes() {
    return this.central
      .prepare('SELECT * FROM nodes ORDER BY id')
      .all()
      .map((row) => ({ ...row, covers: JSON.parse(row.covers || '[]') }))
  }

  serviceNodes() {
    return this.nodes().filter((node) => node.role === 'service')
  }

  mergeJob() {
    return this.central.prepare('SELECT * FROM merge_job WHERE id = 1').get() || null
  }

  canReach(nodeId) {
    return !this.unreachable.has(nodeId)
  }

  fragmentCount(nodeId) {
    if (this.dbBroken.has(nodeId)) return Number.POSITIVE_INFINITY
    return this.nodeDb(nodeId).prepare('SELECT COUNT(*) AS n FROM fragments').get().n
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
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('人只能站在服务节点上')
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    this.setMeta('actor_node', node.id)
    this.setMeta('actor_user', user)
    return this.reply(`${user} 现在从${node.region}进入，直达${node.name}`)
  }

  setWindows(syncPeriod, drPeriod) {
    const sync = Number(syncPeriod)
    const dr = Number(drPeriod)
    if (!Number.isInteger(sync) || sync < 1) throw fail('上报窗口至少是 1 拍')
    if (!Number.isInteger(dr) || dr < 1) throw fail('灾备窗口至少是 1 拍')
    const tick = this.tick()
    this.setMeta('sync_period', sync)
    this.setMeta('dr_period', dr)
    this.central
      .prepare(
        `UPDATE nodes
         SET report_period = ?, window_notice = ?, last_report = ?, retries = 0
         WHERE role = 'service'`
      )
      .run(sync, tick, tick)
    this.event(tick, 'info', `已通知中央：上报改为每 ${sync} 拍，灾备每 ${dr} 拍。这段空档不算漏报。`)
    return this.reply(`上报每 ${sync} 拍，灾备每 ${dr} 拍`)
  }

  setReportPeriod(nodeId, period) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只有服务节点有上报周期')
    const next = Number(period)
    if (!Number.isInteger(next) || next < 1) throw fail('上报窗口至少是 1 拍')
    const tick = this.tick()
    this.central
      .prepare(
        `UPDATE nodes
         SET report_period = ?, window_notice = ?, last_report = ?, retries = 0
         WHERE id = ?`
      )
      .run(next, tick, tick, node.id)
    this.event(tick, 'info', `${node.name}通知中央：上报改为每 ${next} 拍。`)
    return this.reply(`${node.name}之后每 ${next} 拍上报`)
  }

  setRetention(enabled) {
    this.setMeta('retention', enabled ? '1' : '0')
    return this.reply(enabled ? '用完的日志会另写成 log 文件' : '用完的日志直接删除')
  }

  setReachable(nodeId, reachable) {
    this.getNode(nodeId)
    if (reachable) this.unreachable.delete(nodeId)
    else this.unreachable.add(nodeId)
    return this.reply(reachable ? `${nodeId} 可以访问` : `${nodeId} 无法访问`)
  }

  setIndexLink(nodeId, up) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('索引只派发给服务节点')
    if (up) this.indexDeaf.delete(nodeId)
    else this.indexDeaf.add(nodeId)
    return this.reply(up ? `${node.name}重新接收索引` : `${node.name}这一轮收不到索引，留到下次联系再补`)
  }

  breakDatabase(nodeId) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('这里只标记服务节点的库')
    this.dbBroken.add(nodeId)
    return this.reply(`${node.name}的数据库不可达，等它自己申请挂失`)
  }

  reportRuntime(nodeId, reason = '运行报错') {
    return this.applyLoss(nodeId, String(reason || '运行报错'))
  }

  write({ nodeId, userId, entries }) {
    const node = this.getNode(nodeId)
    this.assertWritable(node)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(entries) || entries.length === 0) throw fail('这次没有要写入的内容')
    const db = this.nodeDb(node.id)
    const written = []
    for (const entry of entries) {
      const key = String(entry.key || '').trim()
      const value = String(entry.value ?? '')
      if (!key) throw fail('每一份数据都要有名字')
      if (!value) throw fail(`${key} 还没有内容`)
      const updatedAt = this.issuedAt(node.id)
      db.prepare(
        `INSERT INTO fragments (user_id, data_key, value, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(user_id, data_key) DO UPDATE SET
           value = excluded.value,
           updated_at = excluded.updated_at`
      ).run(user, key, value, updatedAt)
      db.prepare(
        `INSERT INTO outbox (tick, user_id, data_key, value, updated_at, op)
         VALUES (?, ?, ?, ?, ?, 'put')`
      ).run(this.tick(), user, key, value, updatedAt)
      written.push(`${key}=${value}`)
    }
    this.setMeta('actor_node', node.id)
    this.setMeta('actor_user', user)
    const message = `在${node.name}写下 ${user} 的 ${written.join('、')}。数据留在这台节点，日志在中央确认前留在待上报。`
    this.event(this.tick(), 'write', message)
    return this.reply(message)
  }

  remove({ nodeId, userId, keys }) {
    const node = this.getNode(nodeId)
    this.assertWritable(node)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要删除的键')
    const db = this.nodeDb(node.id)
    const removed = []
    for (const raw of keys) {
      const key = String(raw || '').trim()
      if (!key) throw fail('每一份数据都要有名字')
      const existing = db.prepare('SELECT * FROM fragments WHERE user_id = ? AND data_key = ?').get(user, key)
      if (!existing) throw fail(`${node.name}上没有 ${user} 的 ${key}，不能在这里删除`)
      const updatedAt = this.issuedAt(node.id)
      db.prepare('DELETE FROM fragments WHERE user_id = ? AND data_key = ?').run(user, key)
      db.prepare('DELETE FROM index_entries WHERE user_id = ? AND data_key = ?').run(user, key)
      db.prepare(
        `INSERT INTO outbox (tick, user_id, data_key, value, updated_at, op)
         VALUES (?, ?, ?, '', ?, 'delete')`
      ).run(this.tick(), user, key, updatedAt)
      removed.push(key)
    }
    const message = `在${node.name}删除 ${user} 的 ${removed.join('、')}。删除日志在中央确认前留在待上报。`
    this.event(this.tick(), 'delete', message)
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
    if (node.status !== 'lost' && node.status !== 'joining') this.applyLoss(node.id, '数据库不可达')
    throw fail(`${node.name}的数据库不可达，已申请挂失`)
  }

  read({ nodeId, userId, keys }) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能向服务节点读取')
    if (node.status === 'lost') throw fail(`${node.name}已挂失，请求停在原地`)
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务`)
    this.guardNode(node)
    const user = String(userId || '').trim()
    if (!user) throw fail('先写上用户是谁')
    if (!Array.isArray(keys) || keys.length === 0) throw fail('先指定要读取的键')
    const results = keys.map((raw) => this.readOne(node, user, String(raw || '').trim()))
    this.setMeta('actor_node', node.id)
    this.setMeta('actor_user', user)
    const message = this.describeRead(node, results)
    this.event(this.tick(), results.some((item) => item.source === 'rejected') ? 'reject' : 'read', message)
    return this.reply(message, { results })
  }

  readOne(node, user, key) {
    if (!key) return { key, source: 'rejected', message: '没有键名' }
    const local = this.nodeDb(node.id)
      .prepare('SELECT * FROM fragments WHERE user_id = ? AND data_key = ?')
      .get(user, key)
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
    const index = this.nodeDb(node.id)
      .prepare('SELECT * FROM index_entries WHERE user_id = ? AND data_key = ?')
      .get(user, key)
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
      this.nodeDb(node.id).prepare('UPDATE index_entries SET ip = ? WHERE alias = ?').run(resolved.ip, index.alias)
      home = resolved
      via = 'alias'
    }
    const remote = this.nodeDb(home.id)
      .prepare('SELECT * FROM fragments WHERE user_id = ? AND data_key = ?')
      .get(user, key)
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
      this.applyLoss(node.id, '数据库不可达')
      return false
    }
    const period = Number(node.report_period)
    const last = Number(node.last_report)
    if (tick - last < period) return false
    const deadline = last + period + this.tolerance()
    if (!this.canReach(node.id)) {
      if (tick >= deadline) {
        const fails = Number(node.retries) + 1
        this.central.prepare('UPDATE nodes SET retries = ? WHERE id = ?').run(fails, node.id)
        if (fails >= 2) {
          try {
            this.forceLoss(node.id, '未按预期上报')
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
    const db = this.nodeDb(node.id)
    const rows = db.prepare('SELECT * FROM outbox ORDER BY updated_at, id').all()
    const accepted = []
    let applied = 0
    let filtered = 0
    for (const row of rows) {
      const decision = this.ingest(tick, { ...row, source: node.id })
      if (decision === 'applied') applied += 1
      if (decision === 'duplicate') filtered += 1
      if (decision === 'applied' || decision === 'duplicate') accepted.push(row.id)
    }
    if (accepted.length > 0) {
      db.prepare(`DELETE FROM outbox WHERE id IN (${accepted.map(() => '?').join(', ')})`).run(...accepted)
    }
    this.central.prepare('UPDATE nodes SET last_report = ?, retries = 0 WHERE id = ?').run(tick, node.id)
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
    const duplicate = this.central
      .prepare(
        `SELECT 1 AS ok FROM receipts
         WHERE source_node = ? AND user_id = ? AND data_key = ? AND updated_at = ? AND op = ?`
      )
      .get(row.source, row.user_id, row.data_key, row.updated_at, op)
    if (duplicate) return 'duplicate'
    this.central
      .prepare(
        `INSERT INTO receipts (source_node, user_id, data_key, updated_at, op)
         VALUES (?, ?, ?, ?, ?)`
      )
      .run(row.source, row.user_id, row.data_key, row.updated_at, op)
    this.central
      .prepare(
        `INSERT INTO logs (tick, user_id, data_key, value, updated_at, source_node, organized, op)
         VALUES (?, ?, ?, ?, ?, ?, 0, ?)`
      )
      .run(tick, row.user_id, row.data_key, row.value ?? '', row.updated_at, row.source, op)
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
    this.central
      .prepare(
        `INSERT INTO index_master (user_id, data_key, node_id, ip, alias, updated_at, suspended)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(user_id, data_key) DO UPDATE SET
           node_id = excluded.node_id,
           ip = excluded.ip,
           alias = excluded.alias,
           updated_at = excluded.updated_at,
           suspended = excluded.suspended
         WHERE excluded.updated_at >= index_master.updated_at`
      )
      .run(entry.user_id, entry.data_key, entry.node_id, entry.ip, entry.alias, entry.updated_at, entry.suspended)
    const current = this.central
      .prepare('SELECT node_id, updated_at FROM index_master WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    return Boolean(current && current.node_id === entry.node_id && current.updated_at === entry.updated_at)
  }

  deleteMaster(entry) {
    const current = this.central
      .prepare('SELECT updated_at FROM index_master WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    if (current && current.updated_at > entry.updated_at) return false
    this.central.prepare('DELETE FROM index_master WHERE user_id = ? AND data_key = ?').run(entry.user_id, entry.data_key)
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
    const existing = this.central
      .prepare('SELECT id, updated_at FROM buffers WHERE node_id = ? AND user_id = ? AND data_key = ?')
      .get(nodeId, entry.user_id, entry.data_key)
    if (existing && existing.updated_at > entry.updated_at) return
    if (existing) {
      this.central
        .prepare(
          `UPDATE buffers
           SET target_node = ?, ip = ?, alias = ?, updated_at = ?, suspended = ?, op = ?
           WHERE id = ?`
        )
        .run(entry.node_id, entry.ip, entry.alias, entry.updated_at, entry.suspended, entry.op || 'put', existing.id)
      return
    }
    this.central
      .prepare(
        `INSERT INTO buffers
           (node_id, user_id, data_key, target_node, ip, alias, updated_at, suspended, op)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        nodeId,
        entry.user_id,
        entry.data_key,
        entry.node_id,
        entry.ip,
        entry.alias,
        entry.updated_at,
        entry.suspended,
        entry.op || 'put'
      )
  }

  enqueuePending(nodeId, entry) {
    const existing = this.central
      .prepare('SELECT updated_at FROM pending_index WHERE node_id = ? AND user_id = ? AND data_key = ?')
      .get(nodeId, entry.user_id, entry.data_key)
    if (existing && existing.updated_at > entry.updated_at) return
    this.central
      .prepare(
        `INSERT INTO pending_index
           (node_id, user_id, data_key, target_node, ip, alias, updated_at, suspended, op)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(node_id, user_id, data_key) DO UPDATE SET
           target_node = excluded.target_node,
           ip = excluded.ip,
           alias = excluded.alias,
           updated_at = excluded.updated_at,
           suspended = excluded.suspended,
           op = excluded.op`
      )
      .run(
        nodeId,
        entry.user_id,
        entry.data_key,
        entry.node_id,
        entry.ip,
        entry.alias,
        entry.updated_at,
        entry.suspended,
        entry.op || 'put'
      )
  }

  deliverPending(nodeId, tick) {
    if (this.indexDeaf.has(nodeId) || this.dbBroken.has(nodeId) || !this.canReach(nodeId)) return 0
    const rows = this.central
      .prepare('SELECT * FROM pending_index WHERE node_id = ? ORDER BY updated_at')
      .all(nodeId)
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
    if (rows.length > 0) this.central.prepare('DELETE FROM pending_index WHERE node_id = ?').run(nodeId)
    return rows.length
  }

  applyIndex(nodeId, entry, tick) {
    const db = this.nodeDb(nodeId)
    const current = db
      .prepare('SELECT updated_at FROM index_entries WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    if (current && current.updated_at > entry.updated_at) return
    db.prepare(
      `INSERT INTO index_entries (user_id, data_key, node_id, ip, alias, updated_at, suspended)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(user_id, data_key) DO UPDATE SET
         node_id = excluded.node_id,
         ip = excluded.ip,
         alias = excluded.alias,
         updated_at = excluded.updated_at,
         suspended = excluded.suspended`
    ).run(entry.user_id, entry.data_key, entry.node_id, entry.ip, entry.alias, entry.updated_at, entry.suspended)
    if (entry.node_id === nodeId) return
    const fragment = db
      .prepare('SELECT updated_at, value FROM fragments WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    if (!fragment || entry.updated_at < fragment.updated_at) return
    db.prepare('DELETE FROM fragments WHERE user_id = ? AND data_key = ?').run(entry.user_id, entry.data_key)
    const node = this.getNode(nodeId)
    this.event(tick, 'drop', `${node.name}按时间去掉过时的 ${entry.data_key}（旧值 ${fragment.value}）。这份数据的地址改为 ${entry.ip}。`)
  }

  applyDelete(nodeId, entry, tick) {
    const db = this.nodeDb(nodeId)
    const current = db
      .prepare('SELECT updated_at FROM index_entries WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    if (!current || current.updated_at <= entry.updated_at) {
      db.prepare('DELETE FROM index_entries WHERE user_id = ? AND data_key = ?').run(entry.user_id, entry.data_key)
    }
    const fragment = db
      .prepare('SELECT updated_at, value FROM fragments WHERE user_id = ? AND data_key = ?')
      .get(entry.user_id, entry.data_key)
    if (!fragment || entry.updated_at < fragment.updated_at) return
    db.prepare('DELETE FROM fragments WHERE user_id = ? AND data_key = ?').run(entry.user_id, entry.data_key)
    const node = this.getNode(nodeId)
    this.event(tick, 'drop', `${node.name}按时间删掉 ${entry.data_key}（旧值 ${fragment.value}）。`)
  }

  organize(tick) {
    const pending = this.central.prepare('SELECT COUNT(*) AS n FROM logs').get().n
    if (pending === 0) {
      this.event(tick, 'dr', `第 ${tick} 拍到了灾备窗口，没有新的完整日志需要整理。`)
      return
    }
    const stored = this.foldLogs(tick)
    const kept = this.meta('retention') === '1' ? '另写了 log 文件。' : '用过的日志已从中央删除。'
    this.event(tick, 'dr', `第 ${tick} 拍，${pending} 条完整日志收成稳定数据，交给灾备 ${stored} 份。${kept}`)
  }

  foldLogs(tick) {
    const logs = this.central.prepare('SELECT * FROM logs ORDER BY updated_at, id').all()
    if (logs.length === 0) return 0
    const collapsed = new Map()
    for (const log of logs) {
      const id = `${log.user_id}\0${log.data_key}`
      const prev = collapsed.get(id)
      if (!prev || log.updated_at >= prev.updated_at) collapsed.set(id, log)
    }
    let stored = 0
    for (const log of collapsed.values()) {
      const home = this.central
        .prepare('SELECT node_id FROM index_master WHERE user_id = ? AND data_key = ?')
        .get(log.user_id, log.data_key)
      const homeId = home ? home.node_id : log.source_node
      for (const dr of this.nodes().filter((node) => node.role === 'dr')) {
        const covered = homeId && dr.covers.includes(homeId)
        const db = this.nodeDb(dr.id)
        if (!covered || log.op === 'delete') {
          if (!covered) {
            db.prepare('DELETE FROM records WHERE user_id = ? AND data_key = ?').run(log.user_id, log.data_key)
            continue
          }
          const existing = db
            .prepare('SELECT updated_at FROM records WHERE user_id = ? AND data_key = ?')
            .get(log.user_id, log.data_key)
          if (!existing || existing.updated_at <= log.updated_at) {
            db.prepare('DELETE FROM records WHERE user_id = ? AND data_key = ?').run(log.user_id, log.data_key)
          }
          continue
        }
        const existing = db
          .prepare('SELECT updated_at FROM records WHERE user_id = ? AND data_key = ?')
          .get(log.user_id, log.data_key)
        if (existing && existing.updated_at > log.updated_at) continue
        db.prepare(
          `INSERT INTO records (user_id, data_key, value, updated_at, home_node, organized_tick)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id, data_key) DO UPDATE SET
             value = excluded.value,
             updated_at = excluded.updated_at,
             home_node = excluded.home_node,
             organized_tick = excluded.organized_tick`
        ).run(log.user_id, log.data_key, log.value, log.updated_at, homeId, tick)
        stored += 1
      }
    }
    if (this.meta('retention') === '1') {
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
    const ids = logs.map((log) => log.id)
    this.central.prepare(`DELETE FROM logs WHERE id IN (${ids.map(() => '?').join(', ')})`).run(...ids)
    return stored
  }

  temporaryIndex(source) {
    const db = this.nodeDb(source.id)
    const table = new Map()
    for (const row of db.prepare('SELECT * FROM index_entries').all()) {
      table.set(`${row.user_id}\0${row.data_key}`, { ...row, alias: row.alias || row.node_id })
    }
    const own = db.prepare('SELECT * FROM fragments').all()
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
    const regionName = String(region || '').trim()
    if (!regionName) throw fail('新节点要落在一个区域')
    const source = this.lowestLoad()
    if (!source) throw fail('现在没有正在服务的节点可以提供索引')
    const seq = Number(this.meta('node_seq'))
    this.setMeta('node_seq', seq + 1)
    const pattern = this.layout || this.currentConfig()
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
    this.central
      .prepare(
        `INSERT INTO nodes
           (id, name, role, region, ip, alias, status, covers, report_period, last_report, retries)
         VALUES (?, ?, 'service', ?, ?, ?, 'joining', '[]', ?, ?, 0)`
      )
      .run(node.id, node.name, node.region, node.ip, node.alias, this.syncPeriod(), this.tick())
    const db = this.openNodeFile(node)
    const temporary = this.temporaryIndex(source)
    const insert = db.prepare(
      `INSERT INTO index_entries (user_id, data_key, node_id, ip, alias, updated_at, suspended)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    for (const row of temporary.rows) {
      insert.run(row.user_id, row.data_key, row.node_id, row.ip, row.alias || row.node_id, row.updated_at, row.suspended)
    }
    const message = `${node.name}已连上中央。索引从当前负载最低的${source.name}拉来，并临时补上它自己的 ${temporary.ownCount} 份数据，共 ${temporary.rows.length} 条。这张临时表不写回${source.name}。中央为它建了缓冲区，就绪前不投入服务。它还没有被灾备声明负责。`
    this.event(this.tick(), 'join', message)
    return this.reply(message, { nodeId: node.id })
  }

  ready(nodeId) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'joining') throw fail(`${node.name}不在对齐过程中`)
    const rows = this.central.prepare('SELECT * FROM buffers WHERE node_id = ? ORDER BY updated_at, id').all(nodeId)
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
    this.central.prepare('DELETE FROM buffers WHERE node_id = ?').run(nodeId)
    this.central.prepare(`UPDATE nodes SET status = 'up', last_report = ? WHERE id = ?`).run(this.tick(), nodeId)
    const message = `${node.name}已就绪，缓冲区里的 ${rows.length} 条索引已补上，开始服务。`
    this.event(this.tick(), 'ready', message)
    return this.reply(message)
  }

  setCovers(drId, nodeIds) {
    const dr = this.getNode(drId)
    if (dr.role !== 'dr') throw fail('只有灾备节点需要声明负责范围')
    const services = new Set(this.serviceNodes().map((node) => node.id))
    const covers = [...new Set(nodeIds.map((id) => String(id)))].filter((id) => services.has(id))
    this.central.prepare('UPDATE nodes SET covers = ? WHERE id = ?').run(JSON.stringify(covers), dr.id)
    const names = covers.map((id) => this.getNode(id).name)
    const message = names.length ? `${dr.name}现在负责${names.join('、')}` : `${dr.name}现在不负责任何服务节点`
    this.event(this.tick(), 'info', message)
    return this.reply(message)
  }

  lose(nodeId, { cause = '中央强制挂失' } = {}) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能挂失服务节点')
    if (node.status === 'lost') throw fail(`${node.name}已经挂失`)
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务，不用走挂失`)
    const job = this.mergeJob()
    if (job && (job.target_id === node.id || node.status === 'merging')) return this.rollbackMerge(node.id, cause)
    if (job) throw fail('已经有节点在融合，先完成这次再挂失下一台')
    const target = this.lowestLoad(this.serviceNodes().filter((item) => item.status === 'up' && item.id !== node.id))
    if (!target) throw fail('没有还能接替的服务节点')
    this.central.prepare(`UPDATE nodes SET status = 'lost' WHERE id = ?`).run(node.id)
    this.central.prepare(`UPDATE nodes SET status = 'merging' WHERE id = ?`).run(target.id)
    this.central
      .prepare('INSERT INTO merge_job (id, lost_id, target_id, started_tick) VALUES (1, ?, ?, ?)')
      .run(node.id, target.id, this.tick())
    this.suspendLost(node.id)
    let pending = 0
    if (!this.dbBroken.has(node.id)) {
      pending = this.nodeDb(node.id).prepare('SELECT COUNT(*) AS n FROM outbox').get().n
    }
    const load = this.fragmentCount(target.id)
    let message = `${cause}：${node.name}（${node.ip}）挂失。指向它的索引挂起，相关请求停住、不发出。真实数据将装入负载较低的${target.name}（当前碎片 ${load}），不按地理远近。装入完成前不接受针对这台丢失节点的请求。`
    if (pending > 0) message += `挂失时还有 ${pending} 条日志没上报，恢复只用已经到过中央或灾备的数据。`
    this.event(this.tick(), 'lose', message)
    return this.reply(message)
  }

  applyLoss(nodeId, cause = '节点申请挂失') {
    const node = this.getNode(nodeId)
    if (node.role !== 'service') throw fail('只能挂失服务节点')
    if (node.status === 'lost') return this.reply(`${node.name}已经挂失`)
    if (node.status === 'joining') throw fail(`${node.name}还没投入服务，不用走挂失`)
    return this.lose(nodeId, { cause })
  }

  forceLoss(nodeId, cause) {
    const node = this.getNode(nodeId)
    if (node.status === 'lost') return this.reply(`${node.name}已经挂失`)
    const job = this.mergeJob()
    if (node.status === 'merging' || (job && job.target_id === nodeId)) return this.rollbackMerge(nodeId, cause)
    return this.lose(nodeId, { cause })
  }

  suspendLost(lostId) {
    this.central.prepare('UPDATE index_master SET suspended = 1 WHERE node_id = ?').run(lostId)
    this.central.prepare('UPDATE buffers SET suspended = 1 WHERE target_node = ?').run(lostId)
    this.central.prepare('UPDATE pending_index SET suspended = 1 WHERE target_node = ?').run(lostId)
    for (const node of this.serviceNodes()) {
      if (node.status === 'lost' || node.id === lostId) continue
      if (!this.files.has(node.id) || this.dbBroken.has(node.id)) continue
      this.nodeDb(node.id).prepare('UPDATE index_entries SET suspended = 1 WHERE node_id = ?').run(lostId)
    }
  }

  recover(nodeId) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'lost') throw fail('只能对已经挂失的服务节点继续恢复')
    if (this.mergeJob()) throw fail('已经有节点在融合')
    const target = this.lowestLoad(this.serviceNodes().filter((item) => item.status === 'up' && item.id !== node.id))
    if (!target) throw fail('没有还能接替的服务节点')
    this.central.prepare(`UPDATE nodes SET status = 'merging' WHERE id = ?`).run(target.id)
    this.central
      .prepare('INSERT INTO merge_job (id, lost_id, target_id, started_tick) VALUES (1, ?, ?, ?)')
      .run(node.id, target.id, this.tick())
    const message = `继续把${node.name}的数据装入${target.name}。这是所有权转移。`
    this.event(this.tick(), 'lose', message)
    return this.reply(message)
  }

  rollbackMerge(targetId, cause) {
    const job = this.mergeJob()
    const target = this.getNode(targetId)
    if (target.status !== 'lost') this.central.prepare(`UPDATE nodes SET status = 'lost' WHERE id = ?`).run(targetId)
    if (job && job.target_id === targetId) this.central.prepare('DELETE FROM merge_job WHERE id = 1').run()
    this.suspendLost(targetId)
    const message = `立刻回退尚未完成的装入，并将${target.name}标记挂失。数据仍留在灾备。原因：${cause}。`
    this.event(this.tick(), 'lose', message)
    return this.reply(message)
  }

  finishMerge() {
    const job = this.mergeJob()
    if (!job) throw fail('现在没有正在融合的节点')
    const lost = this.getNode(job.lost_id)
    const target = this.getNode(job.target_id)
    let records = this.drRecordsFor(lost.id)
    const pendingLogs = this.central.prepare('SELECT COUNT(*) AS n FROM logs').get().n
    if (pendingLogs > 0) {
      this.foldLogs(this.tick())
      records = this.drRecordsFor(lost.id)
      this.event(this.tick(), 'dr', `灾备数据取到之后，中央还有 ${pendingLogs} 条日志，已再次更新灾备。`)
    }
    if (!this.canReach(target.id) || this.dbBroken.has(target.id)) {
      return this.rollbackMerge(target.id, '接替节点无法访问')
    }
    const unique = []
    const seen = new Set()
    for (const record of records) {
      const id = `${record.user_id}\0${record.data_key}`
      if (seen.has(id)) continue
      seen.add(id)
      unique.push(record)
    }
    const restore = this.takeSnapshot(unique)
    let keptLocal = 0
    let loaded = 0
    try {
      for (const record of unique) {
        if (!this.canReach(target.id) || this.dbBroken.has(target.id)) throw fail('接替节点无法访问')
        const local = this.nodeDb(target.id)
          .prepare('SELECT * FROM fragments WHERE user_id = ? AND data_key = ?')
          .get(record.user_id, record.data_key)
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
          this.nodeDb(target.id)
            .prepare(
              `INSERT INTO fragments (user_id, data_key, value, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, data_key) DO UPDATE SET
                 value = excluded.value,
                 updated_at = excluded.updated_at`
            )
            .run(record.user_id, record.data_key, record.value, record.updated_at)
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
      this.central.prepare(`UPDATE nodes SET status = 'up' WHERE id = ?`).run(target.id)
      this.central.prepare('DELETE FROM merge_job WHERE id = 1').run()
    } catch (error) {
      try {
        restore()
      } catch {
        // 回退数据失败时仍然把接替节点标成挂失，避免半套地址留在外面。
      }
      const rolled = this.rollbackMerge(target.id, error.message || '接替节点无法访问')
      return rolled
    }
    const still = this.central
      .prepare('SELECT COUNT(*) AS n FROM index_master WHERE node_id = ? AND suspended = 1')
      .get(lost.id).n
    let message = `灾备数据已按时间并入${target.name}：装入 ${loaded} 份，本机较新的 ${keptLocal} 份保留。原 ${lost.ip} 丢失，新地址为 ${target.ip}。各节点只更新了这些索引。`
    const uncovered = this.nodes().filter((node) => node.role === 'dr' && !node.covers.includes(target.id))
    if (records.length > 0 && uncovered.length > 0) {
      message += `${uncovered.map((node) => node.name).join('、')}没有声明负责${target.name}，丢失节点留在这些灾备上的对应记录已移出。`
    }
    if (still > 0) message += `还有 ${still} 条索引在灾备里没有对应数据，继续挂起。`
    this.event(this.tick(), 'merge', message)
    return this.reply(message)
  }

  takeSnapshot(pairs) {
    const fragments = []
    const indexes = []
    const masters = []
    const records = []
    for (const pair of pairs) {
      const { user_id, data_key } = pair
      for (const node of this.serviceNodes()) {
        if (!this.files.has(node.id) || this.dbBroken.has(node.id)) continue
        const db = this.nodeDb(node.id)
        fragments.push({
          nodeId: node.id,
          user_id,
          data_key,
          row: db.prepare('SELECT * FROM fragments WHERE user_id = ? AND data_key = ?').get(user_id, data_key) || null
        })
        indexes.push({
          nodeId: node.id,
          user_id,
          data_key,
          row: db.prepare('SELECT * FROM index_entries WHERE user_id = ? AND data_key = ?').get(user_id, data_key) || null
        })
      }
      masters.push({
        user_id,
        data_key,
        row:
          this.central.prepare('SELECT * FROM index_master WHERE user_id = ? AND data_key = ?').get(user_id, data_key) ||
          null
      })
      for (const dr of this.nodes().filter((node) => node.role === 'dr')) {
        records.push({
          drId: dr.id,
          user_id,
          data_key,
          row:
            this.nodeDb(dr.id).prepare('SELECT * FROM records WHERE user_id = ? AND data_key = ?').get(user_id, data_key) ||
            null
        })
      }
    }
    return () => {
      for (const item of fragments) this.restoreFragment(item)
      for (const item of indexes) this.restoreIndex(item)
      for (const item of masters) this.restoreMaster(item)
      for (const item of records) this.restoreRecord(item)
    }
  }

  restoreFragment(item) {
    const db = this.nodeDb(item.nodeId)
    if (!item.row) {
      db.prepare('DELETE FROM fragments WHERE user_id = ? AND data_key = ?').run(item.user_id, item.data_key)
      return
    }
    db.prepare(
      `INSERT INTO fragments (user_id, data_key, value, updated_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(user_id, data_key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`
    ).run(item.row.user_id, item.row.data_key, item.row.value, item.row.updated_at)
  }

  restoreIndex(item) {
    const db = this.nodeDb(item.nodeId)
    if (!item.row) {
      db.prepare('DELETE FROM index_entries WHERE user_id = ? AND data_key = ?').run(item.user_id, item.data_key)
      return
    }
    db.prepare(
      `INSERT INTO index_entries (user_id, data_key, node_id, ip, alias, updated_at, suspended)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(user_id, data_key) DO UPDATE SET
         node_id = excluded.node_id,
         ip = excluded.ip,
         alias = excluded.alias,
         updated_at = excluded.updated_at,
         suspended = excluded.suspended`
    ).run(
      item.row.user_id,
      item.row.data_key,
      item.row.node_id,
      item.row.ip,
      item.row.alias,
      item.row.updated_at,
      item.row.suspended
    )
  }

  restoreMaster(item) {
    if (!item.row) {
      this.central.prepare('DELETE FROM index_master WHERE user_id = ? AND data_key = ?').run(item.user_id, item.data_key)
      return
    }
    this.central
      .prepare(
        `INSERT INTO index_master (user_id, data_key, node_id, ip, alias, updated_at, suspended)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(user_id, data_key) DO UPDATE SET
           node_id = excluded.node_id,
           ip = excluded.ip,
           alias = excluded.alias,
           updated_at = excluded.updated_at,
           suspended = excluded.suspended`
      )
      .run(
        item.row.user_id,
        item.row.data_key,
        item.row.node_id,
        item.row.ip,
        item.row.alias,
        item.row.updated_at,
        item.row.suspended
      )
  }

  restoreRecord(item) {
    const db = this.nodeDb(item.drId)
    if (!item.row) {
      db.prepare('DELETE FROM records WHERE user_id = ? AND data_key = ?').run(item.user_id, item.data_key)
      return
    }
    db.prepare(
      `INSERT INTO records (user_id, data_key, value, updated_at, home_node, organized_tick)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(user_id, data_key) DO UPDATE SET
         value = excluded.value,
         updated_at = excluded.updated_at,
         home_node = excluded.home_node,
         organized_tick = excluded.organized_tick`
    ).run(
      item.row.user_id,
      item.row.data_key,
      item.row.value,
      item.row.updated_at,
      item.row.home_node,
      item.row.organized_tick
    )
  }

  retargetDrRecord(lostId, targetId, winning) {
    for (const dr of this.nodes().filter((node) => node.role === 'dr')) {
      const db = this.nodeDb(dr.id)
      if (dr.covers.includes(targetId)) {
        db.prepare(
          `UPDATE records
           SET home_node = ?, value = ?, updated_at = ?, organized_tick = ?
           WHERE user_id = ? AND data_key = ? AND home_node = ?`
        ).run(targetId, winning.value, winning.updated_at, this.tick(), winning.user_id, winning.data_key, lostId)
        continue
      }
      db.prepare('DELETE FROM records WHERE user_id = ? AND data_key = ? AND home_node = ?').run(
        winning.user_id,
        winning.data_key,
        lostId
      )
    }
  }

  drRecordsFor(homeId) {
    const records = []
    for (const dr of this.nodes().filter((node) => node.role === 'dr' && node.covers.includes(homeId))) {
      records.push(...this.nodeDb(dr.id).prepare('SELECT * FROM records WHERE home_node = ?').all(homeId))
    }
    records.sort((a, b) => {
      if (a.updated_at === b.updated_at) return b.organized_tick - a.organized_tick
      return a.updated_at < b.updated_at ? 1 : -1
    })
    return records
  }

  rejoin(nodeId, { region, name } = {}) {
    const node = this.getNode(nodeId)
    if (node.role !== 'service' || node.status !== 'lost') throw fail('只有挂失节点回来时才清空并作为新节点')
    const job = this.mergeJob()
    if (job && (job.lost_id === nodeId || job.target_id === nodeId)) throw fail('融合尚未结束，先处理完这次装入')
    this.dbBroken.delete(nodeId)
    this.unreachable.delete(nodeId)
    this.indexDeaf.delete(nodeId)
    if (!this.files.has(nodeId)) this.openNodeFile(node)
    this.nodeDb(nodeId).exec('DELETE FROM fragments; DELETE FROM index_entries; DELETE FROM outbox;')
    return this.join({ region: region || node.region, name: name || `${node.region}新节点` })
  }

  changeIp(nodeId, ip) {
    const node = this.getNode(nodeId)
    const next = String(ip || '').trim()
    if (!next) throw fail('要写上新的地址')
    this.central.prepare('UPDATE nodes SET ip = ? WHERE id = ?').run(next, node.id)
    this.central.prepare('UPDATE index_master SET ip = ? WHERE node_id = ?').run(next, node.id)
    this.central.prepare('UPDATE buffers SET ip = ? WHERE target_node = ?').run(next, node.id)
    this.central.prepare('UPDATE pending_index SET ip = ? WHERE target_node = ?').run(next, node.id)
    return this.reply(`${node.name}的地址改为 ${next}。别名仍是 ${node.alias}。各节点继续用旧地址，不通时再向中央要一次。`)
  }

  getState() {
    const tick = this.tick()
    const syncPeriod = this.syncPeriod()
    const drPeriod = this.drPeriod()
    const nodes = this.nodes().map((node) => this.presentNode(node))
    const job = this.mergeJob()
    const events = this.central
      .prepare('SELECT id, tick, kind, message FROM events ORDER BY id DESC LIMIT 60')
      .all()
      .reverse()
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
      indexMaster: this.central.prepare('SELECT * FROM index_master ORDER BY user_id, data_key').all(),
      logs: this.central
        .prepare(
          `SELECT id, tick, user_id, data_key, value, updated_at, source_node, op
           FROM logs ORDER BY id DESC LIMIT 12`
        )
        .all()
        .reverse(),
      logCount: this.central.prepare('SELECT COUNT(*) AS n FROM logs').get().n,
      unorganizedLogCount: this.central.prepare('SELECT COUNT(*) AS n FROM logs').get().n,
      mergeJob: job ? { lostId: job.lost_id, targetId: job.target_id, startedTick: job.started_tick } : null,
      events
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
      const db = this.nodeDb(node.id)
      return {
        ...base,
        dbBroken: false,
        fragmentCount: db.prepare('SELECT COUNT(*) AS n FROM fragments').get().n,
        outbox: db.prepare('SELECT id, user_id, data_key, value, updated_at, op FROM outbox ORDER BY id').all(),
        fragments: db
          .prepare('SELECT user_id, data_key, value, updated_at FROM fragments ORDER BY user_id, data_key')
          .all(),
        indexEntries: db
          .prepare(
            'SELECT user_id, data_key, node_id, ip, alias, updated_at, suspended FROM index_entries ORDER BY user_id, data_key'
          )
          .all(),
        buffer: this.central
          .prepare(
            `SELECT user_id, data_key, target_node, ip, alias, updated_at, suspended, op
             FROM buffers WHERE node_id = ? ORDER BY updated_at`
          )
          .all(node.id)
      }
    }
    if (node.role === 'dr') {
      return {
        ...base,
        records: this.nodeDb(node.id)
          .prepare(
            `SELECT user_id, data_key, value, updated_at, home_node, organized_tick
             FROM records ORDER BY user_id, data_key`
          )
          .all()
      }
    }
    return base
  }
}
