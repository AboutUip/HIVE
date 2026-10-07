import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'

function closed() {
  const error = new Error('库已关闭')
  error.expose = true
  return error
}

function copy(row) {
  return row ? { ...row } : null
}

function copies(list) {
  return list.map((row) => ({ ...row }))
}

function mapNode(row) {
  if (!row) return null
  return { ...row, covers: JSON.parse(row.covers || '[]') }
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT NOT NULL,
  region TEXT NOT NULL,
  ip TEXT NOT NULL,
  alias TEXT NOT NULL,
  status TEXT NOT NULL,
  covers TEXT NOT NULL DEFAULT '[]',
  report_period INTEGER NOT NULL DEFAULT 2,
  last_report INTEGER NOT NULL DEFAULT 0,
  retries INTEGER NOT NULL DEFAULT 0,
  window_notice INTEGER
);
CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tick INTEGER NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  source_node TEXT NOT NULL,
  organized INTEGER NOT NULL DEFAULT 0,
  op TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
  source_node TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  op TEXT NOT NULL,
  PRIMARY KEY (source_node, user_id, data_key, updated_at, op)
);
CREATE TABLE IF NOT EXISTS index_master (
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  node_id TEXT NOT NULL,
  ip TEXT NOT NULL,
  alias TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  suspended INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, data_key)
);
CREATE TABLE IF NOT EXISTS buffers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  node_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  target_node TEXT NOT NULL,
  ip TEXT NOT NULL,
  alias TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  suspended INTEGER NOT NULL DEFAULT 0,
  op TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_index (
  node_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  target_node TEXT NOT NULL,
  ip TEXT NOT NULL,
  alias TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  suspended INTEGER NOT NULL DEFAULT 0,
  op TEXT NOT NULL,
  PRIMARY KEY (node_id, user_id, data_key)
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tick INTEGER NOT NULL,
  kind TEXT NOT NULL,
  message TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS merge_job (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  lost_id TEXT NOT NULL,
  target_id TEXT NOT NULL,
  started_tick INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS fragments (
  holder_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (holder_id, user_id, data_key)
);
CREATE TABLE IF NOT EXISTS index_entries (
  holder_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  node_id TEXT NOT NULL,
  ip TEXT NOT NULL,
  alias TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  suspended INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (holder_id, user_id, data_key)
);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  holder_id TEXT NOT NULL,
  tick INTEGER NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  op TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
  holder_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  data_key TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  home_node TEXT NOT NULL,
  organized_tick INTEGER NOT NULL,
  PRIMARY KEY (holder_id, user_id, data_key)
);
`

export class SqliteStore {
  constructor(dataDir) {
    this.dataDir = dataDir
    this.file = path.join(dataDir, 'hive.db')
    this.db = null
    this.depth = 0
  }

  open() {
    if (this.db) return
    fs.mkdirSync(this.dataDir, { recursive: true })
    this.db = new DatabaseSync(this.file)
    this.db.exec('PRAGMA journal_mode = WAL')
    this.db.exec('PRAGMA synchronous = FULL')
    this.db.exec('PRAGMA foreign_keys = ON')
    this.db.exec('PRAGMA busy_timeout = 5000')
    this.db.exec(SCHEMA)
    this.depth = 0
  }

  close() {
    if (!this.db) return
    if (this.depth > 0) {
      try {
        this.db.exec('ROLLBACK')
      } catch {
        // 连接已经不可用时，关闭本身仍要完成。
      }
      this.depth = 0
    }
    this.db.close()
    this.db = null
  }

  isOpen() {
    return Boolean(this.db)
  }

  reset() {
    this.close()
    fs.mkdirSync(this.dataDir, { recursive: true })
    fs.rmSync(this.dataDir, { recursive: true, force: true })
    fs.mkdirSync(this.dataDir, { recursive: true })
    this.open()
    this.transaction(() => this.clearAll())
  }

  hasLayout() {
    return this.getMeta('tick') !== null
  }

  inTransaction() {
    return this.depth > 0
  }

  transaction(fn) {
    this.require()
    this.depth += 1
    const nested = this.depth > 1
    const save = `hive_${this.depth}`
    try {
      this.db.exec(nested ? `SAVEPOINT ${save}` : 'BEGIN IMMEDIATE')
      const result = fn()
      this.db.exec(nested ? `RELEASE ${save}` : 'COMMIT')
      return result
    } catch (error) {
      try {
        if (nested) {
          this.db.exec(`ROLLBACK TO ${save}`)
          this.db.exec(`RELEASE ${save}`)
        } else {
          this.db.exec('ROLLBACK')
        }
      } catch {
        // BEGIN 本身失败时，没有可以回退的事务。
      }
      throw error
    } finally {
      this.depth -= 1
    }
  }

  require() {
    if (!this.db) throw closed()
  }

  getMeta(key) {
    this.require()
    const row = this.db.prepare('SELECT value FROM meta WHERE key = ?').get(key)
    return row ? row.value : null
  }

  setMeta(key, value) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO meta (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`
      )
      .run(key, String(value))
  }

  addEvent({ tick, kind, message }) {
    this.require()
    this.db.prepare('INSERT INTO events (tick, kind, message) VALUES (?, ?, ?)').run(tick, kind, message)
  }

  listEvents(limit) {
    this.require()
    const rows = this.db
      .prepare('SELECT id, tick, kind, message FROM events ORDER BY id DESC LIMIT ?')
      .all(limit)
    return copies(rows).reverse()
  }

  insertNode(node) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO nodes
           (id, name, role, region, ip, alias, status, covers, report_period, last_report, retries, window_notice)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        node.id,
        node.name,
        node.role,
        node.region,
        node.ip,
        node.alias,
        node.status,
        JSON.stringify(node.covers || []),
        node.report_period,
        node.last_report,
        node.retries,
        node.window_notice ?? null
      )
  }

  updateNode(id, patch) {
    this.require()
    const sets = []
    const params = []
    for (const key of ['name', 'role', 'region', 'ip', 'alias', 'status', 'report_period', 'last_report', 'retries', 'window_notice']) {
      if (patch[key] !== undefined) {
        sets.push(`${key} = ?`)
        params.push(patch[key])
      }
    }
    if (patch.covers !== undefined) {
      sets.push('covers = ?')
      params.push(JSON.stringify(patch.covers))
    }
    if (sets.length === 0) return
    params.push(id)
    this.db.prepare(`UPDATE nodes SET ${sets.join(', ')} WHERE id = ?`).run(...params)
  }

  getNode(id) {
    this.require()
    return mapNode(this.db.prepare('SELECT * FROM nodes WHERE id = ?').get(id))
  }

  listNodes() {
    this.require()
    return this.db
      .prepare('SELECT * FROM nodes ORDER BY id')
      .all()
      .map((row) => mapNode(row))
  }

  insertLog(log) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO logs (tick, user_id, data_key, value, updated_at, source_node, organized, op)
         VALUES (?, ?, ?, ?, ?, ?, 0, ?)`
      )
      .run(log.tick, log.user_id, log.data_key, log.value ?? '', log.updated_at, log.source_node, log.op)
  }

  listLogs() {
    this.require()
    return copies(this.db.prepare('SELECT * FROM logs ORDER BY updated_at, id').all())
  }

  listRecentLogs(limit) {
    this.require()
    const rows = this.db
      .prepare(
        `SELECT id, tick, user_id, data_key, value, updated_at, source_node, op
         FROM logs ORDER BY id DESC LIMIT ?`
      )
      .all(limit)
    return copies(rows).reverse()
  }

  countLogs() {
    this.require()
    return this.db.prepare('SELECT COUNT(*) AS n FROM logs').get().n
  }

  deleteLogs(ids) {
    this.require()
    const statement = this.db.prepare('DELETE FROM logs WHERE id = ?')
    for (const id of ids) statement.run(id)
  }

  hasReceipt(receipt) {
    this.require()
    const row = this.db
      .prepare(
        `SELECT 1 AS ok FROM receipts
         WHERE source_node = ? AND user_id = ? AND data_key = ? AND updated_at = ? AND op = ?`
      )
      .get(receipt.source_node, receipt.user_id, receipt.data_key, receipt.updated_at, receipt.op)
    return Boolean(row)
  }

  insertReceipt(receipt) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO receipts (source_node, user_id, data_key, updated_at, op)
         VALUES (?, ?, ?, ?, ?)`
      )
      .run(receipt.source_node, receipt.user_id, receipt.data_key, receipt.updated_at, receipt.op)
  }

  getMaster(userId, key) {
    this.require()
    return copy(
      this.db.prepare('SELECT * FROM index_master WHERE user_id = ? AND data_key = ?').get(userId, key)
    )
  }

  putMaster(entry) {
    this.require()
    this.db
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
      .run(entry.user_id, entry.data_key, entry.node_id, entry.ip, entry.alias, entry.updated_at, entry.suspended)
  }

  deleteMaster(userId, key) {
    this.require()
    this.db.prepare('DELETE FROM index_master WHERE user_id = ? AND data_key = ?').run(userId, key)
  }

  listMasters() {
    this.require()
    return copies(this.db.prepare('SELECT * FROM index_master ORDER BY user_id, data_key').all())
  }

  suspendMasters(nodeId) {
    this.require()
    this.db.prepare('UPDATE index_master SET suspended = 1 WHERE node_id = ?').run(nodeId)
  }

  updateMasterIp(nodeId, ip) {
    this.require()
    this.db.prepare('UPDATE index_master SET ip = ? WHERE node_id = ?').run(ip, nodeId)
  }

  countSuspendedMasters(nodeId) {
    this.require()
    return this.db
      .prepare('SELECT COUNT(*) AS n FROM index_master WHERE node_id = ? AND suspended = 1')
      .get(nodeId).n
  }

  getBuffer(nodeId, userId, key) {
    this.require()
    return copy(
      this.db
        .prepare('SELECT * FROM buffers WHERE node_id = ? AND user_id = ? AND data_key = ?')
        .get(nodeId, userId, key)
    )
  }

  insertBuffer(entry) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO buffers
           (node_id, user_id, data_key, target_node, ip, alias, updated_at, suspended, op)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        entry.node_id,
        entry.user_id,
        entry.data_key,
        entry.target_node,
        entry.ip,
        entry.alias,
        entry.updated_at,
        entry.suspended,
        entry.op
      )
  }

  updateBuffer(id, entry) {
    this.require()
    this.db
      .prepare(
        `UPDATE buffers
         SET target_node = ?, ip = ?, alias = ?, updated_at = ?, suspended = ?, op = ?
         WHERE id = ?`
      )
      .run(entry.target_node, entry.ip, entry.alias, entry.updated_at, entry.suspended, entry.op, id)
  }

  listBuffers(nodeId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT user_id, data_key, target_node, ip, alias, updated_at, suspended, op
           FROM buffers WHERE node_id = ? ORDER BY updated_at, id`
        )
        .all(nodeId)
    )
  }

  deleteBuffers(nodeId) {
    this.require()
    this.db.prepare('DELETE FROM buffers WHERE node_id = ?').run(nodeId)
  }

  suspendBuffers(targetNodeId) {
    this.require()
    this.db.prepare('UPDATE buffers SET suspended = 1 WHERE target_node = ?').run(targetNodeId)
  }

  updateBufferIp(targetNodeId, ip) {
    this.require()
    this.db.prepare('UPDATE buffers SET ip = ? WHERE target_node = ?').run(ip, targetNodeId)
  }

  getPending(nodeId, userId, key) {
    this.require()
    return copy(
      this.db
        .prepare('SELECT * FROM pending_index WHERE node_id = ? AND user_id = ? AND data_key = ?')
        .get(nodeId, userId, key)
    )
  }

  putPending(entry) {
    this.require()
    this.db
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
        entry.node_id,
        entry.user_id,
        entry.data_key,
        entry.target_node,
        entry.ip,
        entry.alias,
        entry.updated_at,
        entry.suspended,
        entry.op
      )
  }

  listPending(nodeId) {
    this.require()
    return copies(
      this.db.prepare('SELECT * FROM pending_index WHERE node_id = ? ORDER BY updated_at').all(nodeId)
    )
  }

  deletePending(nodeId) {
    this.require()
    this.db.prepare('DELETE FROM pending_index WHERE node_id = ?').run(nodeId)
  }

  suspendPending(targetNodeId) {
    this.require()
    this.db.prepare('UPDATE pending_index SET suspended = 1 WHERE target_node = ?').run(targetNodeId)
  }

  updatePendingIp(targetNodeId, ip) {
    this.require()
    this.db.prepare('UPDATE pending_index SET ip = ? WHERE target_node = ?').run(ip, targetNodeId)
  }

  getMergeJob() {
    this.require()
    return copy(this.db.prepare('SELECT * FROM merge_job WHERE id = 1').get())
  }

  insertMergeJob(job) {
    this.require()
    this.db
      .prepare('INSERT INTO merge_job (id, lost_id, target_id, started_tick) VALUES (1, ?, ?, ?)')
      .run(job.lost_id, job.target_id, job.started_tick)
  }

  deleteMergeJob() {
    this.require()
    this.db.prepare('DELETE FROM merge_job WHERE id = 1').run()
  }

  getFragment(holderId, userId, key) {
    this.require()
    return copy(
      this.db
        .prepare('SELECT user_id, data_key, value, updated_at FROM fragments WHERE holder_id = ? AND user_id = ? AND data_key = ?')
        .get(holderId, userId, key)
    )
  }

  putFragment(holderId, row) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO fragments (holder_id, user_id, data_key, value, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(holder_id, user_id, data_key) DO UPDATE SET
           value = excluded.value,
           updated_at = excluded.updated_at`
      )
      .run(holderId, row.user_id, row.data_key, row.value, row.updated_at)
  }

  deleteFragment(holderId, userId, key) {
    this.require()
    this.db.prepare('DELETE FROM fragments WHERE holder_id = ? AND user_id = ? AND data_key = ?').run(holderId, userId, key)
  }

  listFragments(holderId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT user_id, data_key, value, updated_at FROM fragments
           WHERE holder_id = ? ORDER BY user_id, data_key`
        )
        .all(holderId)
    )
  }

  countFragments(holderId) {
    this.require()
    return this.db.prepare('SELECT COUNT(*) AS n FROM fragments WHERE holder_id = ?').get(holderId).n
  }

  getIndex(holderId, userId, key) {
    this.require()
    return copy(
      this.db
        .prepare(
          `SELECT user_id, data_key, node_id, ip, alias, updated_at, suspended
           FROM index_entries WHERE holder_id = ? AND user_id = ? AND data_key = ?`
        )
        .get(holderId, userId, key)
    )
  }

  putIndex(holderId, entry) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO index_entries
           (holder_id, user_id, data_key, node_id, ip, alias, updated_at, suspended)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(holder_id, user_id, data_key) DO UPDATE SET
           node_id = excluded.node_id,
           ip = excluded.ip,
           alias = excluded.alias,
           updated_at = excluded.updated_at,
           suspended = excluded.suspended`
      )
      .run(
        holderId,
        entry.user_id,
        entry.data_key,
        entry.node_id,
        entry.ip,
        entry.alias,
        entry.updated_at,
        entry.suspended
      )
  }

  deleteIndex(holderId, userId, key) {
    this.require()
    this.db
      .prepare('DELETE FROM index_entries WHERE holder_id = ? AND user_id = ? AND data_key = ?')
      .run(holderId, userId, key)
  }

  listIndexes(holderId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT user_id, data_key, node_id, ip, alias, updated_at, suspended
           FROM index_entries WHERE holder_id = ? ORDER BY user_id, data_key`
        )
        .all(holderId)
    )
  }

  updateIndexIpByAlias(holderId, alias, ip) {
    this.require()
    this.db.prepare('UPDATE index_entries SET ip = ? WHERE holder_id = ? AND alias = ?').run(ip, holderId, alias)
  }

  suspendIndexes(holderId, targetNodeId) {
    this.require()
    this.db
      .prepare('UPDATE index_entries SET suspended = 1 WHERE holder_id = ? AND node_id = ?')
      .run(holderId, targetNodeId)
  }

  listOutbox(holderId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT id, user_id, data_key, value, updated_at, op, tick
           FROM outbox WHERE holder_id = ? ORDER BY updated_at, id`
        )
        .all(holderId)
    )
  }

  insertOutbox(holderId, row) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO outbox (holder_id, tick, user_id, data_key, value, updated_at, op)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
      .run(holderId, row.tick, row.user_id, row.data_key, row.value, row.updated_at, row.op)
  }

  deleteOutbox(holderId, ids) {
    this.require()
    const statement = this.db.prepare('DELETE FROM outbox WHERE holder_id = ? AND id = ?')
    for (const id of ids) statement.run(holderId, id)
  }

  countOutbox(holderId) {
    this.require()
    return this.db.prepare('SELECT COUNT(*) AS n FROM outbox WHERE holder_id = ?').get(holderId).n
  }

  getRecord(holderId, userId, key) {
    this.require()
    return copy(
      this.db
        .prepare(
          `SELECT user_id, data_key, value, updated_at, home_node, organized_tick
           FROM records WHERE holder_id = ? AND user_id = ? AND data_key = ?`
        )
        .get(holderId, userId, key)
    )
  }

  putRecord(holderId, row) {
    this.require()
    this.db
      .prepare(
        `INSERT INTO records
           (holder_id, user_id, data_key, value, updated_at, home_node, organized_tick)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(holder_id, user_id, data_key) DO UPDATE SET
           value = excluded.value,
           updated_at = excluded.updated_at,
           home_node = excluded.home_node,
           organized_tick = excluded.organized_tick`
      )
      .run(holderId, row.user_id, row.data_key, row.value, row.updated_at, row.home_node, row.organized_tick)
  }

  deleteRecord(holderId, userId, key) {
    this.require()
    this.db.prepare('DELETE FROM records WHERE holder_id = ? AND user_id = ? AND data_key = ?').run(holderId, userId, key)
  }

  listRecords(holderId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT user_id, data_key, value, updated_at, home_node, organized_tick
           FROM records WHERE holder_id = ? ORDER BY user_id, data_key`
        )
        .all(holderId)
    )
  }

  listRecordsByHome(holderId, homeId) {
    this.require()
    return copies(
      this.db
        .prepare(
          `SELECT user_id, data_key, value, updated_at, home_node, organized_tick
           FROM records WHERE holder_id = ? AND home_node = ?`
        )
        .all(holderId, homeId)
    )
  }

  updateRecordHome(holderId, row) {
    this.require()
    this.db
      .prepare(
        `UPDATE records
         SET home_node = ?, value = ?, updated_at = ?, organized_tick = ?
         WHERE holder_id = ? AND user_id = ? AND data_key = ? AND home_node = ?`
      )
      .run(
        row.home_node,
        row.value,
        row.updated_at,
        row.organized_tick,
        holderId,
        row.user_id,
        row.data_key,
        row.from_home
      )
  }

  deleteRecordHome(holderId, userId, key, homeId) {
    this.require()
    this.db
      .prepare('DELETE FROM records WHERE holder_id = ? AND user_id = ? AND data_key = ? AND home_node = ?')
      .run(holderId, userId, key, homeId)
  }

  latestStamp(holderId) {
    this.require()
    const row = this.db
      .prepare(
        `SELECT MAX(updated_at) AS at FROM (
           SELECT updated_at FROM fragments WHERE holder_id = ?
           UNION ALL
           SELECT updated_at FROM outbox WHERE holder_id = ?
         )`
      )
      .get(holderId, holderId)
    return row?.at || null
  }

  clearHolder(holderId) {
    this.require()
    this.db.prepare('DELETE FROM fragments WHERE holder_id = ?').run(holderId)
    this.db.prepare('DELETE FROM index_entries WHERE holder_id = ?').run(holderId)
    this.db.prepare('DELETE FROM outbox WHERE holder_id = ?').run(holderId)
  }

  clearAll() {
    this.require()
    this.db.exec(`
      DELETE FROM logs;
      DELETE FROM receipts;
      DELETE FROM index_master;
      DELETE FROM buffers;
      DELETE FROM pending_index;
      DELETE FROM events;
      DELETE FROM merge_job;
      DELETE FROM nodes;
      DELETE FROM meta;
      DELETE FROM fragments;
      DELETE FROM index_entries;
      DELETE FROM outbox;
      DELETE FROM records;
    `)
  }
}
