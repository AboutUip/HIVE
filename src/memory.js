function closed() {
  const error = new Error('库已关闭')
  error.expose = true
  return error
}

function blank() {
  return {
    meta: {},
    events: [],
    eventSeq: 0,
    nodes: {},
    logs: [],
    logSeq: 0,
    receipts: {},
    masters: {},
    buffers: [],
    bufferSeq: 0,
    pending: {},
    merge: null,
    fragments: {},
    indexes: {},
    outbox: [],
    outboxSeq: 0,
    records: {}
  }
}

function copy(row) {
  return row ? { ...row } : null
}

function byText(a, b) {
  if (a < b) return -1
  if (a > b) return 1
  return 0
}

function receiptKey(row) {
  return [row.source_node, row.user_id, row.data_key, row.updated_at, row.op].join('\0')
}

function pairKey(userId, key) {
  return `${userId}\0${key}`
}

function heldKey(holderId, userId, key) {
  return `${holderId}\0${userId}\0${key}`
}

function pendingKey(nodeId, userId, key) {
  return `${nodeId}\0${userId}\0${key}`
}

export class MemoryStore {
  constructor() {
    this.data = blank()
    this.depth = 0
    this.openFlag = false
    this.undoStack = []
    this.activeUndo = null
    this.changed = new Set()
  }

  open() {
    this.openFlag = true
  }

  close() {
    this.openFlag = false
    this.depth = 0
    this.undoStack = []
    this.activeUndo = null
    this.changed = new Set()
  }

  isOpen() {
    return this.openFlag
  }

  reset() {
    this.data = blank()
    this.depth = 0
    this.undoStack = []
    this.activeUndo = null
    this.changed = new Set()
    this.openFlag = true
  }

  hasLayout() {
    this.require()
    return Object.prototype.hasOwnProperty.call(this.data.meta, 'tick')
  }

  inTransaction() {
    return this.depth > 0
  }

  mark(table) {
    if (!this.activeUndo) return
    this.changed.add(table)
    if (!this.activeUndo.has(table)) this.activeUndo.set(table, structuredClone(this.data[table]))
  }

  changedTables() {
    return [...this.changed]
  }

  countersSnapshot() {
    return {
      eventSeq: this.data.eventSeq,
      logSeq: this.data.logSeq,
      bufferSeq: this.data.bufferSeq,
      outboxSeq: this.data.outboxSeq
    }
  }

  restoreCounters(snapshot) {
    this.data.eventSeq = snapshot.eventSeq
    this.data.logSeq = snapshot.logSeq
    this.data.bufferSeq = snapshot.bufferSeq
    this.data.outboxSeq = snapshot.outboxSeq
  }

  transaction(fn) {
    this.require()
    this.depth += 1
    this.undoStack.push(new Map())
    this.activeUndo = this.undoStack.at(-1)
    try {
      const result = fn()
      this.depth -= 1
      this.undoStack.pop()
      this.activeUndo = this.undoStack.at(-1) || null
      return result
    } catch (error) {
      const undo = this.undoStack.at(-1)
      for (const [table, snapshot] of undo) this.data[table] = snapshot
      this.depth -= 1
      this.undoStack.pop()
      this.activeUndo = this.undoStack.at(-1) || null
      if (this.depth === 0) this.changed = new Set()
      throw error
    }
  }

  clearChanged() {
    this.changed = new Set()
  }

  require() {
    if (!this.openFlag) throw closed()
  }

  getMeta(key) {
    this.require()
    return Object.prototype.hasOwnProperty.call(this.data.meta, key) ? this.data.meta[key] : null
  }

  setMeta(key, value) {
    this.require()
    this.mark('meta')
    this.data.meta[key] = String(value)
  }

  addEvent({ tick, kind, message }) {
    this.require()
    this.mark('events')
    this.data.eventSeq += 1
    this.data.events.push({ id: this.data.eventSeq, tick, kind, message })
  }

  listEvents(limit) {
    this.require()
    return this.data.events.slice(-limit).map((row) => ({ ...row }))
  }

  insertNode(node) {
    this.require()
    this.mark('nodes')
    this.data.nodes[node.id] = {
      id: node.id,
      name: node.name,
      role: node.role,
      region: node.region,
      ip: node.ip,
      alias: node.alias,
      status: node.status,
      covers: [...(node.covers || [])],
      report_period: node.report_period,
      last_report: node.last_report,
      retries: node.retries,
      window_notice: node.window_notice ?? null
    }
  }

  updateNode(id, patch) {
    this.require()
    this.mark('nodes')
    const node = this.data.nodes[id]
    if (!node) return
    for (const key of ['name', 'role', 'region', 'ip', 'alias', 'status', 'report_period', 'last_report', 'retries', 'window_notice']) {
      if (patch[key] !== undefined) node[key] = patch[key]
    }
    if (patch.covers !== undefined) node.covers = [...patch.covers]
  }

  getNode(id) {
    this.require()
    const node = this.data.nodes[id]
    return node ? { ...node, covers: [...node.covers] } : null
  }

  listNodes() {
    this.require()
    return Object.values(this.data.nodes)
      .sort((a, b) => byText(a.id, b.id))
      .map((node) => ({ ...node, covers: [...node.covers] }))
  }

  insertLog(log) {
    this.require()
    this.mark('logs')
    this.data.logSeq += 1
    this.data.logs.push({
      id: this.data.logSeq,
      tick: log.tick,
      user_id: log.user_id,
      data_key: log.data_key,
      value: log.value ?? '',
      updated_at: log.updated_at,
      source_node: log.source_node,
      organized: 0,
      op: log.op
    })
  }

  listLogs() {
    this.require()
    return [...this.data.logs]
      .sort((a, b) => byText(a.updated_at, b.updated_at) || a.id - b.id)
      .map((row) => ({ ...row }))
  }

  listRecentLogs(limit) {
    this.require()
    return [...this.data.logs]
      .sort((a, b) => a.id - b.id)
      .slice(-limit)
      .map((row) => ({
        id: row.id,
        tick: row.tick,
        user_id: row.user_id,
        data_key: row.data_key,
        value: row.value,
        updated_at: row.updated_at,
        source_node: row.source_node,
        op: row.op
      }))
  }

  countLogs() {
    this.require()
    return this.data.logs.length
  }

  deleteLogs(ids) {
    this.require()
    this.mark('logs')
    const drop = new Set(ids)
    this.data.logs = this.data.logs.filter((row) => !drop.has(row.id))
  }

  hasReceipt(receipt) {
    this.require()
    return Object.prototype.hasOwnProperty.call(this.data.receipts, receiptKey(receipt))
  }

  insertReceipt(receipt) {
    this.require()
    this.mark('receipts')
    this.data.receipts[receiptKey(receipt)] = true
  }

  getMaster(userId, key) {
    this.require()
    return copy(this.data.masters[pairKey(userId, key)])
  }

  putMaster(entry) {
    this.require()
    this.mark('masters')
    this.data.masters[pairKey(entry.user_id, entry.data_key)] = {
      user_id: entry.user_id,
      data_key: entry.data_key,
      node_id: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended
    }
  }

  deleteMaster(userId, key) {
    this.require()
    this.mark('masters')
    delete this.data.masters[pairKey(userId, key)]
  }

  listMasters() {
    this.require()
    return Object.values(this.data.masters)
      .sort((a, b) => byText(a.user_id, b.user_id) || byText(a.data_key, b.data_key))
      .map((row) => ({ ...row }))
  }

  suspendMasters(nodeId) {
    this.require()
    this.mark('masters')
    for (const row of Object.values(this.data.masters)) {
      if (row.node_id === nodeId) row.suspended = 1
    }
  }

  updateMasterIp(nodeId, ip) {
    this.require()
    this.mark('masters')
    for (const row of Object.values(this.data.masters)) {
      if (row.node_id === nodeId) row.ip = ip
    }
  }

  countSuspendedMasters(nodeId) {
    this.require()
    return Object.values(this.data.masters).filter((row) => row.node_id === nodeId && row.suspended === 1).length
  }

  getBuffer(nodeId, userId, key) {
    this.require()
    return copy(this.data.buffers.find((row) => row.node_id === nodeId && row.user_id === userId && row.data_key === key))
  }

  insertBuffer(entry) {
    this.require()
    this.mark('buffers')
    this.data.bufferSeq += 1
    this.data.buffers.push({
      id: this.data.bufferSeq,
      node_id: entry.node_id,
      user_id: entry.user_id,
      data_key: entry.data_key,
      target_node: entry.target_node,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended,
      op: entry.op
    })
  }

  updateBuffer(id, entry) {
    this.require()
    this.mark('buffers')
    const row = this.data.buffers.find((item) => item.id === id)
    if (!row) return
    row.target_node = entry.target_node
    row.ip = entry.ip
    row.alias = entry.alias
    row.updated_at = entry.updated_at
    row.suspended = entry.suspended
    row.op = entry.op
  }

  listBuffers(nodeId) {
    this.require()
    return this.data.buffers
      .filter((row) => row.node_id === nodeId)
      .sort((a, b) => byText(a.updated_at, b.updated_at) || a.id - b.id)
      .map((row) => ({
        user_id: row.user_id,
        data_key: row.data_key,
        target_node: row.target_node,
        ip: row.ip,
        alias: row.alias,
        updated_at: row.updated_at,
        suspended: row.suspended,
        op: row.op
      }))
  }

  deleteBuffers(nodeId) {
    this.require()
    this.mark('buffers')
    this.data.buffers = this.data.buffers.filter((row) => row.node_id !== nodeId)
  }

  suspendBuffers(targetNodeId) {
    this.require()
    this.mark('buffers')
    for (const row of this.data.buffers) {
      if (row.target_node === targetNodeId) row.suspended = 1
    }
  }

  updateBufferIp(targetNodeId, ip) {
    this.require()
    this.mark('buffers')
    for (const row of this.data.buffers) {
      if (row.target_node === targetNodeId) row.ip = ip
    }
  }

  getPending(nodeId, userId, key) {
    this.require()
    return copy(this.data.pending[pendingKey(nodeId, userId, key)])
  }

  putPending(entry) {
    this.require()
    this.mark('pending')
    this.data.pending[pendingKey(entry.node_id, entry.user_id, entry.data_key)] = {
      node_id: entry.node_id,
      user_id: entry.user_id,
      data_key: entry.data_key,
      target_node: entry.target_node,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended,
      op: entry.op
    }
  }

  listPending(nodeId) {
    this.require()
    return Object.values(this.data.pending)
      .filter((row) => row.node_id === nodeId)
      .sort((a, b) => byText(a.updated_at, b.updated_at))
      .map((row) => ({ ...row }))
  }

  deletePending(nodeId) {
    this.require()
    this.mark('pending')
    for (const key of Object.keys(this.data.pending)) {
      if (this.data.pending[key].node_id === nodeId) delete this.data.pending[key]
    }
  }

  suspendPending(targetNodeId) {
    this.require()
    this.mark('pending')
    for (const row of Object.values(this.data.pending)) {
      if (row.target_node === targetNodeId) row.suspended = 1
    }
  }

  updatePendingIp(targetNodeId, ip) {
    this.require()
    this.mark('pending')
    for (const row of Object.values(this.data.pending)) {
      if (row.target_node === targetNodeId) row.ip = ip
    }
  }

  getMergeJob() {
    this.require()
    return copy(this.data.merge)
  }

  insertMergeJob(job) {
    this.require()
    this.mark('merge')
    this.data.merge = {
      id: 1,
      lost_id: job.lost_id,
      target_id: job.target_id,
      started_tick: job.started_tick
    }
  }

  deleteMergeJob() {
    this.require()
    this.mark('merge')
    this.data.merge = null
  }

  getFragment(holderId, userId, key) {
    this.require()
    const row = this.data.fragments[heldKey(holderId, userId, key)]
    return row ? { user_id: row.user_id, data_key: row.data_key, value: row.value, updated_at: row.updated_at } : null
  }

  putFragment(holderId, row) {
    this.require()
    this.mark('fragments')
    this.data.fragments[heldKey(holderId, row.user_id, row.data_key)] = {
      holder_id: holderId,
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value,
      updated_at: row.updated_at
    }
  }

  deleteFragment(holderId, userId, key) {
    this.require()
    this.mark('fragments')
    delete this.data.fragments[heldKey(holderId, userId, key)]
  }

  listFragments(holderId) {
    this.require()
    return Object.values(this.data.fragments)
      .filter((row) => row.holder_id === holderId)
      .sort((a, b) => byText(a.user_id, b.user_id) || byText(a.data_key, b.data_key))
      .map((row) => ({ user_id: row.user_id, data_key: row.data_key, value: row.value, updated_at: row.updated_at }))
  }

  countFragments(holderId) {
    this.require()
    return Object.values(this.data.fragments).filter((row) => row.holder_id === holderId).length
  }

  getIndex(holderId, userId, key) {
    this.require()
    const row = this.data.indexes[heldKey(holderId, userId, key)]
    if (!row) return null
    return {
      user_id: row.user_id,
      data_key: row.data_key,
      node_id: row.node_id,
      ip: row.ip,
      alias: row.alias,
      updated_at: row.updated_at,
      suspended: row.suspended
    }
  }

  putIndex(holderId, entry) {
    this.require()
    this.mark('indexes')
    this.data.indexes[heldKey(holderId, entry.user_id, entry.data_key)] = {
      holder_id: holderId,
      user_id: entry.user_id,
      data_key: entry.data_key,
      node_id: entry.node_id,
      ip: entry.ip,
      alias: entry.alias,
      updated_at: entry.updated_at,
      suspended: entry.suspended
    }
  }

  deleteIndex(holderId, userId, key) {
    this.require()
    this.mark('indexes')
    delete this.data.indexes[heldKey(holderId, userId, key)]
  }

  listIndexes(holderId) {
    this.require()
    return Object.values(this.data.indexes)
      .filter((row) => row.holder_id === holderId)
      .sort((a, b) => byText(a.user_id, b.user_id) || byText(a.data_key, b.data_key))
      .map((row) => ({
        user_id: row.user_id,
        data_key: row.data_key,
        node_id: row.node_id,
        ip: row.ip,
        alias: row.alias,
        updated_at: row.updated_at,
        suspended: row.suspended
      }))
  }

  updateIndexIpByAlias(holderId, alias, ip) {
    this.require()
    this.mark('indexes')
    for (const row of Object.values(this.data.indexes)) {
      if (row.holder_id === holderId && row.alias === alias) row.ip = ip
    }
  }

  suspendIndexes(holderId, targetNodeId) {
    this.require()
    this.mark('indexes')
    for (const row of Object.values(this.data.indexes)) {
      if (row.holder_id === holderId && row.node_id === targetNodeId) row.suspended = 1
    }
  }

  listOutbox(holderId) {
    this.require()
    return this.data.outbox
      .filter((row) => row.holder_id === holderId)
      .sort((a, b) => byText(a.updated_at, b.updated_at) || a.id - b.id)
      .map((row) => ({
        id: row.id,
        user_id: row.user_id,
        data_key: row.data_key,
        value: row.value,
        updated_at: row.updated_at,
        op: row.op,
        tick: row.tick
      }))
  }

  insertOutbox(holderId, row) {
    this.require()
    this.mark('outbox')
    this.data.outboxSeq += 1
    this.data.outbox.push({
      id: this.data.outboxSeq,
      holder_id: holderId,
      tick: row.tick,
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value,
      updated_at: row.updated_at,
      op: row.op
    })
  }

  deleteOutbox(holderId, ids) {
    this.require()
    this.mark('outbox')
    const drop = new Set(ids)
    this.data.outbox = this.data.outbox.filter((row) => !(row.holder_id === holderId && drop.has(row.id)))
  }

  countOutbox(holderId) {
    this.require()
    return this.data.outbox.filter((row) => row.holder_id === holderId).length
  }

  getRecord(holderId, userId, key) {
    this.require()
    const row = this.data.records[heldKey(holderId, userId, key)]
    if (!row) return null
    return {
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value,
      updated_at: row.updated_at,
      home_node: row.home_node,
      organized_tick: row.organized_tick
    }
  }

  putRecord(holderId, row) {
    this.require()
    this.mark('records')
    this.data.records[heldKey(holderId, row.user_id, row.data_key)] = {
      holder_id: holderId,
      user_id: row.user_id,
      data_key: row.data_key,
      value: row.value,
      updated_at: row.updated_at,
      home_node: row.home_node,
      organized_tick: row.organized_tick
    }
  }

  deleteRecord(holderId, userId, key) {
    this.require()
    this.mark('records')
    delete this.data.records[heldKey(holderId, userId, key)]
  }

  listRecords(holderId) {
    this.require()
    return Object.values(this.data.records)
      .filter((row) => row.holder_id === holderId)
      .sort((a, b) => byText(a.user_id, b.user_id) || byText(a.data_key, b.data_key))
      .map((row) => ({
        user_id: row.user_id,
        data_key: row.data_key,
        value: row.value,
        updated_at: row.updated_at,
        home_node: row.home_node,
        organized_tick: row.organized_tick
      }))
  }

  listRecordsByHome(holderId, homeId) {
    this.require()
    return Object.values(this.data.records)
      .filter((row) => row.holder_id === holderId && row.home_node === homeId)
      .map((row) => ({
        user_id: row.user_id,
        data_key: row.data_key,
        value: row.value,
        updated_at: row.updated_at,
        home_node: row.home_node,
        organized_tick: row.organized_tick
      }))
  }

  updateRecordHome(holderId, row) {
    this.require()
    this.mark('records')
    const current = this.data.records[heldKey(holderId, row.user_id, row.data_key)]
    if (!current || current.home_node !== row.from_home) return
    current.home_node = row.home_node
    current.value = row.value
    current.updated_at = row.updated_at
    current.organized_tick = row.organized_tick
  }

  deleteRecordHome(holderId, userId, key, homeId) {
    this.require()
    this.mark('records')
    const current = this.data.records[heldKey(holderId, userId, key)]
    if (current && current.home_node === homeId) delete this.data.records[heldKey(holderId, userId, key)]
  }

  latestStamp(holderId) {
    this.require()
    const stamps = [
      ...Object.values(this.data.fragments).filter((row) => row.holder_id === holderId).map((row) => row.updated_at),
      ...this.data.outbox.filter((row) => row.holder_id === holderId).map((row) => row.updated_at),
      ...Object.values(this.data.indexes).filter((row) => row.holder_id === holderId).map((row) => row.updated_at)
    ]
    if (stamps.length === 0) return null
    return stamps.sort(byText).at(-1)
  }

  clearHolder(holderId) {
    this.require()
    this.mark('fragments')
    this.mark('indexes')
    this.mark('outbox')
    for (const key of Object.keys(this.data.fragments)) {
      if (this.data.fragments[key].holder_id === holderId) delete this.data.fragments[key]
    }
    for (const key of Object.keys(this.data.indexes)) {
      if (this.data.indexes[key].holder_id === holderId) delete this.data.indexes[key]
    }
    this.data.outbox = this.data.outbox.filter((row) => row.holder_id !== holderId)
  }
}
