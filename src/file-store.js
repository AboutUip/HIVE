import fs from 'node:fs'
import path from 'node:path'
import { MemoryStore } from './memory.js'

function removeFile(file) {
  try {
    fs.unlinkSync(file)
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
}

export class FileStore extends MemoryStore {
  constructor(dataDir, options = {}) {
    super()
    this.dir = dataDir
    this.file = path.join(dataDir, 'state.json')
    this.walFile = path.join(dataDir, 'hive.wal')
    this.snapshotEvery = Number(options.snapshotEvery) || 50
    this.walMaxBytes = Number(options.walMaxBytes) || 8 * 1024 * 1024
    this.writesSinceSnapshot = 0
    this.walBytes = 0
  }

  open() {
    super.open()
    if (fs.existsSync(this.file)) {
      this.data = JSON.parse(fs.readFileSync(this.file, 'utf8'))
    }
    if (fs.existsSync(this.walFile)) this.replay()
  }

  replay() {
    const raw = fs.readFileSync(this.walFile, 'utf8')
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue
      let entry
      try {
        entry = JSON.parse(line)
      } catch {
        // 写到一半的行，跳过。丢的只是那一笔事务。
        continue
      }
      for (const [table, value] of Object.entries(entry.tables || {})) this.data[table] = value
      if (entry.counters) this.restoreCounters(entry.counters)
    }
    removeFile(this.walFile)
    this.writesSinceSnapshot = 0
    this.walBytes = 0
  }

  reset() {
    super.reset()
    fs.mkdirSync(this.dir, { recursive: true })
    removeFile(this.file)
    removeFile(this.walFile)
    this.writesSinceSnapshot = 0
    this.walBytes = 0
  }

  transaction(fn) {
    const outer = !this.inTransaction()
    let ok = false
    try {
      const result = super.transaction(fn)
      ok = true
      return result
    } finally {
      if (outer && this.isOpen()) {
        try {
          if (ok) this.persist()
        } finally {
          this.clearChanged()
        }
      }
    }
  }

  persist() {
    const tables = this.changedTables()
    if (tables.length === 0) return
    fs.mkdirSync(this.dir, { recursive: true })
    const payload = {
      tables: Object.fromEntries(tables.map((table) => [table, this.data[table]])),
      counters: this.countersSnapshot()
    }
    const line = `${JSON.stringify(payload)}\n`
    const fd = fs.openSync(this.walFile, 'a')
    try {
      fs.writeFileSync(fd, line)
      fs.fsyncSync(fd)
    } finally {
      fs.closeSync(fd)
    }
    this.writesSinceSnapshot += 1
    this.walBytes += Buffer.byteLength(line)
    if (this.writesSinceSnapshot >= this.snapshotEvery || this.walBytes >= this.walMaxBytes) this.snapshot()
  }

  snapshot() {
    const tmp = `${this.file}.${process.pid}.tmp`
    const fd = fs.openSync(tmp, 'w')
    try {
      fs.writeFileSync(fd, JSON.stringify(this.data))
      fs.fsyncSync(fd)
    } finally {
      fs.closeSync(fd)
    }
    fs.renameSync(tmp, this.file)
    removeFile(this.walFile)
    this.writesSinceSnapshot = 0
    this.walBytes = 0
  }
}
