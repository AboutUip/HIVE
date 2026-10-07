import fs from 'node:fs'
import path from 'node:path'
import { MemoryStore } from './memory.js'

export class FileStore extends MemoryStore {
  constructor(dataDir) {
    super()
    this.dir = dataDir
    this.file = path.join(dataDir, 'state.json')
  }

  open() {
    super.open()
    if (!fs.existsSync(this.file)) return
    this.data = JSON.parse(fs.readFileSync(this.file, 'utf8'))
  }

  reset() {
    super.reset()
    fs.mkdirSync(this.dir, { recursive: true })
    fs.rmSync(this.file, { force: true })
  }

  transaction(fn) {
    const outer = !this.inTransaction()
    try {
      return super.transaction(fn)
    } finally {
      if (outer && this.isOpen()) this.persist()
    }
  }

  persist() {
    fs.mkdirSync(this.dir, { recursive: true })
    const tmp = `${this.file}.${process.pid}.tmp`
    const fd = fs.openSync(tmp, 'w')
    try {
      fs.writeFileSync(fd, JSON.stringify(this.data))
      fs.fsyncSync(fd)
    } finally {
      fs.closeSync(fd)
    }
    fs.renameSync(tmp, this.file)
  }
}
