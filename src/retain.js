import fs from 'node:fs'
import path from 'node:path'

function removeFile(file) {
  try {
    fs.unlinkSync(file)
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
}

export function appendRetained(file, lines, maxBytes = 10 * 1024 * 1024) {
  if (!lines.length) return
  if (fs.existsSync(file) && maxBytes > 0 && fs.statSync(file).size >= maxBytes) {
    removeFile(`${file}.1`)
    fs.renameSync(file, `${file}.1`)
  }
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.appendFileSync(file, `${lines.join('\n')}\n`)
}
