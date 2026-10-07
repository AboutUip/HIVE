import http from 'node:http'
import { timingSafeEqual } from 'node:crypto'

export function fail(message, status = 400) {
  const error = new Error(message)
  error.expose = true
  error.status = status
  return error
}

export function parseAddress(address) {
  const index = String(address || '').lastIndexOf(':')
  const host = index > 0 ? address.slice(0, index) : ''
  const port = Number(index > 0 ? address.slice(index + 1) : '')
  if (!host || !Number.isInteger(port) || port < 1) throw fail(`地址无法连接：${address || '空'}`)
  return { host, port }
}

function sameToken(expected, got) {
  const left = Buffer.from(String(expected))
  const right = Buffer.from(String(got || ''))
  if (left.length !== right.length) return false
  return timingSafeEqual(left, right)
}

function authorized(req, token) {
  const header = String(req.headers.authorization || '')
  const got = header.startsWith('Bearer ') ? header.slice(7) : ''
  return sameToken(token, got)
}

function send(res, status, body) {
  const payload = JSON.stringify(body ?? {})
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(payload) })
  res.end(payload)
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    req.on('data', (chunk) => {
      size += chunk.length
      if (size > 1_000_000) {
        reject(fail('请求过大', 413))
        req.destroy()
        return
      }
      chunks.push(chunk)
    })
    req.on('end', () => {
      if (chunks.length === 0) {
        resolve(null)
        return
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')))
      } catch {
        reject(fail('请求不是 JSON'))
      }
    })
    req.on('error', reject)
  })
}

export function serve({ host, port, token, handler }) {
  if (!token) throw fail('集群令牌不能是空的')
  const server = http.createServer(async (req, res) => {
    try {
      if (!authorized(req, token)) {
        send(res, 401, { error: '令牌不对' })
        return
      }
      const url = new URL(req.url, 'http://127.0.0.1')
      const body = await readBody(req)
      const result = await handler({ method: req.method || 'GET', path: url.pathname, query: url.searchParams, body })
      send(res, result?.status || 200, result?.body ?? {})
    } catch (error) {
      const status = error.status || 500
      send(res, status, { error: error.expose ? error.message : '内部错误' })
    }
  })
  return new Promise((resolve) => {
    server.listen(port, host, () => {
      const bound = server.address().port
      resolve({
        host,
        port: bound,
        close() {
          return new Promise((done) => {
            server.close(() => done())
            server.closeIdleConnections()
            server.closeAllConnections()
          })
        }
      })
    })
  })
}

export function call({ host, port, method = 'GET', path, body, token, timeoutMs = 2000 }) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? null : JSON.stringify(body)
    const req = http.request(
      {
        host,
        port,
        method,
        path,
        timeout: timeoutMs,
        headers: {
          authorization: `Bearer ${token}`,
          ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {})
        }
      },
      (res) => {
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8')
          let parsed = null
          if (text) {
            try {
              parsed = JSON.parse(text)
            } catch {
              parsed = { error: text }
            }
          }
          if (res.statusCode >= 400) {
            reject(fail(parsed?.error || `请求失败 ${res.statusCode}`, res.statusCode))
            return
          }
          resolve(parsed ?? {})
        })
      }
    )
    req.on('timeout', () => {
      req.destroy()
      reject(fail('连接超时', 504))
    })
    req.on('error', () => reject(fail('无法连接', 502)))
    if (payload) req.write(payload)
    req.end()
  })
}

export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
