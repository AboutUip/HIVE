export function rejectsOlder(current, updatedAt) {
  return Boolean(current && current.updated_at > updatedAt)
}

export function putMaster(store, entry) {
  const current = store.getMaster(entry.user_id, entry.data_key)
  if (rejectsOlder(current, entry.updated_at)) return false
  store.putMaster({
    user_id: entry.user_id,
    data_key: entry.data_key,
    node_id: entry.node_id,
    ip: entry.ip,
    alias: entry.alias,
    updated_at: entry.updated_at,
    suspended: entry.suspended
  })
  return true
}

export function removeMaster(store, entry) {
  const current = store.getMaster(entry.user_id, entry.data_key)
  if (rejectsOlder(current, entry.updated_at)) return false
  store.deleteMaster(entry.user_id, entry.data_key)
  return true
}

export function applyLocal(store, holderId, entry) {
  if (entry.op === 'delete') {
    const current = store.getIndex(holderId, entry.user_id, entry.data_key)
    if (!current || current.updated_at <= entry.updated_at) store.deleteIndex(holderId, entry.user_id, entry.data_key)
    const fragment = store.getFragment(holderId, entry.user_id, entry.data_key)
    if (!fragment || entry.updated_at < fragment.updated_at) return
    store.deleteFragment(holderId, entry.user_id, entry.data_key)
    return
  }
  const current = store.getIndex(holderId, entry.user_id, entry.data_key)
  if (rejectsOlder(current, entry.updated_at)) return
  store.putIndex(holderId, {
    user_id: entry.user_id,
    data_key: entry.data_key,
    node_id: entry.node_id,
    ip: entry.ip,
    alias: entry.alias,
    updated_at: entry.updated_at,
    suspended: entry.suspended ? 1 : 0
  })
  if (entry.node_id === holderId) return
  const fragment = store.getFragment(holderId, entry.user_id, entry.data_key)
  if (!fragment || entry.updated_at < fragment.updated_at) return
  store.deleteFragment(holderId, entry.user_id, entry.data_key)
}

export function collapseLogs(logs) {
  const collapsed = new Map()
  for (const log of logs) {
    const id = `${log.user_id}\0${log.data_key}`
    const prev = collapsed.get(id)
    if (!prev || log.updated_at >= prev.updated_at) collapsed.set(id, log)
  }
  return [...collapsed.values()]
}
