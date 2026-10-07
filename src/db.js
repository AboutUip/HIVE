import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'

export function openDatabase(file) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const db = new DatabaseSync(file)
  db.exec('PRAGMA foreign_keys = ON')
  return db
}

export function initCentral(db) {
  db.exec(`
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
  `)
}

export function initService(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS fragments (
      user_id TEXT NOT NULL,
      data_key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (user_id, data_key)
    );
    CREATE TABLE IF NOT EXISTS index_entries (
      user_id TEXT NOT NULL,
      data_key TEXT NOT NULL,
      node_id TEXT NOT NULL,
      ip TEXT NOT NULL,
      alias TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      suspended INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (user_id, data_key)
    );
    CREATE TABLE IF NOT EXISTS outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tick INTEGER NOT NULL,
      user_id TEXT NOT NULL,
      data_key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      op TEXT NOT NULL
    );
  `)
}

export function initDr(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS records (
      user_id TEXT NOT NULL,
      data_key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      home_node TEXT NOT NULL,
      organized_tick INTEGER NOT NULL,
      PRIMARY KEY (user_id, data_key)
    );
  `)
}

export function dbPath(dataDir, id) {
  return path.join(dataDir, `${id}.db`)
}
