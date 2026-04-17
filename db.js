/**
 * SQLite Database
 * Prefers better-sqlite3 for fast synchronous local CRUD.
 * Falls back to sql.js only if native module is unavailable.
 * Database file: ./data/voice_agent.db
 */

import initSqlJs from 'sql.js';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import log from './logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, 'data');
if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });

const DB_PATH = path.join(DATA_DIR, 'voice_agent.db');
log.info('DATABASE', `Path: ${DB_PATH}`);

let db;
let driverName = 'sql.js';

try {
  const { default: BetterSqlite3 } = await import('better-sqlite3');
  db = new BetterSqlite3(DB_PATH);
  driverName = 'better-sqlite3';
  log.info('DATABASE', 'Loaded better-sqlite3 driver');
} catch (err) {
  log.warn('DATABASE', `better-sqlite3 unavailable, falling back to sql.js: ${err.message}`);

  const SQL = await initSqlJs();
  let rawDb;
  if (existsSync(DB_PATH)) {
    const buffer = readFileSync(DB_PATH);
    rawDb = new SQL.Database(buffer);
    log.info('DATABASE', 'Loaded existing database via sql.js fallback');
  } else {
    rawDb = new SQL.Database();
    log.info('DATABASE', 'Created new database via sql.js fallback');
  }

  let saveDepth = 0;
  let pendingSave = false;

  function saveDb() {
    try {
      if (saveDepth > 0) {
        pendingSave = true;
        return;
      }
      const data = rawDb.export();
      writeFileSync(DB_PATH, Buffer.from(data));
      pendingSave = false;
    } catch (saveErr) {
      log.error('DATABASE', 'Failed to save database', saveErr.message);
    }
  }

  function stmtToObjects(stmt) {
    const cols = stmt.getColumnNames();
    const rows = [];
    while (stmt.step()) {
      const values = stmt.get();
      const obj = {};
      for (let i = 0; i < cols.length; i++) {
        obj[cols[i]] = values[i];
      }
      rows.push(obj);
    }
    stmt.free();
    return rows;
  }

  db = {
    prepare(sql) {
      return {
        all(...params) {
          try {
            const stmt = rawDb.prepare(sql);
            if (params.length > 0) stmt.bind(params);
            return stmtToObjects(stmt);
          } catch (queryErr) {
            log.error('DATABASE', `all() failed: ${sql.slice(0, 100)}`, queryErr.message);
            throw queryErr;
          }
        },
        get(...params) {
          try {
            const stmt = rawDb.prepare(sql);
            if (params.length > 0) stmt.bind(params);
            let result;
            if (stmt.step()) {
              const cols = stmt.getColumnNames();
              const values = stmt.get();
              result = {};
              for (let i = 0; i < cols.length; i++) {
                result[cols[i]] = values[i];
              }
            }
            stmt.free();
            return result;
          } catch (queryErr) {
            log.error('DATABASE', `get() failed: ${sql.slice(0, 100)}`, queryErr.message);
            throw queryErr;
          }
        },
        run(...params) {
          try {
            rawDb.run(sql, params);
            saveDb();
          } catch (queryErr) {
            log.error('DATABASE', `run() failed: ${sql.slice(0, 100)}`, queryErr.message);
            throw queryErr;
          }
        },
      };
    },
    exec(sql) {
      try {
        rawDb.exec(sql);
        saveDb();
      } catch (execErr) {
        log.error('DATABASE', `exec() failed: ${sql.slice(0, 100)}`, execErr.message);
        throw execErr;
      }
    },
    transaction(fn) {
      return (...args) => {
        try {
          saveDepth += 1;
          rawDb.exec('BEGIN');
          const result = fn(...args);
          rawDb.exec('COMMIT');
          return result;
        } catch (txErr) {
          try { rawDb.exec('ROLLBACK'); } catch {}
          log.error('DATABASE', 'transaction() failed', txErr.message);
          throw txErr;
        } finally {
          saveDepth = Math.max(0, saveDepth - 1);
          if (saveDepth === 0 && pendingSave) saveDb();
        }
      };
    },
    pragma() {},
  };
}

log.info('DATABASE', `Driver: ${driverName}`);

// ─── Schema ──────────────────────────────────────────────────────
log.step('DATABASE', 'init', 1, 'Creating tables...');

db.exec(`CREATE TABLE IF NOT EXISTS customers (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  phone TEXT NOT NULL,
  balance REAL NOT NULL DEFAULT 0,
  dpd INTEGER NOT NULL DEFAULT 0,
  follow_up_count INTEGER NOT NULL DEFAULT 0,
  priority_score REAL NOT NULL DEFAULT 0,
  last_call_date TEXT,
  ptp_date TEXT,
  ptp_status TEXT,
  assigned_agent TEXT NOT NULL DEFAULT 'fresh_call',
  assigned_tone TEXT NOT NULL DEFAULT 'polite',
  final_response TEXT,
  scheduled_retry_at TEXT,
  retry_reason TEXT,
  gender TEXT NOT NULL DEFAULT 'unknown',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
)`);

// Add gender column if missing (existing DBs) — safe no-op if already present
try {
  const hasGender = db.prepare("SELECT COUNT(*) as c FROM pragma_table_info('customers') WHERE name='gender'").get();
  if (!hasGender || hasGender.c === 0) {
    db.exec(`ALTER TABLE customers ADD COLUMN gender TEXT NOT NULL DEFAULT 'unknown'`);
    log.info('DATABASE', 'Added gender column to customers');
  }
} catch(e) { /* already exists or pragma not supported — safe to ignore */ }

db.exec(`CREATE TABLE IF NOT EXISTS call_logs (
  id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  customer_name TEXT NOT NULL,
  call_date_time TEXT NOT NULL DEFAULT (datetime('now')),
  duration INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued',
  agent_type TEXT NOT NULL DEFAULT 'fresh_call',
  tone TEXT NOT NULL DEFAULT 'polite',
  outcome TEXT,
  ptp_date TEXT,
  notes TEXT,
  recording_url TEXT,
  transcript TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
)`);

db.exec(`CREATE TABLE IF NOT EXISTS call_conversations (
  id TEXT PRIMARY KEY,
  call_sid TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  customer_name TEXT NOT NULL,
  agent_type TEXT DEFAULT 'fresh_call',
  tone TEXT DEFAULT 'polite',
  caller_name TEXT DEFAULT 'Iqra',
  voice TEXT DEFAULT 'Polly.Aditi',
  language TEXT DEFAULT 'ur-PK',
  balance REAL DEFAULT 0,
  dpd INTEGER DEFAULT 0,
  ptp_status TEXT,
  follow_up_count INTEGER DEFAULT 0,
  messages TEXT DEFAULT '[]',
  status TEXT DEFAULT 'active',
  final_response TEXT,
  ptp_date TEXT,
  notes TEXT,
  schedule_retry_hours INTEGER,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
)`);

db.exec(`PRAGMA journal_mode = WAL`);
db.exec(`PRAGMA synchronous = NORMAL`);
db.exec(`PRAGMA cache_size = 10000`);

const custCount = db.prepare('SELECT COUNT(*) as c FROM customers').get()?.c || 0;
const logCount = db.prepare('SELECT COUNT(*) as c FROM call_logs').get()?.c || 0;
log.success('DATABASE', `Ready — ${custCount} customers, ${logCount} call logs`);

export default db;
