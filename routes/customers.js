import { Router } from 'express';
import db from '../db.js';
import { v4 as uuidv4 } from 'uuid';
import log from '../logger.js';

const router = Router();

function normalizePhoneKey(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (!digits) return '';
  // Compare by canonical local suffix to handle +92/0092/0 prefixes consistently.
  return digits.length > 10 ? digits.slice(-10) : digits;
}

function parseDpd(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const rounded = Math.round(n);
  if (rounded < 1 || rounded > 30) return null;
  return rounded;
}

function parseBalance(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n < 0) return null;
  return n;
}

// GET all customers
router.get('/', (req, res) => {
  log.step('CUSTOMERS', 'list', 1, 'Querying all customers from SQLite');
  try {
    const rows = db.prepare('SELECT * FROM customers ORDER BY priority_score DESC').all();
    log.success('CUSTOMERS', `Fetched ${rows.length} customers`);
    res.json({ data: rows });
  } catch (err) {
    log.fail('CUSTOMERS', 'Query failed', err.message);
    res.status(500).json({ error: { message: err.message } });
  }
});

// POST create customer
router.post('/', (req, res) => {
  const { name, phone, balance, dpd, follow_up_count, priority_score, ptp_status, ptp_date, assigned_agent, assigned_tone, final_response, gender } = req.body;
  const parsedDpd = parseDpd(dpd);
  if (parsedDpd === null) {
    return res.status(400).json({ error: { message: 'DPD must be between 1 and 30' } });
  }
  const parsedBalance = parseBalance(balance);
  if (parsedBalance === null) {
    return res.status(400).json({ error: { message: 'Negative balance is invalid' } });
  }
  const id = uuidv4();
  log.step('CUSTOMERS', 'create', 1, 'Validating input', { name, phone });
  log.step('CUSTOMERS', 'create', 2, 'Generated ID', id);
  log.step('CUSTOMERS', 'create', 3, 'Inserting into SQLite', { balance: parsedBalance, dpd, priority_score, assigned_agent, assigned_tone });
  try {
    db.prepare(`
      INSERT INTO customers (id, name, phone, balance, dpd, follow_up_count, priority_score, ptp_status, ptp_date, assigned_agent, assigned_tone, final_response, gender)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, name, phone, parsedBalance, parsedDpd, follow_up_count || 0, priority_score || 0, ptp_status || null, ptp_date || null, assigned_agent || 'fresh_call', assigned_tone || 'polite', final_response || null, gender || 'unknown');
    log.success('CUSTOMERS', `Created customer: ${name} (${id})`);
    res.json({ data: { id }, error: null });
  } catch (err) {
    log.fail('CUSTOMERS', `Insert failed for ${name}`, err.message);
    res.status(400).json({ error: { message: err.message } });
  }
});

// PUT update customer
router.put('/:id', (req, res) => {
  const { id } = req.params;
  const fields = req.body;
  if ('dpd' in fields) {
    const parsedDpd = parseDpd(fields.dpd);
    if (parsedDpd === null) {
      return res.status(400).json({ error: { message: 'DPD must be between 1 and 30' } });
    }
    fields.dpd = parsedDpd;
  }
  if ('balance' in fields) {
    const parsedBalance = parseBalance(fields.balance);
    if (parsedBalance === null) {
      return res.status(400).json({ error: { message: 'Negative balance is invalid' } });
    }
    fields.balance = parsedBalance;
  }
  // Ignore any client-provided updated_at — server will set accurate timestamp
  if ('updated_at' in fields) delete fields.updated_at;
  const keys = Object.keys(fields).filter(k => k !== 'id');
  if (keys.length === 0) {
    return res.json({ error: null });
  }
  
  // Always update server-side timestamp to avoid delayed/out-of-order updates
  const sets = keys.map(k => `${k} = ?`).concat("updated_at = datetime('now')").join(', ');
  const vals = keys.map(k => fields[k] ?? null);
  
  try {
    db.prepare(`UPDATE customers SET ${sets} WHERE id = ?`).run(...vals, id);
    res.json({ error: null });
  } catch (err) {
    res.status(400).json({ error: { message: err.message } });
  }
});

// DELETE customer
router.delete('/:id', (req, res) => {
  const id = req.params.id;
  try {
    const runDelete = db.transaction(() => {
      db.prepare('DELETE FROM call_logs WHERE customer_id = ?').run(id);
      db.prepare('DELETE FROM call_conversations WHERE customer_id = ?').run(id);
      db.prepare('DELETE FROM customers WHERE id = ?').run(id);
    });
    runDelete();
    res.json({ error: null });
  } catch (err) {
    res.status(400).json({ error: { message: err.message } });
  }
});

// DELETE all customers
router.delete('/', (req, res) => {
  db.prepare('DELETE FROM call_conversations').run();
  db.prepare('DELETE FROM call_logs').run();
  db.prepare('DELETE FROM customers').run();
  res.json({ error: null });
});

router.post('/import', (req, res) => {
  const rows = Array.isArray(req.body?.customers) ? req.body.customers : [];

  if (rows.length === 0) {
    return res.status(400).json({ error: { message: 'customers array is required' } });
  }

  log.step('CUSTOMERS', 'import', 1, `Starting bulk import of ${rows.length} customers`);

  try {
    let imported = 0;
    let skipped = 0;
    let duplicatePhoneSkipped = 0;
    let invalidDpdSkipped = 0;
    let invalidBalanceSkipped = 0;

    const existingRows = db.prepare("SELECT phone FROM customers WHERE phone IS NOT NULL AND phone != ''").all();
    const seenPhoneKeys = new Set(
      existingRows
        .map((r) => normalizePhoneKey(r.phone))
        .filter(Boolean)
    );

    const insert = db.prepare(`
      INSERT INTO customers (id, name, phone, balance, dpd, follow_up_count, priority_score, ptp_status, ptp_date, assigned_agent, assigned_tone, final_response, gender)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const runImport = db.transaction(() => {
      for (const row of rows) {
        const name = String(row?.name || '').trim();
        const phone = String(row?.phone || '').trim();
        const phoneKey = normalizePhoneKey(phone);
        const parsedDpd = parseDpd(row?.dpd);

        if (!name || !phone) {
          skipped += 1;
          continue;
        }

        if (parsedDpd === null) {
          skipped += 1;
          invalidDpdSkipped += 1;
          continue;
        }

        const parsedBalance = parseBalance(row?.balance);
        if (parsedBalance === null) {
          skipped += 1;
          invalidBalanceSkipped += 1;
          continue;
        }

        if (!phoneKey || seenPhoneKeys.has(phoneKey)) {
          skipped += 1;
          duplicatePhoneSkipped += 1;
          continue;
        }

        try {
          insert.run(
            uuidv4(),
            name,
            phone,
            parsedBalance,
            parsedDpd,
            Number(row?.follow_up_count) || 0,
            Number(row?.priority_score) || 0,
            row?.ptp_status || null,
            row?.ptp_date || null,
            row?.assigned_agent || 'fresh_call',
            row?.assigned_tone || 'polite',
            row?.final_response || null,
            row?.gender || 'unknown',
          );
          seenPhoneKeys.add(phoneKey);
          imported += 1;
        } catch (err) {
          skipped += 1;
          log.warn('CUSTOMERS', `Skipped import row for ${name || phone}`, err.message);
        }
      }
    });

    runImport();

    log.success('CUSTOMERS', `Bulk import completed: ${imported} imported, ${skipped} skipped (${duplicatePhoneSkipped} duplicate-phone, ${invalidDpdSkipped} invalid-dpd, ${invalidBalanceSkipped} invalid-balance)`);
    res.json({ data: { imported, skipped, duplicatePhoneSkipped, invalidDpdSkipped, invalidBalanceSkipped }, error: null });
  } catch (err) {
    log.fail('CUSTOMERS', 'Bulk import failed', err.message);
    res.status(400).json({ error: { message: err.message } });
  }
});

export default router;
