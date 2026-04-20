import { Router } from 'express';
import db from '../db.js';
import { v4 as uuidv4 } from 'uuid';
import log from '../logger.js';

const router = Router();

// GET all call logs
router.get('/', (req, res) => {
  log.step('CALL-LOGS', 'list', 1, 'Querying all call logs');
  try {
    const rows = db.prepare('SELECT * FROM call_logs ORDER BY call_date_time DESC').all();
    log.success('CALL-LOGS', `Fetched ${rows.length} call logs`);
    res.json({ data: rows });
  } catch (err) {
    log.fail('CALL-LOGS', 'Query failed', err.message);
    res.status(500).json({ error: { message: err.message } });
  }
});

// GET one call log
router.get('/:id', (req, res) => {
  const { id } = req.params;
  try {
    const row = db.prepare('SELECT * FROM call_logs WHERE id = ?').get(id);
    if (!row) return res.status(404).json({ error: { message: 'Call log not found' } });
    res.json({ data: row });
  } catch (err) {
    log.fail('CALL-LOGS', `Query failed for ${id}`, err.message);
    res.status(500).json({ error: { message: err.message } });
  }
});

// POST create call log
router.post('/', (req, res) => {
  const { customer_id, customer_name, call_date_time, duration, status, agent_type, tone, outcome, ptp_date, notes, recording_url, transcript } = req.body;
  const id = uuidv4();
  log.step('CALL-LOGS', 'create', 1, 'Creating call log', { customer_name, agent_type, tone, duration, outcome });
  try {
    db.prepare(`
      INSERT INTO call_logs (id, customer_id, customer_name, call_date_time, duration, status, agent_type, tone, outcome, ptp_date, notes, recording_url, transcript)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(id, customer_id, customer_name, call_date_time || new Date().toISOString(), duration || 0, status || 'queued', agent_type || 'fresh_call', tone || 'polite', outcome || null, ptp_date || null, notes || null, recording_url || null, transcript || null);
    log.success('CALL-LOGS', `Created call log ${id} for ${customer_name}`);
    res.json({ data: { id }, error: null });
  } catch (err) {
    log.fail('CALL-LOGS', `Insert failed for ${customer_name}`, err.message);
    res.status(400).json({ error: { message: err.message } });
  }
});

// PUT update call log
router.put('/:id', (req, res) => {
  const { id } = req.params;
  const fields = req.body;
  const keys = Object.keys(fields).filter(k => k !== 'id');
  log.step('CALL-LOGS', 'update', 1, `Updating call log ${id}`, { fields: keys });
  if (keys.length === 0) return res.json({ error: null });
  
  const sets = keys.map(k => `${k} = ?`).join(', ');
  const vals = keys.map(k => fields[k] ?? null);
  
  try {
    db.prepare(`UPDATE call_logs SET ${sets} WHERE id = ?`).run(...vals, id);
    log.success('CALL-LOGS', `Updated call log ${id}`);
    res.json({ error: null });
  } catch (err) {
    log.fail('CALL-LOGS', `Update failed for ${id}`, err.message);
    res.status(400).json({ error: { message: err.message } });
  }
});

export default router;
