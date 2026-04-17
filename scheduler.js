/**
 * Auto-Redial Scheduler + PTP Reminder System
 * - Checks for overdue retries every 60 seconds
 * - Proactively schedules day-before and day-of PTP reminders
 * - Auto-selects the best caller persona per customer
 * - Reads maxConcurrentCalls + interCallDelaySec from settings
 */

import db from './db.js';
import log from './logger.js';

let schedulerInterval = null;
let isEnabled = true;
let makeCallFn = null;

// ─── Configurable settings (updated via API or defaults) ────
let schedulerConfig = {
  maxConcurrentCalls: 1,   // Sequential: one call at a time (call center style)
  interCallDelaySec: 5,   // 5s gap between calls — avoids Twilio rate limits
  schedulerPollSec: 30,
  autoDialBatchSize: 5,
  // Retry hours per outcome — mirrors frontend Rules tab
  retryNoAnswerHours: 2,
  retryNonCustomerHours: 5,
  retryCallbackHours: 2,
  retryRefusedHours: 24,
};

let nextCallTimer = null; // Tracks pending post-call trigger to avoid duplicates

function normalizeSchedulerPollSec(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 30;
  return Math.max(30, Math.round(n));
}

function getSchedulerPollMs() {
  return normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec) * 1000;
}

function resetSchedulerInterval() {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
  }
  schedulerInterval = setInterval(runSchedulerCycle, getSchedulerPollMs());
}

function moveToNextBusinessMorning(baseDate = new Date()) {
  const dt = new Date(baseDate);
  dt.setHours(9, 0, 0, 0);
  while (dt.getDay() === 0 || dt.getDay() === 6) {
    dt.setDate(dt.getDate() + 1);
  }
  return dt;
}

/** Same logic as calls.js computeScheduledRetryAt — no circular import needed */
function computeRetryAt(baseDate, hours) {
  if (!(hours > 0)) return null;
  const retryTime = new Date(new Date(baseDate).getTime() + hours * 3600000);
  const retryHour = retryTime.getHours();
  if (retryHour >= 18 || retryHour < 9) {
    if (retryHour >= 18) retryTime.setDate(retryTime.getDate() + 1);
    retryTime.setHours(9, 0, 0, 0);
  }
  while (retryTime.getDay() === 0 || retryTime.getDay() === 6) {
    retryTime.setDate(retryTime.getDate() + 1);
    retryTime.setHours(9, 0, 0, 0);
  }
  return retryTime.toISOString();
}

/** Outcome → config field mapping for re-scheduling */
const OUTCOME_TO_RETRY_KEY = {
  no_answer: 'retryNoAnswerHours',
  switched_off: 'retryNoAnswerHours',
  non_customer_pickup: 'retryNonCustomerHours',
  callback_requested: 'retryCallbackHours',
  busy: 'retryCallbackHours',
  partial_payment: 'retryCallbackHours',
  refused: 'retryRefusedHours',
  abuse_detected: 'retryRefusedHours',
};

/**
 * When retry hours change, update scheduled_retry_at for all queued customers
 * whose outcome maps to a changed retry key.
 * Uses last_call_date + new hours, clamped to business hours.
 */
function rescheduleQueuedCustomers(changedKeys) {
  const now = new Date().toISOString();
  let total = 0;
  for (const [outcome, cfgKey] of Object.entries(OUTCOME_TO_RETRY_KEY)) {
    if (!changedKeys.includes(cfgKey)) continue;
    const newHours = schedulerConfig[cfgKey];
    const rows = db.prepare(
      `SELECT id, last_call_date FROM customers
       WHERE final_response = ? AND scheduled_retry_at > ? AND scheduled_retry_at IS NOT NULL`
    ).all(outcome, now);
    for (const row of rows) {
      const base = row.last_call_date ? new Date(row.last_call_date) : new Date();
      const newRetryAt = computeRetryAt(base, newHours);
      if (newRetryAt) {
        db.prepare(`UPDATE customers SET scheduled_retry_at = ? WHERE id = ?`).run(newRetryAt, row.id);
        total++;
      }
    }
  }
  if (total > 0) log.info('SCHEDULER', `Rescheduled ${total} queued customer(s) after config change`);
}

export function updateSchedulerConfig(config) {
  const changedKeys = [];
  if (config.maxConcurrentCalls) schedulerConfig.maxConcurrentCalls = config.maxConcurrentCalls;
  if (config.interCallDelaySec !== undefined) schedulerConfig.interCallDelaySec = config.interCallDelaySec;
  if (config.schedulerPollMinutes !== undefined) {
    schedulerConfig.schedulerPollSec = normalizeSchedulerPollSec(Number(config.schedulerPollMinutes) * 60);
  }
  if (config.schedulerPollSec !== undefined) {
    schedulerConfig.schedulerPollSec = normalizeSchedulerPollSec(config.schedulerPollSec);
  }
  if (config.autoDialBatchSize) schedulerConfig.autoDialBatchSize = config.autoDialBatchSize;
  if (config.retryNoAnswerHours > 0 && config.retryNoAnswerHours !== schedulerConfig.retryNoAnswerHours) {
    schedulerConfig.retryNoAnswerHours = config.retryNoAnswerHours; changedKeys.push('retryNoAnswerHours');
  }
  if (config.retryNonCustomerHours > 0 && config.retryNonCustomerHours !== schedulerConfig.retryNonCustomerHours) {
    schedulerConfig.retryNonCustomerHours = config.retryNonCustomerHours; changedKeys.push('retryNonCustomerHours');
  }
  if (config.retryCallbackHours > 0 && config.retryCallbackHours !== schedulerConfig.retryCallbackHours) {
    schedulerConfig.retryCallbackHours = config.retryCallbackHours; changedKeys.push('retryCallbackHours');
  }
  if (config.retryRefusedHours > 0 && config.retryRefusedHours !== schedulerConfig.retryRefusedHours) {
    schedulerConfig.retryRefusedHours = config.retryRefusedHours; changedKeys.push('retryRefusedHours');
  }
  log.info('SCHEDULER', `Config updated: concurrent=${schedulerConfig.maxConcurrentCalls}, delay=${schedulerConfig.interCallDelaySec}s, poll=${normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec)}s, batch=${schedulerConfig.autoDialBatchSize}, retryHours: no_answer=${schedulerConfig.retryNoAnswerHours}h non_customer=${schedulerConfig.retryNonCustomerHours}h callback=${schedulerConfig.retryCallbackHours}h refused=${schedulerConfig.retryRefusedHours}h`);
  if (schedulerInterval && isEnabled) {
    resetSchedulerInterval();
    log.info('SCHEDULER', `Polling interval restarted at ${normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec)}s`);
  }
  if (changedKeys.length > 0) rescheduleQueuedCustomers(changedKeys);
}

/** Read-only snapshot of current retry hours for use by calls.js */
export function getRetryConfig() {
  return {
    retryNoAnswerHours: schedulerConfig.retryNoAnswerHours,
    retryNonCustomerHours: schedulerConfig.retryNonCustomerHours,
    retryCallbackHours: schedulerConfig.retryCallbackHours,
    retryRefusedHours: schedulerConfig.retryRefusedHours,
  };
}

// ─── Server-side auto-select caller (mirrors frontend logic) ────
const CALLER_MAP = [
  // Female (4)
  { id: 'fatima', name: 'Fatima', voice: 'Kore',   lang: 'ur-PK', gender: 'female', personality: 'confident', bestFor: ['escalation', 'broken_promise', 'negotiation'] },
  { id: 'ayesha', name: 'Ayesha', voice: 'Aoede',  lang: 'ur-PK', gender: 'female', personality: 'warm',      bestFor: ['ptp_reminder', 'ptp_followup', 'general_inquiry'] },
  { id: 'sana',   name: 'Sana',   voice: 'Zephyr', lang: 'ur-PK', gender: 'female', personality: 'friendly',  bestFor: ['fresh_call', 'general_inquiry'] },
  { id: 'zoya',   name: 'Zoya',   voice: 'Achernar', lang: 'ur-PK', gender: 'female', personality: 'soft',    bestFor: ['non_customer', 'after_hours'] },
  // Male (4)
  { id: 'omar',   name: 'Omar',   voice: 'Orus',   lang: 'ur-PK', gender: 'male',   personality: 'friendly',  bestFor: ['fresh_call', 'general_inquiry'] },
  { id: 'ahmed',  name: 'Ahmed',  voice: 'Puck',   lang: 'ur-PK', gender: 'male',   personality: 'friendly',  bestFor: ['ptp_followup', 'ptp_reminder'] },
  { id: 'bilal',  name: 'Bilal',  voice: 'Charon', lang: 'ur-PK', gender: 'male',   personality: 'firm',      bestFor: ['escalation', 'broken_promise'] },
  { id: 'hamza',  name: 'Hamza',  voice: 'Fenrir', lang: 'ur-PK', gender: 'male',   personality: 'serious',   bestFor: ['broken_promise', 'negotiation'] },
];

function autoSelectCaller(customer) {
  const customerGender = (customer.gender || 'male').toLowerCase();
  const agent = customer.assigned_agent || 'fresh_call';
  const tone = customer.assigned_tone || 'polite';
  const females = CALLER_MAP.filter(c => c.gender === 'female');
  const males = CALLER_MAP.filter(c => c.gender === 'male');

  // Male customers with assertive/escalation → use male firm voice
  if (customerGender === 'male' && (tone === 'assertive' || agent === 'escalation')) {
    if (agent === 'escalation' || (agent === 'broken_promise' && tone === 'assertive')) {
      return males.find(c => c.personality === 'firm') || males[0];
    }
    if (agent === 'broken_promise' || agent === 'negotiation') {
      return males.find(c => c.personality === 'serious') || males[0];
    }
  }

  // Prefer female agents for everything else
  if (agent === 'escalation' || agent === 'broken_promise') {
    return females.find(c => c.personality === 'confident') || females[0];
  }
  if (agent === 'ptp_followup' || agent === 'ptp_reminder') {
    return females.find(c => c.personality === 'warm') || females[0];
  }
  if (agent === 'non_customer' || agent === 'after_hours') {
    return females.find(c => c.personality === 'soft') || females[0];
  }
  return females.find(c => c.personality === 'friendly') || females[0];
}

export function setMakeCallFunction(fn) {
  makeCallFn = fn;
}

export function isSchedulerEnabled() {
  return isEnabled;
}

export function startScheduler() {
  if (schedulerInterval) return;
  isEnabled = true;
  log.info('SCHEDULER', `🔄 Auto-redial scheduler STARTED (polls every ${normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec)}s)`);

  resetSchedulerInterval();
  setTimeout(runSchedulerCycle, 5000);
}

export function stopScheduler() {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
  }
  isEnabled = false;
  log.info('SCHEDULER', '⏹ Auto-redial scheduler STOPPED');
}

// ─── Main scheduler cycle: PTP reminders + overdue retries ────
async function runSchedulerCycle() {
  try {
    await checkPtpReminders();
    await checkAndRedial();
  } catch (err) {
    log.error('SCHEDULER', 'Scheduler cycle error', err.message);
  }
}

// ─── PTP Day-Before & Day-Of Reminder Scheduling ────
async function checkPtpReminders() {
  try {
    const now = new Date();
    const today = now.toISOString().split('T')[0]; // YYYY-MM-DD
    const tomorrow = new Date(now);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tomorrowStr = tomorrow.toISOString().split('T')[0];
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];

    // Find customers with PTP date = tomorrow (day-before reminder)
    // and PTP date = today (day-of follow-up)
    // and PTP date = yesterday (broken promise — day after missed PTP)
    // Only those who don't already have a scheduled retry
    const ptpCustomers = db.prepare(`
      SELECT * FROM customers 
      WHERE ptp_status = 'pending' 
        AND ptp_date IS NOT NULL 
        AND scheduled_retry_at IS NULL
        AND (
          DATE(ptp_date) = ? OR DATE(ptp_date) = ? OR DATE(ptp_date) = ?
        )
      ORDER BY priority_score DESC
    `).all(tomorrowStr, today, yesterdayStr);

    if (ptpCustomers.length === 0) return;

    for (const customer of ptpCustomers) {
      const ptpDateStr = customer.ptp_date?.split('T')[0] || '';
      const isDayBefore = ptpDateStr === tomorrowStr;
      const isDayOf = ptpDateStr === today;
      const isDayAfter = ptpDateStr === yesterdayStr;

      let retryAt = new Date();
      retryAt.setHours(10, 0, 0, 0); // Default day scheduling at 10 AM

      // If it's already past 10 AM today, schedule for 30 minutes from now.
      if (retryAt <= now) {
        retryAt = new Date(now.getTime() + 30 * 60 * 1000);
      }

      // Enforce business window (Mon-Fri, 9 AM–6 PM).
      const retryHour = retryAt.getHours();
      if (retryAt.getDay() === 0 || retryAt.getDay() === 6 || retryHour < 9 || retryHour >= 18) {
        retryAt = moveToNextBusinessMorning(now);
      }

      const reason = isDayBefore
        ? 'PTP reminder — payment due tomorrow'
        : isDayOf
          ? 'PTP follow-up — payment due today'
          : 'PTP BROKEN — payment was due yesterday, missed deadline';

      // For broken promise (day after): auto-escalate tone and agent
      let agentType = 'ptp_reminder';
      let newTone = customer.assigned_tone;
      let newPtpStatus = customer.ptp_status;

      if (isDayOf) {
        agentType = 'ptp_followup';
      }

      if (isDayAfter) {
        agentType = 'broken_promise';
        newTone = 'assertive';
        newPtpStatus = 'broken';
        log.warn('SCHEDULER', `⚠️ PTP BROKEN for ${customer.name} — escalating to broken_promise + assertive`);
      }

      db.prepare(`
        UPDATE customers 
        SET scheduled_retry_at = ?, 
            retry_reason = ?,
            assigned_agent = ?,
            assigned_tone = ?,
            ptp_status = ?,
            updated_at = datetime('now')
        WHERE id = ?
      `).run(retryAt.toISOString(), reason, agentType, newTone, newPtpStatus, customer.id);

      const typeLabel = isDayBefore ? 'day-before REMINDER' : isDayOf ? 'day-of FOLLOW-UP' : 'BROKEN PROMISE escalation';
      log.info('SCHEDULER', `📅 PTP ${typeLabel} scheduled for ${customer.name} at ${retryAt.toLocaleTimeString()} (agent: ${agentType}, tone: ${newTone})`);
    }
  } catch (err) {
    log.error('SCHEDULER', 'PTP reminder check error', err.message);
  }
}

// ─── Overdue Retry Dialing (respects concurrency settings) ────
async function checkAndRedial() {
  try {
    const now = new Date().toISOString();
    const nowDate = new Date();

    // Weekend check (Mon-Fri only).
    const weekday = nowDate.getDay();
    if (weekday === 0 || weekday === 6) {
      const nextBusiness = moveToNextBusinessMorning(nowDate);
      db.prepare(`
        UPDATE customers SET scheduled_retry_at = ?, retry_reason = 'Weekend — shifted to next business day 9 AM', updated_at = datetime('now')
        WHERE scheduled_retry_at IS NOT NULL AND scheduled_retry_at <= ?
      `).run(nextBusiness.toISOString(), now);
      log.info('SCHEDULER', `Weekend (${weekday === 0 ? 'Sun' : 'Sat'}) — shifted overdue retries to ${nextBusiness.toISOString()}`);
      return;
    }

    // ── After-hours check ──
    const currentHour = nowDate.getHours();
    if (currentHour >= 18 || currentHour < 9) {
      const next9am = moveToNextBusinessMorning(currentHour >= 18 ? new Date(nowDate.getTime() + 24 * 3600000) : nowDate);
      db.prepare(`
        UPDATE customers SET scheduled_retry_at = ?, retry_reason = 'After-hours — shifted to 9 AM', updated_at = datetime('now')
        WHERE scheduled_retry_at IS NOT NULL AND scheduled_retry_at <= ?
      `).run(next9am.toISOString(), now);
      log.info('SCHEDULER', `After-hours (${currentHour}:00) — shifted overdue retries to ${next9am.toISOString()}`);
      return;
    }

    // ── Count currently active calls to respect concurrency limit ──
    const activeCalls = db.prepare(`
      SELECT COUNT(*) as count FROM call_conversations WHERE status IN ('active', 'ending')
    `).get();
    const activeCount = activeCalls?.count || 0;
    const availableSlots = Math.max(0, schedulerConfig.maxConcurrentCalls - activeCount);

    if (availableSlots === 0) {
      log.info('SCHEDULER', `All ${schedulerConfig.maxConcurrentCalls} concurrent slots busy — waiting`);
      return;
    }

    const batchLimit = Math.min(availableSlots, schedulerConfig.autoDialBatchSize);

    const overdueCustomers = db.prepare(`
      SELECT c.*, 
        (SELECT COUNT(*) FROM call_conversations cc 
         WHERE cc.customer_id = c.id AND cc.status IN ('active', 'ending')) as active_calls,
        (SELECT MAX(cc.updated_at) FROM call_conversations cc 
         WHERE cc.customer_id = c.id) as last_conv_time
      FROM customers c
      WHERE c.scheduled_retry_at IS NOT NULL 
        AND c.scheduled_retry_at <= ?
      ORDER BY c.priority_score DESC
      LIMIT ?
    `).all(now, batchLimit);

    if (overdueCustomers.length === 0) return;

    log.info('SCHEDULER', `Found ${overdueCustomers.length} overdue retries (slots: ${availableSlots}, batch: ${batchLimit})`);

    for (const customer of overdueCustomers) {
      if (customer.active_calls > 0) {
        log.info('SCHEDULER', `Skipping ${customer.name} — already in active call`);
        continue;
      }

      // Cooldown: don't re-dial if last conversation was less than 5 minutes ago
      if (customer.last_conv_time) {
        const lastConvAge = Date.now() - new Date(customer.last_conv_time).getTime();
        if (lastConvAge < 5 * 60 * 1000) {
          log.info('SCHEDULER', `Skipping ${customer.name} — last call was ${Math.round(lastConvAge / 1000)}s ago (cooldown)`);
          continue;
        }
      }

      if (!makeCallFn) {
        log.warn('SCHEDULER', 'makeCallFn not set — cannot auto-dial');
        return;
      }

      log.info('SCHEDULER', `Auto-dialing: ${customer.name} (${customer.phone}) — reason: ${customer.retry_reason || 'scheduled'}`);

      const caller = autoSelectCaller(customer);
      log.info('SCHEDULER', `Auto-selected caller: ${caller.name} • ${caller.voice} • tone: ${customer.assigned_tone || 'polite'} • agent: ${customer.assigned_agent || 'fresh_call'}`);

      // ── Fetch previous call history for context ──
      let previousCallHistory = null;
      try {
        const lastLog = db.prepare(`
          SELECT outcome, ptp_date, notes, transcript, duration, call_date_time, tone, agent_type
          FROM call_logs WHERE customer_id = ? ORDER BY created_at DESC LIMIT 3
        `).all(customer.id);
        if (lastLog && lastLog.length > 0) {
          previousCallHistory = lastLog.map(l => ({
            date: l.call_date_time,
            outcome: l.outcome,
            ptp_date: l.ptp_date,
            notes: l.notes,
            transcript: l.transcript,
            duration: l.duration,
            tone: l.tone,
            agent_type: l.agent_type,
          }));
          log.info('SCHEDULER', `Loaded ${lastLog.length} previous call(s) for ${customer.name}`);
        }
      } catch (e) {
        log.warn('SCHEDULER', `Failed to load call history for ${customer.name}: ${e.message}`);
      }

      try {
        const callResult = await Promise.race([
          makeCallFn({
            customerId: customer.id,
            customerName: customer.name,
            customerPhone: customer.phone,
            agentType: customer.assigned_agent || 'fresh_call',
            tone: customer.assigned_tone || 'polite',
            balance: customer.balance,
            dpd: customer.dpd,
            ptpStatus: customer.ptp_status,
            followUpCount: customer.follow_up_count,
            callerName: caller.name,
            callerVoice: caller.voice,
            callerLanguage: caller.lang,
            customerGender: customer.gender || 'male',
            callerGender: caller.gender,
            previousCallHistory,
            ptpDate: customer.ptp_date,
            retryReason: customer.retry_reason,
          }),
          // 30-second safety timeout — prevents scheduler from hanging forever
          new Promise((_, reject) => setTimeout(() => reject(new Error('Call setup timeout (30s)')), 30000)),
        ]);

        db.prepare(`UPDATE customers SET scheduled_retry_at = NULL, retry_reason = NULL, updated_at = datetime('now') WHERE id = ?`)
          .run(customer.id);

        log.success('SCHEDULER', `Auto-dial triggered for ${customer.name}${callResult?.callSid ? ` (SID: ${callResult.callSid})` : ''}`);
      } catch (err) {
        // Don't let one failed call kill the entire batch
        const isResourceLimit = err.message.includes('Max concurrent') || err.message.includes('in-flight');
        if (isResourceLimit) {
          log.warn('SCHEDULER', `Batch halted — ${err.message}`);
          break; // Stop the batch — no point trying more if we're at capacity
        }
        log.error('SCHEDULER', `Auto-dial failed for ${customer.name}: ${err.message}`);
        // Reschedule failed calls for 5 minutes later (don't lose them)
        const retryAt = new Date(Date.now() + 5 * 60 * 1000).toISOString();
        db.prepare(`UPDATE customers SET scheduled_retry_at = ?, retry_reason = ?, updated_at = datetime('now') WHERE id = ?`)
          .run(retryAt, `Auto-dial failed: ${err.message.slice(0, 100)}`, customer.id);
      }

      // Use configurable inter-call delay
      const delayMs = (schedulerConfig.interCallDelaySec || 2) * 1000;
      if (delayMs > 0) await new Promise(r => setTimeout(r, delayMs));
    }
  } catch (err) {
    log.error('SCHEDULER', 'Scheduler check error', err.message);
  }
}

// Manual trigger
export async function triggerAllOverdue() {
  log.info('SCHEDULER', 'Manual trigger — processing all overdue retries');
  await runSchedulerCycle();
  return { triggered: true };
}

/**
 * Called by calls.js when a call fully completes.
 * Schedules the next overdue customer after the inter-call delay,
 * enabling true sequential one-by-one auto-dialing.
 */
export function onCallCompleted() {
  if (!isEnabled || !makeCallFn) return;

  // Debounce: if a next-call timer is already pending, let it run
  if (nextCallTimer) return;

  const delayMs = Math.max(2000, (schedulerConfig.interCallDelaySec || 5) * 1000);
  log.info('SCHEDULER', `[SEQUENTIAL] Call completed — next customer check in ${delayMs / 1000}s`);

  nextCallTimer = setTimeout(async () => {
    nextCallTimer = null;
    try {
      await checkAndRedial();
    } catch (err) {
      log.error('SCHEDULER', `Post-call sequential dial error: ${err.message}`);
    }
  }, delayMs);
}

// Get scheduler status
export function getSchedulerStatus() {
  const now = new Date().toISOString();
  const overdueCount = db.prepare('SELECT COUNT(*) as count FROM customers WHERE scheduled_retry_at IS NOT NULL AND scheduled_retry_at <= ?').get(now);
  const upcomingCount = db.prepare('SELECT COUNT(*) as count FROM customers WHERE scheduled_retry_at IS NOT NULL AND scheduled_retry_at > ?').get(now);
  const ptpReminders = db.prepare("SELECT COUNT(*) as count FROM customers WHERE ptp_date IS NOT NULL AND ptp_status = 'pending'").get();

  return {
    enabled: isEnabled,
    overdueRetries: overdueCount?.count || 0,
    upcomingRetries: upcomingCount?.count || 0,
    ptpReminders: ptpReminders?.count || 0,
    config: {
      ...schedulerConfig,
      schedulerPollSec: normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec),
      schedulerPollMinutes: Math.max(1, Math.round(normalizeSchedulerPollSec(schedulerConfig.schedulerPollSec) / 60)),
    },
  };
}
