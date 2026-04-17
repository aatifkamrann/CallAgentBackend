/**
 * AI Routes — generate-script, analyze-call
 * Uses Google Gemini API directly (GEMINI_API_KEY)
 */

import { Router } from 'express';
import log from '../logger.js';

const router = Router();
const GEMINI_TEXT_MODEL = 'gemini-2.5-flash';
const STRICT_ROMAN_URDU_GUARD = `ROMAN URDU LANGUAGE LOCK (MANDATORY):
- Reply in PURE Pakistani Roman Urdu only.
- Use English alphabet only.
- Do NOT use Urdu script, Hindi/Devanagari script, Arabic script, or mixed-script text.
- Do NOT switch to English sentences except common banking words already natural in Roman Urdu.
- If you feel like writing non-Roman text, transliterate it into Roman Urdu instead.
- Keep spelling natural for Pakistani Roman Urdu phone conversation style.`;

function getGeminiApiKey() {
  return process.env.GEMINI_API_KEY || process.env.API_KEY || '';
}

function buildFallbackScript({ customerName, balance, dpd, agentType, tone, callerName, maxPtpDays, customerGender, callerGender }) {
  const safeCaller = callerName || 'Omar';
  const safeBalance = Number(balance || 0).toLocaleString();
  const safeDpd = Number(dpd || 0);
  const safePtpDays = Number(maxPtpDays || 5);
  const cG = (customerGender || 'male').toLowerCase();
  const aG = (callerGender || 'male').toLowerCase();
  const honor = cG === 'female' ? 'Sahiba' : 'Sahab';
  const bolVerb = aG === 'female' ? 'bol rahi' : 'bol raha';
  const letiVerb = aG === 'female' ? 'leti' : 'leta';

  const agentGuidance = {
    fresh_call: 'Yeh pehli reminder call hai, calmly balance aur due situation explain karein.',
    broken_promise: 'Customer ne pehle commitment miss ki hai, is liye tone professional magar firm rakhein.',
    ptp_reminder: 'Existing promise-to-pay ko politely remind karein.',
    ptp_followup: 'Aaj ki payment commitment ka short follow-up lein.',
    non_customer: 'Agar customer line par na ho to short message chhor dein.',
    no_answer: 'Agar customer respond na kare to concise voicemail style line rakhein.',
    after_hours: 'Aaj after-hours mention karke kal dobara contact ka note dein.',
    negotiation: 'Customer ko realistic payment window discuss karne par guide karein.',
    escalation: 'High-priority account hai, urgency clearly communicate karein.',
    general_inquiry: 'General payment-related sawalat ka short jawab dein.',
  };

  const toneGuidance = {
    polite: 'Bohat adab aur narmi se baat karein.',
    assertive: `Firm, bold aur sakht andaz mein baat karein. Account ${safeDpd} din se overdue hai — yeh bohot serious hai. Legal proceedings, CIBIL reporting, aur account freeze ka risk clearly batayein. Mazeed delay BILKUL acceptable nahi. Professional firmness rakhein.`,
    empathetic: 'Customer ki situation samajh kar supportive tone rakhein.',
  };

  return [
    'Opening:',
    `Assalam-o-Alaikum ${customerName} ${honor}. Main ${safeCaller} ${bolVerb} hoon bank se. Kya abhi baat karna munasib hoga?`,
    '',
    'Main Body:',
    `${agentGuidance[agentType] || 'Pending dues ka short reminder dein.'} Aap ke credit card par PKR ${safeBalance} pending hain aur account ${safeDpd} din overdue hai.`,
    '',
    'PTP Negotiation:',
    `Agar aap chahein to aaj se ${safePtpDays} din ke andar ek payment date confirm kar dein taake account regularize ho sake.`,
    '',
    'Objection Handling:',
    `${toneGuidance[tone] || 'Professional aur natural tone rakhein.'} Agar customer busy ho to short reminder dein, aur agar payment already ho chuki ho to bolen ke system update thori dair se reflect ho sakta hai.`,
    '',
    'Closing:',
    `Shukriya. Main aap ka response note kar ${letiVerb} hoon. Allah Hafiz.`,
  ].join('\n');
}

function resolveCallerGender(callerGender, callerName) {
  const explicit = String(callerGender || '').toLowerCase();
  if (explicit === 'male' || explicit === 'female') return explicit;

  const name = String(callerName || '').toLowerCase().trim();
  const femaleNames = new Set(['fatima', 'ayesha', 'sana', 'zoya']);
  const maleNames = new Set(['omar', 'ahmed', 'bilal', 'hamza']);
  if (femaleNames.has(name)) return 'female';
  if (maleNames.has(name)) return 'male';
  return 'female';
}

function enforceCallerIdentityInScript(script, { customerName, customerGender, callerName, callerGender }) {
  const safeCustomer = customerName || 'Customer';
  const safeCaller = callerName || 'Omar';
  const cG = (customerGender || 'male').toLowerCase();
  const aG = resolveCallerGender(callerGender, safeCaller);
  const honor = cG === 'female' ? 'Sahiba' : 'Sahab';
  const bolVerb = aG === 'female' ? 'bol rahi' : 'bol raha';
  const openingLine = `Assalam-o-Alaikum ${safeCustomer} ${honor}, main JS Bank se ${safeCaller} ${bolVerb} hoon. Yeh call aapke pending dues ke silsile mein hai.`;

  const raw = String(script || '').trim();
  if (!raw) return openingLine;

  const aiIdentityRe = /main\s+js\s*bank\s+k[ai]\s+ai\s+voice\s+agent\s+bol\s+(?:raha|rahi)\s+hoon\.?/gi;
  const patchedIdentity = raw
    .replace(aiIdentityRe, `main JS Bank se ${safeCaller} ${bolVerb} hoon.`)
    .replace(/\bmain\s+js\s*bank\s+k[ai]\s+([^,.!?\n]+?)\s+bol\s+(?:raha|rahi)\s+hoon\b/gi, `main JS Bank se ${safeCaller} ${bolVerb} hoon`)
    .replace(/\bmain\s+js\s*bank\s+se\s+([^,.!?\n]+?)\s+bol\s+(?:raha|rahi)\s+hoon\b/gi, `main JS Bank se ${safeCaller} ${bolVerb} hoon`)
    .replace(/\b(main\s+se\s+)\b(?:fatima|ayesha|sana|zoya|omar|ahmed|bilal|hamza)\b/gi, `$1${safeCaller}`);

  if (/\bopening\s*:/i.test(patchedIdentity)) {
    const lines = patchedIdentity.split('\n');
    const openingIdx = lines.findIndex((l) => /^\s*opening\s*:\s*$/i.test(l));
    if (openingIdx !== -1) {
      let i = openingIdx + 1;
      while (i < lines.length && !lines[i].trim()) i += 1;
      if (i < lines.length) {
        lines[i] = openingLine;
      } else {
        lines.push(openingLine);
      }
      return lines.join('\n');
    }
  }

  return `${openingLine}\n\n${patchedIdentity}`;
}

function buildFallbackAnalysis(duration = 0) {
  if (duration === 0) {
    return { final_response: 'no_answer', ptp_date: null, ptp_status: null, notes: 'Fallback: Phone nahi uthaya', schedule_retry_hours: 2 };
  }
  if (duration <= 15) {
    return { final_response: 'non_customer_pickup', ptp_date: null, ptp_status: null, notes: 'Fallback: Kisi aur ne call receive ki', schedule_retry_hours: 5, non_customer_relation: 'other' };
  }
  if (duration <= 45) {
    return { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Fallback: Dobara contact ki zarurat hai', schedule_retry_hours: 2 };
  }
  return { final_response: 'ptp_secured', ptp_date: new Date(Date.now() + 3 * 86400000).toISOString().split('T')[0], ptp_status: 'pending', notes: 'Fallback: PTP secured assume kiya gaya', schedule_retry_hours: 0 };
}

const SEMANTIC_VALID_OUTCOMES = new Set([
  'ptp_secured', 'non_customer_pickup', 'switched_off', 'negotiation_barrier', 'refused',
  'callback_requested', 'partial_payment', 'payment_done', 'busy', 'abuse_detected', 'no_answer',
]);

const SEMANTIC_RETRY_HOURS = {
  no_answer: 2,
  switched_off: 2,
  non_customer_pickup: 5,
  callback_requested: 2,
  busy: 2,
  refused: 24,
  negotiation_barrier: 5,
  abuse_detected: 24,
  partial_payment: 2,
  ptp_secured: 0,
  payment_done: 0,
};

function normalizeRomanUrduNote(value = '') {
  const normalized = String(value || '')
    .replace(/[\u0600-\u06FF\u0900-\u097F]/g, ' ')
    .replace(/[^A-Za-z0-9\s.,!?':\-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || 'Roman Urdu note unavailable';
}

function extractPtpDateFromSemanticText(text = '') {
  const today = new Date();
  const asIso = (daysAhead) => new Date(Date.now() + daysAhead * 86400000).toISOString().split('T')[0];
  const normalized = normalizeDemoSpeech(text);
  if (!normalized) return asIso(2);

  if (/\b(kal|kl|tomorrow)\b/.test(normalized)) return asIso(1);
  if (/\b(parson|parso|day after tomorrow)\b/.test(normalized)) return asIso(2);
  if (/\b(tarson|narson)\b/.test(normalized)) return asIso(3);

  const dateMatch = normalized.match(/\b(\d{1,2})\s*(ko|tk|tak|tarikh|date|april|may|june|july|march|february|january|august|september|october|november|december)\b/);
  if (dateMatch) {
    const day = Number(dateMatch[1]);
    if (day >= 1 && day <= 31) {
      const monthMap = { january: 0, february: 1, march: 2, april: 3, may: 4, june: 5, july: 6, august: 7, september: 8, october: 9, november: 10, december: 11 };
      const monthToken = dateMatch[2];
      const month = monthMap[monthToken] ?? today.getMonth();
      const target = new Date(today.getFullYear(), month, day);
      if (target <= today) target.setMonth(target.getMonth() + 1);
      return target.toISOString().split('T')[0];
    }
  }

  return asIso(2);
}

function finalizeSemanticAnalysis(result = {}) {
  const normalized = {
    ...result,
    final_response: result?.final_response ?? null,
    ptp_date: result?.ptp_date ?? null,
    ptp_status: result?.ptp_status ?? null,
    notes: normalizeRomanUrduNote(result?.notes || ''),
  };

  if (normalized.final_response !== null && !SEMANTIC_VALID_OUTCOMES.has(normalized.final_response)) {
    normalized.final_response = null;
  }

  if (normalized.final_response !== 'ptp_secured') {
    normalized.ptp_date = null;
    normalized.ptp_status = null;
  } else {
    normalized.ptp_status = 'pending';
    normalized.ptp_date = normalized.ptp_date || extractPtpDateFromSemanticText(`${normalized.notes}`);
  }

  if (normalized.final_response === null) {
    normalized.schedule_retry_hours = typeof normalized.schedule_retry_hours === 'number' ? normalized.schedule_retry_hours : 2;
    normalized.outcome_classified = false;
  } else {
    normalized.schedule_retry_hours = SEMANTIC_RETRY_HOURS[normalized.final_response] ?? 2;
    normalized.outcome_classified = true;
  }

  return normalized;
}

function analyzeCallWithSemanticModel({ duration = 0, transcript, notes, script }) {
  const customerContext = [transcript, notes, script].filter(Boolean).join(' ');
  const lowered = normalizeDemoSpeech(customerContext);
  const customerText = normalizeDemoSpeech(`${transcript || ''} ${notes || ''}`);

  if (!lowered || (duration === 0 && !String(transcript || '').trim())) {
    return finalizeSemanticAnalysis({
      final_response: null,
      ptp_date: null,
      ptp_status: null,
      notes: 'Customer ne call pick ya engage nahi ki, outcome classify nahi kiya gaya',
      schedule_retry_hours: 2,
    });
  }

  const refusalSignals = [
    /\bnahi\s+d(oonga|unga|oongi|ungi)\b/, /\bnahi\s+kar(unga|ungi)\b/, /\brefuse\b/, /\bno\s+payment\b/
  ];
  const abuseSignals = [
    /\bbhenchod\b/, /\bmadarchod\b/, /\bchutiya\b/, /\bfuck\b/, /\bshit\b/, /\bharamkhor\b/
  ];
  const switchedOffSignals = [
    /\bswitched\s+off\b/, /\bband\s+hai\b/, /\bout\s+of\s+coverage\b/, /\bnot\s+reachable\b/
  ];

  if (detectNonCustomerSemantic(transcript, notes, script)) {
    return finalizeSemanticAnalysis({ final_response: 'non_customer_pickup', ptp_date: null, ptp_status: null, notes: 'Non-customer ne call receive ki', schedule_retry_hours: 5 });
  }
  if (detectPaymentDoneSemantic(transcript, notes, script)) {
    return finalizeSemanticAnalysis({ final_response: 'payment_done', ptp_date: null, ptp_status: null, notes: 'Customer ne payment already done confirm ki', schedule_retry_hours: 0 });
  }

  const ptpDateSignal = /\b(kal|kl|parson|parso|tarson|narson|\d{1,2}\s*(ko|tk|tak|tarikh|date|april|may|june|july|march|february|january|august|september|october|november|december))\b/.test(customerText);
  const ptpCommitSignal = /\b(karunga|karungi|dunga|dungi|de\s+dunga|de\s+dungi|pay\s+kar|payment\s+kar|commit|pakka)\b/.test(customerText);
  const ptpPaymentIntent = /\b(payment|pay|paisa|paise|amount|due|dues|jama)\b/.test(customerText);
  const callbackOnlySignal = /\b(call\s+later|baad\s+mein\s+call|dobara\s+call|phir\s+call|abhi\s+busy|busy\s+hoon)\b/.test(customerText);
  if ((ptpCommitSignal || (ptpDateSignal && ptpPaymentIntent)) && !(callbackOnlySignal && !ptpPaymentIntent)) {
    return finalizeSemanticAnalysis({
      final_response: 'ptp_secured',
      ptp_date: extractPtpDateFromSemanticText(customerText),
      ptp_status: 'pending',
      notes: 'Customer ne payment commitment diya',
      schedule_retry_hours: 0,
    });
  }

  if (/\b(partial|aadha|half|kuch\s+amount|kam\s+amount)\b/.test(customerText) && /\b(pay|payment|dunga|dungi|karunga|karungi)\b/.test(customerText)) {
    return finalizeSemanticAnalysis({ final_response: 'partial_payment', ptp_date: null, ptp_status: null, notes: 'Customer ne partial payment ka signal diya', schedule_retry_hours: 2 });
  }
  if (/\b(busy|abhi\s+busy|later|baad\s+mein|thora\s+der|dobara\s+call)\b/.test(customerText) && duration <= 45) {
    return finalizeSemanticAnalysis({ final_response: duration <= 20 ? 'busy' : 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Customer busy tha aur callback request di', schedule_retry_hours: 2 });
  }
  if (detectCallbackRequestedSemantic(transcript, notes, script)) {
    return finalizeSemanticAnalysis({ final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Customer ne callback ya later request diya', schedule_retry_hours: 2 });
  }
  if (detectNegotiationBarrierSemantic(transcript, notes, script)) {
    return finalizeSemanticAnalysis({ final_response: 'negotiation_barrier', ptp_date: null, ptp_status: null, notes: 'Customer extension ya installment maang raha hai', schedule_retry_hours: 5 });
  }
  if (switchedOffSignals.some((p) => p.test(lowered))) {
    return finalizeSemanticAnalysis({ final_response: 'switched_off', ptp_date: null, ptp_status: null, notes: 'Number switched off ya unreachable tha', schedule_retry_hours: 2 });
  }
  if (abuseSignals.some((p) => p.test(lowered))) {
    return finalizeSemanticAnalysis({ final_response: 'abuse_detected', ptp_date: null, ptp_status: null, notes: 'Customer ne abusive language use ki', schedule_retry_hours: 24 });
  }
  if (refusalSignals.some((p) => p.test(lowered))) {
    return finalizeSemanticAnalysis({ final_response: 'refused', ptp_date: null, ptp_status: null, notes: 'Customer ne payment se inkaar kiya', schedule_retry_hours: 24 });
  }

  if (duration === 0) {
    return finalizeSemanticAnalysis({ final_response: null, ptp_date: null, ptp_status: null, notes: 'Phone pick nahi hua, outcome classify nahi kiya gaya', schedule_retry_hours: 2 });
  }
  if (duration <= 15) {
    return finalizeSemanticAnalysis({ final_response: 'non_customer_pickup', ptp_date: null, ptp_status: null, notes: 'Mukhtasar call hui, non-customer likely tha', schedule_retry_hours: 5, non_customer_relation: 'other' });
  }
  if (duration <= 45) {
    return finalizeSemanticAnalysis({ final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Dobara contact ki zarurat hai', schedule_retry_hours: 2 });
  }
  return finalizeSemanticAnalysis({ final_response: 'ptp_secured', ptp_date: extractPtpDateFromSemanticText(customerText), ptp_status: 'pending', notes: 'Semantic fallback mein payment commitment assume ki gayi', schedule_retry_hours: 0 });
}

// ─── Shared AI caller ────────────────────────────────────────────
async function callGemini(prompt, temperature = 0.7, maxTokens = 2048) {
  const apiKey = getGeminiApiKey();
  if (!apiKey) {
    log.fail('GEMINI', 'No GEMINI_API_KEY/API_KEY set in .env');
    return { ok: false, text: '', error: 'GEMINI_API_KEY or API_KEY not set' };
  }

  log.step('GEMINI', 'api-call', 1, `Sending prompt (${prompt.length} chars), temp=${temperature}, maxTokens=${maxTokens}`);
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_TEXT_MODEL}:generateContent?key=${apiKey.slice(0,8)}...`;
  log.step('GEMINI', 'api-call', 2, `POST to Gemini API`);

  try {
    const guardedPrompt = `${STRICT_ROMAN_URDU_GUARD}\n\n${prompt}`;
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_TEXT_MODEL}:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: guardedPrompt }] }],
          generationConfig: { temperature, maxOutputTokens: maxTokens },
        }),
      }
    );
    const data = await res.json();
    if (!res.ok) {
      log.fail('GEMINI', `API error (${res.status})`, JSON.stringify(data).slice(0, 300));
      return { ok: false, text: '', error: JSON.stringify(data) };
    }
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
    log.success('GEMINI', `Response received (${text.length} chars)`);
    return { ok: true, text };
  } catch (err) {
    log.fail('GEMINI', 'Network/fetch error', err.message);
    return { ok: false, text: '', error: err.message };
  }
}

function hasAiKey() {
  return !!getGeminiApiKey();
}

function normalizeDemoSpeech(text = '') {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s?]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isShortPickupReply(text = '') {
  const normalized = normalizeDemoSpeech(text);
  if (!normalized || normalized.length > 32) return false;

  return [
    /^(hello|helo)(?:\s+\w+){0,2}$/,
    /^(assalam|assalamualaikum|assalam o alaikum)(?:\s+\w+){0,2}$/,
    /^(ji|jee|haan|han|hmm|hmmm|kon|kaun|boliye)$/,
    /^(ji|jee)\s+(boliye|haan|han)$/,
    /^(kon|kaun)\s+hai$/,
  ].some((pattern) => pattern.test(normalized));
}

function hasImmediateEndSignal(text = '') {
  const normalized = normalizeDemoSpeech(text);
  if (!normalized) return false;
  return [
    'wrong number', 'ghalat number', 'galat number', 'woh nahi hain', 'wo nahi hain',
    'ye unka number nahi', 'ye mera number nahi', 'abhi busy hoon', 'main busy hoon',
    'baad mein call', 'dobara call', 'call later', 'refuse', 'nahi baat kar sakta', 'nahi baat kar sakti'
  ].some((phrase) => normalized.includes(phrase));
}

function detectPaymentDoneSemantic(...chunks) {
  const text = chunks
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  const normalizedText = text
    .replace(/\bpe\s*ment\b/g, 'payment')
    .replace(/\bpay\s*ment\b/g, 'payment')
    .replace(/\bpe\s*kr\b/g, 'pay kar')
    .replace(/\bpay\s*kr\b/g, 'pay kar')
    .replace(/\bkar\s*chu\s*ka\b/g, 'kar chuka')
    .replace(/\bkr\s*chu\s*ka\b/g, 'kar chuka')
    .replace(/\bde\s*chu\s*ka\b/g, 'de chuka');

  const futureIntentPatterns = [
    /kal\s+pay/, /pay\s+kar(na|unga|ungi|dunga|dungi)/, /payment\s+kar(na|unga|ungi|dunga|dungi)/,
    /installment/, /qist/, /settlement/, /arrange\s+kar/, /baad\s+mein\s+pay/
  ];
  if (futureIntentPatterns.some((p) => p.test(normalizedText))) return false;

  const paidSignals = [
    /already\s+paid/, /i\s+have\s+paid/, /i\s+already\s+paid/, /payment\s+done/,
    /paid\s+it/, /bill\s+paid/, /dues\s+clear/, /clear\s+kar\s+di/, /clear\s+kr\s+di/,
    /jama\s+kar\s+di/, /payment\s+kar\s+di/, /payment\s+kr\s+di/, /pay\s+kar\s+di/,
    /payment\s+ho\s+chuk[ai]/, /payment\s+already\s+ho\s+gayi/,
    /pay\s+kar\s+chuk[ai]/, /payment\s+kar\s+chuk[ai]/, /de\s+chuk[ai]/,
    /transaction\s+(ho\s+gayi|done)/, /amount\s+deduct\s+ho\s+gaya/, /reference\s+number/
  ];

  const proofSignals = [
    /trx/, /transaction\s*id/, /reference\s*(id|number)?/, /receipt/, /screenshot/, /sms\s+aya/
  ];

  const strongPaidClaimSignals = [
    /\b(main|mai|mein|mn|i)\b.*\b(pay|payment|de)\b.*\bkar\b.*\bchuk[ai]\b/,
    /\b(main|mai|mein|mn|i)\b.*\b(pay|payment)\b.*\bkar\b.*\bdi(y|)a\b/,
    /\b(main|mai|mein|mn|i)\b.*\bde\s+chuk[ai]\b/,
    /\balready\b.*\b(pay|payment|paid)\b/,
  ];

  const paidMatchCount = paidSignals.reduce((count, pattern) => count + (pattern.test(normalizedText) ? 1 : 0), 0);
  const hasProofSignal = proofSignals.some((p) => p.test(normalizedText));
  const hasStrongPaidClaim = strongPaidClaimSignals.some((p) => p.test(normalizedText));

  return hasStrongPaidClaim || (paidMatchCount >= 1 && (paidMatchCount >= 2 || hasProofSignal || normalizedText.includes('already')));
}

function detectCallbackRequestedSemantic(...chunks) {
  const text = chunks
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  const callbackSignals = [
    /\bcall\s+later\b/, /\bbaad\s+mein\b/, /\bdobara\s+call\b/, /\bphir\s+call\b/,
    /\babhi\s+busy\b/, /\bbusy\s+hoon\b/, /\babhi\s+free\s+nahi\b/, /\bthori\s+der\s+baad\b/,
    /\b(10|15|20|30|\d+)\s*(min|minute|minut|mint)\b/, /\b(1|2|3|\d+)\s*(ghanta|ghante|hour|hours)\b/,
  ];

  return callbackSignals.some((p) => p.test(text));
}

function detectNegotiationBarrierSemantic(...chunks) {
  const text = chunks
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  const barrierSignals = [
    /\binstallment\b/, /\bqist\b/, /\bextension\b/, /\bwaqt\s+chahiye\b/,
    /\bek\s+mahina\b/, /\bnext\s+month\b/, /\bagle\s+mahine\b/, /\bmonth\s+end\b/,
    /\bsettlement\b/, /\bkam\s+kar\s+do\b/, /\bthora\s+kam\b/, /\brestructure\b/
  ];

  return barrierSignals.some((p) => p.test(text));
}

function detectNonCustomerSemantic(...chunks) {
  const text = chunks
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!text) return false;

  const nonCustomerSignals = [
    /\bwrong\s+number\b/, /\bgalat\s+number\b/, /\bghalat\s+number\b/,
    /\b(main|mai|mein|mn)\b.*\b(nahi|nhi)\b.*\b(hun|hoon)\b/,
    /\b(woh|wo)\b.*\b(nahi\s+hain|available\s+nahi)\b/,
    /\b(main|mai|mein)\b.*\b(unka|unki|inka|inki)\b/,
    /\b(mujhe\s+nahi\s+pata|nahi\s+jaanta|nahi\s+jaanti)\b/,
    /\bmessage\s+nahi\s+de\s+sakt/,
  ];

  return nonCustomerSignals.some((p) => p.test(text));
}

// ─── Generate Script ─────────────────────────────────────────────
router.post('/generate-script', async (req, res) => {
  log.divider('GENERATE SCRIPT');
  try {
    const { customerName, balance, dpd, agentType, tone, ptpStatus, followUpCount, callerName, systemPrompt, temperature, maxTokens, maxPtpDays, customerGender, callerGender } = req.body;
    const fallbackScript = buildFallbackScript({ customerName, balance, dpd, agentType, tone, callerName, maxPtpDays, customerGender, callerGender });

    log.step('AI', 'gen-script', 1, 'Request received', { customerName, agentType, tone, balance, dpd });

    if (!customerName || !agentType || !tone) {
      log.fail('AI', 'Missing required fields: customerName, agentType, or tone');
      return res.status(400).json({ error: 'customerName, agentType, and tone are required' });
    }

    if (!hasAiKey()) {
      log.warn('AI', 'No Gemini key — using fallback script');
      return res.json({ success: true, fallback: true, script: fallbackScript, callerName: callerName || 'Omar' });
    }

    log.step('AI', 'gen-script', 2, 'Building prompt', { ptpStatus, followUpCount, callerName: callerName || 'Omar' });

    const toneInstructions = {
      polite: 'Bohat izzat aur adab se baat karo, customer ko aaram se samjhao. Soft magar professional tone rakho.',
      assertive: `Firm, bold aur sakht andaz mein baat karo. Payment ki seriousness CLEARLY batao. Agar dues zyada hain to customer ko batao ke yeh account ab serious stage par hai, legal proceedings ya reporting ka risk hai. Account freeze, CIBIL/credit bureau reporting, aur recovery proceedings ka mention karo. Customer ko samjhao ke ab mazeed delay afford NAHI kar sakte. Tone strict rakho lekin abusive NAHI — professional firmness with urgency. DPD zyada ho to BOLDLY bolo: "Aap ka account ${dpd || 'kaafi'} din se overdue hai, yeh ab bohot serious matter hai."`,
      empathetic: 'Customer ki mushkil samjho, unki situation ka ehsaas dikhao. Supportive tone rakho lekin payment ki taraf guide karo.',
    };
    const agentInstructions = {
      fresh_call: 'Yeh pehli baar call hai. Customer ko introduce karo aur balance ke baare mein batao.',
      broken_promise: 'Customer ne pehle promise kiya tha lekin payment nahi ki. Serious tone.',
      ptp_reminder: 'Kal PTP date hai. Customer ko yaad dilao.',
      ptp_followup: 'Aaj PTP date hai. Check karo ke payment hui ya nahi.',
      non_customer: 'Kisi aur ne phone uthaya. Politely message choro.',
      no_answer: 'Phone nahi uthaya ya band hai. Voicemail ya SMS choro.',
      after_hours: 'Office hours ke baad hai. Kal subah retry schedule karo.',
      negotiation: 'Customer extension maang raha hai. Flexibility dikhao.',
      escalation: 'High priority account. Supervisor attention zaroori hai.',
      general_inquiry: 'Customer ke general sawaal ka jawab do.',
    };

    const ptpLimit = maxPtpDays || 5;
    const basePrompt = systemPrompt
      ? systemPrompt.replace(/{maxPtpDays}/g, String(ptpLimit))
      : `Tum ek AI debt collection voice agent ho Pakistani bank ke liye.\nSIRF Roman Urdu mein script generate karo.\nScript mein Opening, Main Body, PTP Negotiation, Objection Handling, Closing include karo.\nPTP maximum ${ptpLimit} din.`;

    const cG = (customerGender || 'male').toLowerCase();
    const aG = resolveCallerGender(callerGender, callerName || 'Omar');
    const genderNote = `\n\nGENDER RULES:\n- Customer gender: ${cG}. Address as "${cG === 'female' ? 'Sahiba/Madam' : 'Sahab/Sir'}".\n- Agent gender: ${aG}. Use "${aG === 'female' ? 'bol rahi/chahti/samajh gayi/karungi/karti/sakti' : 'bol raha/chahta/samajh gaya/karunga/karta/sakta'}" verb forms. NEVER mix male/female forms.`;
    const openingTemplate = `Assalam-o-Alaikum ${customerName} ${cG === 'female' ? 'Sahiba' : 'Sahab'}, main JS Bank se ${callerName || 'Omar'} ${aG === 'female' ? 'bol rahi' : 'bol raha'} hoon. Yeh call aapke pending dues ke silsile mein hai.`;
    const prompt = `${basePrompt}\n\nCustomer Details:\n- Naam: ${customerName}\n- Gender: ${cG}\n- Outstanding Balance: PKR ${Number(balance).toLocaleString()}\n- Days Past Due (DPD): ${dpd}\n- PTP Status: ${ptpStatus || 'None'}\n- Follow-up Count: ${followUpCount || 0}\n- Caller Name: ${callerName || 'Omar'}\n- Caller Gender: ${aG}\n\nAgent Type: ${agentType}\nInstructions: ${agentInstructions[agentType] || 'General call karo.'}\n\nTone: ${tone}\nTone Instructions: ${toneInstructions[tone] || 'Professional baat karo.'}\n\nCaller ${callerName || 'Omar'} ki taraf se call hai.${genderNote}\n\nSTRICT OPENING RULE (MANDATORY):\n- Opening line MUST be EXACTLY this sentence:\n${openingTemplate}\n- NEVER say: "AI voice agent" or "AI agent" in script.\n- Keep caller identity as the real caller name only.`;

    log.step('AI', 'gen-script', 3, 'Calling Gemini API...');
    const result = await callGemini(prompt, temperature ?? 0.7, maxTokens ?? 2048);
    if (!result.ok || !result.text?.trim()) {
      log.warn('AI', 'Script generation unavailable — using fallback script', result.error || 'Empty AI response');
      return res.json({ success: true, fallback: true, script: fallbackScript, callerName: callerName || 'Omar' });
    }

    const normalizedScript = enforceCallerIdentityInScript(result.text, {
      customerName,
      customerGender,
      callerName: callerName || 'Omar',
      callerGender: aG,
    });
    log.success('AI', `Script generated for ${customerName} (${normalizedScript.length} chars)`);
    res.json({ success: true, script: normalizedScript, callerName: callerName || 'Omar' });
  } catch (err) {
    log.error('AI', 'generate-script exception', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Analyze Call ────────────────────────────────────────────────
router.post('/analyze-call', async (req, res) => {
  log.divider('ANALYZE CALL');
  try {
    const { customerName, balance, dpd, agentType, tone, ptpStatus, followUpCount, duration, script, transcript, notes } = req.body;

    log.step('AI', 'analyze', 1, 'Request received', { customerName, duration, agentType, tone });

    if (!customerName) {
      log.fail('AI', 'Missing customerName');
      return res.status(400).json({ error: 'customerName is required' });
    }

    log.step('AI', 'analyze', 2, 'Using semantic/regex outcome model (Gemini outcome extraction disabled)');
    const result = analyzeCallWithSemanticModel({ duration, transcript, notes, script });
    log.success('AI', `Final semantic analysis: ${result.final_response}`, { ptp_date: result.ptp_date, retry: result.schedule_retry_hours });
    res.json({ success: true, semantic: true, ...result });
  } catch (err) {
    log.error('AI', 'analyze-call exception', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Testing Conversation (browser-based voice testing) ─────────
router.post('/testing-conversation', async (req, res) => {
  log.divider('TESTING CONVERSATION');
  try {
    const { customerName, balance, dpd, agentType, tone, ptpStatus, followUpCount, callerName, maxPtpDays, action, conversationHistory, customerSpeech, agentWasSaying, customerGender, callerGender } = req.body;

    if (!action) return res.status(400).json({ error: 'action is required' });

    const maxPtp = maxPtpDays || 5;
    const maxPtpDate = new Date(Date.now() + maxPtp * 86400000).toISOString().split('T')[0];
    const name = callerName || 'Omar';
    const cGender = (customerGender || 'male').toLowerCase();
    const aGender = (callerGender || 'male').toLowerCase();
    const honor = cGender === 'female' ? 'Sahiba' : 'Sahab';
    const sirMadam = cGender === 'female' ? 'Madam' : 'Sir';
    const bolVerb = aGender === 'female' ? 'bol rahi' : 'bol raha';
    const chahVerb = aGender === 'female' ? 'chahti' : 'chahta';

    if (action === 'greeting') {
      const prompt = `Tum ${name} ho (${aGender}), ek AI debt collection agent Pakistani bank ke liye. Customer ${customerName} (${cGender}) ko call karo. Customer ko "${honor}" ya "${sirMadam}" bolo. Balance PKR ${Number(balance).toLocaleString()}, ${dpd} din overdue. Pehle greeting do — salam, apna naam, aur pucho ke kya baat kar sakte ho. Apne liye "${bolVerb}" aur "${chahVerb}" verb forms use karo. 2-3 sentences max. Roman Urdu mein.`;
      if (!hasAiKey()) return res.json({ success: true, text: `Assalam o Alaikum ${customerName} ${honor}, main ${name} ${bolVerb} hoon bank ki taraf se. Kya main ${customerName} ${honor} se baat kar ${aGender === 'female' ? 'rahi' : 'raha'} hoon?` });
      const result = await callGemini(prompt, 0.7, 256);
      return res.json({ success: true, text: result.ok ? result.text : `Assalam o Alaikum ${customerName} ${honor}, main ${name} ${bolVerb} hoon bank ki taraf se.` });
    }

    if (action === 'respond' || action === 'interrupted') {
      const history = (conversationHistory || []).map(m => `${m.role === 'agent' ? name : 'Customer'}: ${m.text}`).join('\n');
      const customerTurns = (conversationHistory || []).filter((m) => m.role === 'customer').length;
      const earlyPickupReply = isShortPickupReply(customerSpeech);
      const immediateEndSignal = hasImmediateEndSignal(customerSpeech);

      if (earlyPickupReply && customerTurns <= 1 && !immediateEndSignal) {
        log.warn('AI', 'Blocked early demo disconnect on short pickup reply', { action, customerSpeech, customerTurns });
        return res.json({
          success: true,
          text: `Ji, assalam o alaikum ${customerName} ${honor}. Main ${name} bank ki taraf se ${bolVerb} hoon. Kya main ${customerName} ${honor} se baat kar ${aGender === 'female' ? 'rahi' : 'raha'} hoon?`,
          endCall: false,
          guarded: true,
          reason: 'short_pickup_reply',
        });
      }

      let prompt = `Tum ${name} ho (${aGender}) — debt collection agent. Customer: ${customerName} (${cGender}, address as "${honor}" or "${sirMadam}"), Balance: PKR ${Number(balance).toLocaleString()}, DPD: ${dpd}, PTP Status: ${ptpStatus || 'None'}.\nMax PTP date: ${maxPtpDate}\nIMPORTANT: Use correct gender verb forms — for yourself use "${bolVerb}", "${chahVerb}" etc. Address customer as "${honor}" or "${sirMadam}", NEVER as "${cGender === 'female' ? 'Sir/Sahab' : 'Madam/Sahiba'}".\n\nConversation:\n${history}\n\n`;
      if (action === 'interrupted') {
        prompt += `Customer ne tumhe interrupt kiya jab tum keh rahe the: "${agentWasSaying}". Customer ne kaha: "${customerSpeech}". Natural response do — acknowledge interruption. Roman Urdu, 1-3 sentences. Agar customer sirf hello/ji/kon keh raha hai to foran apna intro dobara clear tareeqay se do aur conversation continue karo. Is surat mein call band mat karo.`;
      } else {
        prompt += `Customer ne kaha: "${customerSpeech}". Respond naturally. Roman Urdu, 1-3 sentences. Agar customer ka jawab sirf short pickup reply ho jaise "hello", "ji", "kon", "haan", ya "assalam o alaikum", to tumhara kaam apna intro dobara dena aur agla step continue karna hai — is stage par call khatam nahi karni. [END_CALL] sirf tab likho jab wrong number/non-customer confirm ho, customer wazeh tor par mana kare, busy/callback clearly bole, ya proper closing ho chuki ho.`;
      }
      prompt += '\nReturn ONLY your response text, no JSON.';

      if (!hasAiKey()) return res.json({ success: true, text: 'Ji, main samajh gayi. Aap ki payment ke baare mein baat karte hain.' });
      const result = await callGemini(prompt, 0.7, 256);
      const text = result.ok ? result.text : 'Ji, main samajh gayi.';
      const aiRequestedEnd = text.includes('[END_CALL]');
      const safeToEnd = aiRequestedEnd && (immediateEndSignal || (!earlyPickupReply && customerTurns >= 2));
      if (aiRequestedEnd && !safeToEnd) {
        log.warn('AI', 'Blocked unsafe demo end request', { action, customerSpeech, customerTurns, response: text.slice(0, 200) });
      }
      return res.json({ success: true, text: text.replace('[END_CALL]', '').trim(), endCall: safeToEnd });
    }

    if (action === 'analyze') {
      const history = (conversationHistory || []).map(m => `${m.role === 'agent' ? name : 'Customer'}: ${m.text}`).join('\n');
      const today = new Date().toISOString().split('T')[0];
      const prompt = `Yeh ek test call ki transcript hai. Analyze karo:\n${history}\n\nReturn ONLY valid JSON:\n{"final_response":"ptp_secured|no_answer|non_customer_pickup|callback_requested|refused|negotiation_barrier|partial_payment","ptp_date":"YYYY-MM-DD or null (${today} to ${maxPtpDate})","ptp_status":"pending or null","notes":"Roman Urdu 1 line","schedule_retry_hours":number}`;

      if (!hasAiKey()) {
        const turns = (conversationHistory || []).length;
        const mock = turns > 6
          ? { final_response: 'ptp_secured', ptp_date: new Date(Date.now() + 3 * 86400000).toISOString().split('T')[0], ptp_status: 'pending', notes: 'Mock: PTP secured', schedule_retry_hours: 0 }
          : { final_response: 'callback_requested', ptp_date: null, ptp_status: null, notes: 'Mock: Callback requested', schedule_retry_hours: 2 };
        return res.json({ success: true, mock: true, ...mock });
      }

      const result = await callGemini(prompt, 0.2, 512);
      try {
        let raw = (result.text || '').trim();
        if (raw.startsWith('```')) raw = raw.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
        const parsed = JSON.parse(raw);
        if (parsed.final_response !== 'ptp_secured') { parsed.ptp_date = null; parsed.ptp_status = null; }
        return res.json({ success: true, ...parsed });
      } catch {
        return res.json({ success: true, final_response: 'callback_requested', notes: 'Parse error', schedule_retry_hours: 2 });
      }
    }

    res.status(400).json({ error: `Unknown action: ${action}` });
  } catch (err) {
    log.error('AI', 'testing-conversation exception', err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
