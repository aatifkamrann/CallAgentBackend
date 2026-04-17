/**
 * Centralized Logger — CLEAN console (TIMING only) + full file logging.
 * Console shows: Server ready + TIMING milestones + errors only.
 * Everything else → file only (server/applogs.txt).
 */

import { appendFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const COLORS = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOG_FILE_PATH = path.join(__dirname, 'applogs.txt');

function ts() {
  return new Date().toISOString();
}

function pad(str, len) {
  return str.padEnd(len);
}

function sanitizeText(value) {
  if (value === undefined || value === null) return value;
  return String(value)
    .replace(/(?:Ã.|Â.|â.|€™|€œ|€|™|œ|ž|š)+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeData(value) {
  if (value === undefined || value === null) return value;
  if (typeof value === 'string') return sanitizeText(value);
  if (Array.isArray(value)) return value.map(sanitizeData);
  if (value instanceof Error) {
    return {
      message: sanitizeText(value.message),
      stack: sanitizeText(value.stack || ''),
    };
  }
  if (typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, sanitizeData(item)]));
  }
  return value;
}

function serialize(data) {
  if (data === undefined || data === null || data === '') return '';
  if (data instanceof Error) {
    const message = sanitizeText(data.message);
    const stack = sanitizeText(data.stack || '');
    return `${message}${stack ? ` | ${stack}` : ''}`;
  }
  if (typeof data === 'string') return sanitizeText(data);
  try {
    return JSON.stringify(sanitizeData(data));
  } catch {
    return sanitizeText(String(data));
  }
}

function writeToFile(level, module, msg, data) {
  const details = serialize(data);
  const cleanMsg = sanitizeText(msg);
  const line = `[${ts()}] ${pad(level, 7)} [${pad(module, 12).trimEnd()}] ${cleanMsg}${details ? ` ${details}` : ''}\n`;
  try {
    appendFileSync(LOG_FILE_PATH, line, 'utf8');
  } catch {
  }
}

function logToConsole(method, formatted, data) {
  const cleanFormatted = sanitizeText(formatted);
  const cleanData = sanitizeData(data);
  if (data !== undefined) {
    console[method](cleanFormatted, cleanData);
    return;
  }
  console[method](cleanFormatted);
}

const log = {
  filePath: LOG_FILE_PATH,

  // Console + file — ONLY for TIMING milestones and critical info
  info(module, msg, data) {
    // Only show TIMING and MEDIA transcripts on console
    if (module === 'TIMING' || msg.includes('🎤') || msg.includes('🤖 Agent (top-transcript)')) {
      logToConsole('log', `${COLORS.gray}[${ts()}]${COLORS.reset} ${COLORS.green}INFO ${COLORS.reset} ${COLORS.cyan}[${pad(module, 12)}]${COLORS.reset} ${msg}`, data);
    }
    writeToFile('INFO', module, msg, data);
  },

  // File only — no console noise
  warn(module, msg, data) {
    writeToFile('WARN', module, msg, data);
  },

  // Console + file — errors always visible
  error(module, msg, data) {
    logToConsole('error', `${COLORS.gray}[${ts()}]${COLORS.reset} ${COLORS.red}ERROR${COLORS.reset} ${COLORS.cyan}[${pad(module, 12)}]${COLORS.reset} ${msg}`, data);
    writeToFile('ERROR', module, msg, data);
  },

  // File only
  step(module, operation, stepNum, msg, data) {
    writeToFile('STEP', module, `${operation} #${stepNum} → ${msg}`, data);
  },

  // File only
  success(module, msg, data) {
    writeToFile('SUCCESS', module, msg, data);
  },

  // Console + file — failures always visible
  fail(module, msg, data) {
    logToConsole('error', `${COLORS.gray}[${ts()}]${COLORS.reset} ${COLORS.red}❌ FAIL${COLORS.reset} ${COLORS.cyan}[${pad(module, 12)}]${COLORS.reset} ${msg}`, data);
    writeToFile('FAIL', module, msg, data);
  },

  // File only
  request(method, routePath, body) {
    const bodyPreview = body ? serialize(body).slice(0, 300) : '';
    writeToFile('REQUEST', 'HTTP', `${method} ${routePath}`, bodyPreview);
  },

  // File only
  response(method, routePath, status, duration) {
    writeToFile('RESPONSE', 'HTTP', `${method} ${routePath} ${status} (${duration}ms)`);
  },

  // Console + file — section dividers
  divider(label) {
    const line = `${'─'.repeat(20)} ${label} ${'─'.repeat(40)}`;
    console.log(`${COLORS.gray}${line}${COLORS.reset}`);
    writeToFile('DIVIDER', 'SYSTEM', line);
  },
};

export { sanitizeText as sanitizeLogText };

export default log;
