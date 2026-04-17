/**
 * AI Voice Agent — Express Server
 * SQLite database, Gemini Native Audio AI, Twilio calls with Media Streams
 * Dashboard WebSocket for real-time telemetry
 */

import 'dotenv/config';

// Force UTF-8 output so Unicode log characters (arrows, emoji) render correctly
// on Windows terminals. Must be set before any logging occurs.
if (process.platform === 'win32') {
  try { process.stdout.setDefaultEncoding('utf8'); } catch {}
  try { process.stderr.setDefaultEncoding('utf8'); } catch {}
}

import express from 'express';
import cors from 'cors';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import log from './logger.js';

import customersRouter from './routes/customers.js';
import callLogsRouter from './routes/call-logs.js';
import aiRouter from './routes/ai.js';
import callsRouter from './routes/calls.js';
import voicePreviewRouter from './routes/voice-preview.js';
import { handleMediaStream, registerDashboardClient, makeCallInternal, getActiveCallCount, getActiveCalls, setAfterCallHook } from './routes/calls.js';
import { startScheduler, stopScheduler, isSchedulerEnabled, getSchedulerStatus, triggerAllOverdue, setMakeCallFunction, updateSchedulerConfig, onCallCompleted } from './scheduler.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3001;
const AUTH_USERNAME = (process.env.APP_AUTH_USERNAME || '').trim();
const AUTH_PASSWORD = process.env.APP_AUTH_PASSWORD || '';
const AUTH_TOKEN_SECRET = process.env.APP_AUTH_SECRET || '';
const LOCAL_AUTH_ENABLED = Boolean(AUTH_USERNAME && AUTH_PASSWORD && AUTH_TOKEN_SECRET);
const AUTH_TOKEN_TTL_MS = 24 * 60 * 60 * 1000;

const HEALTH_TIMEOUT_MS = 5000;
const GEMINI_NATIVE_MODEL = 'gemini-2.5-flash-native-audio-preview-12-2025';

function getGeminiApiKey() {
  return process.env.GEMINI_API_KEY || process.env.API_KEY || '';
}

function getServerBaseUrl() {
  return process.env.SERVER_URL || process.env.BASE_URL || '';
}

async function fetchWithTimeout(url, options = {}, timeout = HEALTH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function toBase64Url(value) {
  return Buffer.from(value).toString('base64url');
}

function signAuthPayload(payload) {
  if (!AUTH_TOKEN_SECRET) return '';
  return crypto.createHmac('sha256', AUTH_TOKEN_SECRET).update(payload).digest('base64url');
}

function createAuthToken(username) {
  if (!LOCAL_AUTH_ENABLED) return '';
  const payload = JSON.stringify({ sub: username, exp: Date.now() + AUTH_TOKEN_TTL_MS });
  const encodedPayload = toBase64Url(payload);
  const signature = signAuthPayload(encodedPayload);
  if (!signature) return '';
  return `${encodedPayload}.${signature}`;
}

function verifyAuthToken(token) {
  if (!LOCAL_AUTH_ENABLED) return null;
  if (!token || typeof token !== 'string' || !token.includes('.')) return null;
  const [encodedPayload, signature] = token.split('.');
  if (!encodedPayload || !signature) return null;

  const expectedSignature = signAuthPayload(encodedPayload);
  const actual = Buffer.from(signature);
  const expected = Buffer.from(expectedSignature);
  if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) return null;

  try {
    const payload = JSON.parse(Buffer.from(encodedPayload, 'base64url').toString('utf8'));
    if (!payload?.sub || !payload?.exp || payload.exp < Date.now()) return null;
    return payload;
  } catch {
    return null;
  }
}

function getAuthTokenFromRequest(req) {
  const header = req.headers.authorization || '';
  if (header.startsWith('Bearer ')) return header.slice(7).trim();
  return '';
}

function isPublicApiPath(pathname) {
  return pathname === '/api/auth/login'
    || pathname === '/api/auth/session'
    || pathname === '/api/health'
    || pathname.startsWith('/api/twiml')
    || pathname.startsWith('/api/call-status')
    || pathname.startsWith('/api/recording-status')
    || pathname.startsWith('/api/recordings/');
}

async function checkGeminiHealth() {
  const apiKey = getGeminiApiKey();
  if (!apiKey) {
    return {
      configured: false,
      ready: false,
      mode: 'mock',
      model: GEMINI_NATIVE_MODEL,
      message: 'GEMINI_API_KEY or API_KEY missing',
    };
  }

  try {
    const response = await fetchWithTimeout(`https://generativelanguage.googleapis.com/v1alpha/models?key=${apiKey}`);
    if (!response.ok) {
      const details = await response.text();
      return {
        configured: true,
        ready: false,
        mode: 'live',
        model: GEMINI_NATIVE_MODEL,
        message: `Gemini check failed (${response.status}) ${details.slice(0, 120)}`,
      };
    }

    return {
      configured: true,
      ready: true,
      mode: 'live',
      model: GEMINI_NATIVE_MODEL,
      message: 'Gemini API reachable',
    };
  } catch (err) {
    return {
      configured: true,
      ready: false,
      mode: 'live',
      model: GEMINI_NATIVE_MODEL,
      message: `Gemini check error: ${err.message}`,
    };
  }
}

async function checkTwilioHealth() {
  // Strip any accidental quotes/whitespace from env vars
  const accountSid = (process.env.TWILIO_ACCOUNT_SID || '').replace(/['"]/g, '').trim();
  const authToken = (process.env.TWILIO_AUTH_TOKEN || '').replace(/['"]/g, '').trim();
  const twilioPhone = (process.env.TWILIO_PHONE_NUMBER || '').replace(/['"]/g, '').trim();
  const serverUrl = getServerBaseUrl();

  // Debug log (safe — only shows prefix/length)
  // Debug info logged to file only
  log.info('TWILIO', `SID: ${accountSid ? accountSid.slice(0,6) + '...' + accountSid.slice(-4) : 'EMPTY'} (${accountSid.length}), Token: ${authToken ? authToken.length + ' chars' : 'EMPTY'}, Phone: ${twilioPhone || 'EMPTY'}`);

  if (!accountSid || !authToken || !twilioPhone) {
    return {
      configured: false,
      ready: false,
      mode: 'mock',
      phoneConfigured: !!twilioPhone,
      serverUrlConfigured: !!serverUrl,
      message: 'Twilio credentials or phone number missing',
    };
  }

  if (!serverUrl) {
    return {
      configured: true,
      ready: false,
      mode: 'live',
      phoneConfigured: true,
      serverUrlConfigured: false,
      message: 'SERVER_URL or BASE_URL missing for Twilio webhooks/media streams',
    };
  }

  try {
    const response = await fetchWithTimeout(`https://api.twilio.com/2010-04-01/Accounts/${accountSid}.json`, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${accountSid}:${authToken}`).toString('base64'),
      },
    });

    if (!response.ok) {
      const details = await response.text();
      return {
        configured: true,
        ready: false,
        mode: 'live',
        phoneConfigured: true,
        serverUrlConfigured: true,
        message: `Twilio check failed (${response.status}) ${details.slice(0, 120)}`,
      };
    }

    return {
      configured: true,
      ready: true,
      mode: 'live',
      phoneConfigured: true,
      serverUrlConfigured: true,
      message: 'Twilio account and webhook base URL look ready',
    };
  } catch (err) {
    return {
      configured: true,
      ready: false,
      mode: 'live',
      phoneConfigured: true,
      serverUrlConfigured: true,
      message: `Twilio check error: ${err.message}`,
    };
  }
}

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

app.use((req, res, next) => {
  if (!req.path.startsWith('/api') || isPublicApiPath(req.path)) {
    next();
    return;
  }

  const payload = verifyAuthToken(getAuthTokenFromRequest(req));
  if (!payload) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  req.authUser = { username: payload.sub };
  next();
});

// ─── Request/Response Logger — SILENT (logs to file only, not console) ──
app.use((req, res, next) => {
  const start = Date.now();
  const originalSend = res.send.bind(res);
  res.send = (body) => {
    log.response(req.method, req.path, res.statusCode, Date.now() - start);
    return originalSend(body);
  };
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    log.response(req.method, req.path, res.statusCode, Date.now() - start);
    return originalJson(body);
  };
  next();
});

app.post('/api/auth/login', (req, res) => {
  if (!LOCAL_AUTH_ENABLED) {
    res.status(404).json({ error: 'Local auth is disabled' });
    return;
  }

  const username = String(req.body?.username || '').trim();
  const password = String(req.body?.password || '');
  const normalizedInput = username.toLowerCase();
  const normalizedConfigured = AUTH_USERNAME.toLowerCase();
  const configuredAlias = `${normalizedConfigured}@astrikdigital`;
  const userMatches = normalizedInput === normalizedConfigured || normalizedInput === configuredAlias;

  if (!userMatches || password !== AUTH_PASSWORD) {
    res.status(401).json({ error: 'Invalid username or password' });
    return;
  }

  const token = createAuthToken(AUTH_USERNAME);
  res.json({ token, user: { email: AUTH_USERNAME } });
});

app.get('/api/auth/session', (req, res) => {
  if (!LOCAL_AUTH_ENABLED) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  const payload = verifyAuthToken(getAuthTokenFromRequest(req));
  if (!payload) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  res.json({ user: { email: payload.sub } });
});

// ─── API Routes ──────────────────────────────────────────────────
app.use('/api/customers', customersRouter);
app.use('/api/call-logs', callLogsRouter);
app.use('/api/ai', aiRouter);
app.use('/api', callsRouter);
app.use('/api', voicePreviewRouter);

// ─── Scheduler Routes ────────────────────────────────────────────
app.get('/api/scheduler/status', (req, res) => {
  res.json(getSchedulerStatus());
});
app.post('/api/scheduler/start', (req, res) => {
  startScheduler();
  res.json({ success: true, enabled: true });
});
app.post('/api/scheduler/stop', (req, res) => {
  stopScheduler();
  res.json({ success: true, enabled: false });
});
app.post('/api/scheduler/trigger', async (req, res) => {
  const result = await triggerAllOverdue();
  res.json(result);
});

app.post('/api/scheduler/config', (req, res) => {
  const { maxConcurrentCalls, interCallDelaySec, autoDialBatchSize,
    retryNoAnswerHours, retryNonCustomerHours, retryCallbackHours, retryRefusedHours,
    schedulerPollMinutes } = req.body || {};
  updateSchedulerConfig({ maxConcurrentCalls, interCallDelaySec, autoDialBatchSize,
        schedulerPollMinutes,
                          retryNoAnswerHours, retryNonCustomerHours, retryCallbackHours, retryRefusedHours });
  res.json({ success: true });
});

// ─── Health Check ────────────────────────────────────────────────
app.get('/api/health', async (req, res) => {
  log.info('HEALTH', 'Health check requested');

  const [gemini, twilio] = await Promise.all([
    checkGeminiHealth(),
    checkTwilioHealth(),
  ]);

  const scheduler = getSchedulerStatus();
  const database = {
    ready: true,
    type: 'SQLite',
    path: 'server/data/voice_agent.db',
  };

  res.json({
    status: gemini.ready && twilio.ready ? 'ok' : 'degraded',
    liveModeReady: gemini.ready && twilio.ready,
    database,
    gemini,
    twilio,
    mediaStreams: true,
    nativeAudio: true,
    activeCalls: {
      count: getActiveCallCount(),
      calls: getActiveCalls(),
    },
    scheduler: {
      enabled: isSchedulerEnabled(),
      ...scheduler,
    },
    logging: {
      ready: true,
      file: 'server/applogs.txt',
    },
    timestamp: new Date().toISOString(),
  });
});

// ─── Serve frontend in production ────────────────────────────────
const distPath = path.join(__dirname, '..', 'dist');
app.use(express.static(distPath));
app.get('*', (req, res) => {
  if (!req.path.startsWith('/api')) {
    res.sendFile(path.join(distPath, 'index.html'));
  }
});

// ─── Global Error Handler ────────────────────────────────────────
app.use((err, req, res, next) => {
  log.error('SERVER', `Unhandled error on ${req.method} ${req.path}`, err.message);
  log.error('SERVER', 'Stack trace', err.stack);
  res.status(500).json({ error: 'Internal server error', message: err.message });
});

// ─── Process-level crash protection — never let one call crash the whole server ──
process.on('uncaughtException', (err) => {
  log.error('PROCESS', `⚠️ UNCAUGHT EXCEPTION (server stays alive): ${err.message}`);
  log.error('PROCESS', 'Stack:', err.stack);
});
process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? reason.stack : '';
  log.error('PROCESS', `⚠️ UNHANDLED REJECTION (server stays alive): ${msg}`);
  if (stack) log.error('PROCESS', 'Stack:', stack);
});

// ─── Create HTTP server + WebSocket servers ──────────────────────
const server = createServer(app);

const wssMedia = new WebSocketServer({ noServer: true });
const wssDashboard = new WebSocketServer({ noServer: true });

// Handle WebSocket upgrades for both /media-stream and /dashboard paths
server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);

  if (url.pathname === '/media-stream') {
    wssMedia.handleUpgrade(request, socket, head, (ws) => {
      const conversationId = url.searchParams.get('conversationId') || '';
      ws._wsKind = 'media';
      ws._skipPongTermination = true;
      log.info('WS', `Media Stream upgrade accepted, convId: ${conversationId}`);
      try {
        handleMediaStream(ws, conversationId);
      } catch (err) {
        log.error('WS', `handleMediaStream threw on setup (convId=${conversationId}): ${err.message}`);
        try { ws.close(); } catch {}
      }
    });
  } else if (url.pathname === '/dashboard') {
    wssDashboard.handleUpgrade(request, socket, head, (ws) => {
      ws._wsKind = 'dashboard';
      log.info('WS', 'Dashboard client connected');
      registerDashboardClient(ws);
    });
  } else {
    socket.destroy();
  }
});

// ─── WebSocket keepalive — ping every 25s to detect dead connections ──
const WS_PING_INTERVAL = 25000;
setInterval(() => {
  for (const wsServer of [wssMedia, wssDashboard]) {
    wsServer.clients.forEach((ws) => {
      if (ws._isAlive === false) {
        if (ws._skipPongTermination) {
          log.warn('WS', `Skipping pong termination for ${ws._wsKind || 'socket'} WebSocket`);
          ws._isAlive = true;
          return;
        }
        log.warn('WS', 'Terminating dead WebSocket (no pong)');
        return ws.terminate();
      }
      ws._isAlive = false;
      try { ws.ping(); } catch {}
    });
  }
}, WS_PING_INTERVAL);

// Mark clients alive on pong (set in upgrade handlers via connection event)
for (const wsServer of [wssMedia, wssDashboard]) {
  wsServer.on('connection', (ws) => {
    ws._isAlive = true;
    ws.on('pong', () => { ws._isAlive = true; });
  });
}

server.listen(PORT, () => {
  console.log(`\n✅ Server ready on http://localhost:${PORT}\n`);

  // Inject make-call function into scheduler and AUTO-START
  setMakeCallFunction(makeCallInternal);
  // Wire post-call hook: scheduler dials next customer immediately after each call ends
  setAfterCallHook(onCallCompleted);
  startScheduler();
});
