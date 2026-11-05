import express from 'express';
import pg from 'pg';
import pino from 'pino';
import { timingSafeEqual } from 'node:crypto';
import { WhatsAppClient } from './whatsapp.js';
import { SendQueue } from './queue.js';
import { normalizePhone, toWhatsAppText } from './format.js';

const log = pino({ level: process.env.LOG_LEVEL || 'info' });

const {
  INSTANCE_ID,
  API_TOKEN,
  DATABASE_URL,
  SESSION_ID = 'default',
  PORT = 8000,
  DEFAULT_COUNTRY_CODE = '',
  GAP_MIN_MS = 2000,
  GAP_MAX_MS = 4000,
  DAILY_LIMIT = 300,
  MAX_QUEUE = 200,
} = process.env;

for (const [k, v] of Object.entries({ INSTANCE_ID, API_TOKEN, DATABASE_URL })) {
  if (!v) {
    log.fatal(`Missing required env var ${k}`);
    process.exit(1);
  }
}
if (API_TOKEN.length < 24) {
  log.fatal('API_TOKEN must be at least 24 characters');
  process.exit(1);
}

// ---------- Postgres (session storage) ----------
const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: /sslmode=require|neon\.tech|supabase/.test(DATABASE_URL) ? { rejectUnauthorized: false } : undefined,
  max: 6,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
});
pool.on('error', (e) => log.error(e, 'pg pool error'));

// ---------- WhatsApp + queue ----------
const wa = new WhatsAppClient(pool, SESSION_ID);
const queue = new SendQueue((job) => wa.sendText(job), {
  gapMin: Number(GAP_MIN_MS),
  gapMax: Number(GAP_MAX_MS),
  maxLength: Number(MAX_QUEUE),
});

let sentToday = 0;
let dayKey = new Date().toISOString().slice(0, 10);
function countAgainstDailyLimit(n) {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== dayKey) {
    dayKey = today;
    sentToday = 0;
  }
  if (sentToday + n > Number(DAILY_LIMIT)) {
    throw Object.assign(new Error('Daily message limit reached'), { status: 429 });
  }
  sentToday += n;
}

// ---------- Auth ----------
const safeEqual = (a, b) => {
  const A = Buffer.from(String(a ?? ''));
  const B = Buffer.from(String(b ?? ''));
  return A.length === B.length && timingSafeEqual(A, B);
};

function auth(req, res, next) {
  const bearer = (req.get('authorization') || '').replace(/^Bearer\s+/i, '');
  const id = req.params.instanceId ?? req.body?.instance_id ?? req.get('x-instance-id') ?? req.query.instance_id;
  const token = req.params.token ?? req.body?.api_token ?? req.get('x-api-token') ?? (bearer || undefined) ?? req.query.api_token;
  if (!safeEqual(id, INSTANCE_ID) || !safeEqual(token, API_TOKEN)) {
    return res.status(401).json({ success: false, error: 'Invalid instance_id or api_token' });
  }
  next();
}

// ---------- Core send logic ----------
async function queueMessages(rawRecipients, rawMessage) {
  const list = (Array.isArray(rawRecipients) ? rawRecipients : [rawRecipients]).filter(Boolean);
  if (!list.length) throw Object.assign(new Error('phone/recipient is required'), { status: 400 });
  if (!rawMessage || !String(rawMessage).trim()) {
    throw Object.assign(new Error('message is required'), { status: 400 });
  }

  const text = toWhatsAppText(rawMessage);
  if (text.length > 4000) throw Object.assign(new Error('message too long (max 4000 chars)'), { status: 400 });

  const phones = [...new Set(list.map((p) => normalizePhone(p, DEFAULT_COUNTRY_CODE)))];
  if (phones.some((p) => !p)) throw Object.assign(new Error('One or more phone numbers are invalid'), { status: 400 });

  countAgainstDailyLimit(phones.length);
  return phones.map((phone) => ({ phone, job: queue.enqueue({ phone, text }) }));
}

const wrap = (fn) => (req, res) =>
  fn(req, res).catch((err) => {
    const status = err.status || 500;
    if (status >= 500) log.error(err);
    res.status(status).json({ success: false, error: err.message });
  });

// ---------- HTTP ----------
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '100kb' }));
app.use(express.urlencoded({ extended: false, limit: '100kb' }));

// Public, minimal: for Koyeb health checks and uptime pingers
app.get('/health', (_req, res) => res.json({ ok: true, whatsapp: wa.status, lease: wa.lease.held }));

// Link the number: open https://wa.yourdomain.com/qr?instance_id=...&api_token=...
app.get('/qr', auth, (req, res) => {
  if (req.query.format === 'json') {
    return res.json({ status: wa.status, qr: wa.qrDataUrl });
  }
  res.set('Cache-Control', 'no-store');
  let body;
  if (wa.status === 'open') body = '<h2>✅ WhatsApp is connected</h2>';
  else if (wa.qrDataUrl) {
    body = `<h2>Scan with WhatsApp → Linked devices</h2><img src="${wa.qrDataUrl}" alt="QR" width="360" height="360">`;
  } else body = `<h2>Status: ${wa.status}</h2><p>Waiting for a QR code…</p>`;
  res.send(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5"><title>WhatsApp Gateway</title>
<body style="font-family:system-ui;text-align:center;padding:2rem">${body}</body>`);
});

// Native endpoint (what the WordPress plugin calls)
app.post('/api/send-message', auth, wrap(async (req, res) => {
  const { phone, recipient, recipients, message } = req.body;
  const queued = await queueMessages(recipients ?? phone ?? recipient, message);

  if (req.body.wait === true || req.query.wait === '1') {
    const results = await Promise.allSettled(queued.map((q) => q.job.promise));
    return res.json({
      success: results.every((r) => r.status === 'fulfilled'),
      results: queued.map((q, i) => ({
        phone: q.phone,
        id: q.job.id,
        status: results[i].status === 'fulfilled' ? 'sent' : 'failed',
        idMessage: results[i].value?.idMessage,
        error: results[i].reason?.message,
      })),
    });
  }
  res.status(202).json({
    success: true,
    queued: queued.map((q) => ({ phone: q.phone, id: q.job.id })),
    queueSize: queue.size,
  });
}));

app.get('/api/status/:jobId', auth, (req, res) => {
  const job = queue.getJob(req.params.jobId);
  job ? res.json(job) : res.status(404).json({ error: 'Unknown job id' });
});

// Green API-compatible routes: change only the base URL in the WP plugin
app.post('/waInstance:instanceId/sendMessage/:token', auth, wrap(async (req, res) => {
  const [q] = await queueMessages(req.body.chatId, req.body.message);
  const result = await q.job.promise.catch((e) => {
    throw Object.assign(new Error(e.message), { status: 502 });
  });
  res.json({ idMessage: result.idMessage });
}));

app.get('/waInstance:instanceId/getStateInstance/:token', auth, (_req, res) => {
  res.json({ stateInstance: wa.status === 'open' ? 'authorized' : 'notAuthorized' });
});

// Recovery hatch: log out, wipe the stored session and get a fresh QR at /qr
app.post('/api/session/reset', auth, wrap(async (_req, res) => {
  await wa.resetSession();
  res.json({ success: true, message: 'Session reset. Open /qr to link the number again.' });
}));

app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

// ---------- Boot / shutdown ----------
const server = app.listen(Number(PORT), '0.0.0.0', () => log.info(`Gateway listening on :${PORT}`));
wa.start(); // self-healing loop: acquires the session lease, connects, reconnects, never throws

process.on('unhandledRejection', (e) => log.error(e, 'unhandledRejection'));

let shuttingDown = false;
async function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info(`${sig} received, shutting down`);
  // Render waits ~30s after SIGTERM before SIGKILL; finish well inside that window
  setTimeout(() => {
    log.error('graceful shutdown timed out, forcing exit');
    process.exit(1);
  }, 25_000).unref();
  try {
    await wa.stop(); // closes socket, flushes ALL pending key writes, releases the session lease
  } catch (e) {
    log.error(e, 'error while stopping WhatsApp client');
  }
  server.close();
  await pool.end().catch(() => {});
  process.exit(0);
}
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => shutdown(sig));
