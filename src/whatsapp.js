import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  generateMessageIDV2,
} from '@whiskeysockets/baileys';
import pino from 'pino';
import QRCode from 'qrcode';
import { PgAuthStore, clearSessionRows, ensureSchema, purgeOncePerTag, purgeSignalSessions } from './authStore.js';
import { SessionLease } from './sessionLease.js';
import { MessageStore, TtlCache } from './messageStore.js';

const log = pino({ level: process.env.LOG_LEVEL || 'info' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

// How long a send waits for the server to NACK it (463 etc.) before we report it as sent.
const ACK_ERROR_WAIT_MS = Number(process.env.ACK_ERROR_WAIT_MS ?? 1200);
// How long a send waits for the socket to come back (reconnect / deploy hand-over) before failing.
const READY_WAIT_MS = Number(process.env.READY_WAIT_MS ?? 25_000);
// One-shot session repair: change this value (any string) to purge Signal sessions once on next boot.
const SESSION_REPAIR_TAG = process.env.SESSION_REPAIR_TAG || '';

export class WhatsAppClient {
  #stopping = false;
  #stopSignal;
  #resolveStop;
  #loop = null;
  #resetRequested = false;
  #repairRequested = false;
  #bootRepairDone = false;
  #pruneTimer = null;
  #ackErrors = new Map(); // message id -> error code string (bounded)

  constructor(pool, sessionId = 'default') {
    this.pool = pool;
    this.sessionId = sessionId;
    this.sock = null;
    this.store = null;
    this.status = 'starting'; // starting | standby | qr | open | closed
    this.qrDataUrl = null;
    this.retries = 0;
    this.replaced = 0;

    this.messages = new MessageStore(pool, sessionId, log);
    this.retryCache = new TtlCache(10 * 60_000);
    this.lease = new SessionLease(pool, sessionId, {
      log,
      onLost: () => this.#forceClose(new Error('session lease lost')),
    });
    this.#stopSignal = new Promise((r) => (this.#resolveStop = r));
  }

  /** Starts the self-healing connection loop. Never throws, never needs to be called twice. */
  start() {
    if (!this.#loop) this.#loop = this.#run();
  }

  get isReady() {
    return this.status === 'open' && !!this.sock;
  }

  // ---------- lifecycle ----------

  async #run() {
    // schema first (retry: DB may still be waking up)
    while (!this.#stopping) {
      try {
        await ensureSchema(this.pool);
        break;
      } catch (err) {
        log.error({ err: err.message }, 'schema setup failed, retrying in 5s');
        await this.#sleep(5000);
      }
    }
    this.messages.prune();
    this.#pruneTimer = setInterval(() => this.messages.prune(), 6 * 3600_000);
    this.#pruneTimer.unref();

    while (!this.#stopping) {
      try {
        if (!this.lease.held) {
          this.status = 'standby';
          if (!(await this.lease.acquire())) break;
        }
        if (!this.#bootRepairDone) {
          this.#bootRepairDone = true;
          const r = await purgeOncePerTag(this.pool, this.sessionId, this.lease.owner, SESSION_REPAIR_TAG);
          if (r.ran) log.warn({ deleted: r.deleted, tag: SESSION_REPAIR_TAG }, 'SESSION_REPAIR_TAG: purged Signal sessions; fresh ones will be negotiated');
        }
        const outcome = await this.#session();
        await this.#afterClose(outcome);
      } catch (err) {
        const delay = Math.min(30_000, 1000 * 2 ** this.retries++);
        log.error({ err: err.message, delay }, 'session loop error, retrying');
        this.status = 'closed';
        await this.#sleep(delay);
      }
    }
  }

  /** One socket lifetime. Resolves with the close reason once the socket is down AND all key writes are flushed. */
  async #session() {
    const store = new PgAuthStore(this.pool, this.sessionId, {
      owner: this.lease.owner,
      log,
      onFatal: (err) => this.#forceClose(err),
    });
    await store.init(); // always reload from the DB: memory can never be "ahead" of what is persisted
    this.store = store;

    const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: undefined }));
    const sock = makeWASocket({
      version,
      auth: store.state,
      // BAILEYS_LOG_LEVEL=info shows "recv retry request", "sent retry receipt", "received error in ack"
      logger: pino({ level: process.env.BAILEYS_LOG_LEVEL || 'warn' }),
      browser: Browsers.ubuntu('Chrome'),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
      // Lets Baileys answer retry receipts by re-encrypting the original message
      // (without it recipients are stuck on "Waiting for this message").
      getMessage: async (key) => this.messages.get(key?.id),
      msgRetryCounterCache: this.retryCache,
    });
    this.sock = sock;

    let dead = false;
    try {
      return await new Promise((resolve) => {
        sock.ev.on('creds.update', () => {
          if (dead) return;
          store.saveCreds().catch((err) => log.error({ err: err.message }, 'saveCreds failed'));
        });

        sock.ev.on('messages.update', (updates) => {
          for (const { key, update } of updates) {
            if (!key?.fromMe || !key.id || update?.status !== 0 /* WAMessageStatus.ERROR */) continue;
            const code = String(update.messageStubParameters?.[0] ?? 'unknown');
            this.#ackErrors.set(key.id, code);
            if (this.#ackErrors.size > 500) this.#ackErrors.delete(this.#ackErrors.keys().next().value);
            log.error({ id: key.id, to: key.remoteJid, code }, 'WhatsApp server rejected an outgoing message (ack error)');
          }
        });

        sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
          if (dead) return;
          if (qr) {
            this.qrDataUrl = await QRCode.toDataURL(qr, { margin: 2, width: 360 });
            this.status = 'qr';
            log.info('QR code ready - open /qr to link the number');
          }
          if (connection === 'open') {
            this.status = 'open';
            this.qrDataUrl = null;
            this.retries = 0;
            this.replaced = 0;
            log.info('WhatsApp connected');
          }
          if (connection === 'close') {
            this.status = 'closed';
            resolve({ code: lastDisconnect?.error?.output?.statusCode, error: lastDisconnect?.error });
          }
        });
      });
    } finally {
      dead = true;
      this.sock = null;
      try { sock.end(undefined); } catch { /* already closed */ }
      // drain the write queue BEFORE the lease can be released or a new socket started
      await store.close().catch((err) => log.error({ err: err.message }, 'store flush failed'));
      this.store = null;
    }
  }

  async #afterClose({ code, error }) {
    if (this.#stopping) return;

    if (this.#repairRequested && !this.#resetRequested && code !== DisconnectReason.loggedOut) {
      this.#repairRequested = false;
      const r = await purgeSignalSessions(this.pool, this.sessionId, this.lease.owner);
      log.warn({ deleted: r.deleted }, 'Signal sessions purged (socket was down, store flushed); reconnecting with a clean slate');
      this.retries = 0;
      return;
    }

    if (this.#resetRequested || code === DisconnectReason.loggedOut) {
      this.#resetRequested = false;
      log.warn('Logged out / reset requested - clearing session, a new QR will be generated');
      await clearSessionRows(this.pool, this.sessionId, this.lease.owner);
      this.retries = 0;
      await this.#sleep(1000);
      return;
    }
    if (!this.lease.held) return; // lease lost: loop re-enters standby and waits for it again

    if (code === DisconnectReason.connectionReplaced) {
      // Someone else is connected with these credentials. Reconnecting immediately makes both
      // sides fight and corrupts the Signal state, so back off hard.
      const delay = Math.min(300_000, 15_000 * 2 ** this.replaced++);
      log.error({ delay }, 'connection replaced: another client is using this WhatsApp session');
      await this.#sleep(delay);
      return;
    }
    if (code === DisconnectReason.restartRequired) return; // normal right after pairing: reconnect now

    const delay = Math.min(30_000, 1000 * 2 ** this.retries++);
    log.warn({ code, err: error?.message, delay }, 'Connection closed, reconnecting');
    await this.#sleep(delay);
  }

  #forceClose(err) {
    try { this.sock?.end(err); } catch { /* ignore */ }
  }

  #sleep(ms) {
    return Promise.race([sleep(ms), this.#stopSignal]);
  }

  /** Graceful shutdown: close socket, flush every pending key write, release the lease. */
  async stop() {
    this.#stopping = true;
    this.#resolveStop();
    this.lease.stop();
    clearInterval(this.#pruneTimer);
    this.#forceClose(new Error('shutdown'));
    await this.#loop?.catch(() => {});
    await this.lease.release();
  }

  /**
   * Heal diverged Signal state WITHOUT re-scanning the QR: drops the socket (so the store flushes and
   * its cache dies), deletes sessions + sender keys, reconnects. creds and pre-keys are untouched.
   */
  async repairSessions() {
    this.#repairRequested = true;
    this.#forceClose(new Error('repair'));
  }

  /** Log out from WhatsApp (if connected), wipe the stored session and show a fresh QR. */
  async resetSession() {
    this.#resetRequested = true;
    try { await this.sock?.logout(); } catch { /* socket may already be down */ }
    this.#forceClose(new Error('reset'));
  }

  // ---------- sending ----------

  async #waitReady(ms) {
    const until = Date.now() + ms;
    while (!this.isReady) {
      if (this.#stopping || Date.now() >= until) throw new Error('WhatsApp is not connected');
      await this.#sleep(500);
    }
  }

  /** Send one text message with a human-like typing simulation. */
  async sendText({ phone, text }) {
    await this.#waitReady(READY_WAIT_MS); // rides out reconnects and Render deploy hand-over
    const sock = this.sock;

    const lookup = await sock.onWhatsApp(phone);
    const hit = lookup?.[0];
    if (!hit?.exists) {
      throw Object.assign(new Error(`${phone} is not on WhatsApp`), { permanent: true });
    }
    const jid = hit.jid;

    // Typing duration scales with message length (~1s to 2.5s) plus jitter
    const typingMs = clamp(600 + text.length * 15, 1000, 2500) + rand(0, 400);
    await sock.presenceSubscribe(jid);
    await sock.sendPresenceUpdate('composing', jid);
    await sleep(typingMs);
    await sock.sendPresenceUpdate('paused', jid);

    // Persist a provisional copy BEFORE relaying: a retry receipt can arrive within milliseconds,
    // and getMessage() must never miss (a miss = recipient stuck on "Waiting for this message").
    const messageId = generateMessageIDV2(sock.user?.id);
    await this.messages.put(messageId, { conversation: text });

    const res = await sock.sendMessage(jid, { text }, { messageId });
    const id = res?.key?.id ?? messageId;
    await this.messages.put(id, res?.message ?? { conversation: text }); // replace with the exact encrypted payload

    // WhatsApp NACKs asynchronously (e.g. 463). sendMessage() already resolved, so watch briefly.
    const deadline = Date.now() + ACK_ERROR_WAIT_MS;
    while (!this.#ackErrors.has(id) && Date.now() < deadline) await sleep(100);
    const nack = this.#ackErrors.get(id);
    if (nack) {
      throw Object.assign(new Error(`WhatsApp rejected the message (ack error ${nack})`), {
        permanent: /^4/.test(nack), // 4xx will not fix itself; retrying a 463 only digs the hole deeper
        ackError: nack,
      });
    }
    return { idMessage: id, jid };
  }
}
