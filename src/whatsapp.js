import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
} from '@whiskeysockets/baileys';
import pino from 'pino';
import QRCode from 'qrcode';
import { PgAuthStore, clearSessionRows, ensureSchema } from './authStore.js';
import { SessionLease } from './sessionLease.js';
import { MessageStore, TtlCache } from './messageStore.js';

const log = pino({ level: process.env.LOG_LEVEL || 'info' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

export class WhatsAppClient {
  #stopping = false;
  #stopSignal;
  #resolveStop;
  #loop = null;
  #resetRequested = false;
  #pruneTimer = null;

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
      logger: pino({ level: 'warn' }),
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

  /** Log out from WhatsApp (if connected), wipe the stored session and show a fresh QR. */
  async resetSession() {
    this.#resetRequested = true;
    try { await this.sock?.logout(); } catch { /* socket may already be down */ }
    this.#forceClose(new Error('reset'));
  }

  // ---------- sending ----------

  /** Send one text message with a human-like typing simulation. */
  async sendText({ phone, text }) {
    if (!this.isReady) throw new Error('WhatsApp is not connected');
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

    const res = await sock.sendMessage(jid, { text });
    await this.messages.put(res?.key?.id, res?.message); // needed to answer retry receipts
    return { idMessage: res?.key?.id, jid };
  }
}
