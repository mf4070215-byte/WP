import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
} from '@whiskeysockets/baileys';
import pino from 'pino';
import QRCode from 'qrcode';
import { usePgAuthState } from './authStore.js';

const log = pino({ level: process.env.LOG_LEVEL || 'info' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

export class WhatsAppClient {
  constructor(pool, sessionId = 'default') {
    this.pool = pool;
    this.sessionId = sessionId;
    this.sock = null;
    this.status = 'starting'; // starting | qr | open | closed
    this.qrDataUrl = null;
    this.retries = 0;
    this.clearAuth = async () => {};
  }

  async start() {
    const { state, saveCreds, clear } = await usePgAuthState(this.pool, this.sessionId);
    this.clearAuth = clear;

    const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: undefined }));

    const sock = makeWASocket({
      version,
      auth: state,
      logger: pino({ level: 'warn' }),
      browser: Browsers.ubuntu('Chrome'),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
    });
    this.sock = sock;

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        this.qrDataUrl = await QRCode.toDataURL(qr, { margin: 2, width: 360 });
        this.status = 'qr';
        log.info('QR code ready - open /qr to link the number');
      }

      if (connection === 'open') {
        this.status = 'open';
        this.qrDataUrl = null;
        this.retries = 0;
        log.info('WhatsApp connected');
      }

      if (connection === 'close') {
        this.status = 'closed';
        const code = lastDisconnect?.error?.output?.statusCode;
        if (code === DisconnectReason.loggedOut) {
          log.warn('Logged out from phone - clearing session, a new QR will be generated');
          await this.clearAuth();
          this.retries = 0;
          setTimeout(() => this.start().catch((e) => log.error(e)), 1000);
        } else {
          const delay = Math.min(30_000, 1000 * 2 ** this.retries++);
          log.warn({ code, delay }, 'Connection closed, reconnecting');
          setTimeout(() => this.start().catch((e) => log.error(e)), delay);
        }
      }
    });
  }

  get isReady() {
    return this.status === 'open' && !!this.sock;
  }

  /** Send one text message with a human-like typing simulation. */
  async sendText({ phone, text }) {
    if (!this.isReady) throw new Error('WhatsApp is not connected');

    const lookup = await this.sock.onWhatsApp(phone);
    const hit = lookup?.[0];
    if (!hit?.exists) {
      throw Object.assign(new Error(`${phone} is not on WhatsApp`), { permanent: true });
    }
    const jid = hit.jid;

    // Typing duration scales with message length (~1s to 2.5s) plus jitter
    const typingMs = clamp(600 + text.length * 15, 1000, 2500) + rand(0, 400);
    await this.sock.presenceSubscribe(jid);
    await this.sock.sendPresenceUpdate('composing', jid);
    await sleep(typingMs);
    await this.sock.sendPresenceUpdate('paused', jid);

    const res = await this.sock.sendMessage(jid, { text });
    return { idMessage: res?.key?.id, jid };
  }
}
