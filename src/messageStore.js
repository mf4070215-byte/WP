// Sent-message store backing Baileys' `getMessage`.
//
// When the recipient's device cannot decrypt a message it sends a retry receipt and Baileys
// must re-encrypt the ORIGINAL message. Without getMessage() it has nothing to re-send, and the
// recipient is stuck on "Waiting for this message. This may take a while." forever.
import { BufferJSON, proto } from '@whiskeysockets/baileys';

export class MessageStore {
  #mem = new Map();

  constructor(pool, session, log = console, { max = 2000, ttlDays = 3 } = {}) {
    this.pool = pool;
    this.session = session;
    this.log = log;
    this.max = max;
    this.ttlDays = ttlDays;
  }

  async put(id, message) {
    if (!id || !message) return;
    this.#mem.delete(id);
    this.#mem.set(id, message);
    if (this.#mem.size > this.max) this.#mem.delete(this.#mem.keys().next().value);
    try {
      await this.pool.query(
        `INSERT INTO wa_messages (session, id, value) VALUES ($1, $2, $3)
         ON CONFLICT (session, id) DO UPDATE SET value = EXCLUDED.value`,
        [this.session, id, JSON.stringify(message, BufferJSON.replacer)]
      );
    } catch (err) {
      this.log.warn({ err: err.message }, 'could not persist sent message (kept in memory only)');
    }
  }

  async get(id) {
    if (!id) return undefined;
    if (this.#mem.has(id)) return this.#mem.get(id);
    try {
      const { rows } = await this.pool.query('SELECT value FROM wa_messages WHERE session = $1 AND id = $2', [
        this.session,
        id,
      ]);
      if (!rows[0]) return undefined;
      return proto.Message.fromObject(JSON.parse(rows[0].value, BufferJSON.reviver));
    } catch (err) {
      this.log.warn({ err: err.message }, 'getMessage lookup failed');
      return undefined;
    }
  }

  async prune() {
    try {
      await this.pool.query(`DELETE FROM wa_messages WHERE session = $1 AND created_at < now() - make_interval(days => $2::int)`, [
        this.session,
        this.ttlDays,
      ]);
    } catch (err) {
      this.log.warn({ err: err.message }, 'message prune failed');
    }
  }
}

/** Minimal TTL cache implementing Baileys' CacheStore (get/set/del/flushAll), for msgRetryCounterCache. */
export class TtlCache {
  #m = new Map();
  constructor(ttlMs = 10 * 60_000, max = 5000) {
    this.ttlMs = ttlMs;
    this.max = max;
  }
  get(key) {
    const e = this.#m.get(key);
    if (!e) return undefined;
    if (e.exp < Date.now()) {
      this.#m.delete(key);
      return undefined;
    }
    return e.v;
  }
  set(key, value) {
    this.#m.delete(key);
    this.#m.set(key, { v: value, exp: Date.now() + this.ttlMs });
    if (this.#m.size > this.max) this.#m.delete(this.#m.keys().next().value);
  }
  del(key) {
    this.#m.delete(key);
  }
  flushAll() {
    this.#m.clear();
  }
}
