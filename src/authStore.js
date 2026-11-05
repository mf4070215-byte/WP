// Baileys auth state persisted in Postgres (Supabase), built so the Signal
// cryptographic state can never silently diverge from what the phone expects.
//
// What this guarantees (and why the old adapter did not):
//  1. ORDERED writes   - every write goes through ONE serial queue. The old code fired
//                        parallel upserts on a 4-connection pool, so an older session
//                        ratchet/creds snapshot could commit AFTER a newer one and silently
//                        roll the Signal state back -> "Waiting for this message".
//  2. DURABLE-BEFORE-ACK - keys.set()/saveCreds() resolve only after COMMIT. Failed writes
//                        are retried with backoff, never dropped; a terminal failure
//                        triggers onFatal() so the socket is torn down instead of running
//                        with keys that exist only in RAM.
//  3. FENCED writes    - every write transaction verifies (and row-locks) the instance's
//                        session lease, so a second instance during a Render zero-downtime
//                        deploy can never overwrite the live instance's keys.
//  4. NEVER-NULL-ON-ERROR - a failed read throws; it is never turned into "key missing".
//  5. ATOMIC batches   - one keys.set() = one DB transaction (upserts + deletes together).
//
// The row format (session, key, value=BufferJSON text) is unchanged, so an existing
// session keeps working - no QR re-scan needed.
import { initAuthCreds, BufferJSON, proto } from '@whiskeysockets/baileys';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ser = (v) => JSON.stringify(v, BufferJSON.replacer);
const de = (s) => JSON.parse(s, BufferJSON.reviver);

export class LeaseLostError extends Error {
  constructor(msg = 'Session lease lost: another instance owns this WhatsApp session') {
    super(msg);
    this.name = 'LeaseLostError';
  }
}

const DDL = [
  `CREATE TABLE IF NOT EXISTS wa_auth (
     session    TEXT NOT NULL,
     key        TEXT NOT NULL,
     value      TEXT NOT NULL,
     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     PRIMARY KEY (session, key)
   )`,
  `ALTER TABLE wa_auth ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
  `CREATE TABLE IF NOT EXISTS wa_session_lock (
     session    TEXT PRIMARY KEY,
     owner      TEXT NOT NULL,
     expires_at TIMESTAMPTZ NOT NULL
   )`,
  `CREATE TABLE IF NOT EXISTS wa_messages (
     session    TEXT NOT NULL,
     id         TEXT NOT NULL,
     value      TEXT NOT NULL,
     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     PRIMARY KEY (session, id)
   )`,
  `CREATE INDEX IF NOT EXISTS wa_messages_created_idx ON wa_messages (created_at)`,
  // These tables hold private keys. Supabase exposes the public schema through its REST
  // API, so lock them: RLS on + no policies = only the direct Postgres role (this app) can read.
  `ALTER TABLE wa_auth ENABLE ROW LEVEL SECURITY`,
  `ALTER TABLE wa_session_lock ENABLE ROW LEVEL SECURITY`,
  `ALTER TABLE wa_messages ENABLE ROW LEVEL SECURITY`,
];

/** Idempotent; serialized with an advisory xact lock so two booting instances can't race the DDL. */
export async function ensureSchema(pool) {
  const c = await pool.connect();
  let failure;
  try {
    await c.query('BEGIN');
    await c.query('SELECT pg_advisory_xact_lock(727001)');
    for (const sql of DDL) await c.query(sql);
    await c.query('COMMIT');
  } catch (err) {
    failure = err;
    try { await c.query('ROLLBACK'); } catch { /* ignore */ }
    throw err;
  } finally {
    c.release(failure);
  }
}

/**
 * Run fn(client) in a transaction that first proves we still hold the session lease.
 * SELECT ... FOR SHARE also row-locks the lease so it cannot be taken over mid-write.
 */
export async function withLeaseTx(pool, session, owner, fn) {
  const c = await pool.connect();
  let failure;
  try {
    await c.query('BEGIN');
    if (owner) {
      const { rowCount } = await c.query(
        'SELECT 1 FROM wa_session_lock WHERE session = $1 AND owner = $2 AND expires_at > now() FOR SHARE',
        [session, owner]
      );
      if (!rowCount) throw new LeaseLostError();
    }
    const result = await fn(c);
    await c.query('COMMIT');
    return result;
  } catch (err) {
    failure = err;
    try { await c.query('ROLLBACK'); } catch { /* ignore */ }
    throw err;
  } finally {
    // passing an error destroys the connection instead of returning a possibly broken one to the pool
    c.release(failure && !(failure instanceof LeaseLostError) ? failure : undefined);
  }
}

/** Wipe a session's auth rows (used on logout / explicit reset). Fenced by the lease. */
export function clearSessionRows(pool, session, owner) {
  return withLeaseTx(pool, session, owner, (c) =>
    c.query('DELETE FROM wa_auth WHERE session = $1', [session])
  );
}

export class PgAuthStore {
  #pool;
  #session;
  #owner;
  #log;
  #onFatal;
  #cacheMax;
  #cache = new Map(); // key -> raw JSON string, LRU, only values known to be committed
  #dirty = new Map(); // key -> raw JSON string | null, writes in flight (never evicted)
  #chain = Promise.resolve();
  #credsJob = null;
  #failed = null;

  constructor(pool, session, { owner = null, log = console, onFatal = null, cacheMax = 5000 } = {}) {
    this.#pool = pool;
    this.#session = session;
    this.#owner = owner;
    this.#log = log;
    this.#onFatal = onFatal;
    this.#cacheMax = cacheMax;
  }

  async init() {
    const { rows } = await this.#retry('load creds', () =>
      this.#pool.query('SELECT value FROM wa_auth WHERE session = $1 AND key = $2', [this.#session, 'creds'])
    );
    let creds;
    if (rows[0]) {
      creds = de(rows[0].value);
      // A present-but-broken row must never be replaced by fresh creds: that would silently
      // burn the linked device. Fail loudly instead.
      if (!creds?.noiseKey || !creds?.signedIdentityKey || !creds?.signedPreKey) {
        throw new Error('Stored creds are incomplete/corrupt - refusing to overwrite them');
      }
    } else {
      creds = initAuthCreds();
    }
    this.creds = creds;
    this.state = {
      creds,
      keys: {
        get: (type, ids) => this.#getKeys(type, ids),
        set: (data) => this.#setKeys(data),
      },
    };
    return this;
  }

  // ---------- reads ----------

  #decode(type, raw) {
    if (raw == null) return null;
    const v = de(raw);
    return type === 'app-state-sync-key' ? proto.Message.AppStateSyncKeyData.fromObject(v) : v;
  }

  #remember(key, raw) {
    this.#cache.delete(key);
    this.#cache.set(key, raw);
    if (this.#cache.size > this.#cacheMax) this.#cache.delete(this.#cache.keys().next().value);
  }

  async #getKeys(type, ids) {
    const out = {};
    const miss = [];
    for (const id of ids) {
      const k = `${type}-${id}`;
      if (this.#dirty.has(k)) {
        out[id] = this.#decode(type, this.#dirty.get(k));
      } else if (this.#cache.has(k)) {
        const raw = this.#cache.get(k);
        this.#remember(k, raw); // LRU touch
        out[id] = this.#decode(type, raw);
      } else {
        miss.push(id);
      }
    }
    if (miss.length) {
      const keys = miss.map((id) => `${type}-${id}`);
      // A failed read THROWS (after retries). It must never look like "no session here".
      const { rows } = await this.#retry('read keys', () =>
        this.#pool.query('SELECT key, value FROM wa_auth WHERE session = $1 AND key = ANY($2::text[])', [
          this.#session,
          keys,
        ])
      );
      const found = new Map(rows.map((r) => [r.key, r.value]));
      for (const id of miss) {
        const k = `${type}-${id}`;
        const raw = found.get(k) ?? null;
        // a write may have landed while we were reading: never overwrite newer state with this read
        if (raw !== null && !this.#dirty.has(k) && !this.#cache.has(k)) this.#remember(k, raw);
        out[id] = this.#dirty.has(k) ? this.#decode(type, this.#dirty.get(k)) : this.#decode(type, raw);
      }
    }
    return out;
  }

  // ---------- writes ----------

  async #setKeys(data) {
    this.#assertWritable();
    const upserts = new Map();
    const deletes = new Set();
    for (const type of Object.keys(data)) {
      for (const id of Object.keys(data[type])) {
        const v = data[type][id];
        const k = `${type}-${id}`;
        if (v) upserts.set(k, ser(v));
        else deletes.add(k);
      }
    }
    if (!upserts.size && !deletes.size) return;

    // visible to reads immediately, so nothing can observe an older value while the write is in flight
    for (const [k, raw] of upserts) this.#dirty.set(k, raw);
    for (const k of deletes) this.#dirty.set(k, null);

    await this.#enqueue(() =>
      this.#tx(async (c) => {
        if (upserts.size) {
          await c.query(
            `INSERT INTO wa_auth (session, key, value, updated_at)
             SELECT $1::text, t.k, t.v, now() FROM unnest($2::text[], $3::text[]) AS t(k, v)
             ON CONFLICT (session, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
            [this.#session, [...upserts.keys()], [...upserts.values()]]
          );
        }
        if (deletes.size) {
          await c.query('DELETE FROM wa_auth WHERE session = $1 AND key = ANY($2::text[])', [
            this.#session,
            [...deletes],
          ]);
        }
      })
    );

    // committed: promote in-flight values to the read cache (unless a newer write superseded them)
    for (const [k, raw] of upserts) {
      if (this.#dirty.get(k) === raw) {
        this.#dirty.delete(k);
        this.#remember(k, raw);
      }
    }
    for (const k of deletes) {
      if (this.#dirty.get(k) === null) {
        this.#dirty.delete(k);
        this.#cache.delete(k);
      }
    }
  }

  /**
   * Persist creds. Coalesced: if a save is already queued it will serialize the *latest*
   * in-memory creds when it runs, so a burst of creds.update events costs one write and
   * can never write an older snapshot after a newer one.
   */
  saveCreds = async () => {
    this.#assertWritable();
    if (!this.#credsJob) {
      this.#credsJob = this.#enqueue(async () => {
        this.#credsJob = null;
        const raw = ser(this.creds);
        await this.#tx((c) =>
          c.query(
            `INSERT INTO wa_auth (session, key, value, updated_at) VALUES ($1, 'creds', $2, now())
             ON CONFLICT (session, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
            [this.#session, raw]
          )
        );
      });
    }
    return this.#credsJob;
  };

  // ---------- plumbing ----------

  #tx(fn) {
    return withLeaseTx(this.#pool, this.#session, this.#owner, fn);
  }

  #assertWritable() {
    if (this.#failed) throw this.#failed;
  }

  #enqueue(fn) {
    const run = this.#chain.then(() => {
      this.#assertWritable();
      return this.#retry('persist', fn, 6);
    });
    this.#chain = run.catch(() => {}); // later ops are skipped by #assertWritable once failed
    return run.catch((err) => {
      this.#fail(err);
      throw err;
    });
  }

  #fail(err) {
    if (this.#failed) return;
    this.#failed = err;
    this.#log.error({ err: err.message }, 'auth store write failed permanently - tearing down the socket');
    try { this.#onFatal?.(err); } catch (e) { this.#log.error(e); }
  }

  async #retry(label, fn, attempts = 5) {
    let delay = 300;
    for (let i = 1; ; i++) {
      try {
        return await fn();
      } catch (err) {
        if (err instanceof LeaseLostError || i >= attempts) throw err;
        this.#log.warn({ err: err.message, attempt: i }, `${label} failed, retrying`);
        await sleep(delay);
        delay = Math.min(delay * 2, 5000);
      }
    }
  }

  /** Resolves when every write queued so far has been committed. */
  async flush() {
    let c;
    do {
      c = this.#chain;
      await c;
    } while (c !== this.#chain);
  }

  async close() {
    await this.flush();
  }
}

/** Convenience wrapper with the classic Baileys shape: { state, saveCreds }. */
export async function usePgAuthState(pool, session = 'default', opts = {}) {
  const store = await new PgAuthStore(pool, session, opts).init();
  return {
    state: store.state,
    saveCreds: store.saveCreds,
    flush: () => store.flush(),
    close: () => store.close(),
    store,
  };
}
