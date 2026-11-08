// A lease (row with an expiry + heartbeat) that guarantees ONE live Baileys socket per
// WhatsApp session. Needed because Render zero-downtime deploys (and restarts) briefly run
// the old and the new instance together; two sockets on the same creds each hold their own
// in-memory Signal state and overwrite each other's keys.
//
// Uses table rows + DB time (not advisory locks), so it also works through Supabase's
// pooled connections (Supavisor) and is immune to clock skew between instances.
import { randomUUID } from 'node:crypto';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class SessionLease {
  constructor(pool, session, { ttlMs = 30_000, heartbeatMs = 10_000, pollMs = 2_000, log = console, onLost } = {}) {
    this.pool = pool;
    this.session = session;
    this.owner = randomUUID();
    this.ttlMs = ttlMs;
    this.heartbeatMs = heartbeatMs;
    this.pollMs = pollMs;
    this.log = log;
    this.onLost = onLost;
    this.held = false;
    this.stopped = false;
    this.timer = null;
    this.lastOk = 0;
  }

  /** Waits until the lease is ours. Resolves false if stop() was called first. */
  async acquire() {
    let announced = false;
    while (!this.stopped) {
      try {
        const { rowCount } = await this.pool.query(
          `INSERT INTO wa_session_lock (session, owner, expires_at)
           VALUES ($1, $2, now() + make_interval(secs => $3::double precision))
           ON CONFLICT (session) DO UPDATE
             SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
             WHERE wa_session_lock.expires_at < now() OR wa_session_lock.owner = EXCLUDED.owner`,
          [this.session, this.owner, this.ttlMs / 1000]
        );
        if (rowCount === 1) {
          this.held = true;
          this.lastOk = Date.now();
          this.#startHeartbeat();
          this.log.info({ owner: this.owner }, 'session lease acquired');
          return true;
        }
        if (!announced) {
          this.log.info('another instance holds the session lease; waiting for it to finish');
          announced = true;
        }
      } catch (err) {
        this.log.warn({ err: err.message }, 'lease acquire failed, retrying');
      }
      await sleep(this.pollMs);
    }
    return false;
  }

  #startHeartbeat() {
    clearInterval(this.timer);
    this.timer = setInterval(() => this.#beat(), this.heartbeatMs);
  }

  async #beat() {
    if (!this.held) return;
    try {
      const { rowCount } = await this.pool.query(
        `UPDATE wa_session_lock SET expires_at = now() + make_interval(secs => $3::double precision)
         WHERE session = $1 AND owner = $2`,
        [this.session, this.owner, this.ttlMs / 1000]
      );
      if (rowCount === 0) return this.#lost('lease row taken over by another instance');
      this.lastOk = Date.now();
    } catch (err) {
      this.log.warn({ err: err.message }, 'lease heartbeat failed');
      // if we cannot renew for most of the TTL, someone else may legitimately take over: stop first
      if (Date.now() - this.lastOk > this.ttlMs * 0.8) this.#lost('could not renew lease (database unreachable)');
    }
  }

  #lost(reason) {
    if (!this.held) return;
    this.held = false;
    clearInterval(this.timer);
    this.log.error({ reason }, 'session lease lost');
    try { this.onLost?.(reason); } catch (e) { this.log.error(e); }
  }

  /** Stop acquiring/renewing (does not touch the DB row; call release() for that). */
  stop() {
    this.stopped = true;
    clearInterval(this.timer);
  }

  async release() {
    clearInterval(this.timer);
    const wasHeld = this.held;
    this.held = false;
    if (!wasHeld) return;
    try {
      await this.pool.query('DELETE FROM wa_session_lock WHERE session = $1 AND owner = $2', [this.session, this.owner]);
      this.log.info('session lease released');
    } catch (err) {
      this.log.warn({ err: err.message }, 'lease release failed (it will expire on its own)');
    }
  }
}
