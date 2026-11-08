// Integration tests against a REAL Postgres. Run: TEST_DATABASE_URL=postgres://... node --test test/
import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import * as Auth from '../src/authStore.js';
import { SessionLease } from '../src/sessionLease.js';
import { BufferJSON } from '@whiskeysockets/baileys';

const URL = process.env.TEST_DATABASE_URL;
if (!URL) throw new Error('set TEST_DATABASE_URL');
const pool = new pg.Pool({ connectionString: URL, max: 8 });
const quiet = { info() {}, warn() {}, error() {}, debug() {} };
let n = 0;
const sid = () => `t${Date.now()}_${n++}`;
const row = async (s, k) => (await pool.query('SELECT value FROM wa_auth WHERE session=$1 AND key=$2', [s, k])).rows[0]?.value;

test.before(async () => { await Auth.ensureSchema(pool); });
test.after(async () => { await pool.end(); });

async function leased(session) {
  const lease = new SessionLease(pool, session, { log: quiet });
  assert.equal(await lease.acquire(), true);
  return lease;
}

test('ordering: 200 concurrent writes of one session key end with the LAST value', async () => {
  const s = sid(); const lease = await leased(s);
  const store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  const writes = [];
  for (let i = 0; i < 200; i++) writes.push(store.state.keys.set({ session: { 'a.0': Buffer.from(`v${i}`) } }));
  await Promise.all(writes);
  const v = JSON.parse(await row(s, 'session-a.0'), BufferJSON.reviver);
  assert.equal(Buffer.from(v).toString(), 'v199');
  await store.close(); lease.stop(); await lease.release();
});

test('fencing: a stale owner can never write after takeover', async () => {
  const s = sid(); const a = await leased(s);
  const store = await new Auth.PgAuthStore(pool, s, { owner: a.owner, log: quiet }).init();
  await store.state.keys.set({ session: { 'x.0': Buffer.from('mine') } });
  await pool.query(`UPDATE wa_session_lock SET owner='intruder', expires_at=now()+interval '1 min' WHERE session=$1`, [s]);
  await assert.rejects(store.state.keys.set({ session: { 'x.0': Buffer.from('stale') } }), /lease/i);
  assert.equal(Buffer.from(JSON.parse(await row(s, 'session-x.0'), BufferJSON.reviver)).toString(), 'mine');
  a.stop();
});

test('close() persists the LATEST in-memory creds even if creds.update was dropped', async () => {
  const s = sid(); const lease = await leased(s);
  const store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  await store.saveCreds();
  store.creds.nextPreKeyId = 9999;           // Baileys mutated creds, event never reached saveCreds
  await store.close();
  const saved = JSON.parse(await row(s, 'creds'), BufferJSON.reviver);
  assert.equal(saved.nextPreKeyId, 9999);
  lease.stop(); await lease.release();
});

test('init() never lets nextPreKeyId point at pre-keys that already exist (would overwrite live keys)', async () => {
  const s = sid(); const lease = await leased(s);
  let store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  await store.saveCreds();
  const pk = {}; for (let i = 1; i <= 50; i++) pk[i] = { private: Buffer.alloc(32, i), public: Buffer.alloc(32, i) };
  await store.state.keys.set({ 'pre-key': pk });
  store.creds.nextPreKeyId = 1;               // creds snapshot older than the keys (crash between the two writes)
  await store.saveCreds(); await store.close();
  store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  assert.ok(store.creds.nextPreKeyId > 50, `nextPreKeyId=${store.creds.nextPreKeyId}`);
  await store.close(); lease.stop(); await lease.release();
});

test('purgeSignalSessions removes ratchets but keeps creds, pre-keys, app-state, lid-mapping', async () => {
  const s = sid(); const lease = await leased(s);
  const store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  await store.saveCreds();
  await store.state.keys.set({
    session: { 'a.0': Buffer.from('1'), 'b.0': Buffer.from('2') },
    'sender-key': { g1: Buffer.from('3') },
    'sender-key-memory': { g1: { x: true } },
    'pre-key': { 7: { private: Buffer.alloc(32), public: Buffer.alloc(32) } },
    'lid-mapping': { 61412345678: '9999@lid' },
  });
  const r = await Auth.purgeSignalSessions(pool, s, lease.owner);
  assert.equal(r.deleted, 4);
  const left = (await pool.query('SELECT key FROM wa_auth WHERE session=$1 ORDER BY key', [s])).rows.map((x) => x.key);
  assert.deepEqual(left, ['creds', 'lid-mapping-61412345678', 'pre-key-7']);
  await store.close(); lease.stop(); await lease.release();
});

test('purgeOncePerTag runs once per distinct tag, never twice for the same tag', async () => {
  const s = sid(); const lease = await leased(s);
  const store = await new Auth.PgAuthStore(pool, s, { owner: lease.owner, log: quiet }).init();
  await store.state.keys.set({ session: { 'a.0': Buffer.from('1') } });
  assert.equal((await Auth.purgeOncePerTag(pool, s, lease.owner, 'v1')).ran, true);
  await store.state.keys.set({ session: { 'a.0': Buffer.from('2') } });
  assert.equal((await Auth.purgeOncePerTag(pool, s, lease.owner, 'v1')).ran, false);
  assert.equal(await row(s, 'session-a.0') !== undefined, true);
  assert.equal((await Auth.purgeOncePerTag(pool, s, lease.owner, 'v2')).ran, true);
  assert.equal(await row(s, 'session-a.0'), undefined);
  assert.equal((await Auth.purgeOncePerTag(pool, s, lease.owner, '')).ran, false);
  await store.close(); lease.stop(); await lease.release();
});
