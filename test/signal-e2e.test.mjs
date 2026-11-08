// End-to-end Signal test THROUGH the Postgres store using Baileys' own transaction layer + libsignal:
// two parties, bursts of concurrent messages, store restarts in between. If the store ever rolled a
// ratchet back or lost a pre-key, a decrypt below would throw (the "Waiting for this message" cause).
import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { addTransactionCapability, Curve, generateSignalPubKey } from '@whiskeysockets/baileys';
import { makeLibSignalRepository } from '@whiskeysockets/baileys/lib/Signal/libsignal.js';
import * as Auth from '../src/authStore.js';
import { SessionLease } from '../src/sessionLease.js';

const pool = new pg.Pool({ connectionString: process.env.TEST_DATABASE_URL, max: 10 });
const quiet = { info() {}, warn() {}, error() {}, debug() {}, trace() {}, child() { return quiet; }, level: 'silent' };
test.before(() => Auth.ensureSchema(pool));
test.after(() => pool.end());

async function party(session) {
  const lease = new SessionLease(pool, session, { log: quiet });
  await lease.acquire();
  const open = async () => {
    const store = await new Auth.PgAuthStore(pool, session, { owner: lease.owner, log: quiet }).init();
    const keys = addTransactionCapability(store.state.keys, quiet, { maxCommitRetries: 3, delayBetweenTriesMs: 50 });
    const repo = makeLibSignalRepository({ creds: store.creds, keys }, quiet);
    return { store, keys, repo };
  };
  return { lease, open };
}

test('1:1 Signal ratchet survives bursts + store restarts via Postgres', async () => {
  const A = await party(`alice_${Date.now()}`), B = await party(`bob_${Date.now()}`);
  let a = await A.open(), b = await B.open();
  await a.store.saveCreds(); await b.store.saveCreds();

  // Bob publishes one pre-key; Alice builds a session from his bundle
  const pre = Curve.generateKeyPair();
  await b.keys.set({ 'pre-key': { 1: pre } });
  const bc = b.store.creds;
  await a.repo.injectE2ESession({
    jid: 'bob@s.whatsapp.net',
    session: {
      registrationId: bc.registrationId,
      identityKey: generateSignalPubKey(bc.signedIdentityKey.public),
      signedPreKey: { keyId: bc.signedPreKey.keyId, publicKey: generateSignalPubKey(bc.signedPreKey.keyPair.public), signature: bc.signedPreKey.signature },
      preKey: { keyId: 1, publicKey: generateSignalPubKey(pre.public) },
    },
  });

  let sent = 0;
  const roundTrip = async (burst) => {
    // Alice -> Bob (burst, sequential encrypt like the gateway's single-lane queue)
    const cts = [];
    for (let i = 0; i < burst; i++) cts.push({ i: sent++, ...(await a.repo.encryptMessage({ jid: 'bob@s.whatsapp.net', data: Buffer.from(`m${sent}`) })) });
    for (const c of cts) {
      const pt = await b.repo.decryptMessage({ jid: 'alice@s.whatsapp.net', type: c.type, ciphertext: c.ciphertext });
      assert.equal(Buffer.from(pt).toString(), `m${c.i + 1}`);
    }
    // Bob -> Alice reply advances the ratchet the other way
    const r = await b.repo.encryptMessage({ jid: 'alice@s.whatsapp.net', data: Buffer.from('ack') });
    const back = await a.repo.decryptMessage({ jid: 'bob@s.whatsapp.net', type: r.type, ciphertext: r.ciphertext });
    assert.equal(Buffer.from(back).toString(), 'ack');
  };

  for (let k = 0; k < 5; k++) await roundTrip(10);
  const gone = await pool.query(`SELECT 1 FROM wa_auth WHERE key = 'pre-key-1' AND session LIKE 'bob_%'`);
  assert.equal(gone.rowCount, 0, 'consumed one-time pre-key must be deleted');

  // "Render restart": flush, drop every in-memory object, reload purely from Postgres
  await a.store.close(); await b.store.close();
  a = await A.open(); b = await B.open();
  for (let k = 0; k < 5; k++) await roundTrip(10);
  await a.store.close(); await b.store.close();
  A.lease.stop(); B.lease.stop();
});
