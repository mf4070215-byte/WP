// Baileys auth state stored in Postgres, so the WhatsApp session survives
// redeploys and restarts on hosts with no persistent disk (Koyeb free tier).
import { initAuthCreds, BufferJSON, proto } from '@whiskeysockets/baileys';

export async function usePgAuthState(pool, session = 'default') {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wa_auth (
      session TEXT NOT NULL,
      key     TEXT NOT NULL,
      value   TEXT NOT NULL,
      PRIMARY KEY (session, key)
    )`);

  const read = async (key) => {
    const { rows } = await pool.query(
      'SELECT value FROM wa_auth WHERE session = $1 AND key = $2',
      [session, key]
    );
    return rows[0] ? JSON.parse(rows[0].value, BufferJSON.reviver) : null;
  };

  const write = (key, data) =>
    pool.query(
      `INSERT INTO wa_auth (session, key, value) VALUES ($1, $2, $3)
       ON CONFLICT (session, key) DO UPDATE SET value = EXCLUDED.value`,
      [session, key, JSON.stringify(data, BufferJSON.replacer)]
    );

  const remove = (key) =>
    pool.query('DELETE FROM wa_auth WHERE session = $1 AND key = $2', [session, key]);

  const creds = (await read('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          await Promise.all(
            ids.map(async (id) => {
              let v = await read(`${type}-${id}`);
              if (type === 'app-state-sync-key' && v) {
                v = proto.Message.AppStateSyncKeyData.fromObject(v);
              }
              out[id] = v;
            })
          );
          return out;
        },
        set: async (data) => {
          const tasks = [];
          for (const category of Object.keys(data)) {
            for (const id of Object.keys(data[category])) {
              const v = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(v ? write(key, v) : remove(key));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => write('creds', creds),
    clear: () => pool.query('DELETE FROM wa_auth WHERE session = $1', [session]),
  };
}
