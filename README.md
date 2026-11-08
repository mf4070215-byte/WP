# WhatsApp Gateway (Baileys + Express)

Self-hosted, Green API-compatible WhatsApp gateway. Sessions are stored in Postgres (Supabase)
so redeploys and restarts never require re-scanning the QR code.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Public health check / uptime ping |
| GET | `/qr?instance_id=…&api_token=…` | Shows QR page to link the number |
| POST | `/api/send-message` | Native endpoint (JSON) |
| GET | `/api/status/:jobId` | Delivery status of a queued message |
| POST | `/waInstance{ID}/sendMessage/{TOKEN}` | Green API-compatible (`chatId`, `message`) |
| GET | `/waInstance{ID}/getStateInstance/{TOKEN}` | Green API-compatible state |

### POST /api/send-message
```json
{
  "instance_id": "zyn-wa-01",
  "api_token": "YOUR_TOKEN",
  "phone": "0412345678",
  "message": "Hi Sam, your booking is confirmed.<br><a href=\"https://site.com/?zyn_manage=abc\">Manage booking</a>"
}
```
- `phone` may also be `recipient`, or pass `recipients: ["...", "..."]` to notify client + admin in one call.
- HTML is converted to WhatsApp formatting automatically.
- Returns `202` immediately with job ids. Add `"wait": true` to hold the response until sent.

## Local run
```bash
cp .env.example .env   # fill in values
npm install
npm run dev
```

## Deploy on Render (+ Supabase)

1. Push this folder to GitHub. Render -> New -> **Web Service** -> Runtime **Docker**.
2. Use an **always-on paid instance**. Free instances spin down after ~15 min without HTTP traffic,
   which drops the WhatsApp socket on every sleep/wake.
3. Health check path: `/health`. Env vars: `INSTANCE_ID`, `API_TOKEN`, `DATABASE_URL`,
   `DEFAULT_COUNTRY_CODE`, `SESSION_ID`, `PORT=8000`.
4. `DATABASE_URL`: Supabase -> Connect -> **Session pooler** string (port 5432). Render is IPv4-only and
   Supabase's direct host is IPv6-only on most projects. The transaction pooler (6543) also works.
5. Open `https://<app>.onrender.com/qr?instance_id=...&api_token=...` and scan.
6. Optional: raise *Max Shutdown Delay* in Render settings to 30s+ so the final key flush always completes.

## How session persistence works

* `wa_auth` - creds + every Signal key (sessions, pre-keys, sender-keys, app-state keys), same row format as before.
* `wa_session_lock` - lease so only ONE instance owns the socket (deploy overlap safe, writes are fenced by it).
* `wa_messages` - sent messages for 3 days, used to answer retry receipts (`getMessage`).
* All writes are serialized, retried, and committed before Baileys acks the message.
* `POST /api/session/reset` logs out and wipes the session if you ever need a clean re-link.

## Fixing "Waiting for this message" (v2 notes)

* **Baileys is pinned to an exact version with a lockfile** (`npm ci`). The old `^6.7.9` silently resolved to the
  6.7 *legacy* line, which has no LID mapping and no tctoken support. Do not loosen the pin.
* **Heal already-diverged sessions without a QR scan**: set `SESSION_REPAIR_TAG` to any new value and redeploy
  (runs once per value), or `POST /api/session/repair`. creds + pre-keys are kept, Signal sessions are rebuilt.
* `BAILEYS_LOG_LEVEL=info` temporarily: look for `recv retry request` (healthy self-repair),
  `recv retry request, but message not available` (getMessage miss) and `received error in ack` (e.g. 463).
* Sends now fail loudly on server NACKs (463) instead of reporting `sent`, and wait up to `READY_WAIT_MS`
  for the socket during reconnects / deploy hand-over.
* Tests (real Postgres): `TEST_DATABASE_URL=postgres://... node --test test/store.test.mjs test/signal-e2e.test.mjs`
* Do NOT set `shouldSyncHistoryMessage: () => false` on Baileys 7: it blocks the initial LID mappings.
