# WhatsApp Gateway (Baileys + Express)

Self-hosted, Green API-compatible WhatsApp gateway. Sessions are stored in Postgres
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

## Deploy
See the deployment steps in the chat answer / below section "Koyeb".

### Koyeb
1. Push this folder to a GitHub repo.
2. Create a free Postgres (Neon or Supabase) and copy its connection string.
3. Koyeb → Create Service → GitHub → select repo → Builder: **Dockerfile**.
4. Instance: **Free**. Port **8000** (HTTP). Health check path **/health**.
5. Env vars: `INSTANCE_ID`, `API_TOKEN`, `DATABASE_URL`, `DEFAULT_COUNTRY_CODE`, `PORT=8000`.
6. Deploy, then open `https://<app>.koyeb.app/qr?instance_id=…&api_token=…` and scan.

### Keep it awake
Koyeb's free instance sleeps after 1 hour without traffic. Ping `/health` every 5 min
(UptimeRobot, or the Cloudflare Worker cron below).

```js
// Cloudflare Worker with a Cron Trigger of */5 * * * *
export default {
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(fetch(env.GATEWAY_URL + '/health'));
  },
};
```
