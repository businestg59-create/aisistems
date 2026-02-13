# Telegram Business Bot (Render + Supabase)

## Local run

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env       # Windows: Copy-Item .env.example .env
python -m app
```

For local development use `MODE=polling`.

## Environment variables

Required:

- `BOT_TOKEN`
- `DATABASE_URL` (Supabase Postgres DSN)
- `MODE` (`webhook` or `polling`)
- `WEBHOOK_BASE_URL` (`https://<service>.onrender.com` for Render)
- `WEBHOOK_PATH` (default: `/tg/webhook`)
- `OPENAI_API_KEY` (if RAG/LLM is enabled)

Optional:

- `ADMIN_CHAT_ID`
- `OPENAI_MODEL` (default: `gpt-4.1-mini`)
- `OPENAI_EMBEDDING_MODEL` (default: `text-embedding-3-small`)
- `KB_SITES` (comma-separated URLs)

## Database schema (Supabase)

1. Open Supabase SQL Editor.
2. Run `sql/schema.sql`.

Schema includes:

- `connections`, `clients`, `leads`, `settings`, `escalations`
- `kb_chunks` with `vector(1536)`
- indexes on `(business_connection_id, client_chat_id)`
- fields `escalation_open`, `escalation_last_at`, `created_at`, `updated_at`

## Render deployment

### Blueprint way (recommended)

1. Push repo to GitHub.
2. In Render: `New +` -> `Blueprint`.
3. Select this repository.
4. Render reads `render.yaml` and creates Web Service.
5. Fill secret env vars (`BOT_TOKEN`, `DATABASE_URL`, `WEBHOOK_BASE_URL`, etc.).

Start command:

```bash
uvicorn app.webapp:app --host 0.0.0.0 --port $PORT
```

### Manual Web Service way

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app.webapp:app --host 0.0.0.0 --port $PORT`

## Webhook mode behavior

FastAPI app entrypoint is `app.webapp:app`.

On startup, if `MODE=webhook`:

- builds `full_webhook_url = WEBHOOK_BASE_URL + WEBHOOK_PATH`
- calls `bot.set_webhook(..., drop_pending_updates=False)`

Webhook endpoint:

- `POST /tg/webhook` (or your custom `WEBHOOK_PATH`)

## Health endpoints

- `GET /` -> `OK`
- `GET /health` -> quick JSON (`{"ok": true|false}`), only lightweight DB `SELECT 1` (timeout 1s), no OpenAI calls
- `GET /ready` -> `ok: true` only when DB is reachable

## UptimeRobot setup

1. Monitor type: `HTTP(s)`.
2. URL: `https://<service>.onrender.com/health`.
3. Method: `GET`.
4. Interval: every 5 minutes.

## RAG ingestion to Supabase

Command:

```bash
python -m app.rag.ingest
```

It crawls `KB_SITES`, chunks content, builds embeddings, and upserts data into `kb_chunks` in Postgres (no local Chroma).
