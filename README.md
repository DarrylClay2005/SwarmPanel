# SwarmPanel
Web control panel for all 8 music bots plus Aria.

## Features
- Live dashboard across all bot nodes (track, guild/channel, filter, queue depth, heartbeat).
- Direct bot controls from the panel to queue a track or playlist on a specific worker node.
- Per-guild loop controls from the panel with `off`, `song`, and `queue` modes.
- Bot explorer to view each bot's guilds and channels.
- Database control section to truncate a specific table or all tables in a schema.
- Session login protection for panel access.
- GitHub Pages remote front-end at `https://darrylclay2005.github.io/SwarmPanel/` for phone-friendly access.

## Setup
1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill required values.
4. Export env vars (or use your process manager).

## Environment variables
- Database:
  - `PANEL_DB_HOST`
  - `PANEL_DB_PORT`
  - `PANEL_DB_USER`
  - `PANEL_DB_PASSWORD` (required)
  - `PANEL_DB_DEFAULT_SCHEMA`
- Panel auth/session:
  - `PANEL_ADMIN_USERNAME`
  - `PANEL_ADMIN_PASSWORD` (required)
  - `PANEL_SESSION_SECRET`
  - `PANEL_CORS_ALLOWED_ORIGINS`
  - `PANEL_API_TOKEN_TTL_SECONDS`
  - `PANEL_PAGES_PUBLIC_URL`
- Discord tokens:
  - `GWS_DISCORD_TOKEN`
  - `HARMONIC_DISCORD_TOKEN`
  - `MAESTRO_DISCORD_TOKEN`
  - `MELODIC_DISCORD_TOKEN`
  - `NEXUS_DISCORD_TOKEN`
  - `RHYTHM_DISCORD_TOKEN`
  - `SYMPHONY_DISCORD_TOKEN`
  - `TUNESTREAM_DISCORD_TOKEN`
  - `ARIA_DISCORD_TOKEN`

## Run
- `uvicorn app.main:app --host 0.0.0.0 --port 8787`
- Live GitHub Pages backend tunnel:
  - `scripts/start_live_backend.sh 8000`
  - Keep the script running while you want the GitHub Pages site connected.

## Notes
- Destructive DB actions require explicit confirmation text in the UI.
- Keep this panel behind trusted network access and strong credentials.
- Bot inventory/channels are fetched via Discord REST with each bot token.
- If a token is missing, that bot still appears in the dashboard but Discord inventory calls are disabled.
- The GitHub Pages site is a static front-end. It still needs a public SwarmPanel backend URL to talk to.
- `scripts/start_quick_tunnel.sh` opens a Cloudflare quick tunnel so the GitHub Pages site can reach your local panel without using your local IP on the phone.
