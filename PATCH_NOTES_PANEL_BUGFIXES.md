# SwarmPanel bugfix patch notes

## Confirmed and fixed

1. **Foreign key integrity lock-leak** (`app/database.py`)
   - `truncate_table()` and `truncate_schema()` now restore `FOREIGN_KEY_CHECKS = 1` in `finally` blocks.
   - Added DB pool guards before destructive table operations.

2. **Concurrent websocket crash** (`app/main.py`)
   - Broadcast now iterates over a copy of `active_connections`.
   - Both disconnect cleanup paths now tolerate already-removed sockets with `try/except ValueError`.

3. **Discord API rate-limit bomb** (`app/discord_api.py`)
   - Guild hints are now resolved only when missing from the already-fetched guild list.
   - This prevents redundant `fetch_guild()` calls for every known guild on inventory refresh.

4. **Live cache memory leak** (`app/static/final_app.js`)
   - Added bounded/TTL pruning for `liveSessionPositionCache`.
   - Active sessions are preserved while stale track signatures are removed.

5. **DOM thrashing in live position tick** (`app/static/final_app.js`)
   - The 1-second tick no longer re-renders the entire sessions table and now-playing cards.
   - It updates only timestamp/progress text nodes via `data-position-key` / `data-progress-key`.

6. **DDL in high-frequency dashboard polling** (`app/database.py`)
   - `aria_interactions` table creation moved out of `get_dashboard_data()` and into startup connection initialization.
   - Dashboard polling now stays read-focused.

7. **Stacked dashboard HTTP requests** (`app/static/final_app.js`)
   - Dashboard, diagnostics, and metrics refresh now use recursive `setTimeout` loops that wait for the previous request to finish.
   - This prevents overlapping dashboard fetches during DB lag.

8. **Ephemeral session secret** (`app/config.py`)
   - Removed automatic random fallback for `PANEL_SESSION_SECRET`.
   - Missing session secret now fails fast through settings validation so users/API tokens do not silently invalidate on every restart.

9. **Dead background command queue** (`app/main.py`)
   - The unused `command_queue` startup worker is no longer started.
   - A disabled compatibility stub remains so old imports do not crash.

## Validation performed

- Python syntax compile checked for:
  - `app/config.py`
  - `app/database.py`
  - `app/discord_api.py`
  - `app/main.py`
- JavaScript syntax checked with `node --check app/static/final_app.js`.

## Important deployment note

Make sure your real `.env` contains a stable `PANEL_SESSION_SECRET`. Example:

```env
PANEL_SESSION_SECRET=replace_with_a_long_random_stable_value
```

Do not regenerate this on every restart.
