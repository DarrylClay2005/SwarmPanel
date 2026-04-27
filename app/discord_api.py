import asyncio
import logging
from typing import Any

import aiohttp


logger = logging.getLogger("swarm_panel")
DISCORD_API_BASE = "https://discord.com/api/v10"

CHANNEL_TYPE_NAMES = {
    0: "text",
    2: "voice",
    4: "category",
    5: "announcement",
    13: "stage",
    15: "forum",
}


class DiscordAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, path: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.path = path


class DiscordInventoryService:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.cache = {}
        self.cache_ttl = 300.0  # 5-minute memory cache

    async def connect(self) -> None:
        if self.session:
            return
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _request_json(
        self,
        token: str,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if not token:
            raise DiscordAPIError("Missing bot token for request")
        if not self.session:
            raise DiscordAPIError("Discord session not initialized")

        headers = {"Authorization": f"Bot {token}"}
        url = f"{DISCORD_API_BASE}{path}"

        for _ in range(4):
            async with self.session.request(method, url, headers=headers, params=params) as resp:
                if resp.status == 429:
                    payload = await resp.json(content_type=None)
                    await asyncio.sleep(float(payload.get("retry_after", 1.0)) + 0.1)
                    continue

                if resp.status >= 400:
                    body = await resp.text()
                    raise DiscordAPIError(
                        f"{method} {path} failed ({resp.status}): {body[:200]}",
                        status_code=resp.status,
                        path=path,
                    )

                return await resp.json(content_type=None)

        raise DiscordAPIError(f"{method} {path} exceeded retry budget")


    async def _cached_request(self, token: str, path: str, params: dict[str, Any] | None = None) -> Any:
        import time
        param_key = tuple(sorted(params.items())) if params else ()
        cache_key = f"{token}:{path}:{param_key}"
        
        now = time.time()
        if cache_key in self.cache:
            data, exp = self.cache[cache_key]
            if now < exp:
                return data
                
        data = await self._request_json(token, "GET", path, params=params)
        self.cache[cache_key] = (data, now + self.cache_ttl)
        return data

    async def fetch_identity(self, token: str) -> dict[str, Any]:
        data = await self._cached_request(token, "/users/@me")
        user_id = str(data.get("id")) if data.get("id") else None
        avatar = data.get("avatar")
        return {
            "id": user_id,
            "username": data.get("username"),
            "global_name": data.get("global_name"),
            "discriminator": data.get("discriminator"),
            "avatar": avatar,
            "avatar_url": f"https://cdn.discordapp.com/avatars/{user_id}/{avatar}.png?size=128" if user_id and avatar else None,
        }

    async def fetch_guilds(self, token: str) -> list[dict[str, Any]]:
        data = await self._cached_request(token, "/users/@me/guilds", params={"limit": 200})
        guilds = []
        for guild in data if isinstance(data, list) else []:
            guilds.append(
                {
                    "id": str(guild.get("id")),
                    "name": guild.get("name") or f"Guild {guild.get('id')}",
                    "owner": bool(guild.get("owner")),
                    "permissions": guild.get("permissions"),
                }
            )
        return guilds

    async def fetch_guild(self, token: str, guild_id: int | str) -> dict[str, Any]:
        try:
            data = await self._cached_request(token, f"/guilds/{int(guild_id)}")
            return {"id": str(data.get("id")), "name": data.get("name") or f"Guild {guild_id}"}
        except DiscordAPIError as e:
            if e.status_code in (403, 404):
                return {"id": str(guild_id), "name": f"Unknown/Inaccessible Guild {guild_id}"}
            raise

    async def fetch_guild_channels(self, token: str, guild_id: int | str) -> list[dict[str, Any]]:
        try:
            data = await self._cached_request(token, f"/guilds/{int(guild_id)}/channels")
        except DiscordAPIError as e:
            if e.status_code in (403, 404):
                return []
            raise
        channels = []
        for channel in data if isinstance(data, list) else []:
            channel_type = int(channel.get("type", -1))
            channels.append(
                {
                    "id": str(channel.get("id")),
                    "name": channel.get("name") or f"Channel {channel.get('id')}",
                    "type": channel_type,
                    "type_name": CHANNEL_TYPE_NAMES.get(channel_type, str(channel_type)),
                    "parent_id": str(channel["parent_id"]) if channel.get("parent_id") else None,
                }
            )
        return channels

    async def fetch_inventory(
        self,
        token: str,
        *,
        include_channels: bool = True,
        guild_hints: list[int | str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"identity": None, "guilds": [], "errors": []}

        try:
            payload["identity"] = await self.fetch_identity(token)
        except Exception as exc:
            payload["errors"].append(f"identity: {exc}")

        guilds: list[dict[str, Any]] = []
        try:
            guilds = await self.fetch_guilds(token)
        except Exception as exc:
            payload["errors"].append(f"guilds: {exc}")
            for guild_id in guild_hints or []:
                try:
                    guilds.append(await self.fetch_guild(token, int(guild_id)))
                except DiscordAPIError as hint_exc:
                    if hint_exc.status_code == 404:
                        payload["errors"].append(f"guild_hint {guild_id}: stale guild reference skipped")
                        continue
                    guilds.append({"id": str(guild_id), "name": f"Guild {guild_id}", "channels_error": str(hint_exc)})
                except Exception as hint_exc:
                    guilds.append({"id": str(guild_id), "name": f"Guild {guild_id}", "channels_error": str(hint_exc)})

        deduped_guilds: list[dict[str, Any]] = []
        seen_guild_ids: set[str] = set()
        for guild in guilds:
            guild_id = str(guild.get("id"))
            if guild_id in seen_guild_ids:
                continue
            seen_guild_ids.add(guild_id)
            deduped_guilds.append(guild)

        # Only resolve hinted guilds that were not returned by /users/@me/guilds.
        # This avoids N redundant Discord API calls on every inventory refresh.
        for guild_id in guild_hints or []:
            guild_id_str = str(guild_id)
            if guild_id_str in seen_guild_ids:
                continue
            try:
                guild = await self.fetch_guild(token, int(guild_id))
                deduped_guilds.append(guild)
                seen_guild_ids.add(str(guild.get("id") or guild_id_str))
            except DiscordAPIError as hint_exc:
                if hint_exc.status_code == 404:
                    payload["errors"].append(f"guild_hint {guild_id}: stale guild reference skipped")
                    continue
                deduped_guilds.append({"id": guild_id_str, "name": f"Guild {guild_id}", "channels_error": str(hint_exc)})
                seen_guild_ids.add(guild_id_str)
            except Exception as hint_exc:
                deduped_guilds.append({"id": guild_id_str, "name": f"Guild {guild_id}", "channels_error": str(hint_exc)})
                seen_guild_ids.add(guild_id_str)
        guilds = deduped_guilds

        for guild in guilds:
            if include_channels:
                try:
                    guild["channels"] = await self.fetch_guild_channels(token, guild["id"])
                except DiscordAPIError as exc:
                    guild["channels"] = []
                    guild["channels_error"] = "Guild is no longer reachable by this bot." if exc.status_code == 404 else str(exc)
                except Exception as exc:
                    guild["channels"] = []
                    guild["channels_error"] = str(exc)
            else:
                guild["channels"] = []
        payload["guilds"] = guilds
        return payload

    async def resolve_guild_channel_names(
        self,
        token: str,
        placements: list[tuple[int | str, int | str | None]],
    ) -> dict[tuple[int, int | None], dict[str, str | None]]:
        output: dict[tuple[str, str | None], dict[str, str | None]] = {}
        by_guild: dict[str, set[str | None]] = {}

        for guild_id, channel_id in placements:
            by_guild.setdefault(str(guild_id), set()).add(str(channel_id) if channel_id is not None else None)

        for guild_id, channel_ids in by_guild.items():
            guild_name = f"Guild {guild_id}"
            channel_name_map: dict[int, str] = {}
            try:
                guild = await self.fetch_guild(token, guild_id)
                guild_name = guild.get("name") or guild_name
            except Exception:
                pass

            try:
                channels = await self.fetch_guild_channels(token, guild_id)
                channel_name_map = {str(ch["id"]): ch.get("name") or str(ch["id"]) for ch in channels}
            except Exception:
                pass

            for channel_id in channel_ids:
                output[(guild_id, channel_id)] = {
                    "guild_name": guild_name,
                    "channel_name": channel_name_map.get(channel_id) if channel_id is not None else None,
                }
        return output
