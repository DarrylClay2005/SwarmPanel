from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode


@dataclass(frozen=True)
class BotDefinition:
    key: str
    display_name: str
    kind: str
    token_env: str
    db_schema: Optional[str] = None
    table_prefix: Optional[str] = None


DISCORD_PERMISSION_BITS = {
    "Ban Members": 2,
    "Manage Channels": 4,
    "View Channels": 10,
    "Send Messages": 11,
    "Manage Messages": 13,
    "Embed Links": 14,
    "Attach Files": 15,
    "Read Message History": 16,
    "Connect": 20,
    "Speak": 21,
    "Use Voice Activity": 25,
    "Manage Roles": 28,
    "Use Application Commands": 31,
    "Request To Speak": 32,
    "Timeout Members": 40,
}

MUSIC_BOT_PERMISSIONS = [
    "View Channels",
    "Send Messages",
    "Embed Links",
    "Attach Files",
    "Read Message History",
    "Connect",
    "Speak",
    "Use Voice Activity",
    "Use Application Commands",
    "Request To Speak",
    "Manage Channels",
]

ARIA_BOT_PERMISSIONS = [
    "View Channels",
    "Send Messages",
    "Embed Links",
    "Attach Files",
    "Read Message History",
    "Use Application Commands",
    "Manage Messages",
    "Manage Channels",
    "Manage Roles",
    "Ban Members",
    "Timeout Members",
]

BOT_ACCENTS = {
    "gws": "#cba6f7",
    "harmonic": "#89b4fa",
    "maestro": "#a6e3a1",
    "melodic": "#fab387",
    "nexus": "#f38ba8",
    "rhythm": "#94e2d5",
    "symphony": "#f9e2af",
    "tunestream": "#b4befe",
    "aria": "#cba6f7",
}

BOT_CAPABILITY_SUMMARIES = {
    "music": "Music worker node: slash commands, queue controls, voice/stage playback, feedback messages, embeds, buttons, and channel status/topic updates.",
    "orchestrator": "Aria orchestrator: slash commands, swarm routing, AI/game/economy tools, scheduled messages, moderation actions, roles, channel locks, and server utilities.",
}


MUSIC_BOTS = [
    BotDefinition("gws", "GWS", "music", "GWS_DISCORD_TOKEN", "discord_music_gws", "gws"),
    BotDefinition("harmonic", "Harmonic", "music", "HARMONIC_DISCORD_TOKEN", "discord_music_harmonic", "harmonic"),
    BotDefinition("maestro", "Maestro", "music", "MAESTRO_DISCORD_TOKEN", "discord_music_maestro", "maestro"),
    BotDefinition("melodic", "Melodic", "music", "MELODIC_DISCORD_TOKEN", "discord_music_melodic", "melodic"),
    BotDefinition("nexus", "Nexus", "music", "NEXUS_DISCORD_TOKEN", "discord_music_nexus", "nexus"),
    BotDefinition("rhythm", "Rhythm", "music", "RHYTHM_DISCORD_TOKEN", "discord_music_rhythm", "rhythm"),
    BotDefinition("symphony", "Symphony", "music", "SYMPHONY_DISCORD_TOKEN", "discord_music_symphony", "symphony"),
    BotDefinition("tunestream", "Tunestream", "music", "TUNESTREAM_DISCORD_TOKEN", "discord_music_tunestream", "tunestream"),
]

ARIA_BOT = BotDefinition("aria", "Aria", "orchestrator", "ARIA_DISCORD_TOKEN")

ALL_BOTS = [*MUSIC_BOTS, ARIA_BOT]
BOT_INDEX = {bot.key: bot for bot in ALL_BOTS}


def permission_value(permission_names: list[str]) -> int:
    return sum(1 << DISCORD_PERMISSION_BITS[name] for name in permission_names)


def permissions_for_bot(bot: BotDefinition) -> list[str]:
    if bot.kind == "orchestrator":
        return ARIA_BOT_PERMISSIONS
    return MUSIC_BOT_PERMISSIONS


def invite_url_for_bot(client_id: str, permissions: int, guild_id: str | None = None) -> str:
    params = {
        "client_id": client_id,
        "permissions": str(permissions),
        "scope": "bot applications.commands",
    }
    if guild_id:
        params["guild_id"] = str(guild_id)
        params["disable_guild_select"] = "true"
    return f"https://discord.com/oauth2/authorize?{urlencode(params)}"
