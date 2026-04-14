from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BotDefinition:
    key: str
    display_name: str
    kind: str
    token_env: str
    db_schema: Optional[str] = None
    table_prefix: Optional[str] = None


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
