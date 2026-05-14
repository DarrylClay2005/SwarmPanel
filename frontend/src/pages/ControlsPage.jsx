import { useCallback, useEffect, useState } from "react";
import { RefreshCw, Send, WandSparkles } from "lucide-react";
import { apiFetch, cachedFetch, clearCache, query } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { CONTROL_ACTIONS } from "../config.js";
import { ChannelSelect, ControlState } from "../components/swarm.jsx";
import { Page, SectionHead } from "../components/ui.jsx";
import { payloadForAction } from "../utils/control.js";
import { uniqueBy } from "../utils/format.js";

const FILTER_OPTIONS = [
  ["none", "None"],
  ["nightcore", "Nightcore"],
  ["bassboost", "Bassboost"],
  ["vaporwave", "Vaporwave"],
  ["8d", "8D"],
  ["karaoke", "Karaoke"],
  ["tremolo", "Tremolo"],
  ["vibrato", "Vibrato"],
  ["lowpass", "Low Pass"],
  ["lofi", "Lo-fi"],
  ["electronic", "Electronic"],
  ["party", "Party"],
  ["radio", "Radio"],
  ["cinema", "Cinema"],
];

export default function ControlsPage({ ctx }) {
  const [catalog, setCatalog] = useState({ bots: [], loading: true });
  const [dashboard, setDashboard] = useState(null);
  const [inventory, setInventory] = useState(null);
  const [controlState, setControlState] = useState(null);
  const [matrix, setMatrix] = useState(null);
  const [form, setForm] = useState({
    bot_key: "",
    guild_id: ctx.session.guild_id || ctx.session.account_guild_id || "",
    action: "PLAY",
    source_url: "",
    voice_channel_id: "",
    text_channel_id: "",
    loop_mode: "queue",
    filter_mode: "none",
  });
  const [busy, setBusy] = useState(false);

  const loadBase = useCallback(async () => {
    const [bots, dash] = await Promise.all([cachedFetch("/api/bots", { ttl: 30_000 }), apiFetch("/api/dashboard")]);
    const musicBots = (bots.bots || []).filter((bot) => bot.kind === "music");
    setCatalog({ bots: musicBots, loading: false });
    setDashboard(dash);
    setForm((current) => ({
      ...current,
      bot_key: current.bot_key || musicBots[0]?.key || "",
      guild_id: current.guild_id || dash.sessions?.[0]?.guild_id || "",
    }));
  }, []);

  useEffect(() => {
    loadBase().catch((error) => ctx.showToast(error.message, "error"));
  }, [ctx, loadBase]);

  useEffect(() => {
    if (!form.bot_key) return;
    apiFetch(`/api/bots/${form.bot_key}/inventory`).then(setInventory).catch((error) => setInventory({ error: error.message, guilds: [] }));
  }, [form.bot_key]);

  const loadReadiness = useCallback(async () => {
    if (!form.bot_key || !form.guild_id) return;
    const [state, controlMatrix] = await Promise.allSettled([
      apiFetch(`/api/bots/${form.bot_key}/control-state${query({ guild_id: form.guild_id })}`),
      apiFetch(`/api/guilds/${form.guild_id}/control-matrix`),
    ]);
    setControlState(state.status === "fulfilled" ? state.value : { error: state.reason.message });
    setMatrix(controlMatrix.status === "fulfilled" ? controlMatrix.value : { error: controlMatrix.reason.message, bots: [] });
  }, [form.bot_key, form.guild_id]);

  useEffect(() => {
    loadReadiness();
  }, [loadReadiness]);

  useLiveRefresh(loadReadiness, { enabled: Boolean(form.bot_key && form.guild_id), interval: 8_000 });
  useLiveRefresh(loadBase, { interval: 24_000 });

  useEffect(() => {
    const session = controlState?.session;
    if (!session) return;
    setForm((current) => ({
      ...current,
      voice_channel_id: current.voice_channel_id || session.home_channel_id || session.channel_id || "",
      text_channel_id: current.text_channel_id || session.feedback_channel_id || "",
      loop_mode: session.loop_mode || current.loop_mode || "queue",
      filter_mode: session.filter_mode || current.filter_mode || "none",
    }));
  }, [controlState]);

  const guilds = inventory?.guilds || [];
  const selectedGuild = guilds.find((guild) => String(guild.id) === String(form.guild_id));
  const channels = selectedGuild?.channels || inventory?.channels || [];
  const voiceChannels = channels.filter((channel) => [2, 13].includes(Number(channel.type)));
  const textChannels = channels.filter((channel) => [0, 5].includes(Number(channel.type)));
  const sessionGuilds = uniqueBy((dashboard?.sessions || []).map((session) => ({ id: session.guild_id, name: session.guild_name || `Guild ${session.guild_id}` })), "id");

  function update(key, value) {
    setForm((current) => ({
      ...current,
      [key]: value,
      ...(["bot_key", "guild_id"].includes(key) ? { voice_channel_id: "", text_channel_id: "" } : {}),
    }));
  }

  async function submit(event) {
    event.preventDefault();
    setBusy(true);
    try {
      const payload = payloadForAction(form);
      const data = await apiFetch("/api/bots/control", {
        method: "POST",
        body: JSON.stringify({ bot_key: form.bot_key, guild_id: form.guild_id, action: form.action, payload }),
      });
      clearCache();
      ctx.showToast(data.message || `${form.action} accepted.`, "success");
      loadReadiness().catch(() => {});
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Page title="Direct Playback Control" eyebrow="Controls" actions={<button type="button" onClick={loadBase}><RefreshCw size={16} />Refresh</button>}>
      <section className="control-layout">
        <form className="panel form-panel" onSubmit={submit}>
          <label className="field"><span>Bot</span><select value={form.bot_key} onChange={(event) => update("bot_key", event.target.value)}>{catalog.bots.map((bot) => <option value={bot.key} key={bot.key}>{bot.display_name}</option>)}</select></label>
          <label className="field"><span>Guild</span><input value={form.guild_id} onChange={(event) => update("guild_id", event.target.value)} list="known-guilds" required /><datalist id="known-guilds">{sessionGuilds.map((guild) => <option key={guild.id} value={guild.id}>{guild.name}</option>)}</datalist></label>
          <label className="field"><span>Action</span><select value={form.action} onChange={(event) => update("action", event.target.value)}>{CONTROL_ACTIONS.filter((action) => ctx.isAdmin || action !== "RESTART").map((action) => <option key={action} value={action}>{action}</option>)}</select></label>
          {form.action === "PLAY" ? <label className="field"><span>Source URL or search</span><input value={form.source_url} onChange={(event) => update("source_url", event.target.value)} placeholder="https://youtube.com/... or search terms" /></label> : null}
          {["PLAY", "SET_HOME", "SMART_RECOMMEND"].includes(form.action) ? (
            <div className="two-col">
              <label className="field"><span>Voice</span><ChannelSelect value={form.voice_channel_id} channels={voiceChannels} onChange={(value) => update("voice_channel_id", value)} /></label>
              <label className="field"><span>Text</span><ChannelSelect value={form.text_channel_id} channels={textChannels} onChange={(value) => update("text_channel_id", value)} optional /></label>
            </div>
          ) : null}
          {form.action === "LOOP" ? <label className="field"><span>Loop</span><select value={form.loop_mode} onChange={(event) => update("loop_mode", event.target.value)}><option value="off">Off</option><option value="song">Song</option><option value="queue">Queue</option></select></label> : null}
          {form.action === "FILTER" ? <label className="field"><span>Filter</span><select value={form.filter_mode} onChange={(event) => update("filter_mode", event.target.value)}>{FILTER_OPTIONS.map(([value, label]) => <option value={value} key={value}>{label}</option>)}</select></label> : null}
          <div className="live-defaults">
            <strong>Live defaults</strong>
            <span>Voice: {controlState?.session?.home_channel_name || controlState?.session?.channel_name || form.voice_channel_id || "unknown"}</span>
            <span>Text: {controlState?.session?.feedback_channel_name || form.text_channel_id || "none"}</span>
            <span>Loop: {form.loop_mode || "queue"} / Filter: {form.filter_mode || "none"}</span>
          </div>
          <div className="actions-row">
            <button className="primary" type="submit" disabled={busy}><Send size={16} />{busy ? "Sending" : "Send Control"}</button>
            <button type="button" onClick={() => update("action", "SMART_RECOMMEND")}><WandSparkles size={16} />Smart Rec</button>
          </div>
        </form>
        <aside className="panel">
          <SectionHead title="Readiness" />
          <ControlState state={controlState} />
          <SectionHead title="Guild Matrix" count={matrix?.bots?.length || 0} />
          <div className="mini-stack">{(matrix?.bots || []).map((bot) => <ControlState state={bot} compact key={bot.key} />)}</div>
        </aside>
      </section>
    </Page>
  );
}
