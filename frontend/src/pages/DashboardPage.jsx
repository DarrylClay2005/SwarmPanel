import { useCallback, useEffect } from "react";
import { useState } from "react";
import { Activity, Bot, ListMusic, RefreshCw, Siren } from "lucide-react";
import { apiFetch, cachedFetch, prefetchFetch, query } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { BotCard, IntelligenceView, SessionTable } from "../components/swarm.jsx";
import { Metric, MetricGrid, Notice, Page, SectionHead, SkeletonGrid } from "../components/ui.jsx";

function mergeRowsByKey(previous = [], next = [], key = "key") {
  if (!Array.isArray(next) || next.length === 0) return Array.isArray(previous) ? previous : [];
  const rows = new Map();
  (Array.isArray(previous) ? previous : []).forEach((row, index) => {
    const rowKey = String(row?.[key] || row?.id || index);
    rows.set(rowKey, row);
  });
  next.forEach((row, index) => {
    const rowKey = String(row?.[key] || row?.id || index);
    rows.set(rowKey, { ...(rows.get(rowKey) || {}), ...row });
  });
  return Array.from(rows.values());
}

function mergeDashboard(current, next) {
  if (!next) return current;
  if (!current) return next;
  return {
    ...current,
    ...next,
    bots: mergeRowsByKey(current.bots, next.bots, "key"),
    sessions: Array.isArray(next.sessions) ? next.sessions : current.sessions,
  };
}

function sessionsFromBots(bots) {
  return bots.flatMap((bot) => (bot.sessions || []).map((session) => ({
    ...session,
    bot_name: bot.display_name,
    bot_key: bot.key,
  })));
}

export default function DashboardPage({ ctx }) {
  const [state, setState] = useState({
    dashboard: null,
    bots: null,
    intelligence: null,
    loading: true,
    refreshing: false,
    error: "",
  });

  const load = useCallback(async ({ background = false } = {}) => {
    setState((current) => ({
      ...current,
      loading: !current.dashboard && !current.bots && !background,
      refreshing: Boolean(current.dashboard || current.bots) || background,
      error: background ? current.error : "",
    }));
    const guildId = ctx.session.guild_id || ctx.session.account_guild_id;
    try {
      const [dashboard, bots, intelligence] = await Promise.allSettled([
        apiFetch("/api/dashboard"),
        cachedFetch("/api/bots", { ttl: 30_000, staleTtl: 180_000, storage: "local" }),
        apiFetch(`/api/music-intelligence${query({ guild_id: guildId, limit: 10 })}`).catch((error) => ({ error: error.message })),
      ]);
      setState((current) => ({
        dashboard: dashboard.status === "fulfilled" ? mergeDashboard(current.dashboard, dashboard.value) : current.dashboard,
        bots: bots.status === "fulfilled" ? bots.value : current.bots,
        intelligence: intelligence.status === "fulfilled" ? intelligence.value : current.intelligence,
        loading: false,
        refreshing: false,
        error: dashboard.status === "rejected" ? dashboard.reason.message : "",
      }));
    } catch (error) {
      setState((current) => ({ ...current, loading: false, refreshing: false, error: error.message }));
    }
  }, [ctx.session.account_guild_id, ctx.session.guild_id]);

  useEffect(() => {
    load();
    prefetchFetch("/api/bots", { ttl: 60_000, staleTtl: 300_000, storage: "local" });
  }, [load]);

  useLiveRefresh(() => load({ background: true }), { interval: 10_000 });

  const dashboard = state.dashboard || {};
  const catalogBots = state.bots?.bots || [];
  const bots = dashboard.bots?.length ? dashboard.bots : catalogBots;
  const sessions = Array.isArray(dashboard.sessions) ? dashboard.sessions : sessionsFromBots(bots);
  const active = sessions.filter((session) => session.is_playing || session.session_state === "playing").length;
  const stale = bots.filter((bot) => String(bot.heartbeat_status || "").includes("stale") || bot.status === "offline").length;
  const isInitialLoad = state.loading && !bots.length;

  return (
    <Page
      title="Swarm Command Deck"
      eyebrow="Dashboard"
      actions={<button type="button" onClick={() => load()} disabled={state.refreshing}><RefreshCw size={16} />{state.refreshing ? "Updating" : "Refresh"}</button>}
    >
      {state.error ? <Notice tone="error">{state.error}</Notice> : null}
      <MetricGrid>
        <Metric icon={Bot} label="Bots" value={bots.length || catalogBots.length || 0} />
        <Metric icon={Activity} label="Live Sessions" value={active} />
        <Metric icon={Siren} label="Stale Nodes" value={stale} />
        <Metric icon={ListMusic} label="Queued" value={sessions.reduce((sum, item) => sum + Number(item.queue_count || 0), 0)} />
      </MetricGrid>
      {isInitialLoad ? <SkeletonGrid count={6} /> : (
        <section className="dashboard-grid" aria-busy={state.refreshing ? "true" : "false"}>
          <div className="panel wide live-bots-panel">
            <SectionHead title="Live Bots" count={bots.length} />
            <div className="bot-grid">
              {bots.map((bot) => <BotCard bot={bot} key={bot.key} />)}
            </div>
          </div>
          <div className="panel">
            <SectionHead title="Music Intelligence" />
            <IntelligenceView data={state.intelligence?.data} />
          </div>
          <div className="panel wide">
            <SectionHead title="Active Sessions" count={sessions.length} />
            <SessionTable sessions={sessions} />
          </div>
        </section>
      )}
    </Page>
  );
}
