import { useCallback, useEffect, useState } from "react";
import { Activity, KeyRound, ListMusic, Mail, RefreshCw, Search, Send, ShieldAlert, Trash2 } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { DataTable } from "../components/swarm.jsx";
import { Metric, MetricGrid, Page, SectionHead, SkeletonGrid } from "../components/ui.jsx";

export default function AccountsPage({ ctx }) {
  const [q, setQ] = useState("");
  const [rows, setRows] = useState([]);
  const [stability, setStability] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [passwords, setPasswords] = useState({});
  const load = useCallback(async ({ background = false } = {}) => {
    try {
      if (!background) setLoading(true);
      const [accounts, stabilityData, metricsData] = await Promise.allSettled([
        apiFetch(`/api/swarm-accounts/admin${query({ query: q, limit: 100 })}`),
        apiFetch("/api/stability"),
        apiFetch("/api/metrics"),
      ]);
      if (accounts.status === "fulfilled") setRows(accounts.value.data?.accounts || accounts.value.data || []);
      if (stabilityData.status === "fulfilled") setStability(stabilityData.value);
      if (metricsData.status === "fulfilled") setMetrics(metricsData.value);
    } catch (error) {
      if (!background) ctx.showToast(error.message, "error");
    } finally {
      if (!background) setLoading(false);
    }
  }, [ctx, q]);
  useEffect(() => { load(); }, [load]);
  useLiveRefresh(() => load({ background: true }), { interval: 30_000 });
  async function mutate(path, payload, message) {
    try {
      await apiFetch(path, { method: "POST", body: JSON.stringify(payload) });
      ctx.showToast(message, "success");
      await load();
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  return (
    <Page title="SwarmPanel Recovery" eyebrow="Accounts" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      <MetricGrid>
        <Metric icon={Activity} label="Metric Bots" value={metrics?.totals?.bots || 0} />
        <Metric icon={ListMusic} label="Live Queue" value={metrics?.totals?.queued_tracks || 0} />
        <Metric icon={ListMusic} label="Backup Queue" value={metrics?.totals?.backup_tracks || 0} />
        <Metric icon={ShieldAlert} label="Recovery Cooldowns" value={stability?.cooldowns?.length || 0} />
      </MetricGrid>
      {loading ? <SkeletonGrid count={4} /> : (
        <section className="dashboard-grid">
          <div className="panel wide">
            <SectionHead title="Recovery Metrics" count={metrics?.bots?.length || 0} />
            <DataTable rows={(metrics?.bots || []).map((bot) => ({
              bot: bot.display_name || bot.key,
              status: bot.status,
              guilds: bot.metrics?.length || 0,
              queue: (bot.metrics || []).reduce((sum, row) => sum + Number(row.queue_count || 0), 0),
              backup: (bot.metrics || []).reduce((sum, row) => sum + Number(row.backup_queue_count || 0), 0),
              recovering: (bot.metrics || []).filter((row) => row.recovery_pending).length,
              stale: (bot.metrics || []).filter((row) => row.stale).length,
            }))} />
          </div>
          <div className="panel">
            <SectionHead title="Cooldowns" count={stability?.cooldowns?.length || 0} />
            <DataTable rows={stability?.cooldowns || []} />
          </div>
          <div className="panel wide">
            <SectionHead title="Recent Repairs" count={stability?.recent_repairs?.length || 0} />
            <DataTable rows={stability?.recent_repairs || []} />
          </div>
        </section>
      )}
      <div className="toolbar"><div className="search-box"><Search size={16} /><input value={q} onChange={(event) => setQ(event.target.value)} placeholder="Search accounts" /></div></div>
      <DataTable rows={rows} actions={(row) => (
        <div className="table-actions">
          <button type="button" onClick={() => mutate("/api/swarm-accounts/email-verified", { account_id: row.id, verified: !row.email_verified_at }, "Email flag updated.")}><Mail size={14} />Email</button>
          <input className="mini-input" type="password" placeholder="new password" value={passwords[row.id] || ""} onChange={(event) => setPasswords((current) => ({ ...current, [row.id]: event.target.value }))} />
          <button type="button" onClick={() => mutate("/api/swarm-accounts/reset-password", { account_id: row.id, new_password: passwords[row.id] || "" }, "Password reset.")}><KeyRound size={14} />Reset</button>
          <button type="button" onClick={() => mutate("/api/swarm-accounts/resend-verification", { account_id: row.id }, "Verification sent.")}><Send size={14} />Resend</button>
          <button className="danger" type="button" onClick={() => mutate("/api/swarm-accounts/delete", { account_id: row.id }, "Account deleted.")}><Trash2 size={14} />Delete</button>
        </div>
      )} />
    </Page>
  );
}
