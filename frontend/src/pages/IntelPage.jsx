import { useCallback, useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { apiFetch } from "../api.js";
import { EventList, JsonPanel } from "../components/swarm.jsx";
import { Page, SectionHead } from "../components/ui.jsx";

export default function IntelPage({ ctx }) {
  const [state, setState] = useState({ events: [], metrics: null, stability: null });
  const load = useCallback(async () => {
    const [events, metrics, stability] = await Promise.allSettled([
      apiFetch("/api/events?limit=80"),
      apiFetch("/api/metrics"),
      apiFetch("/api/stability"),
    ]);
    setState({
      events: events.status === "fulfilled" ? events.value.events || [] : [],
      metrics: metrics.status === "fulfilled" ? metrics.value : { error: metrics.reason?.message },
      stability: stability.status === "fulfilled" ? stability.value : { error: stability.reason?.message },
    });
  }, []);
  useEffect(() => { load(); const timer = window.setInterval(load, 8000); return () => window.clearInterval(timer); }, [load]);
  return (
    <Page title="Errors And Metrics" eyebrow="Intel" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      <section className="dashboard-grid">
        <div className="panel wide"><SectionHead title="Events" count={state.events.length} /><EventList events={state.events} /></div>
        <div className="panel"><SectionHead title="Metrics" /><JsonPanel data={state.metrics} /></div>
        <div className="panel"><SectionHead title="Stability" /><JsonPanel data={state.stability} /></div>
      </section>
    </Page>
  );
}
