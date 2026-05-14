import { useCallback, useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { apiFetch, cachedFetch } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { InviteCard } from "../components/swarm.jsx";
import { Notice, Page } from "../components/ui.jsx";

export default function InvitesPage({ ctx }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const load = useCallback(async ({ background = false } = {}) => {
    try {
      setData(background ? await apiFetch("/api/bots") : await cachedFetch("/api/bots", { ttl: 30_000 }));
      setError("");
    } catch (loadError) {
      if (!background) setError(loadError.message);
    }
  }, []);
  useEffect(() => { load(); }, [load]);
  useLiveRefresh(() => load({ background: true }), { interval: 45_000 });
  return (
    <Page title="Bot Access" eyebrow="Invites" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      {error ? <Notice tone="error">{error}</Notice> : null}
      <div className="invite-grid">
        {(data?.invite_bots || []).map((bot) => <InviteCard bot={bot} key={bot.key} />)}
      </div>
    </Page>
  );
}
