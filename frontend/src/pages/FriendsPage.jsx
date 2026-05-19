import { useCallback, useEffect, useState } from "react";
import { Check, RefreshCw, X } from "lucide-react";
import { apiFetch } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { EmptyState, Page, SkeletonGrid } from "../components/ui.jsx";
import { initials } from "../utils/format.js";

export default function FriendsPage({ ctx }) {
  const [state, setState] = useState({ incoming: [], outgoing: [], friends: [] });
  const [loading, setLoading] = useState(true);

  const load = useCallback(async ({ background = false } = {}) => {
    if (!background) setLoading(true);
    try {
      const [requests, friends] = await Promise.all([apiFetch("/api/friends/requests"), apiFetch("/api/me/friends")]);
      setState({ incoming: requests.incoming || [], outgoing: requests.outgoing || [], friends: friends.friends || [] });
    } catch (error) {
      if (!background) ctx.showToast(error.message, "error");
    } finally {
      if (!background) setLoading(false);
    }
  }, [ctx]);

  useEffect(() => { load(); }, [load]);
  useLiveRefresh(() => load({ background: true }), { interval: 20_000 });

  async function respond(id, action) {
    try {
      await apiFetch(`/api/friends/requests/${id}`, { method: "POST", body: JSON.stringify({ action }) });
      await load();
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }

  return (
    <Page title="Friends" eyebrow="Swarm Social" actions={<button type="button" onClick={() => load()}><RefreshCw size={16} />Refresh</button>}>
      {loading ? <SkeletonGrid count={3} /> : (
        <section className="settings-grid">
          <FriendColumn title="Incoming" rows={state.incoming} action={(row) => <><button type="button" onClick={() => respond(row.id, "accept")}><Check size={16} />Accept</button><button type="button" onClick={() => respond(row.id, "decline")}><X size={16} />Decline</button></>} />
          <FriendColumn title="Outgoing" rows={state.outgoing} action={(row) => <button type="button" onClick={() => respond(row.id, "cancel")}><X size={16} />Cancel</button>} />
          <FriendColumn title="Friends" rows={state.friends} action={() => null} />
        </section>
      )}
    </Page>
  );
}

function FriendColumn({ title, rows, action }) {
  return (
    <section className="panel form-panel">
      <div className="section-head"><h2>{title}</h2><span>{rows.length}</span></div>
      {rows.map((row) => <article className="event" key={row.id || `${row.username}-${row.guild_id}`}><UserLine user={row} /><div className="inline-controls">{action(row)}</div></article>)}
      {!rows.length ? <EmptyState title="None" /> : null}
    </section>
  );
}

function UserLine({ user }) {
  const src = user.avatar_url || user.server_icon_url;
  return <span className="mini-row"><span className="avatar">{src ? <img src={src} alt="" loading="lazy" decoding="async" /> : initials(user.display_name || user.username)}</span><span><strong>{user.display_name || user.username}</strong><small>{user.server_name || user.guild_id}</small></span></span>;
}
