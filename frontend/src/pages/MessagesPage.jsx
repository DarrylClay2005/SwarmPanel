import { useCallback, useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import { MessageCircle, RefreshCw, Search, Send } from "lucide-react";
import { apiFetch, cachedFetch, query } from "../api.js";
import { useLiveRefresh } from "../hooks/useLiveRefresh.js";
import { EmptyState, Page, SkeletonGrid } from "../components/ui.jsx";
import { initials } from "../utils/format.js";

export default function MessagesPage({ ctx }) {
  const location = useLocation();
  const [threads, setThreads] = useState([]);
  const [messages, setMessages] = useState([]);
  const [selected, setSelected] = useState(null);
  const [search, setSearch] = useState("");
  const [results, setResults] = useState([]);
  const [body, setBody] = useState("");
  const [loading, setLoading] = useState(true);
  const selectedId = selected?.account_id || selected?.id;

  const loadThreads = useCallback(async ({ background = false } = {}) => {
    if (!background) setLoading(true);
    try {
      const data = await apiFetch("/api/messages/threads");
      setThreads(data.threads || []);
      if (!selected && data.threads?.length) setSelected(data.threads[0]);
    } catch (error) {
      if (!background) ctx.showToast(error.message, "error");
    } finally {
      if (!background) setLoading(false);
    }
  }, [ctx, selected]);

  const loadMessages = useCallback(async ({ background = false } = {}) => {
    if (!selectedId) return;
    try {
      const data = await apiFetch(`/api/messages/${selectedId}`);
      setMessages(data.messages || []);
      if (background) loadThreads({ background: true });
    } catch (error) {
      if (!background) ctx.showToast(error.message, "error");
    }
  }, [ctx, loadThreads, selectedId]);

  useEffect(() => { loadThreads(); }, [loadThreads]);
  useEffect(() => { if (location.state?.user) setSelected(location.state.user); }, [location.state]);
  useEffect(() => { loadMessages(); }, [loadMessages]);
  useEffect(() => {
    if (!search.trim()) {
      setResults([]);
      return undefined;
    }
    const timer = window.setTimeout(async () => {
      try {
        const data = await cachedFetch(`/api/users/directory${query({ q: search, limit: 8 })}`, { ttl: 10_000 });
        setResults(data.users || []);
      } catch (_error) {
        setResults([]);
      }
    }, 220);
    return () => window.clearTimeout(timer);
  }, [search]);
  useLiveRefresh(() => loadMessages({ background: true }), { enabled: Boolean(selectedId), interval: 12_000 });

  async function send(event) {
    event.preventDefault();
    if (!selectedId || !body.trim()) return;
    try {
      const data = await apiFetch(`/api/messages/${selectedId}`, { method: "POST", body: JSON.stringify({ body: body.trim() }) });
      setMessages((current) => [...current, data.message]);
      setBody("");
      loadThreads({ background: true });
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }

  return (
    <Page title="Messages" eyebrow="Swarm Social" actions={<button type="button" onClick={() => loadThreads()}><RefreshCw size={16} />Refresh</button>}>
      <section className="settings-grid">
        <aside className="panel list-panel">
          <div className="search-box"><Search size={16} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Find a user" /></div>
          {results.map((user) => <button className="collection-row" type="button" key={user.id} onClick={() => { setSelected(user); setSearch(""); setResults([]); }}><UserAvatar user={user} /><span><strong>{user.display_name || user.username}</strong><small>{user.server_name || user.guild_id}</small></span></button>)}
          {loading ? <SkeletonGrid count={2} /> : threads.map((thread) => <button className={`collection-row ${Number(selectedId) === Number(thread.account_id) ? "active" : ""}`} type="button" key={thread.account_id} onClick={() => setSelected(thread)}><UserAvatar user={thread} /><span><strong>{thread.display_name || thread.username}</strong><small>{thread.unread_count ? `${thread.unread_count} new` : thread.last_message || "No messages"}</small></span></button>)}
          {!loading && !threads.length && !results.length ? <EmptyState title="No conversations yet" /> : null}
        </aside>
        <section className="panel form-panel">
          <div className="section-head"><h2>{selected?.display_name || selected?.username || "Conversation"}</h2><MessageCircle size={18} /></div>
          <div className="event-list">
            {messages.map((message) => {
              const mine = message.username === ctx.session.username && String(message.guild_id) === String(ctx.session.account_guild_id || ctx.session.guild_id);
              return <article className="event" key={message.id}><strong>{mine ? "You" : (message.display_name || message.username)}</strong><p>{message.body}</p></article>;
            })}
            {!selectedId ? <EmptyState title="Choose someone to message" /> : null}
          </div>
          {selectedId ? <form className="comment-form" onSubmit={send}><input value={body} onChange={(event) => setBody(event.target.value)} maxLength={2000} placeholder="Write a message" /><button type="submit"><Send size={16} />Send</button></form> : null}
        </section>
      </section>
    </Page>
  );
}

function UserAvatar({ user }) {
  const src = user?.avatar_url || user?.server_icon_url;
  return <span className="avatar">{src ? <img src={src} alt="" loading="lazy" decoding="async" /> : initials(user?.display_name || user?.username || "SP")}</span>;
}
