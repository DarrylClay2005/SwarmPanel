import { useEffect, useState } from "react";
import { Search } from "lucide-react";
import { apiFetch, cachedFetch, query } from "../api.js";
import { UserCard } from "../components/swarm.jsx";
import { EmptyState, Page } from "../components/ui.jsx";

export default function UsersPage({ ctx }) {
  const [q, setQ] = useState("");
  const [users, setUsers] = useState([]);
  function loadUsers() {
    cachedFetch(`/api/users/directory${query({ q })}`, { ttl: 20_000, staleTtl: 120_000 }).then((data) => setUsers(data.users || [])).catch((error) => ctx.showToast(error.message, "error"));
  }
  useEffect(() => {
    const timer = window.setTimeout(() => {
      loadUsers();
    }, 220);
    return () => window.clearTimeout(timer);
  }, [ctx, q]);
  return (
    <Page title="Swarm Directory" eyebrow="Users">
      <div className="toolbar"><div className="search-box"><Search size={16} /><input value={q} onChange={(event) => setQ(event.target.value)} placeholder="Search users, servers, favorite bots" /></div></div>
      <div className="user-grid">{users.map((user) => <UserCard user={user} ctx={ctx} onChanged={loadUsers} key={`${user.username}-${user.guild_id}`} />)}</div>
      {!users.length ? <EmptyState title="No users found" /> : null}
    </Page>
  );
}
