import { useCallback, useEffect, useState } from "react";
import { KeyRound, Mail, RefreshCw, Search, Send, Trash2 } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { DataTable } from "../components/swarm.jsx";
import { Page } from "../components/ui.jsx";

export default function AccountsPage({ ctx }) {
  const [q, setQ] = useState("");
  const [rows, setRows] = useState([]);
  const [passwords, setPasswords] = useState({});
  const load = useCallback(async () => {
    try {
      const data = await apiFetch(`/api/swarm-accounts/admin${query({ query: q, limit: 100 })}`);
      setRows(data.data?.accounts || data.data || []);
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }, [ctx, q]);
  useEffect(() => { load(); }, [load]);
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
