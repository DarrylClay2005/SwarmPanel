import { useCallback, useEffect, useState } from "react";
import { Image as ImageIcon, KeyRound, Mail, RefreshCw, ShieldCheck, Siren, Table2, Trash2, Users } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { DataTable } from "../components/swarm.jsx";
import { Metric, MetricGrid, Page, SectionHead } from "../components/ui.jsx";

function tableName(table) {
  if (!table || typeof table !== "object") return String(table || "");
  return table.name || table.table_name || table.TABLE_NAME || "";
}

export default function GalleryAdminPage({ ctx }) {
  const [summary, setSummary] = useState(null);
  const [tables, setTables] = useState([]);
  const [table, setTable] = useState("");
  const [rows, setRows] = useState([]);
  const [passwords, setPasswords] = useState({});
  const load = useCallback(async () => {
    try {
      const [admin, tableData] = await Promise.all([apiFetch("/api/image-gallery/admin"), apiFetch("/api/image-gallery/tables")]);
      setSummary(admin.data);
      setTables(tableData.tables || []);
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }, [ctx]);
  useEffect(() => { load(); }, [load]);
  async function loadTable() {
    try {
      const data = await apiFetch(`/api/image-gallery/table-data${query({ table_name: table, limit: 100 })}`);
      setRows(data.data || []);
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  async function mutate(path, payload, message) {
    try {
      await apiFetch(path, { method: "POST", body: JSON.stringify(payload) });
      ctx.showToast(message, "success");
      await load();
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  const users = summary?.users || summary?.recent_users || [];
  const reports = summary?.reports || summary?.recent_reports || [];
  const media = summary?.media || summary?.recent_media || [];
  return (
    <Page title="Image Gallery Admin" eyebrow="Owner Workspace" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      <MetricGrid>
        <Metric icon={Users} label="Users" value={summary?.counts?.users ?? users.length} />
        <Metric icon={ImageIcon} label="Media" value={summary?.counts?.media ?? media.length} />
        <Metric icon={Siren} label="Reports" value={summary?.counts?.reports ?? reports.length} />
        <Metric icon={Table2} label="Tables" value={tables.length} />
      </MetricGrid>
      <section className="dashboard-grid">
        <div className="panel wide">
          <SectionHead title="Users" count={users.length} />
          <DataTable rows={users} actions={(row) => (
            <div className="table-actions">
              <button type="button" onClick={() => mutate("/api/image-gallery/users/email-verified", { user_id: row.id, verified: !row.email_verified_at }, "Email flag updated.")}><Mail size={14} />Email</button>
              <button type="button" onClick={() => mutate("/api/image-gallery/users/age-verified", { user_id: row.id, verified: !row.age_verified_at }, "Age flag updated.")}><ShieldCheck size={14} />Age</button>
              <input className="mini-input" type="password" placeholder="new password" value={passwords[row.id] || ""} onChange={(event) => setPasswords((current) => ({ ...current, [row.id]: event.target.value }))} />
              <button type="button" onClick={() => mutate("/api/image-gallery/users/reset-password", { user_id: row.id, new_password: passwords[row.id] || "" }, "Password reset.")}><KeyRound size={14} />Reset</button>
              <button className="danger" type="button" onClick={() => mutate("/api/image-gallery/users/delete", { user_id: row.id }, "User deleted.")}><Trash2 size={14} />Delete</button>
            </div>
          )} />
        </div>
        <div className="panel">
          <SectionHead title="Reports" count={reports.length} />
          <DataTable rows={reports} actions={(row) => <button type="button" onClick={() => mutate("/api/image-gallery/reports/status", { report_id: row.id, status: "resolved" }, "Report resolved.")}>Resolve</button>} />
        </div>
        <div className="panel wide">
          <SectionHead title="Table Browser" />
          <div className="toolbar"><select value={table} onChange={(event) => setTable(event.target.value)}><option value="">Choose table</option>{tables.map((item) => {
            const name = tableName(item);
            return <option key={name} value={name}>{name}</option>;
          })}</select><button type="button" onClick={loadTable}><Table2 size={16} />Load</button></div>
          <DataTable rows={rows} />
        </div>
      </section>
    </Page>
  );
}
