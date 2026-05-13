import { useCallback, useEffect, useState } from "react";
import { RefreshCw, Table2 } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { DataTable } from "../components/swarm.jsx";
import { Page } from "../components/ui.jsx";

function tableName(table) {
  if (!table || typeof table !== "object") return String(table || "");
  return table.name || table.table_name || table.TABLE_NAME || "";
}

export default function DatabasesPage({ ctx }) {
  const [schemas, setSchemas] = useState([]);
  const [selection, setSelection] = useState({ schema: "", table: "" });
  const [rows, setRows] = useState([]);
  const load = useCallback(async () => {
    try {
      const data = await apiFetch("/api/databases?include_tables=true");
      setSchemas(data.schemas || []);
      const first = data.schemas?.[0];
      setSelection((current) => current.schema ? current : { schema: first?.schema || "", table: tableName(first?.tables?.[0]) });
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }, [ctx]);
  useEffect(() => { load(); }, [load]);
  async function loadRows() {
    if (!selection.schema || !selection.table) return;
    try {
      const data = await apiFetch(`/api/database/data${query({ schema_name: selection.schema, table_name: selection.table, limit: 100 })}`);
      setRows(data.data || []);
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  const tables = schemas.find((schema) => schema.schema === selection.schema)?.tables || [];
  return (
    <Page title="Database Viewer" eyebrow="Admin Data" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh Schemas</button>}>
      <div className="panel toolbar">
        <select value={selection.schema} onChange={(event) => setSelection({ schema: event.target.value, table: "" })}>{schemas.map((schema) => <option key={schema.schema} value={schema.schema}>{schema.schema}</option>)}</select>
        <select value={selection.table} onChange={(event) => setSelection((current) => ({ ...current, table: event.target.value }))}>{tables.map((table) => {
          const name = tableName(table);
          return <option key={name} value={name}>{name}</option>;
        })}</select>
        <button type="button" onClick={loadRows}><Table2 size={16} />Load</button>
      </div>
      <DataTable rows={rows} />
    </Page>
  );
}
