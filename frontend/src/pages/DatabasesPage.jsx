import { useCallback, useEffect, useState } from "react";
import { RefreshCw, Table2 } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { DataTable } from "../components/swarm.jsx";
import { Notice, Page, SkeletonGrid } from "../components/ui.jsx";

function tableName(table) {
  if (!table || typeof table !== "object") return String(table || "");
  return table.name || table.table_name || table.TABLE_NAME || "";
}

function rowsFromPayload(payload) {
  const data = payload?.data;
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.rows)) return data.rows;
  if (Array.isArray(payload?.rows)) return payload.rows;
  return [];
}

export default function DatabasesPage({ ctx }) {
  const [schemas, setSchemas] = useState([]);
  const [selection, setSelection] = useState({ schema: "", table: "" });
  const [rows, setRows] = useState([]);
  const [loadingRows, setLoadingRows] = useState(false);
  const [error, setError] = useState("");
  const load = useCallback(async () => {
    try {
      const data = await apiFetch("/api/databases?include_tables=true");
      setSchemas(data.schemas || []);
      const first = data.schemas?.[0];
      setSelection((current) => current.schema ? current : { schema: first?.schema || "", table: tableName(first?.tables?.[0]) });
      setError("");
    } catch (error) {
      setError(error.message);
      ctx.showToast(error.message, "error");
    }
  }, [ctx]);
  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    const tables = schemas.find((schema) => schema.schema === selection.schema)?.tables || [];
    if (!selection.table && tables.length) {
      setSelection((current) => ({ ...current, table: tableName(tables[0]) }));
      setRows([]);
    }
  }, [schemas, selection.schema, selection.table]);
  async function loadRows() {
    if (!selection.schema || !selection.table) return;
    setLoadingRows(true);
    setError("");
    try {
      const data = await apiFetch(`/api/database/data${query({ schema_name: selection.schema, table_name: selection.table, limit: 100 })}`);
      setRows(rowsFromPayload(data));
    } catch (error) {
      setRows([]);
      setError(error.message);
      ctx.showToast(error.message, "error");
    } finally {
      setLoadingRows(false);
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
        <button type="button" onClick={loadRows} disabled={loadingRows || !selection.table}><Table2 size={16} />{loadingRows ? "Loading" : "Load"}</button>
      </div>
      {error ? <Notice tone="error">{error}</Notice> : null}
      {loadingRows ? <SkeletonGrid count={3} /> : <DataTable rows={rows} />}
    </Page>
  );
}
