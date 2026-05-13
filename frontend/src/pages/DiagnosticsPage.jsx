import { useCallback, useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { apiFetch, query } from "../api.js";
import { JsonPanel } from "../components/swarm.jsx";
import { Notice, Page } from "../components/ui.jsx";

export default function DiagnosticsPage({ ctx }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const load = useCallback(async (force = false) => {
    try {
      setData(await apiFetch(`/api/system-diagnostics${query({ force })}`));
      setError("");
    } catch (loadError) {
      setError(loadError.message);
    }
  }, []);
  useEffect(() => { load(false); }, [load]);
  return (
    <Page title="System Runtime" eyebrow="Diagnostics" actions={<button type="button" onClick={() => load(true)}><RefreshCw size={16} />Force</button>}>
      {error ? <Notice tone="error">{error}</Notice> : null}
      <JsonPanel data={data} />
    </Page>
  );
}
