import { useEffect, useState } from "react";
import { Save } from "lucide-react";
import { apiFetch, clearCache } from "../api.js";
import { DEFAULT_PREFERENCES } from "../config.js";
import { Choice, Page } from "../components/ui.jsx";

export default function AppearancePage({ ctx }) {
  const [draft, setDraft] = useState(ctx.preferences);
  useEffect(() => setDraft(ctx.preferences), [ctx.preferences]);
  async function save(event) {
    event.preventDefault();
    try {
      const data = await apiFetch("/api/users/preferences", { method: "POST", body: JSON.stringify(draft) });
      ctx.setPreferences({ ...DEFAULT_PREFERENCES, ...(data.preferences || draft) });
      clearCache("/api/users/preferences");
      ctx.showToast("Appearance saved.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  return (
    <Page title="Panel Look" eyebrow="Appearance">
      <form className="panel form-panel appearance-form" onSubmit={save}>
        <div className="two-col">
          <label className="field color-field"><span>Accent</span><input type="color" value={draft.accent_color || "#89b4fa"} onChange={(event) => setDraft((current) => ({ ...current, accent_color: event.target.value }))} /></label>
          <label className="field color-field"><span>Background</span><input type="color" value={draft.background_color || "#0b0e18"} onChange={(event) => setDraft((current) => ({ ...current, background_color: event.target.value, background_mode: "custom_color" }))} /></label>
        </div>
        <div className="three-col">
          <Choice label="Layout" value={draft.layout_mode} values={["standard", "focused", "wide"]} onChange={(value) => setDraft((current) => ({ ...current, layout_mode: value }))} />
          <Choice label="Density" value={draft.density} values={["comfortable", "compact"]} onChange={(value) => setDraft((current) => ({ ...current, density: value }))} />
          <Choice label="Cards" value={draft.card_shape} values={["soft", "crisp"]} onChange={(value) => setDraft((current) => ({ ...current, card_shape: value }))} />
          <Choice label="Font" value={draft.font_scale} values={["normal", "large", "dense"]} onChange={(value) => setDraft((current) => ({ ...current, font_scale: value }))} />
          <Choice label="Motion" value={draft.motion} values={["standard", "reduced"]} onChange={(value) => setDraft((current) => ({ ...current, motion: value }))} />
          <Choice label="Tabs" value={draft.tab_style} values={["pills", "underline", "minimal"]} onChange={(value) => setDraft((current) => ({ ...current, tab_style: value }))} />
        </div>
        <label className="field"><span>Background Image URL</span><input value={draft.background_image_url || ""} onChange={(event) => setDraft((current) => ({ ...current, background_image_url: event.target.value, background_mode: "custom_image" }))} /></label>
        <div className="actions-row"><button className="primary" type="submit"><Save size={16} />Save</button></div>
      </form>
    </Page>
  );
}
