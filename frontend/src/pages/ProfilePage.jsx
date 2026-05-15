import { useCallback, useEffect, useState } from "react";
import { Check, KeyRound, Mail, Save, ShieldCheck } from "lucide-react";
import { apiFetch, cachedFetch, clearCache } from "../api.js";
import { Page, SkeletonGrid } from "../components/ui.jsx";
import { initials, pick } from "../utils/format.js";

export default function ProfilePage({ ctx }) {
  const [data, setData] = useState(null);
  const [form, setForm] = useState({});
  const [identity, setIdentity] = useState({ email: "", code: "", current_password: "", new_password: "" });
  const [busy, setBusy] = useState("");
  const load = useCallback(async () => {
    const payload = await cachedFetch("/api/users/me", { ttl: 20_000, staleTtl: 120_000 });
    setData(payload);
    setForm(payload.profile || {});
    setIdentity((current) => ({ ...current, email: payload.profile?.email || "" }));
  }, []);
  useEffect(() => { load().catch((error) => ctx.showToast(error.message, "error")); }, [ctx, load]);
  function clearProfileCache() {
    clearCache("/api/users/me");
    clearCache("/api/users/directory");
  }

  async function save(event) {
    event.preventDefault();
    setBusy("profile");
    try {
      const payload = pick(form, ["display_name", "avatar_url", "bio", "favorite_bot", "theme_accent", "public_profile", "server_invite_url", "server_name", "server_icon_url"]);
      const updated = await apiFetch("/api/users/me", { method: "POST", body: JSON.stringify(payload) });
      setForm(updated.profile);
      setData((current) => ({ ...(current || {}), ...updated }));
      clearProfileCache();
      ctx.showToast("Profile saved.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy("");
    }
  }
  async function saveEmail(event) {
    event.preventDefault();
    setBusy("email");
    try {
      const updated = await apiFetch("/api/session/email", { method: "POST", body: JSON.stringify({ email: identity.email }) });
      setData((current) => ({ ...(current || {}), profile: updated.profile || current?.profile }));
      setForm(updated.profile || form);
      clearProfileCache();
      ctx.showToast(updated.email_verification_sent ? "Verification sent." : "Email saved.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy("");
    }
  }
  async function verifyEmail(event) {
    event.preventDefault();
    setBusy("verify");
    try {
      const updated = await apiFetch("/api/session/email/verify", { method: "POST", body: JSON.stringify({ code: identity.code }) });
      setData((current) => ({ ...(current || {}), profile: updated.profile || current?.profile }));
      setForm(updated.profile || form);
      setIdentity((current) => ({ ...current, code: "" }));
      clearProfileCache();
      ctx.showToast("Email verified.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy("");
    }
  }
  async function changePassword(event) {
    event.preventDefault();
    setBusy("password");
    try {
      const updated = await apiFetch("/api/session/password", { method: "POST", body: JSON.stringify({ current_password: identity.current_password, new_password: identity.new_password }) });
      setData((current) => ({ ...(current || {}), profile: updated.profile || current?.profile }));
      setIdentity((current) => ({ ...current, current_password: "", new_password: "" }));
      ctx.showToast("Password changed.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy("");
    }
  }
  return (
    <Page title="Server Identity" eyebrow="Profile">
      {!data ? <SkeletonGrid count={2} /> : (
        <section className="settings-grid">
          <form className="panel form-panel" onSubmit={save}>
            <div className="profile-preview">
              <div className="avatar">{form.avatar_url || form.server_icon_url ? <img src={form.avatar_url || form.server_icon_url} alt="" loading="lazy" decoding="async" /> : initials(form.display_name || data.username)}</div>
              <div>
                <strong>{form.display_name || data.username}</strong>
                <p className="muted">{form.server_name || `Guild ${data.guild_id || data.account_guild_id || "profile"}`}</p>
              </div>
            </div>
            <label className="field"><span>Display Name</span><input value={form.display_name || ""} onChange={(event) => setForm((current) => ({ ...current, display_name: event.target.value }))} disabled={!data.editable} /></label>
            <label className="field"><span>Server Name</span><input value={form.server_name || ""} onChange={(event) => setForm((current) => ({ ...current, server_name: event.target.value }))} disabled={!data.editable} /></label>
            <label className="field"><span>Avatar URL</span><input value={form.avatar_url || ""} onChange={(event) => setForm((current) => ({ ...current, avatar_url: event.target.value }))} disabled={!data.editable} /></label>
            <label className="field"><span>Server Icon URL</span><input value={form.server_icon_url || ""} onChange={(event) => setForm((current) => ({ ...current, server_icon_url: event.target.value }))} disabled={!data.editable} /></label>
            <label className="field"><span>Bio</span><textarea value={form.bio || ""} onChange={(event) => setForm((current) => ({ ...current, bio: event.target.value }))} disabled={!data.editable} /></label>
            <div className="two-col">
              <label className="field"><span>Favorite Bot</span><select value={form.favorite_bot || ""} onChange={(event) => setForm((current) => ({ ...current, favorite_bot: event.target.value }))} disabled={!data.editable}><option value="">None</option>{(data.favorite_bot_options || []).map((bot) => <option key={bot.key} value={bot.key}>{bot.display_name}</option>)}</select></label>
              <label className="field color-field"><span>Accent</span><input type="color" value={form.theme_accent || "#89b4fa"} onChange={(event) => setForm((current) => ({ ...current, theme_accent: event.target.value }))} disabled={!data.editable} /></label>
            </div>
            <label className="field"><span>Discord Invite</span><input value={form.server_invite_url || ""} onChange={(event) => setForm((current) => ({ ...current, server_invite_url: event.target.value }))} disabled={!data.editable} /></label>
            <label className="check-row"><input type="checkbox" checked={form.public_profile !== false} onChange={(event) => setForm((current) => ({ ...current, public_profile: event.target.checked }))} disabled={!data.editable} />Public profile</label>
            <button className="primary" type="submit" disabled={!data.editable || busy === "profile"}><Save size={16} />{busy === "profile" ? "Saving" : "Save"}</button>
          </form>
          <div className="panel form-panel">
            <h2><ShieldCheck size={18} /> Account</h2>
            <form className="mini-form" onSubmit={saveEmail}><label className="field"><span>Email</span><input type="email" value={identity.email} onChange={(event) => setIdentity((current) => ({ ...current, email: event.target.value }))} disabled={!data.editable} /></label><button type="submit" disabled={!data.editable || busy === "email"}><Mail size={16} />{busy === "email" ? "Saving" : "Save Email"}</button></form>
            <form className="mini-form" onSubmit={verifyEmail}><label className="field"><span>Code</span><input value={identity.code} onChange={(event) => setIdentity((current) => ({ ...current, code: event.target.value }))} disabled={!data.editable} /></label><button type="submit" disabled={!data.editable || busy === "verify"}><Check size={16} />{busy === "verify" ? "Checking" : "Verify"}</button></form>
            <form className="mini-form" onSubmit={changePassword}><label className="field"><span>Current</span><input type="password" value={identity.current_password} onChange={(event) => setIdentity((current) => ({ ...current, current_password: event.target.value }))} disabled={!data.editable} /></label><label className="field"><span>New</span><input type="password" value={identity.new_password} onChange={(event) => setIdentity((current) => ({ ...current, new_password: event.target.value }))} disabled={!data.editable} /></label><button type="submit" disabled={!data.editable || busy === "password"}><KeyRound size={16} />{busy === "password" ? "Saving" : "Change"}</button></form>
          </div>
        </section>
      )}
    </Page>
  );
}
