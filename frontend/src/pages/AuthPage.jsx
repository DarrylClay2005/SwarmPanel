import { useState } from "react";
import { LogIn } from "lucide-react";
import { apiFetch } from "../api.js";
import { Page, Segmented } from "../components/ui.jsx";

export default function AuthPage({ ctx }) {
  const [mode, setMode] = useState("login");
  const [form, setForm] = useState({ username: "", password: "", guild_id: "", email: "" });
  const [busy, setBusy] = useState(false);
  async function submit(event) {
    event.preventDefault();
    setBusy(true);
    try {
      const endpoint = mode === "login" ? "/api/session/login" : "/api/session/register";
      const data = await apiFetch(endpoint, { method: "POST", body: JSON.stringify(form), token: "" });
      ctx.loginWith(data);
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy(false);
    }
  }
  return (
    <Page title={mode === "login" ? "Login" : "Register"} eyebrow="Session">
      <form className="auth-card form-panel" onSubmit={submit}>
        <Segmented value={mode} onChange={setMode} options={[["login", "Login"], ["register", "Register"]]} />
        <label className="field"><span>Username</span><input value={form.username} onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))} required /></label>
        <label className="field"><span>Password</span><input type="password" value={form.password} onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))} /></label>
        {mode === "register" ? <label className="field"><span>Guild ID</span><input value={form.guild_id} onChange={(event) => setForm((current) => ({ ...current, guild_id: event.target.value }))} required /></label> : null}
        {mode === "register" ? <label className="field"><span>Email</span><input type="email" value={form.email} onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))} /></label> : null}
        <button className="primary" type="submit" disabled={busy}><LogIn size={16} />{busy ? "Working" : mode === "login" ? "Login" : "Create Account"}</button>
      </form>
    </Page>
  );
}
