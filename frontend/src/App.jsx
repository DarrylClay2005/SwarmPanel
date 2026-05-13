import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Link,
  NavLink,
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
} from "react-router-dom";
import {
  Activity,
  Bot,
  Check,
  ClipboardList,
  Database,
  Download,
  Gauge,
  Grid3X3,
  HeartPulse,
  Home,
  Image as ImageIcon,
  KeyRound,
  LayoutDashboard,
  ListMusic,
  Lock,
  LogIn,
  LogOut,
  Mail,
  Palette,
  Play,
  PlugZap,
  RefreshCw,
  RotateCcw,
  Save,
  Search,
  Send,
  Settings,
  Shield,
  ShieldCheck,
  Siren,
  Sparkles,
  Table2,
  Trash2,
  UserRound,
  Users,
  WandSparkles,
  X,
} from "lucide-react";
import {
  apiFetch,
  cachedFetch,
  clearCache,
  query,
  readToken,
  writeToken,
  writeUsername,
} from "./api.js";

const DEFAULT_PREFERENCES = {
  accent_color: "#89b4fa",
  background_mode: "default",
  background_color: "#0b0e18",
  background_image_url: "",
  layout_mode: "standard",
  density: "comfortable",
  card_shape: "soft",
  font_scale: "normal",
  motion: "standard",
  profile_layout: "spotlight",
  directory_layout: "grid",
  tab_style: "pills",
  surface_opacity: 0.92,
  surface_blur: 18,
  stream_card_style: "editorial",
  dashboard_density: "comfortable",
};

const CONTROL_ACTIONS = [
  "PLAY",
  "SMART_RECOMMEND",
  "PAUSE",
  "RESUME",
  "SKIP",
  "STOP",
  "CLEAR",
  "SHUFFLE",
  "LOOP",
  "FILTER",
  "LEAVE",
  "SET_HOME",
  "RECOVER",
  "RESTART",
];

function App() {
  const navigate = useNavigate();
  const [token, setToken] = useState(() => readToken());
  const [session, setSession] = useState({ authenticated: false, loading: true });
  const [preferences, setPreferences] = useState(DEFAULT_PREFERENCES);
  const [toast, setToast] = useState(null);

  const showToast = useCallback((message, tone = "info") => {
    setToast({ message, tone, id: Date.now() });
  }, []);

  const loadSession = useCallback(async () => {
    try {
      const data = await apiFetch("/api/session");
      if (data.authenticated && data.token) {
        writeToken(data.token);
        writeUsername(data.username);
        setToken(data.token);
      }
      setSession({ ...data, loading: false });
    } catch (_error) {
      setSession({ authenticated: false, loading: false });
    }
  }, []);

  const loadPreferences = useCallback(async () => {
    if (!readToken()) return;
    try {
      const data = await cachedFetch("/api/users/preferences", { ttl: 30_000 });
      setPreferences({ ...DEFAULT_PREFERENCES, ...(data.preferences || {}) });
    } catch (_error) {
      setPreferences(DEFAULT_PREFERENCES);
    }
  }, []);

  useEffect(() => {
    loadSession();
  }, [loadSession]);

  useEffect(() => {
    if (token) loadPreferences();
  }, [loadPreferences, token]);

  useEffect(() => {
    if (!toast) return undefined;
    const timer = window.setTimeout(() => setToast(null), 3600);
    return () => window.clearTimeout(timer);
  }, [toast]);

  const loginWith = useCallback((payload) => {
    writeToken(payload.token);
    writeUsername(payload.username);
    clearCache();
    setToken(payload.token);
    setSession({
      authenticated: true,
      loading: false,
      username: payload.username,
      role: payload.role,
      guild_id: payload.guild_id,
      account_guild_id: payload.account_guild_id,
      site_owner: payload.site_owner,
      admin_mode: payload.admin_mode,
      image_gallery_owner: payload.image_gallery_owner,
      pages_public_url: payload.pages_public_url,
    });
    showToast(`Signed in as ${payload.username}.`, "success");
    navigate("/");
  }, [navigate, showToast]);

  const logout = useCallback(async () => {
    try {
      await apiFetch("/api/session/logout", { method: "POST" });
    } catch (_error) {
      // Token cleanup still happens locally.
    }
    writeToken("");
    writeUsername("");
    clearCache();
    setToken("");
    setSession({ authenticated: false, loading: false });
    navigate("/login");
  }, [navigate]);

  const switchAdminMode = useCallback(async (enabled) => {
    try {
      const data = await apiFetch("/api/session/admin-mode", {
        method: "POST",
        body: JSON.stringify({ enabled }),
      });
      writeToken(data.token);
      setToken(data.token);
      clearCache();
      setSession((current) => ({ ...current, ...data, authenticated: true, loading: false }));
      showToast(enabled ? "Admin mode enabled." : "Admin mode disabled.", "success");
    } catch (error) {
      showToast(error.message, "error");
    }
  }, [showToast]);

  const ctx = useMemo(() => ({
    token,
    session,
    preferences,
    setPreferences,
    loginWith,
    logout,
    loadSession,
    loadPreferences,
    switchAdminMode,
    showToast,
    isAdmin: Boolean(session.admin_mode),
    isOwner: Boolean(session.site_owner),
    canGallery: Boolean(session.image_gallery_owner),
  }), [loadPreferences, loadSession, loginWith, logout, preferences, session, showToast, switchAdminMode, token]);

  return (
    <div className="app-shell" style={panelStyle(preferences)}>
      <Shell ctx={ctx}>
        <Routes>
          <Route path="/" element={<Protected ctx={ctx}><DashboardPage ctx={ctx} /></Protected>} />
          <Route path="/dashboard" element={<Navigate to="/" replace />} />
          <Route path="/controls" element={<Protected ctx={ctx}><ControlsPage ctx={ctx} /></Protected>} />
          <Route path="/invites" element={<Protected ctx={ctx}><InvitesPage ctx={ctx} /></Protected>} />
          <Route path="/users" element={<Protected ctx={ctx}><UsersPage ctx={ctx} /></Protected>} />
          <Route path="/profile" element={<Protected ctx={ctx}><ProfilePage ctx={ctx} /></Protected>} />
          <Route path="/appearance" element={<Protected ctx={ctx}><AppearancePage ctx={ctx} /></Protected>} />
          <Route path="/diagnostics" element={<Protected ctx={ctx} admin><DiagnosticsPage ctx={ctx} /></Protected>} />
          <Route path="/accounts" element={<Protected ctx={ctx} admin><AccountsPage ctx={ctx} /></Protected>} />
          <Route path="/databases" element={<Protected ctx={ctx} admin><DatabasesPage ctx={ctx} /></Protected>} />
          <Route path="/gallery-admin" element={<Protected ctx={ctx} gallery><GalleryAdminPage ctx={ctx} /></Protected>} />
          <Route path="/intel" element={<Protected ctx={ctx} admin><IntelPage ctx={ctx} /></Protected>} />
          <Route path="/login" element={ctx.session.authenticated ? <Navigate to="/" replace /> : <AuthPage ctx={ctx} />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </Shell>
      {toast ? <div className={`toast toast-${toast.tone}`}>{toast.message}</div> : null}
    </div>
  );
}

function Shell({ ctx, children }) {
  const location = useLocation();
  const authenticated = ctx.session.authenticated;
  return (
    <>
      <header className="topbar">
        <Link className="brand" to="/">
          <span className="brand-mark"><Sparkles size={18} /></span>
          <span>SwarmPanel</span>
        </Link>
        {authenticated ? (
          <nav className="nav" aria-label="Main">
            <NavItem to="/" icon={LayoutDashboard} label="Dashboard" />
            <NavItem to="/controls" icon={Play} label="Controls" />
            <NavItem to="/invites" icon={PlugZap} label="Invites" />
            <NavItem to="/users" icon={Users} label="Users" />
            <NavItem to="/profile" icon={UserRound} label="Profile" />
            <NavItem to="/appearance" icon={Palette} label="Look" />
            {ctx.isAdmin ? <NavItem to="/diagnostics" icon={HeartPulse} label="Diagnostics" /> : null}
            {ctx.isAdmin ? <NavItem to="/accounts" icon={Shield} label="Accounts" /> : null}
            {ctx.isAdmin ? <NavItem to="/databases" icon={Database} label="Data" /> : null}
            {ctx.canGallery ? <NavItem to="/gallery-admin" icon={ImageIcon} label="Gallery" /> : null}
            {ctx.isAdmin ? <NavItem to="/intel" icon={Siren} label="Intel" /> : null}
          </nav>
        ) : <div />}
        <div className="session-bar">
          {authenticated ? (
            <>
              <span className={`mode-pill ${ctx.isAdmin ? "admin" : ""}`}>{ctx.isAdmin ? "Admin" : "User"}</span>
              {ctx.isOwner ? (
                <label className="switch">
                  <input type="checkbox" checked={ctx.isAdmin} onChange={(event) => ctx.switchAdminMode(event.target.checked)} />
                  <span>Admin</span>
                </label>
              ) : null}
              <Link className="profile-link" to="/profile">{ctx.session.username}</Link>
              <button className="icon-button" type="button" onClick={ctx.logout} title="Logout"><LogOut size={18} /></button>
            </>
          ) : location.pathname !== "/login" ? (
            <Link className="button-link primary" to="/login"><LogIn size={16} />Login</Link>
          ) : null}
        </div>
      </header>
      <main className="stage">{children}</main>
    </>
  );
}

function NavItem({ to, icon: Icon, label }) {
  return (
    <NavLink className={({ isActive }) => `nav-item ${isActive ? "active" : ""}`} to={to} end={to === "/"}>
      <Icon size={17} />
      <span>{label}</span>
    </NavLink>
  );
}

function Protected({ ctx, children, admin = false, gallery = false }) {
  if (ctx.session.loading) return <Page title="Loading" eyebrow="Session"><SkeletonGrid count={4} /></Page>;
  if (!ctx.session.authenticated) return <Navigate to="/login" replace />;
  if (admin && !ctx.isAdmin) return <Denied message="Admin mode is required." />;
  if (gallery && !ctx.canGallery) return <Denied message="Image Gallery owner access is required." />;
  return children;
}

function DashboardPage({ ctx }) {
  const [state, setState] = useState({ dashboard: null, bots: null, intelligence: null, loading: true, error: "" });

  const load = useCallback(async () => {
    setState((current) => ({ ...current, loading: true, error: "" }));
    try {
      const [dashboard, bots, intelligence] = await Promise.allSettled([
        apiFetch("/api/dashboard"),
        cachedFetch("/api/bots", { ttl: 30_000 }),
        apiFetch(`/api/music-intelligence${query({ guild_id: ctx.session.guild_id || ctx.session.account_guild_id, limit: 10 })}`).catch((error) => ({ error: error.message })),
      ]);
      setState({
        dashboard: dashboard.status === "fulfilled" ? dashboard.value : null,
        bots: bots.status === "fulfilled" ? bots.value : null,
        intelligence: intelligence.status === "fulfilled" ? intelligence.value : null,
        loading: false,
        error: dashboard.status === "rejected" ? dashboard.reason.message : "",
      });
    } catch (error) {
      setState((current) => ({ ...current, loading: false, error: error.message }));
    }
  }, [ctx.session.account_guild_id, ctx.session.guild_id]);

  useEffect(() => {
    load();
    const timer = window.setInterval(load, 10_000);
    return () => window.clearInterval(timer);
  }, [load]);

  const dashboard = state.dashboard || {};
  const bots = dashboard.bots || [];
  const sessions = dashboard.sessions || bots.flatMap((bot) => (bot.sessions || []).map((session) => ({ ...session, bot_name: bot.display_name, bot_key: bot.key })));
  const active = sessions.filter((session) => session.is_playing || session.session_state === "playing").length;
  const stale = bots.filter((bot) => String(bot.heartbeat_status || "").includes("stale") || bot.status === "offline").length;

  return (
    <Page title="Swarm Command Deck" eyebrow="Dashboard" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      {state.error ? <Notice tone="error">{state.error}</Notice> : null}
      <MetricGrid>
        <Metric icon={Bot} label="Bots" value={bots.length || state.bots?.bots?.length || 0} />
        <Metric icon={Activity} label="Live Sessions" value={active} />
        <Metric icon={Siren} label="Stale Nodes" value={stale} />
        <Metric icon={ListMusic} label="Queued" value={sessions.reduce((sum, item) => sum + Number(item.queue_count || 0), 0)} />
      </MetricGrid>
      {state.loading ? <SkeletonGrid count={6} /> : (
        <section className="dashboard-grid">
          <div className="panel wide">
            <SectionHead title="Live Bots" count={bots.length} />
            <div className="bot-grid">
              {bots.map((bot) => <BotCard bot={bot} key={bot.key} />)}
            </div>
          </div>
          <div className="panel">
            <SectionHead title="Music Intelligence" />
            <IntelligenceView data={state.intelligence?.data} />
          </div>
          <div className="panel wide">
            <SectionHead title="Active Sessions" count={sessions.length} />
            <SessionTable sessions={sessions} />
          </div>
        </section>
      )}
    </Page>
  );
}

function ControlsPage({ ctx }) {
  const [catalog, setCatalog] = useState({ bots: [], loading: true });
  const [dashboard, setDashboard] = useState(null);
  const [inventory, setInventory] = useState(null);
  const [controlState, setControlState] = useState(null);
  const [matrix, setMatrix] = useState(null);
  const [form, setForm] = useState({
    bot_key: "",
    guild_id: ctx.session.guild_id || ctx.session.account_guild_id || "",
    action: "PLAY",
    source_url: "",
    voice_channel_id: "",
    text_channel_id: "",
    loop_mode: "queue",
    filter_mode: "none",
  });
  const [busy, setBusy] = useState(false);

  const loadBase = useCallback(async () => {
    const [bots, dash] = await Promise.all([cachedFetch("/api/bots", { ttl: 30_000 }), apiFetch("/api/dashboard")]);
    const musicBots = (bots.bots || []).filter((bot) => bot.kind === "music");
    setCatalog({ bots: musicBots, loading: false });
    setDashboard(dash);
    setForm((current) => ({
      ...current,
      bot_key: current.bot_key || musicBots[0]?.key || "",
      guild_id: current.guild_id || dash.sessions?.[0]?.guild_id || "",
    }));
  }, []);

  useEffect(() => {
    loadBase().catch((error) => ctx.showToast(error.message, "error"));
  }, [ctx, loadBase]);

  useEffect(() => {
    if (!form.bot_key) return;
    apiFetch(`/api/bots/${form.bot_key}/inventory`).then(setInventory).catch((error) => setInventory({ error: error.message, guilds: [] }));
  }, [form.bot_key]);

  useEffect(() => {
    if (!form.bot_key || !form.guild_id) return;
    apiFetch(`/api/bots/${form.bot_key}/control-state${query({ guild_id: form.guild_id })}`).then(setControlState).catch((error) => setControlState({ error: error.message }));
    apiFetch(`/api/guilds/${form.guild_id}/control-matrix`).then(setMatrix).catch((error) => setMatrix({ error: error.message, bots: [] }));
  }, [form.bot_key, form.guild_id]);

  const guilds = inventory?.guilds || [];
  const selectedGuild = guilds.find((guild) => String(guild.id) === String(form.guild_id));
  const channels = selectedGuild?.channels || inventory?.channels || [];
  const voiceChannels = channels.filter((channel) => [2, 13].includes(Number(channel.type)));
  const textChannels = channels.filter((channel) => [0, 5].includes(Number(channel.type)));
  const sessionGuilds = uniqueBy((dashboard?.sessions || []).map((session) => ({ id: session.guild_id, name: session.guild_name || `Guild ${session.guild_id}` })), "id");

  function update(key, value) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  async function submit(event) {
    event.preventDefault();
    setBusy(true);
    try {
      const payload = payloadForAction(form);
      const data = await apiFetch("/api/bots/control", {
        method: "POST",
        body: JSON.stringify({ bot_key: form.bot_key, guild_id: form.guild_id, action: form.action, payload }),
      });
      clearCache();
      ctx.showToast(data.message || `${form.action} accepted.`, "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Page title="Direct Playback Control" eyebrow="Controls" actions={<button type="button" onClick={loadBase}><RefreshCw size={16} />Refresh</button>}>
      <section className="control-layout">
        <form className="panel form-panel" onSubmit={submit}>
          <label className="field"><span>Bot</span><select value={form.bot_key} onChange={(event) => update("bot_key", event.target.value)}>{catalog.bots.map((bot) => <option value={bot.key} key={bot.key}>{bot.display_name}</option>)}</select></label>
          <label className="field"><span>Guild</span><input value={form.guild_id} onChange={(event) => update("guild_id", event.target.value)} list="known-guilds" required /><datalist id="known-guilds">{sessionGuilds.map((guild) => <option key={guild.id} value={guild.id}>{guild.name}</option>)}</datalist></label>
          <label className="field"><span>Action</span><select value={form.action} onChange={(event) => update("action", event.target.value)}>{CONTROL_ACTIONS.filter((action) => ctx.isAdmin || action !== "RESTART").map((action) => <option key={action} value={action}>{action}</option>)}</select></label>
          {form.action === "PLAY" ? <label className="field"><span>Source URL or search</span><input value={form.source_url} onChange={(event) => update("source_url", event.target.value)} placeholder="https://youtube.com/... or search terms" /></label> : null}
          {["PLAY", "SET_HOME", "SMART_RECOMMEND"].includes(form.action) ? (
            <div className="two-col">
              <label className="field"><span>Voice</span><ChannelSelect value={form.voice_channel_id} channels={voiceChannels} onChange={(value) => update("voice_channel_id", value)} /></label>
              <label className="field"><span>Text</span><ChannelSelect value={form.text_channel_id} channels={textChannels} onChange={(value) => update("text_channel_id", value)} optional /></label>
            </div>
          ) : null}
          {form.action === "LOOP" ? <label className="field"><span>Loop</span><select value={form.loop_mode} onChange={(event) => update("loop_mode", event.target.value)}><option value="off">Off</option><option value="track">Track</option><option value="queue">Queue</option></select></label> : null}
          {form.action === "FILTER" ? <label className="field"><span>Filter</span><select value={form.filter_mode} onChange={(event) => update("filter_mode", event.target.value)}><option value="none">None</option><option value="nightcore">Nightcore</option><option value="bassboost">Bassboost</option><option value="vaporwave">Vaporwave</option><option value="karaoke">Karaoke</option></select></label> : null}
          <div className="actions-row">
            <button className="primary" type="submit" disabled={busy}><Send size={16} />{busy ? "Sending" : "Send Control"}</button>
            <button type="button" onClick={() => update("action", "SMART_RECOMMEND")}><WandSparkles size={16} />Smart Rec</button>
          </div>
        </form>
        <aside className="panel">
          <SectionHead title="Readiness" />
          <ControlState state={controlState} />
          <SectionHead title="Guild Matrix" count={matrix?.bots?.length || 0} />
          <div className="mini-stack">{(matrix?.bots || []).map((bot) => <ControlState state={bot} compact key={bot.key} />)}</div>
        </aside>
      </section>
    </Page>
  );
}

function InvitesPage({ ctx }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const load = useCallback(async () => {
    try {
      setData(await cachedFetch("/api/bots", { ttl: 30_000 }));
      setError("");
    } catch (loadError) {
      setError(loadError.message);
    }
  }, []);
  useEffect(() => { load(); }, [load]);
  return (
    <Page title="Bot Access" eyebrow="Invites" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      {error ? <Notice tone="error">{error}</Notice> : null}
      <div className="invite-grid">
        {(data?.invite_bots || []).map((bot) => <InviteCard bot={bot} key={bot.key} />)}
      </div>
    </Page>
  );
}

function UsersPage({ ctx }) {
  const [q, setQ] = useState("");
  const [users, setUsers] = useState([]);
  useEffect(() => {
    const timer = window.setTimeout(() => {
      apiFetch(`/api/users/directory${query({ q })}`).then((data) => setUsers(data.users || [])).catch((error) => ctx.showToast(error.message, "error"));
    }, 220);
    return () => window.clearTimeout(timer);
  }, [ctx, q]);
  return (
    <Page title="Swarm Directory" eyebrow="Users">
      <div className="toolbar"><div className="search-box"><Search size={16} /><input value={q} onChange={(event) => setQ(event.target.value)} placeholder="Search users, servers, favorite bots" /></div></div>
      <div className="user-grid">{users.map((user) => <UserCard user={user} key={`${user.username}-${user.guild_id}`} />)}</div>
      {!users.length ? <EmptyState title="No users found" /> : null}
    </Page>
  );
}

function ProfilePage({ ctx }) {
  const [data, setData] = useState(null);
  const [form, setForm] = useState({});
  const [identity, setIdentity] = useState({ email: "", code: "", current_password: "", new_password: "" });
  const load = useCallback(async () => {
    const payload = await apiFetch("/api/users/me");
    setData(payload);
    setForm(payload.profile || {});
  }, []);
  useEffect(() => { load().catch((error) => ctx.showToast(error.message, "error")); }, [ctx, load]);
  async function save(event) {
    event.preventDefault();
    try {
      const payload = pick(form, ["display_name", "avatar_url", "bio", "favorite_bot", "theme_accent", "public_profile", "server_invite_url", "server_name", "server_icon_url"]);
      const updated = await apiFetch("/api/users/me", { method: "POST", body: JSON.stringify(payload) });
      setForm(updated.profile);
      ctx.showToast("Profile saved.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  async function saveEmail(event) {
    event.preventDefault();
    try {
      await apiFetch("/api/session/email", { method: "POST", body: JSON.stringify({ email: identity.email }) });
      ctx.showToast("Email saved.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  async function verifyEmail(event) {
    event.preventDefault();
    try {
      await apiFetch("/api/session/email/verify", { method: "POST", body: JSON.stringify({ code: identity.code }) });
      ctx.showToast("Email verified.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  async function changePassword(event) {
    event.preventDefault();
    try {
      await apiFetch("/api/session/password", { method: "POST", body: JSON.stringify({ current_password: identity.current_password, new_password: identity.new_password }) });
      setIdentity((current) => ({ ...current, current_password: "", new_password: "" }));
      ctx.showToast("Password changed.", "success");
    } catch (error) {
      ctx.showToast(error.message, "error");
    }
  }
  return (
    <Page title="Server Identity" eyebrow="Profile">
      {!data ? <SkeletonGrid count={2} /> : (
        <section className="settings-grid">
          <form className="panel form-panel" onSubmit={save}>
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
            <button className="primary" type="submit" disabled={!data.editable}><Save size={16} />Save</button>
          </form>
          <div className="panel form-panel">
            <h2><ShieldCheck size={18} /> Account</h2>
            <form className="mini-form" onSubmit={saveEmail}><label className="field"><span>Email</span><input type="email" value={identity.email} onChange={(event) => setIdentity((current) => ({ ...current, email: event.target.value }))} /></label><button type="submit"><Mail size={16} />Save Email</button></form>
            <form className="mini-form" onSubmit={verifyEmail}><label className="field"><span>Code</span><input value={identity.code} onChange={(event) => setIdentity((current) => ({ ...current, code: event.target.value }))} /></label><button type="submit"><Check size={16} />Verify</button></form>
            <form className="mini-form" onSubmit={changePassword}><label className="field"><span>Current</span><input type="password" value={identity.current_password} onChange={(event) => setIdentity((current) => ({ ...current, current_password: event.target.value }))} /></label><label className="field"><span>New</span><input type="password" value={identity.new_password} onChange={(event) => setIdentity((current) => ({ ...current, new_password: event.target.value }))} /></label><button type="submit"><KeyRound size={16} />Change</button></form>
          </div>
        </section>
      )}
    </Page>
  );
}

function AppearancePage({ ctx }) {
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

function DiagnosticsPage({ ctx }) {
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

function AccountsPage({ ctx }) {
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

function DatabasesPage({ ctx }) {
  const [schemas, setSchemas] = useState([]);
  const [selection, setSelection] = useState({ schema: "", table: "" });
  const [rows, setRows] = useState([]);
  const load = useCallback(async () => {
    try {
      const data = await apiFetch("/api/databases?include_tables=true");
      setSchemas(data.schemas || []);
      const first = data.schemas?.[0];
      setSelection((current) => current.schema ? current : { schema: first?.schema || "", table: first?.tables?.[0]?.name || first?.tables?.[0] || "" });
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
        <select value={selection.table} onChange={(event) => setSelection((current) => ({ ...current, table: event.target.value }))}>{tables.map((table) => <option key={table.name || table} value={table.name || table}>{table.name || table}</option>)}</select>
        <button type="button" onClick={loadRows}><Table2 size={16} />Load</button>
      </div>
      <DataTable rows={rows} />
    </Page>
  );
}

function GalleryAdminPage({ ctx }) {
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
          <div className="toolbar"><select value={table} onChange={(event) => setTable(event.target.value)}><option value="">Choose table</option>{tables.map((item) => <option key={item.name || item} value={item.name || item}>{item.name || item}</option>)}</select><button type="button" onClick={loadTable}><Table2 size={16} />Load</button></div>
          <DataTable rows={rows} />
        </div>
      </section>
    </Page>
  );
}

function IntelPage({ ctx }) {
  const [state, setState] = useState({ events: [], metrics: null, stability: null });
  const load = useCallback(async () => {
    const [events, metrics, stability] = await Promise.allSettled([
      apiFetch("/api/events?limit=80"),
      apiFetch("/api/metrics"),
      apiFetch("/api/stability"),
    ]);
    setState({
      events: events.status === "fulfilled" ? events.value.events || [] : [],
      metrics: metrics.status === "fulfilled" ? metrics.value : { error: metrics.reason?.message },
      stability: stability.status === "fulfilled" ? stability.value : { error: stability.reason?.message },
    });
  }, []);
  useEffect(() => { load(); const timer = window.setInterval(load, 8000); return () => window.clearInterval(timer); }, [load]);
  return (
    <Page title="Errors And Metrics" eyebrow="Intel" actions={<button type="button" onClick={load}><RefreshCw size={16} />Refresh</button>}>
      <section className="dashboard-grid">
        <div className="panel wide"><SectionHead title="Events" count={state.events.length} /><EventList events={state.events} /></div>
        <div className="panel"><SectionHead title="Metrics" /><JsonPanel data={state.metrics} /></div>
        <div className="panel"><SectionHead title="Stability" /><JsonPanel data={state.stability} /></div>
      </section>
    </Page>
  );
}

function AuthPage({ ctx }) {
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

function BotCard({ bot }) {
  const sessions = bot.sessions || [];
  const accent = bot.accent || "#89b4fa";
  return (
    <article className="bot-card" style={{ "--card-accent": accent }}>
      <div className="bot-head"><span className="bot-dot" /><h3>{bot.display_name || bot.name || bot.key}</h3><small>{bot.heartbeat_status || bot.status || "unknown"}</small></div>
      <p>{sessions[0]?.title || bot.db_error || bot.schema || "Waiting for live playback."}</p>
      <div className="chip-row">
        <span>{bot.active_playing_count || sessions.filter((session) => session.is_playing).length} live</span>
        <span>{bot.known_guild_count || bot.guild_count || 0} guilds</span>
        <span>{bot.queue_depth || sessions.reduce((sum, session) => sum + Number(session.queue_count || 0), 0)} queued</span>
      </div>
    </article>
  );
}

function SessionTable({ sessions }) {
  if (!sessions.length) return <EmptyState title="No active sessions" />;
  return <DataTable rows={sessions.map((session) => pick(session, ["bot_name", "guild_name", "guild_id", "channel_name", "title", "is_playing", "queue_count", "filter_mode", "loop_mode"]))} />;
}

function IntelligenceView({ data }) {
  if (!data) return <EmptyState title="No intelligence snapshot" />;
  if (Array.isArray(data)) return <DataTable rows={data} />;
  const rows = data.recommendations || data.guilds || data.bots || data.rows || [];
  return rows.length ? <DataTable rows={rows} /> : <JsonPanel data={data} />;
}

function ControlState({ state, compact = false }) {
  if (!state) return <EmptyState title="No state loaded" compact />;
  if (state.error) return <Notice tone="error">{state.error}</Notice>;
  const session = state.session || {};
  return (
    <article className={`control-state ${compact ? "compact" : ""}`}>
      <div><strong>{state.display_name || state.key}</strong><small>{state.discord?.status || state.db?.status || "unknown"}</small></div>
      <p>{session.title || session.session_state_label || state.discord?.message || "Idle"}</p>
      <div className="chip-row">
        <span>{session.guild_name || state.guild_id}</span>
        <span>{session.channel_name || "No channel"}</span>
        <span>{session.queue_count || 0} queued</span>
      </div>
    </article>
  );
}

function InviteCard({ bot }) {
  return (
    <article className="invite-card">
      <div className="bot-head"><span className="bot-dot" /><h3>{bot.display_name}</h3><small>{bot.token_configured ? "token ready" : "missing token"}</small></div>
      <p>{bot.capability_summary}</p>
      <div className="chip-row">{(bot.permissions || []).slice(0, 6).map((permission) => <span key={permission}>{permission}</span>)}</div>
      {bot.invite_url ? <a className="button-link primary" href={bot.invite_url} target="_blank" rel="noreferrer"><PlugZap size={16} />Invite</a> : <button disabled>Invite unavailable</button>}
    </article>
  );
}

function UserCard({ user }) {
  return (
    <article className="user-card">
      <div className="avatar">{initials(user.display_name || user.username)}</div>
      <div>
        <h3>{user.display_name || user.username}</h3>
        <p>@{user.username} / {user.server_name || `Guild ${user.guild_id}`}</p>
        <div className="chip-row"><span>{user.favorite_bot || "no favorite"}</span><span>{user.public_profile === false ? "private" : "public"}</span></div>
      </div>
    </article>
  );
}

function EventList({ events }) {
  if (!events.length) return <EmptyState title="No events yet" />;
  return (
    <div className="event-list">
      {events.map((event, index) => (
        <article className={`event event-${event.level || "info"}`} key={`${event.timestamp}-${index}`}>
          <div><strong>{event.title || event.type}</strong><small>{event.source} / {formatTime(event.timestamp)}</small></div>
          <p>{event.description || event.message || ""}</p>
        </article>
      ))}
    </div>
  );
}

function ChannelSelect({ value, channels, onChange, optional = false }) {
  return (
    <select value={value} onChange={(event) => onChange(event.target.value)}>
      <option value="">{optional ? "None" : "Choose channel"}</option>
      {channels.map((channel) => <option value={channel.id} key={channel.id}>{channel.name || channel.id}</option>)}
    </select>
  );
}

function DataTable({ rows = [], actions }) {
  if (!rows?.length) return <EmptyState title="No rows" compact />;
  const columns = unique(rows.flatMap((row) => Object.keys(row))).slice(0, 9);
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{columns.map((column) => <th key={column}>{column}</th>)}{actions ? <th>Actions</th> : null}</tr></thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={row.id ?? `${index}-${JSON.stringify(row).slice(0, 20)}`}>
              {columns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
              {actions ? <td>{actions(row)}</td> : null}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JsonPanel({ data }) {
  if (!data) return <EmptyState title="No data loaded" compact />;
  return <pre className="json-panel">{JSON.stringify(data, null, 2)}</pre>;
}

function Choice({ label, value, values, onChange }) {
  return (
    <label className="field">
      <span>{label}</span>
      <select value={value || values[0]} onChange={(event) => onChange(event.target.value)}>
        {values.map((item) => <option key={item} value={item}>{titleCase(item)}</option>)}
      </select>
    </label>
  );
}

function Page({ title, eyebrow, actions, children }) {
  return (
    <div className="page">
      <header className="page-head">
        <div><p>{eyebrow}</p><h1>{title}</h1></div>
        {actions ? <div className="page-actions">{actions}</div> : null}
      </header>
      {children}
    </div>
  );
}

function SectionHead({ title, count }) {
  return <div className="section-head"><h2>{title}</h2>{count !== undefined ? <span>{count}</span> : null}</div>;
}

function MetricGrid({ children }) {
  return <section className="metric-grid">{children}</section>;
}

function Metric({ icon: Icon, label, value }) {
  return <article className="metric"><Icon size={19} /><div><strong>{number(value)}</strong><span>{label}</span></div></article>;
}

function Notice({ tone = "info", children }) {
  return <div className={`notice notice-${tone}`}>{children}</div>;
}

function EmptyState({ title, compact = false }) {
  return <div className={`empty-state ${compact ? "compact" : ""}`}><Sparkles size={22} /><h2>{title}</h2></div>;
}

function Denied({ message }) {
  return <Page title="Access Locked" eyebrow="Permissions"><div className="empty-state"><Lock size={28} /><h2>{message}</h2></div></Page>;
}

function NotFound() {
  return <Page title="Not Found" eyebrow="404"><EmptyState title="That panel page is not available" /></Page>;
}

function SkeletonGrid({ count = 6 }) {
  return <div className="skeleton-grid">{Array.from({ length: count }, (_, index) => <div className="skeleton-card" key={index} />)}</div>;
}

function Segmented({ value, onChange, options }) {
  return <div className="segmented">{options.map(([key, label]) => <button className={value === key ? "active" : ""} type="button" onClick={() => onChange(key)} key={key}>{label}</button>)}</div>;
}

function payloadForAction(form) {
  if (form.action === "PLAY") {
    return { source_url: form.source_url, voice_channel_id: form.voice_channel_id, text_channel_id: form.text_channel_id || 0 };
  }
  if (form.action === "SMART_RECOMMEND") {
    return { voice_channel_id: form.voice_channel_id, text_channel_id: form.text_channel_id || 0 };
  }
  if (form.action === "SET_HOME") {
    return { voice_channel_id: form.voice_channel_id };
  }
  if (form.action === "LOOP") return { loop_mode: form.loop_mode };
  if (form.action === "FILTER") return { filter_mode: form.filter_mode };
  return {};
}

function panelStyle(preferences) {
  const accent = safeHex(preferences.accent_color, "#89b4fa");
  const background = safeHex(preferences.background_color, "#0b0e18");
  return {
    "--accent": accent,
    "--bg": preferences.background_mode === "custom_color" ? background : "#0d1117",
    "--surface-opacity": preferences.surface_opacity ?? 0.92,
    "--surface-blur": `${preferences.surface_blur ?? 18}px`,
  };
}

function safeHex(value, fallback) {
  return /^#[0-9a-f]{6}$/i.test(value || "") ? value : fallback;
}

function pick(row, keys) {
  return Object.fromEntries(keys.map((key) => [key, row?.[key]]).filter(([, value]) => value !== undefined));
}

function unique(values) {
  return Array.from(new Set(values));
}

function uniqueBy(rows, key) {
  const seen = new Set();
  return rows.filter((row) => {
    const value = String(row?.[key] || "");
    if (!value || seen.has(value)) return false;
    seen.add(value);
    return true;
  });
}

function formatCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "object") return JSON.stringify(value).slice(0, 220);
  return String(value);
}

function number(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function initials(value) {
  return String(value || "SP").trim().split(/\s+/).slice(0, 2).map((part) => part[0]).join("").toUpperCase() || "SP";
}

function titleCase(value) {
  return String(value || "").replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export default App;
