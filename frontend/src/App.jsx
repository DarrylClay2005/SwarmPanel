import { useCallback, useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useNavigate } from "react-router-dom";
import {
  apiFetch,
  cachedFetch,
  clearCache,
  prefetchFetch,
  query,
  readToken,
  writeToken,
  writeUsername,
} from "./api.js";
import { Denied, NotFound, Page, SkeletonGrid } from "./components/ui.jsx";
import { Shell } from "./components/Shell.jsx";
import { DEFAULT_PREFERENCES } from "./config.js";
import { useLiveRefresh } from "./hooks/useLiveRefresh.js";
import { panelStyle } from "./utils/control.js";
import DashboardPage from "./pages/DashboardPage.jsx";
import ControlsPage from "./pages/ControlsPage.jsx";
import InvitesPage from "./pages/InvitesPage.jsx";
import UsersPage from "./pages/UsersPage.jsx";
import FriendsPage from "./pages/FriendsPage.jsx";
import MessagesPage from "./pages/MessagesPage.jsx";
import ProfilePage from "./pages/ProfilePage.jsx";
import AppearancePage from "./pages/AppearancePage.jsx";
import DiagnosticsPage from "./pages/DiagnosticsPage.jsx";
import AccountsPage from "./pages/AccountsPage.jsx";
import DatabasesPage from "./pages/DatabasesPage.jsx";
import GalleryAdminPage from "./pages/GalleryAdminPage.jsx";
import IntelPage from "./pages/IntelPage.jsx";
import AuthPage from "./pages/AuthPage.jsx";

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
      const data = await cachedFetch("/api/users/preferences", { ttl: 30_000, staleTtl: 180_000, storage: "local" });
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

  useLiveRefresh(loadSession, { interval: 45_000 });
  useLiveRefresh(loadPreferences, { enabled: Boolean(token), interval: 60_000 });

  useEffect(() => {
    if (!token) return;
    const guildId = session.guild_id || session.account_guild_id;
    prefetchFetch("/api/bots", { ttl: 60_000, staleTtl: 300_000, storage: "local" });
    prefetchFetch("/api/dashboard", { ttl: 8_000, staleTtl: 30_000 });
    prefetchFetch("/api/users/me", { ttl: 30_000, staleTtl: 120_000 });
    prefetchFetch(`/api/music-intelligence${query({ guild_id: guildId, limit: 10 })}`, { ttl: 20_000, staleTtl: 120_000 });
  }, [session.account_guild_id, session.guild_id, token]);

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
          <Route path="/friends" element={<Protected ctx={ctx}><FriendsPage ctx={ctx} /></Protected>} />
          <Route path="/messages" element={<Protected ctx={ctx}><MessagesPage ctx={ctx} /></Protected>} />
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

function Protected({ ctx, children, admin = false, gallery = false }) {
  if (ctx.session.loading) return <Page title="Loading" eyebrow="Session"><SkeletonGrid count={4} /></Page>;
  if (!ctx.session.authenticated) return <Navigate to="/login" replace />;
  if (admin && !ctx.isAdmin) return <Denied message="Admin mode is required." />;
  if (gallery && !ctx.canGallery) return <Denied message="Image Gallery owner access is required." />;
  return children;
}

export default App;
