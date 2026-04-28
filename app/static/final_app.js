// ================================
// 🔧 CONFIG
// ================================
const API_BASE = "/api";
const REMOTE_MODE = Boolean(window.SWARM_PANEL_REMOTE_MODE) || window.location.hostname.endsWith('github.io');
const REMOTE_ORIGIN_KEY = 'swarm_panel_remote_origin';
const REMOTE_TOKEN_KEY = 'swarm_panel_remote_token';
const REMOTE_USERNAME_KEY = 'swarm_panel_remote_username';
const REMOTE_CONFIG_FILE = 'live-config.json';
const rawFetch = window.fetch.bind(window);
const storageFallback = new Map();
let controlCooldown = false;
let dashboardBotsState = [];
let botCatalogState = [];
let inviteCatalogState = [];
let userProfileState = { profile: null, editable: false, favoriteBotOptions: [] };
let userDirectoryState = [];
let userDirectoryRequestId = 0;
let panelPreferencesState = {};
let controlInventoryState = null;
let controlMatrixState = { guildId: null, bots: [], loaded: false, generatedAt: null };
let selectedControlState = { botKey: null, guildId: null, loaded: false, data: null };
let controlInventoryLoading = false;
let controlInventoryRequestId = 0;
let controlMatrixRequestId = 0;
let selectedControlRequestId = 0;
let eventFeedEntries = [];
let eventFeedSocket = null;
let eventFeedReconnectTimer = null;
let eventFeedPollTimer = null;
let eventFeedConnectionState = 'offline';
let systemDiagnosticsState = null;
let metricsSnapshotState = null;
let controlRefreshTimer = null;
let lastDashboardFetchAt = Date.now();
let liveSessionState = [];
let liveSessionPositionCache = new Map();
let dashboardRefreshTimer = null;
let diagnosticsRefreshTimer = null;
let metricsRefreshTimer = null;
const LIVE_POSITION_CACHE_MAX = 300;
const LIVE_POSITION_CACHE_TTL_MS = 60 * 60 * 1000;
let remotePanelOrigin = '';
let remotePanelToken = '';
let remotePanelUsername = '';
let currentPanelSession = { role: 'admin', guild_id: null, account_guild_id: null, username: '', image_gallery_owner: false, admin_mode: true };
let inviteOnlyMode = false;
let panelAppStarted = false;
let panelSessionChecked = false;
let livePositionTickerStarted = false;
const MAX_EVENT_FEED_ENTRIES = 80;
const CENTRAL_TIMEZONE = "America/Chicago";
const centralDateTimeFormatter = new Intl.DateTimeFormat("en-US", { timeZone: CENTRAL_TIMEZONE, year: "numeric", month: "2-digit", day: "2-digit", hour: "numeric", minute: "2-digit", second: "2-digit", hour12: true, timeZoneName: "short" });
const centralTimeFormatter = new Intl.DateTimeFormat("en-US", { timeZone: CENTRAL_TIMEZONE, hour: "numeric", minute: "2-digit", second: "2-digit", hour12: true, timeZoneName: "short" });
const RUNTIME_SESSION_STATES = new Set(['playing', 'paused', 'queued']);
const DEFAULT_PANEL_PREFERENCES = {
    accent_color: '#89b4fa',
    background_mode: 'default',
    background_color: '#0b0e18',
    background_image_url: '',
    layout_mode: 'standard',
    density: 'comfortable',
    card_shape: 'soft',
    font_scale: 'normal',
    motion: 'standard',
};


function getSessionRuntimeKey(session) {
    const botKey = String(session?.bot_key || session?.bot || 'unknown');
    const guildId = String(session?.guild_id || '0');
    const trackKey = String(session?.video_url || session?.title || 'no-track');
    return `${botKey}:${guildId}:${trackKey}`;
}

function pruneLiveSessionPositionCache(activeKeys = new Set(), now = Date.now()) {
    for (const [key, value] of liveSessionPositionCache.entries()) {
        const stale = now - Number(value?.last_seen_ms || 0) > LIVE_POSITION_CACHE_TTL_MS;
        if (stale && !activeKeys.has(key)) liveSessionPositionCache.delete(key);
    }
    if (liveSessionPositionCache.size <= LIVE_POSITION_CACHE_MAX) return;
    const sorted = [...liveSessionPositionCache.entries()]
        .sort((a, b) => Number(a[1]?.last_seen_ms || 0) - Number(b[1]?.last_seen_ms || 0));
    while (liveSessionPositionCache.size > LIVE_POSITION_CACHE_MAX && sorted.length) {
        const [oldestKey] = sorted.shift();
        if (!activeKeys.has(oldestKey)) liveSessionPositionCache.delete(oldestKey);
        else if (liveSessionPositionCache.size > LIVE_POSITION_CACHE_MAX) liveSessionPositionCache.delete(oldestKey);
    }
}

function normalizeLiveSession(session, now = Date.now()) {
    const normalized = { ...session };
    const cacheKey = getSessionRuntimeKey(normalized);
    const cached = liveSessionPositionCache.get(cacheKey);
    const dbPosition = Number(normalized.position_seconds || 0);
    const durationSeconds = Number(normalized.duration_seconds || normalized.length_seconds || 0);
    let basePosition = dbPosition;

    if (cached) {
        if (cached.trackSignature === cacheKey) {
            if (normalized.is_playing) {
                basePosition = Math.max(dbPosition, Number(cached.position_seconds || 0));
            } else if (!normalized.is_playing && normalized.session_state === 'paused') {
                basePosition = Math.max(dbPosition, Number(cached.position_seconds || 0));
            }
        }
    }

    if (durationSeconds > 0) {
        basePosition = Math.min(basePosition, durationSeconds);
    }

    normalized.position_seconds = Math.max(0, Math.floor(basePosition));
    normalized._position_anchor_ms = now;
    liveSessionPositionCache.set(cacheKey, {
        trackSignature: cacheKey,
        position_seconds: normalized.position_seconds,
        duration_seconds: durationSeconds,
        last_seen_ms: now,
        is_playing: Boolean(normalized.is_playing),
        session_state: normalized.session_state || null,
    });
    return normalized;
}

function getDisplayPositionSeconds(session, now = Date.now()) {
    const cacheKey = getSessionRuntimeKey(session);
    const cached = liveSessionPositionCache.get(cacheKey);
    const durationSeconds = Number(session?.duration_seconds || session?.length_seconds || cached?.duration_seconds || 0);
    const sessionAnchorMs = Number(session?._position_anchor_ms || 0);
    const cachedAnchorMs = Number(cached?.last_seen_ms || 0);
    const basePosition = Number(cached?.position_seconds ?? session?.position_seconds ?? 0);
    const anchorCandidates = [sessionAnchorMs, cachedAnchorMs, lastDashboardFetchAt || 0].filter(value => Number.isFinite(value) && value > 0);
    const anchorMs = anchorCandidates.length ? Math.max(...anchorCandidates) : now;
    const shouldAdvance = Boolean(session?.is_playing);
    const elapsedSeconds = shouldAdvance ? Math.max(0, Math.floor((now - anchorMs) / 1000)) : 0;
    let display = basePosition + elapsedSeconds;
    if (durationSeconds > 0) {
        display = Math.min(display, durationSeconds);
    }
    display = Math.max(0, Math.floor(display));
    const nextAnchorMs = shouldAdvance ? now : anchorMs;
    liveSessionPositionCache.set(cacheKey, {
        trackSignature: cacheKey,
        position_seconds: display,
        duration_seconds: durationSeconds,
        last_seen_ms: nextAnchorMs,
        is_playing: Boolean(session?.is_playing),
        session_state: session?.session_state || null,
    });
    return display;
}

function renderLivePositionTick() {
    if (!panelAppStarted || !Array.isArray(liveSessionState) || !liveSessionState.length) return;
    const now = Date.now();
    liveSessionState.forEach(session => {
        const positionSeconds = getDisplayPositionSeconds(session, now);
        const durationSeconds = Number(session.duration_seconds || session.length_seconds || session.track_length_seconds || 0);
        const label = formatDuration(positionSeconds);
        const fullLabel = `${label}${durationSeconds > 0 ? ` / ${formatDuration(durationSeconds)}` : ''}`;
        const key = getSessionRuntimeKey(session);
        document.querySelectorAll(`[data-position-key="${CSS.escape(key)}"]`).forEach(node => {
            node.textContent = node.dataset.positionFull === '1' ? fullLabel : label;
        });
        const progressPercent = formatProgressPercent(positionSeconds, durationSeconds);
        if (progressPercent !== null) {
            document.querySelectorAll(`[data-progress-key="${CSS.escape(key)}"]`).forEach(node => {
                node.style.width = `${progressPercent}%`;
            });
        }
    });
}

// ================================
// 🔒 AUTH HELPER
// ================================
function getStorageCandidates() {
    const stores = [];
    try {
        if (window.localStorage) stores.push(window.localStorage);
    } catch (_err) {}
    try {
        if (window.sessionStorage) stores.push(window.sessionStorage);
    } catch (_err) {}
    return stores;
}

function readStoredValue(key) {
    for (const store of getStorageCandidates()) {
        try {
            const value = store.getItem(key);
            if (value !== null && value !== undefined) {
                return value;
            }
        } catch (_err) {}
    }
    return storageFallback.get(key) || '';
}

function writeStoredValue(key, value) {
    const normalized = String(value || '');
    if (normalized) {
        storageFallback.set(key, normalized);
    } else {
        storageFallback.delete(key);
    }

    for (const store of getStorageCandidates()) {
        try {
            if (normalized) {
                store.setItem(key, normalized);
            } else {
                store.removeItem(key);
            }
        } catch (_err) {}
    }
}

function normalizeRemoteOrigin(value) {
    let normalized = String(value || '').trim();
    if (!normalized) return '';

    const nestedPanelMatch = normalized.match(/[?&]panel=([^&]+)/i);
    if (nestedPanelMatch?.[1]) {
        try {
            normalized = decodeURIComponent(nestedPanelMatch[1]);
        } catch (_err) {
            normalized = nestedPanelMatch[1];
        }
    }

    if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(normalized)) {
        normalized = `https://${normalized}`;
    }

    try {
        const url = new URL(normalized);
        const nestedPanel = url.searchParams.get('panel');
        if (nestedPanel) {
            return normalizeRemoteOrigin(nestedPanel);
        }
        return `${url.protocol}//${url.host}`;
    } catch (_err) {
        normalized = normalized.replace(/\/+$/, '');
        normalized = normalized.replace(/\/(?:index\.html?|login)$/i, '');
        if (normalized.endsWith('/api')) {
            normalized = normalized.slice(0, -4);
        }
    }

    return normalized;
}

function setRemoteOrigin(value, persist = true) {
    const previousOrigin = remotePanelOrigin;
    remotePanelOrigin = normalizeRemoteOrigin(value);
    if (!REMOTE_MODE) return remotePanelOrigin;
    if (previousOrigin && remotePanelOrigin && previousOrigin !== remotePanelOrigin) {
        setRemoteToken('');
    }
    if (persist) {
        writeStoredValue(REMOTE_ORIGIN_KEY, remotePanelOrigin);
    }
    return remotePanelOrigin;
}

function setRemoteToken(value, persist = true) {
    remotePanelToken = String(value || '').trim();
    if (!REMOTE_MODE) return remotePanelToken;
    if (persist) {
        writeStoredValue(REMOTE_TOKEN_KEY, remotePanelToken);
    }
    return remotePanelToken;
}

function setRemoteUsername(value, persist = true) {
    remotePanelUsername = String(value || '').trim();
    if (!REMOTE_MODE) return remotePanelUsername;
    if (persist) {
        writeStoredValue(REMOTE_USERNAME_KEY, remotePanelUsername);
    }
    return remotePanelUsername;
}

function setPanelSessionContext(data = {}) {
    const linkedGuildId = data.account_guild_id || data.guild_id || null;
    const adminMode = data.admin_mode !== undefined
        ? Boolean(data.admin_mode)
        : (data.role || 'admin') === 'admin' && !linkedGuildId;
    currentPanelSession = {
        role: data.role || 'admin',
        guild_id: data.guild_id || null,
        account_guild_id: linkedGuildId,
        username: data.username || remotePanelUsername || '',
        image_gallery_owner: Boolean(data.image_gallery_owner),
        admin_mode: adminMode,
    };
    document.body.classList.toggle('guild-scoped-session', !currentPanelSession.admin_mode && Boolean(currentPanelSession.account_guild_id || currentPanelSession.guild_id));
    document.body.classList.toggle('image-gallery-owner-session', Boolean(currentPanelSession.image_gallery_owner));
    updateAdminModeToggle();
    updateTopbarAccount();
    ensureSessionVisibleTab();
}

function isAdminSession() {
    return Boolean(currentPanelSession.admin_mode);
}

function isGuildScopedSession() {
    return !isAdminSession() && Boolean(currentPanelSession.account_guild_id || currentPanelSession.guild_id);
}

function isImageGalleryOwnerSession() {
    return Boolean(currentPanelSession.image_gallery_owner);
}

function ensureSessionVisibleTab() {
    if (isAdminSession()) return;
    const activePanel = document.querySelector('.swarm-tab-panel.active');
    const adminPanelIds = new Set(['diagnostics-tab', 'intel-tab', 'image-gallery-tab']);
    if (activePanel && adminPanelIds.has(activePanel.id)) {
        activateSwarmTab('overview-tab');
    }
}

function getLinkedGuildId() {
    return currentPanelSession.account_guild_id || currentPanelSession.guild_id || null;
}

function updateAdminModeToggle() {
    const wrap = document.getElementById('admin-mode-toggle-wrap');
    const toggle = document.getElementById('admin-mode-toggle');
    const status = document.getElementById('admin-mode-status');
    const linkedGuildId = getLinkedGuildId();

    if (wrap) wrap.hidden = !linkedGuildId;
    if (toggle) {
        toggle.checked = isAdminSession();
        toggle.disabled = !linkedGuildId;
    }
    if (!status) return;

    if (isAdminSession()) {
        status.textContent = linkedGuildId ? `Admin Mode · Guild ${linkedGuildId}` : 'Admin Mode';
        status.dataset.tone = 'online';
    } else if (linkedGuildId) {
        status.textContent = `Guild Mode · ${linkedGuildId}`;
        status.dataset.tone = 'idle';
    } else {
        status.textContent = currentPanelSession.username || 'Connected';
        status.dataset.tone = 'idle';
    }
}

async function refreshPanelAfterModeSwitch() {
    dashboardBotsState = [];
    botCatalogState = [];
    inviteCatalogState = [];
    controlInventoryState = null;
    clearControlMatrixState();
    clearSelectedControlState();
    resetControlInventorySelectors();
    if (!isAdminSession()) {
        disconnectEventFeed();
        systemDiagnosticsState = null;
        metricsSnapshotState = null;
    }
    await Promise.allSettled([
        fetchDashboard(),
        loadBotSelect(),
        loadUserProfile(),
        loadPanelPreferences(),
        loadUserDirectory(),
    ]);
    if (isAdminSession()) {
        await Promise.allSettled([
            fetchDiagnostics(),
            loadDbSchemas(),
            loadImageGalleryAdmin(),
            loadEventFeedHistory(),
        ]);
        connectEventFeed();
    }
}

async function setAdminMode(enabled) {
    const toggle = document.getElementById('admin-mode-toggle');
    const desired = Boolean(enabled);
    if (!getLinkedGuildId()) {
        updateAdminModeToggle();
        return;
    }
    if (toggle) toggle.disabled = true;
    updateRemoteConnectionStatus(desired ? 'Switching to admin mode...' : 'Switching to guild mode...', 'idle');
    try {
        const res = await fetch(`${API_BASE}/session/admin-mode`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled: desired }),
        });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'Mode switch failed');
        }
        if (data.token) setRemoteToken(data.token);
        setPanelSessionContext(data);
        await refreshPanelAfterModeSwitch();
        updateRemoteConnectionStatus(isAdminSession() ? 'Admin mode active' : 'Guild mode active', isAdminSession() ? 'online' : 'idle');
    } catch (err) {
        if (toggle) toggle.checked = isAdminSession();
        updateRemoteConnectionStatus(err instanceof Error ? err.message : String(err), 'offline');
    } finally {
        if (toggle) toggle.disabled = !getLinkedGuildId();
        updateAdminModeToggle();
    }
}

function resolveApiUrl(input) {
    if (!REMOTE_MODE) return input;
    if (typeof input !== 'string') return input;
    if (!input.startsWith('/')) return input;
    if (!remotePanelOrigin) return input;
    return remotePanelOrigin.replace(/\/$/, "") + input;
}

function buildStaticUrl(path) {
    try {
        return new URL(path, window.location.href).toString();
    } catch (_err) {
        return path;
    }
}

async function loadRemotePanelConfig() {
    if (!REMOTE_MODE) return '';

    try {
        const response = await rawFetch(buildStaticUrl(REMOTE_CONFIG_FILE), {
            cache: 'no-store',
        });
        if (!response.ok) return '';
        const payload = await response.json().catch(() => ({}));
        return normalizeRemoteOrigin(
            payload.panel_url
            || payload.panel
            || payload.remote_panel_origin
            || payload.remote_origin
            || '',
        );
    } catch (_err) {
        return '';
    }
}

function buildWebSocketUrl(path = '/ws') {
    const token = encodeURIComponent(remotePanelToken || '');
    if (REMOTE_MODE) {
        if (!remotePanelOrigin || !remotePanelToken) return null;
        const url = new URL(remotePanelOrigin);
        url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        url.pathname = (url.pathname === "/" ? "" : url.pathname.replace(/\/$/, "")) + path;
        url.search = token ? `token=${token}` : '';
        return url.toString();
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const suffix = token ? `?token=${token}` : '';
    return `${protocol}//${window.location.host}${path}${suffix}`;
}

window.fetch = function patchedFetch(input, init = {}) {
    const target = resolveApiUrl(input);
    const headers = new Headers(init.headers || {});

    if (remotePanelToken && typeof target === 'string' && target.includes('/api/')) {
        headers.set('Authorization', `Bearer ${remotePanelToken}`);
    }

    return rawFetch(target, {
        ...init,
        headers,
    });
};

function handle401(res) {
    if (res.status === 401) {
        if (REMOTE_MODE) {
            panelSessionChecked = false;
            panelAppStarted = false;
            setRemoteToken('');
            disconnectEventFeed();
            showRemoteAuthShell('Your live panel login expired. Sign in again to keep using the phone site.');
            return true;
        }
        window.location.href = '/login';
        return true;
    }
    return false;
}

// ================================
// ⏱️ TIME HELPERS
// ================================
function formatDuration(seconds) {
    const s = Math.floor(seconds || 0);
    const m = Math.floor(s / 60);
    const rem = String(s % 60).padStart(2, '0');
    return `${m}:${rem}`;
}


function currentLivePositionSeconds(baseSeconds, isPlaying) {
    const base = Number(baseSeconds || 0);
    if (!isPlaying) return Math.max(0, Math.floor(base));
    const elapsed = Math.max(0, Math.floor((Date.now() - lastDashboardFetchAt) / 1000));
    return Math.max(0, Math.floor(base + elapsed));
}

function updateLivePositionCounters() {
    document.querySelectorAll('[data-live-position]').forEach(element => {
        const baseSeconds = Number(element.dataset.baseSeconds || 0);
        const isPlaying = String(element.dataset.playing || '').toLowerCase() === 'true';
        element.textContent = formatDuration(currentLivePositionSeconds(baseSeconds, isPlaying));
    });
}

function ensureLivePositionTicker() {
    if (livePositionTickerStarted) return;
    livePositionTickerStarted = true;
    setInterval(() => {
        if (!panelAppStarted) return;
        updateLivePositionCounters();
    }, 1000);
}

function formatHeartbeatAge(seconds) {
    if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) {
        return 'No heartbeat';
    }
    const age = Math.max(0, Math.floor(Number(seconds)));
    if (age < 60) return `${age}s ago`;
    const minutes = Math.floor(age / 60);
    const remainder = age % 60;
    return remainder ? `${minutes}m ${remainder}s ago` : `${minutes}m ago`;
}

function escapeHtml(value) {
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function safePublicUrl(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    try {
        const url = new URL(raw);
        return ['http:', 'https:'].includes(url.protocol) ? url.toString() : '';
    } catch (_err) {
        return '';
    }
}

function safeHexColor(value, fallback = '#89b4fa') {
    const raw = String(value || '').trim();
    return /^#[0-9a-fA-F]{6}$/.test(raw) ? raw.toLowerCase() : fallback;
}

function formatCentralTimestamp(value, withDate = false) {
    if (!value) return '--';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return withDate ? centralDateTimeFormatter.format(date) : centralTimeFormatter.format(date);
}

function maskSecret(value) {
    const raw = String(value || '');
    if (!raw || raw === 'missing') return 'missing';
    if (raw === 'present') return 'present';
    return raw;
}

function activateSwarmTab(target) {
    const buttons = Array.from(document.querySelectorAll('.swarm-tab'));
    const panels = Array.from(document.querySelectorAll('.swarm-tab-panel'));
    if (!target || !buttons.length || !panels.length) return;
    buttons.forEach(item => item.classList.toggle('active', item.dataset.tabTarget === target));
    panels.forEach(panel => panel.classList.toggle('active', panel.id === target));
}

function initTabs() {
    const buttons = Array.from(document.querySelectorAll('.swarm-tab'));
    const panels = Array.from(document.querySelectorAll('.swarm-tab-panel'));
    if (!buttons.length || !panels.length) return;
    buttons.forEach(button => {
        if (button.dataset.bound === '1') return;
        button.dataset.bound = '1';
        button.addEventListener('click', () => {
            const target = button.dataset.tabTarget;
            if (inviteOnlyMode && target !== 'invites-tab') {
                activateSwarmTab('invites-tab');
                return;
            }
            activateSwarmTab(target);
        });
    });
}

function describeBotStatus(status) {
    const normalized = String(status || 'unknown').toLowerCase();
    if (normalized === 'online') {
        return { label: 'Online', color: '#a6e3a1', tone: 'online' };
    }
    if (normalized === 'stale') {
        return { label: 'Stale', color: '#fab387', tone: 'stale' };
    }
    if (normalized === 'offline' || normalized === 'error' || normalized === 'db-unavailable') {
        return { label: 'Offline', color: '#f38ba8', tone: 'offline' };
    }
    return { label: 'Idle', color: '#89b4fa', tone: 'idle' };
}

function describeDiagnosticState(status) {
    const normalized = String(status || 'unknown').toLowerCase();
    if (normalized === 'online') {
        return { label: 'Online', tone: 'online' };
    }
    if (normalized === 'missing') {
        return { label: 'Missing', tone: 'offline' };
    }
    if (normalized === 'error') {
        return { label: 'Error', tone: 'offline' };
    }
    return { label: normalized || 'Unknown', tone: 'idle' };
}

function diagnosticBadge(status) {
    const meta = describeDiagnosticState(status);
    return `<span class="diag-badge diag-badge-${meta.tone}">${escapeHtml(meta.label)}</span>`;
}

function selectedOptionText(selectId) {
    const select = document.getElementById(selectId);
    if (!select) return '';
    return select.options[select.selectedIndex]?.text || '';
}

function getPanelOriginFromQuery() {
    const value = new URLSearchParams(window.location.search).get('panel');
    return normalizeRemoteOrigin(value);
}

function updateRemoteConnectionStatus(message, tone = 'idle') {
    const status = document.getElementById('remote-connection-status');
    if (!status) return;
    status.textContent = message || '';
    status.dataset.tone = tone;
}

function setRemoteAuthError(message = '') {
    const element = document.getElementById('remote-auth-error');
    if (!element) return;
    if (!message) {
        element.style.display = 'none';
        element.textContent = '';
        return;
    }
    element.style.display = 'flex';
    element.textContent = message;
}

function showRemoteAuthShell(message = '') {
    const shell = document.getElementById('remote-auth-shell');
    if (!shell) return;
    const originInput = document.getElementById('remote-api-origin');
    const usernameInput = document.getElementById('remote-username');
    if (originInput && remotePanelOrigin) originInput.value = remotePanelOrigin;
    if (usernameInput && remotePanelUsername) usernameInput.value = remotePanelUsername;
    setRemoteAuthError(message);
    shell.classList.add('visible');
    updateRemoteConnectionStatus(remotePanelOrigin ? 'Ready to sign in' : 'Waiting for live panel sync', remotePanelOrigin ? 'idle' : 'offline');
}

function hideRemoteAuthShell() {
    const shell = document.getElementById('remote-auth-shell');
    if (!shell) return;
    shell.classList.remove('visible');
    setRemoteAuthError('');
}

async function syncPanelSession() {
    if (REMOTE_MODE && !remotePanelOrigin) {
        return false;
    }

    try {
        const res = await fetch(`${API_BASE}/session`);
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.authenticated) {
            return false;
        }

        if (data.token) {
            setRemoteToken(data.token);
        }
        if (data.username) {
            setRemoteUsername(data.username);
        }
        setPanelSessionContext(data);
        panelSessionChecked = true;
        updateRemoteConnectionStatus(
            REMOTE_MODE
                ? 'Connected'
                : 'Connected to local SwarmPanel',
            'online',
        );
        return true;
    } catch (err) {
        console.error('❌ Session sync failed:', err);
        return false;
    }
}

async function loginRemotePanel() {
    const originInput = document.getElementById('remote-api-origin');
    const usernameInput = document.getElementById('remote-username');
    const passwordInput = document.getElementById('remote-password');
    const submitButton = document.getElementById('remote-login-button');

    const configuredOrigin = await loadRemotePanelConfig();
    const nextOrigin = normalizeRemoteOrigin(configuredOrigin || originInput?.value);
    const username = String(usernameInput?.value || '').trim();
    const password = String(passwordInput?.value || '');

    if (!nextOrigin) {
        showRemoteAuthShell('The live panel is still syncing. Try again after the panel finishes starting.');
        return;
    }
    if (!username || !password) {
        showRemoteAuthShell('Enter the SwarmPanel username and password.');
        return;
    }

    setRemoteOrigin(nextOrigin);
    if (originInput) originInput.value = remotePanelOrigin;
    setRemoteUsername(username);
    setRemoteAuthError('');
    if (submitButton) submitButton.disabled = true;

    try {
        const loginUrl = new URL('/api/session/login', `${remotePanelOrigin}/`).toString();
        const res = await rawFetch(loginUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.token) {
            showRemoteAuthShell(data.detail || 'Unable to sign in to the live panel.');
            return;
        }

        setRemoteToken(data.token);
        setPanelSessionContext(data);
        panelSessionChecked = true;
        updateRemoteConnectionStatus('Connected', 'online');
        hideRemoteAuthShell();
        if (passwordInput) passwordInput.value = '';
        await startPanelApplication();
    } catch (err) {
        const message = err instanceof Error
            ? `${err.message}. Verify the live panel is public and reachable from this device.`
            : 'Connection failed. Verify the live panel is public and reachable from this device.';
        showRemoteAuthShell(message);
    } finally {
        if (submitButton) submitButton.disabled = false;
    }
}

async function registerRemotePanel() {
    const originInput = document.getElementById('remote-api-origin');
    const usernameInput = document.getElementById('remote-register-username');
    const guildInput = document.getElementById('remote-register-guild-id');
    const emailInput = document.getElementById('remote-register-email');
    const submitButton = document.getElementById('remote-register-button');

    const configuredOrigin = await loadRemotePanelConfig();
    const nextOrigin = normalizeRemoteOrigin(configuredOrigin || originInput?.value);
    const username = String(usernameInput?.value || '').trim();
    const guildId = String(guildInput?.value || '').trim();
    const email = String(emailInput?.value || '').trim();

    if (!nextOrigin) {
        showRemoteAuthShell('The live panel is still syncing. Try again after the panel finishes starting.');
        return;
    }
    if (!username || !guildId) {
        showRemoteAuthShell('Choose a username and enter your Discord guild ID.');
        return;
    }

    setRemoteOrigin(nextOrigin);
    if (originInput) originInput.value = remotePanelOrigin;
    setRemoteUsername(username);
    setRemoteAuthError('');
    if (submitButton) submitButton.disabled = true;

    try {
        const registerUrl = new URL('/api/session/register', `${remotePanelOrigin}/`).toString();
        const res = await rawFetch(registerUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, guild_id: guildId, email: email || null }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.token) {
            showRemoteAuthShell(data.detail || 'Unable to create that guild account.');
            return;
        }

        setRemoteToken(data.token);
        setPanelSessionContext(data);
        panelSessionChecked = true;
        updateRemoteConnectionStatus('Connected', 'online');
        hideRemoteAuthShell();
        await startPanelApplication();
    } catch (err) {
        const message = err instanceof Error
            ? `${err.message}. Verify the live panel is public and reachable from this device.`
            : 'Connection failed. Verify the live panel is public and reachable from this device.';
        showRemoteAuthShell(message);
    } finally {
        if (submitButton) submitButton.disabled = false;
    }
}

async function logoutPanel() {
    try {
        if (REMOTE_MODE && remotePanelOrigin && remotePanelToken) {
            await fetch(`${API_BASE}/session/logout`, { method: 'POST' }).catch(() => null);
        } else if (!REMOTE_MODE) {
            await fetch(`${API_BASE}/session/logout`, { method: 'POST' }).catch(() => null);
        }
    } finally {
        disconnectEventFeed();
        panelAppStarted = false;
        panelSessionChecked = false;
        setRemoteToken('');
        if (REMOTE_MODE) {
            showRemoteAuthShell('Signed out of the live panel.');
            updateRemoteConnectionStatus('Signed out', 'offline');
            return;
        }
        window.location.href = '/login';
    }
}

async function startPanelApplication() {
    initTabs();
    ["error-feed-level", "error-feed-source", "error-feed-sort"].forEach(id => {
        const el = document.getElementById(id);
        if (el && !el.dataset.bound) {
            el.addEventListener("change", renderEventFeed);
            el.dataset.bound = "1";
        }
    });

    if (panelAppStarted) {
        fetchDashboard();
        if (isAdminSession()) {
            fetchDiagnostics();
        }
        loadBotSelect();
        loadUserProfile();
        loadPanelPreferences();
        loadUserDirectory();
        if (isAdminSession()) {
            loadDbSchemas();
            loadImageGalleryAdmin();
            loadEventFeedHistory();
            connectEventFeed();
        }
        return true;
    }

    panelAppStarted = true;
    fetchDashboard();
    if (isAdminSession()) {
        fetchDiagnostics();
    }
    loadBotSelect();
    loadUserProfile();
    loadPanelPreferences();
    loadUserDirectory();
    if (isAdminSession()) {
        loadDbSchemas();
        loadImageGalleryAdmin();
        loadEventFeedHistory();
        connectEventFeed();
    }
    return true;
}

async function bootstrapPanelApplication() {
    if (REMOTE_MODE) {
        const queryOrigin = getPanelOriginFromQuery();
        const configuredOrigin = await loadRemotePanelConfig();
        if (queryOrigin) {
            setRemoteOrigin(queryOrigin);
        } else if (configuredOrigin) {
            setRemoteOrigin(configuredOrigin);
        } else {
            setRemoteOrigin(readStoredValue(REMOTE_ORIGIN_KEY) || '', false);
        }
        setRemoteToken(readStoredValue(REMOTE_TOKEN_KEY) || '', false);
        setRemoteUsername(readStoredValue(REMOTE_USERNAME_KEY) || '', false);
        if (configuredOrigin && configuredOrigin === remotePanelOrigin) {
            writeStoredValue(REMOTE_ORIGIN_KEY, configuredOrigin);
        }

        if (!remotePanelOrigin) {
            showRemoteAuthShell('The live panel is still syncing. Try again after the panel finishes starting.');
            return false;
        }

        const ok = await syncPanelSession();
        if (!ok) {
            showRemoteAuthShell('Sign in to the live panel to continue.');
            return false;
        }

        hideRemoteAuthShell();
        return startPanelApplication();
    }

    await syncPanelSession();
    return startPanelApplication();
}

function clearControlMatrixState(guildId = null) {
    controlMatrixState = { guildId: guildId ? String(guildId) : null, bots: [], loaded: false, generatedAt: null };
}

function clearSelectedControlState(botKey = null, guildId = null) {
    selectedControlState = {
        botKey: botKey ? String(botKey) : null,
        guildId: guildId ? String(guildId) : null,
        loaded: false,
        data: null,
    };
}

function getSelectedControlBot(botKey, guildId = null) {
    const selectedGuildId = guildId ?? document.getElementById('control-guild-select')?.value;
    if (!botKey || !selectedGuildId) return null;
    if (!selectedControlState.loaded) return null;
    if (String(selectedControlState.botKey) !== String(botKey)) return null;
    if (String(selectedControlState.guildId) !== String(selectedGuildId)) return null;
    return selectedControlState.data;
}

function getBestLiveControlBot(botKey, guildId = null) {
    return getSelectedControlBot(botKey, guildId) || getLiveControlBot(botKey, guildId);
}

function getLiveControlBot(botKey, guildId = null) {
    const selectedGuildId = guildId ?? document.getElementById('control-guild-select')?.value;
    if (!botKey || !selectedGuildId) return null;
    if (!controlMatrixState.loaded || String(controlMatrixState.guildId) !== String(selectedGuildId)) return null;
    return controlMatrixState.bots.find(bot => bot.key === botKey) || null;
}

function getBestControlSession(botKey, guildId = null) {
    const liveBot = getBestLiveControlBot(botKey, guildId);
    if (liveBot?.session) return liveBot.session;
    return getDashboardSession(botKey, guildId);
}

function getControlBotStatus(botKey, guildId = null) {
    const liveBot = getBestLiveControlBot(botKey, guildId);
    if (liveBot?.db?.reachable === false) return describeBotStatus('offline');

    const heartbeatStatus = String(liveBot?.heartbeat?.status || '').toLowerCase();
    // Bots write 'HEALTHY' to swarm_health; treat it the same as 'online'
    if (heartbeatStatus === 'online' || heartbeatStatus === 'healthy') return describeBotStatus('online');
    if (heartbeatStatus === 'stale') return describeBotStatus('stale');
    if (heartbeatStatus === 'offline' || heartbeatStatus === 'error') return describeBotStatus('offline');

    const dashboardBot = getDashboardBot(botKey);
    if (dashboardBot) return describeBotStatus(dashboardBot.status);
    return describeBotStatus('unknown');
}

function getControlHeartbeatAge(botKey, guildId = null) {
    const liveBot = getBestLiveControlBot(botKey, guildId);
    if (liveBot?.heartbeat?.age_seconds !== undefined && liveBot?.heartbeat?.age_seconds !== null) {
        return liveBot.heartbeat.age_seconds;
    }
    return getDashboardBot(botKey)?.heartbeat_age_seconds;
}

function normalizeLoopMode(value) {
    const normalized = String(value || '').toLowerCase();
    return ['off', 'song', 'queue'].includes(normalized) ? normalized : 'queue';
}

function describeSessionState(session) {
    const normalized = String(session?.session_state || '').toLowerCase();
    if (normalized === 'playing') return { key: 'playing', label: session?.session_state_label || 'Playing', icon: '▶️' };
    if (normalized === 'paused') return { key: 'paused', label: session?.session_state_label || 'Paused', icon: '⏸️' };
    if (normalized === 'queued') return { key: 'queued', label: session?.session_state_label || 'Queued', icon: '📥' };
    if (normalized === 'configured') return { key: 'configured', label: session?.session_state_label || 'Configured', icon: '🛰️' };
    return { key: 'idle', label: session?.session_state_label || 'Idle', icon: '•' };
}

function isRuntimeSession(session) {
    return RUNTIME_SESSION_STATES.has(describeSessionState(session).key);
}

function getSessionChannelLabel(session) {
    if (session?.channel_name) return session.channel_name;
    const state = describeSessionState(session).key;
    if (state === 'queued') return 'Pending voice join';
    if (session?.home_channel_name) return session.home_channel_name;
    if (session?.home_channel_id) return `Home ${session.home_channel_id}`;
    return 'Unknown';
}

function setDirectControlsDisabled(disabled) {
    [
        'control-guild-select',
        'control-voice-select',
        'control-loop-select',
        'control-filter-select',
        'control-source-input',
        'queue-from-panel',
        'apply-loop-mode',
        'apply-filter-mode',
        'set-home-channel',
    ].forEach(id => {
        const element = document.getElementById(id);
        if (element) element.disabled = disabled;
    });

    document.querySelectorAll('[data-panel-action]').forEach(button => {
        button.disabled = disabled;
    });
}

function resetControlInventorySelectors(message = 'Loading guilds...') {
    const guildSel = document.getElementById('control-guild-select');
    const voiceSel = document.getElementById('control-voice-select');

    if (guildSel) guildSel.innerHTML = `<option value="">${escapeHtml(message)}</option>`;
    if (voiceSel) voiceSel.innerHTML = '<option value="">No voice channels loaded</option>';
}

function scheduleDashboardRefresh(delayMs = 1500) {
    if (controlRefreshTimer) {
        clearTimeout(controlRefreshTimer);
    }
    controlRefreshTimer = setTimeout(() => {
        controlRefreshTimer = null;
        fetchDashboard();
    }, delayMs);
}

function renderOverview(bots, generatedAt = null) {
    const container = document.getElementById('overview-stats');
    const timestamp = document.getElementById('overview-generated');
    if (!container) return;

    const workerBots = (bots || []).filter(bot => bot.kind === 'music');
    const orchestrator = (bots || []).find(bot => bot.kind === 'orchestrator');
    const onlineWorkers = workerBots.filter(bot => describeBotStatus(bot.status).tone === 'online').length;
    const staleWorkers = workerBots.filter(bot => describeBotStatus(bot.status).tone === 'stale').length;
    const liveSessions = workerBots.reduce((sum, bot) => sum + (Array.isArray(bot.sessions) ? bot.sessions.filter(session => session.is_playing).length : 0), 0);
    const activeGuilds = new Set();

    workerBots.forEach(bot => {
        (bot.sessions || []).forEach(session => {
            if (!isRuntimeSession(session)) return;
            if (session.guild_id !== null && session.guild_id !== undefined) {
                activeGuilds.add(String(session.guild_id));
            }
        });
    });

    const feedMeta = {
        online: { label: 'Feed Live', detail: 'Realtime event bridge connected', tone: 'online' },
        connecting: { label: 'Feed Syncing', detail: 'Negotiating websocket link', tone: 'stale' },
        offline: { label: 'Feed Offline', detail: 'Retry loop armed', tone: 'offline' },
    }[eventFeedConnectionState] || { label: 'Feed Unknown', detail: 'Waiting for status', tone: 'idle' };

    const cards = [
        {
            label: 'Aria Link',
            value: describeBotStatus(orchestrator?.status).label,
            detail: orchestrator ? `Heartbeat ${formatHeartbeatAge(orchestrator.heartbeat_age_seconds)}` : 'No orchestrator snapshot yet',
            tone: describeBotStatus(orchestrator?.status).tone,
        },
        {
            label: 'Workers Online',
            value: `${onlineWorkers}/${workerBots.length}`,
            detail: staleWorkers ? `${staleWorkers} stale heartbeat${staleWorkers === 1 ? '' : 's'}` : 'Heartbeat grid looks clean',
            tone: onlineWorkers ? 'online' : 'offline',
        },
        {
            label: 'Live Sessions',
            value: String(liveSessions),
            detail: `${activeGuilds.size} guild${activeGuilds.size === 1 ? '' : 's'} carrying audio`,
            tone: liveSessions ? 'online' : 'idle',
        },
        {
            label: 'Event Feed',
            value: feedMeta.label,
            detail: feedMeta.detail,
            tone: feedMeta.tone,
        },
    ];

    container.innerHTML = cards.map(card => `
        <article class="overview-card overview-card-${card.tone}">
            <div class="overview-card-label">${card.label}</div>
            <div class="overview-card-value">${card.value}</div>
            <div class="overview-card-detail">${card.detail}</div>
        </article>
    `).join('');

    if (timestamp) {
        timestamp.textContent = generatedAt
            ? `Updated ${new Date(generatedAt).toLocaleTimeString()}`
            : 'Waiting for dashboard sync...';
    }
}

function updateInviteOnlyMode() {
    const hasConnectedMusicBot = dashboardBotsState.some(bot => bot.kind === 'music')
        || botCatalogState.length > 0
        || inviteCatalogState.some(bot => bot.kind === 'music' && bot.connected_to_session_guild);
    inviteOnlyMode = isGuildScopedSession() && !hasConnectedMusicBot;
    const linkedGuildId = getLinkedGuildId();
    document.body.classList.toggle('invite-only-mode', inviteOnlyMode);

    const lockout = document.getElementById('invite-lockout-message');
    if (lockout) {
        lockout.hidden = !inviteOnlyMode;
        lockout.textContent = inviteOnlyMode
            ? `No swarm bot is connected to guild ${linkedGuildId}. Invite at least one bot to unlock the rest of the panel.`
            : '';
    }

    const copy = document.getElementById('invite-panel-copy');
    if (copy) {
        copy.textContent = inviteOnlyMode
            ? 'Add a bot to this Discord server, then refresh once Discord finishes the invite.'
            : 'Add the swarm nodes your server needs, then refresh once Discord finishes the invite.';
    }

    if (inviteOnlyMode) {
        activateSwarmTab('invites-tab');
    }
}

function renderInviteCatalog() {
    const grid = document.getElementById('invite-bot-grid');
    if (!grid) return;

    const bots = inviteCatalogState;
    if (!bots.length) {
        grid.innerHTML = '<div class="control-context-empty">Invite metadata is not available yet.</div>';
        return;
    }

    grid.innerHTML = bots.map(bot => {
        const permissions = Array.isArray(bot.permissions) ? bot.permissions : [];
        const connected = Boolean(bot.connected_to_session_guild);
        const inviteUrl = bot.invite_url || '';
        const initial = String(bot.display_name || bot.key || '?').slice(0, 1).toUpperCase();
        const logo = bot.icon_url
            ? `<img class="invite-bot-logo-img" src="${escapeHtml(bot.icon_url)}" alt="${escapeHtml(bot.display_name)} logo" loading="lazy" />`
            : `<span>${escapeHtml(initial)}</span>`;
        const action = inviteUrl
            ? `<a class="invite-bot-button" href="${escapeHtml(inviteUrl)}" target="_blank" rel="noopener noreferrer">${connected ? 'Invite Again' : 'Invite Bot'}</a>`
            : '<span class="invite-bot-button invite-bot-button-disabled">Missing Client ID</span>';
        const statusLabel = isGuildScopedSession()
            ? (connected ? 'Connected' : 'Needed')
            : 'Invite';
        const statusClass = connected ? 'invite-bot-status' : 'invite-bot-status invite-bot-status-needed';

        return `
            <article class="invite-bot-card" style="--invite-accent: ${escapeHtml(bot.accent || '#89b4fa')};">
                <div class="invite-bot-card-head">
                    <div class="invite-bot-logo">${logo}</div>
                    <div class="invite-bot-title">
                        <h3>${escapeHtml(bot.display_name || bot.key)}</h3>
                        <span>${escapeHtml(bot.kind === 'orchestrator' ? 'Orchestrator' : 'Music worker')}</span>
                    </div>
                    <span class="${statusClass}">${escapeHtml(statusLabel)}</span>
                </div>
                <p class="invite-bot-summary">${escapeHtml(bot.capability_summary || '')}</p>
                <div class="invite-bot-permissions">
                    ${permissions.map(permission => `<span>${escapeHtml(permission)}</span>`).join('')}
                </div>
                <div class="invite-bot-footer">
                    <span class="invite-bot-permission-code">Permissions ${escapeHtml(bot.permission_integer || '0')}</span>
                    ${action}
                </div>
            </article>
        `;
    }).join('');
}

function normalizePanelPreferences(preferences = {}) {
    const merged = { ...DEFAULT_PANEL_PREFERENCES, ...(preferences || {}) };
    const pick = (value, allowed, fallback) => allowed.includes(value) ? value : fallback;
    return {
        accent_color: safeHexColor(merged.accent_color, DEFAULT_PANEL_PREFERENCES.accent_color),
        background_mode: pick(String(merged.background_mode || ''), ['default', 'midnight', 'aurora', 'ember', 'custom_color', 'custom_image'], 'default'),
        background_color: safeHexColor(merged.background_color, DEFAULT_PANEL_PREFERENCES.background_color),
        background_image_url: safePublicUrl(merged.background_image_url),
        layout_mode: pick(String(merged.layout_mode || ''), ['standard', 'focused', 'wide'], 'standard'),
        density: pick(String(merged.density || ''), ['comfortable', 'compact'], 'comfortable'),
        card_shape: pick(String(merged.card_shape || ''), ['soft', 'crisp'], 'soft'),
        font_scale: pick(String(merged.font_scale || ''), ['normal', 'large', 'dense'], 'normal'),
        motion: pick(String(merged.motion || ''), ['standard', 'reduced'], 'standard'),
    };
}

function applyPanelPreferences(preferences = panelPreferencesState) {
    const prefs = normalizePanelPreferences(preferences);
    panelPreferencesState = prefs;
    const root = document.documentElement;
    root.style.setProperty('--user-panel-accent', prefs.accent_color);
    root.style.setProperty('--purple', prefs.accent_color);
    root.style.setProperty('--custom-bg-color', prefs.background_color);
    root.style.setProperty('--custom-bg-image', prefs.background_image_url ? `url("${prefs.background_image_url.replaceAll('"', '%22')}")` : 'none');

    const body = document.body;
    const managedClasses = [
        'panel-bg-default', 'panel-bg-midnight', 'panel-bg-aurora', 'panel-bg-ember', 'panel-bg-custom-color', 'panel-bg-custom-image',
        'panel-layout-standard', 'panel-layout-focused', 'panel-layout-wide',
        'panel-density-comfortable', 'panel-density-compact',
        'panel-shape-soft', 'panel-shape-crisp',
        'panel-font-normal', 'panel-font-large', 'panel-font-dense',
        'panel-motion-standard', 'panel-motion-reduced',
    ];
    body.classList.remove(...managedClasses);
    body.classList.add(
        `panel-bg-${prefs.background_mode.replaceAll('_', '-')}`,
        `panel-layout-${prefs.layout_mode}`,
        `panel-density-${prefs.density}`,
        `panel-shape-${prefs.card_shape}`,
        `panel-font-${prefs.font_scale}`,
        `panel-motion-${prefs.motion}`,
    );
    return prefs;
}

function renderPanelPreferenceInputs(preferences = panelPreferencesState) {
    const prefs = applyPanelPreferences(preferences);
    const values = {
        'panel-accent-color': prefs.accent_color,
        'panel-background-mode': prefs.background_mode,
        'panel-background-color': prefs.background_color,
        'panel-background-image-url': prefs.background_image_url || '',
        'panel-layout-mode': prefs.layout_mode,
        'panel-density': prefs.density,
        'panel-card-shape': prefs.card_shape,
        'panel-font-scale': prefs.font_scale,
        'panel-motion': prefs.motion,
    };
    Object.entries(values).forEach(([id, value]) => {
        const element = document.getElementById(id);
        if (element) element.value = value;
    });
}

function getPanelPreferencesFromInputs() {
    return normalizePanelPreferences({
        accent_color: document.getElementById('panel-accent-color')?.value,
        background_mode: document.getElementById('panel-background-mode')?.value,
        background_color: document.getElementById('panel-background-color')?.value,
        background_image_url: document.getElementById('panel-background-image-url')?.value,
        layout_mode: document.getElementById('panel-layout-mode')?.value,
        density: document.getElementById('panel-density')?.value,
        card_shape: document.getElementById('panel-card-shape')?.value,
        font_scale: document.getElementById('panel-font-scale')?.value,
        motion: document.getElementById('panel-motion')?.value,
    });
}

function setPanelPreferencesStatus(message = '', isError = false) {
    const status = document.getElementById('panel-preferences-status');
    if (!status) return;
    status.textContent = message;
    status.classList.toggle('error', Boolean(isError));
}

function setPanelPreferenceControlsDisabled(disabled) {
    [
        'panel-accent-color',
        'panel-background-mode',
        'panel-background-color',
        'panel-background-image-url',
        'panel-layout-mode',
        'panel-density',
        'panel-card-shape',
        'panel-font-scale',
        'panel-motion',
        'save-panel-preferences',
        'reset-panel-preferences',
    ].forEach(id => {
        const element = document.getElementById(id);
        if (element) element.disabled = disabled;
    });
}

async function loadPanelPreferences() {
    try {
        const res = await fetch(`${API_BASE}/users/preferences`);
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'Preference request failed');
        }
        setPanelPreferenceControlsDisabled(!data.editable);
        renderPanelPreferenceInputs(data.preferences || DEFAULT_PANEL_PREFERENCES);
    } catch (err) {
        setPanelPreferencesStatus(err instanceof Error ? err.message : String(err), true);
        renderPanelPreferenceInputs(panelPreferencesState || DEFAULT_PANEL_PREFERENCES);
    }
}

async function savePanelPreferences(preferences = getPanelPreferencesFromInputs()) {
    setPanelPreferencesStatus('Saving...');
    renderPanelPreferenceInputs(preferences);
    const button = document.getElementById('save-panel-preferences');
    if (button) button.disabled = true;
    try {
        const res = await fetch(`${API_BASE}/users/preferences`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(preferences),
        });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'Preference save failed');
        }
        renderPanelPreferenceInputs(data.preferences || preferences);
        setPanelPreferencesStatus('Look saved.');
    } catch (err) {
        setPanelPreferencesStatus(err instanceof Error ? err.message : String(err), true);
    } finally {
        if (button) button.disabled = false;
    }
}

function resetPanelPreferences() {
    renderPanelPreferenceInputs(DEFAULT_PANEL_PREFERENCES);
    setPanelPreferencesStatus('Default look restored locally. Save to keep it.');
}

function userInitial(profile = {}) {
    return String(profile.display_name || profile.username || 'S').trim().slice(0, 1).toUpperCase() || 'S';
}

function renderCircularAvatar(profile = {}, className = 'user-card-avatar') {
    const avatarUrl = safePublicUrl(profile.avatar_url);
    const label = escapeHtml(profile.display_name || profile.username || 'User');
    if (avatarUrl) {
        return `<img class="${className}" src="${escapeHtml(avatarUrl)}" alt="${label} avatar" loading="lazy" />`;
    }
    return `<div class="${className} user-card-avatar-fallback">${escapeHtml(userInitial(profile))}</div>`;
}

function getCurrentAccountProfile(profile = userProfileState.profile || {}) {
    return {
        ...(profile || {}),
        username: profile?.username || currentPanelSession.username || remotePanelUsername || '',
    };
}

function updateTopbarAccount(profile = userProfileState.profile || {}) {
    const account = getCurrentAccountProfile(profile);
    const name = account.display_name || account.username || 'Swarm User';
    const nameNode = document.getElementById('topbar-user-name');
    const avatarNode = document.getElementById('topbar-profile-avatar');
    if (nameNode) nameNode.textContent = name;
    if (!avatarNode) return;

    const avatarUrl = safePublicUrl(account.avatar_url);
    avatarNode.innerHTML = avatarUrl
        ? `<img src="${escapeHtml(avatarUrl)}" alt="${escapeHtml(name)} avatar" />`
        : escapeHtml(userInitial(account));
}

function setAccountDropdownOpen(open) {
    const dropdown = document.getElementById('account-dropdown');
    const button = document.getElementById('topbar-profile-button');
    if (!dropdown || !button) return;
    dropdown.hidden = !open;
    button.setAttribute('aria-expanded', open ? 'true' : 'false');
}

function toggleAccountDropdown() {
    const dropdown = document.getElementById('account-dropdown');
    setAccountDropdownOpen(Boolean(dropdown?.hidden));
}

function setUserProfileStatus(message = '', isError = false) {
    const status = document.getElementById('user-profile-status');
    if (!status) return;
    status.textContent = message;
    status.classList.toggle('error', Boolean(isError));
}

function populateFavoriteBotSelect(options = [], selected = '') {
    const select = document.getElementById('profile-favorite-bot');
    if (!select) return;
    const items = Array.isArray(options) ? options : [];
    select.innerHTML = [
        '<option value="">No favorite selected</option>',
        ...items.map(bot => `<option value="${escapeHtml(bot.key)}">${escapeHtml(bot.display_name || bot.key)}</option>`),
    ].join('');
    select.value = selected || '';
}

function setUserProfileInputsDisabled(disabled) {
    [
        'profile-display-name',
        'profile-avatar-url',
        'profile-server-name',
        'profile-server-icon-url',
        'profile-server-invite-url',
        'profile-favorite-bot',
        'profile-theme-accent',
        'profile-public-profile',
        'profile-bio',
        'save-user-profile',
        'profile-email',
        'profile-email-code',
        'save-panel-email',
        'verify-panel-email-code',
        'resend-panel-email-verification',
    ].forEach(id => {
        const element = document.getElementById(id);
        if (element) element.disabled = disabled;
    });
}

function renderUserProfile(data = userProfileState) {
    const profile = data.profile || {};
    const editable = Boolean(data.editable);
    const preview = document.getElementById('user-avatar-preview');
    const previewName = document.getElementById('user-profile-preview-name');
    const previewServer = document.getElementById('user-profile-preview-server');
    const previewChips = document.getElementById('user-profile-preview-chips');
    const locked = document.getElementById('user-profile-locked');
    const resendEmailButton = document.getElementById('resend-panel-email-verification');

    populateFavoriteBotSelect(data.favoriteBotOptions, profile.favorite_bot || '');

    const fieldValues = {
        'profile-display-name': profile.display_name || '',
        'profile-email': profile.email || '',
        'profile-email-code': '',
        'profile-avatar-url': profile.avatar_url || '',
        'profile-server-name': profile.server_name || '',
        'profile-server-icon-url': profile.server_icon_url || '',
        'profile-server-invite-url': profile.server_invite_url || '',
        'profile-theme-accent': profile.theme_accent || '#89b4fa',
        'profile-bio': profile.bio || '',
    };
    Object.entries(fieldValues).forEach(([id, value]) => {
        const element = document.getElementById(id);
        if (element) element.value = value;
    });
    const publicInput = document.getElementById('profile-public-profile');
    if (publicInput) publicInput.checked = profile.public_profile !== false;

    const avatarUrl = safePublicUrl(profile.avatar_url);
    if (preview) {
        preview.style.setProperty('--user-accent', profile.theme_accent || '#89b4fa');
        preview.innerHTML = avatarUrl
            ? `<img src="${escapeHtml(avatarUrl)}" alt="${escapeHtml(profile.display_name || profile.username || 'User')} avatar" />`
            : `<span>${escapeHtml(userInitial(profile))}</span>`;
    }
    if (previewName) previewName.textContent = profile.display_name || profile.username || 'Swarm User';
    if (previewServer) previewServer.textContent = profile.server_name || (profile.guild_id ? `Guild ${profile.guild_id}` : 'No server card yet.');
    if (previewChips) {
        const chips = [];
        if (profile.guild_id) chips.push(`Guild: ${profile.guild_id}`);
        if (profile.favorite_bot) chips.push(`Favorite: ${profile.favorite_bot}`);
        chips.push(profile.public_profile === false ? 'Private' : 'Public');
        if (profile.server_invite_url) chips.push('Invite linked');
        if (profile.email) chips.push(profile.email_verified ? 'Email verified' : 'Email pending');
        previewChips.innerHTML = chips.map(chip => `<span>${escapeHtml(chip)}</span>`).join('');
    }
    if (resendEmailButton) {
        resendEmailButton.hidden = !editable || !profile.email || Boolean(profile.email_verified);
    }
    if (locked) {
        locked.hidden = editable;
        locked.textContent = editable ? '' : 'Built-in admin sessions can browse users, but registered accounts edit profiles through their linked guild.';
    }
    setUserProfileInputsDisabled(!editable);
    updateTopbarAccount(profile);
}

async function loadUserProfile() {
    if (!panelAppStarted && !panelSessionChecked) return;
    try {
        const res = await fetch(`${API_BASE}/users/me`);
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'Profile request failed');
        }
        userProfileState = {
            profile: data.profile || null,
            editable: Boolean(data.editable),
            favoriteBotOptions: Array.isArray(data.favorite_bot_options) ? data.favorite_bot_options : [],
        };
        if (data.profile?.panel_preferences) {
            renderPanelPreferenceInputs(data.profile.panel_preferences);
        }
        renderUserProfile(userProfileState);
    } catch (err) {
        setUserProfileStatus(String(err), true);
    }
}

async function savePanelEmailAndSendCode() {
    if (!userProfileState.editable) return;
    setUserProfileStatus('Saving email and sending code...');
    try {
        const res = await fetch(`${API_BASE}/session/email`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email: document.getElementById('profile-email')?.value?.trim() || null }),
        });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || 'Email update failed');
        userProfileState.profile = data.profile || userProfileState.profile;
        renderUserProfile(userProfileState);
        setUserProfileStatus(
            data.profile?.email
                ? data.email_verification_sent
                    ? 'Verification code sent. Enter it in Email Code.'
                    : 'Email saved, but the verification code could not be sent.'
                : 'Email removed.',
            Boolean(data.profile?.email) && !data.email_verification_sent,
        );
    } catch (err) {
        setUserProfileStatus(err instanceof Error ? err.message : String(err), true);
    }
}

async function verifyPanelEmailCode() {
    if (!userProfileState.editable) return;
    setUserProfileStatus('Verifying email code...');
    try {
        const res = await fetch(`${API_BASE}/session/email/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code: document.getElementById('profile-email-code')?.value?.trim() || '' }),
        });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || 'Email verification failed');
        userProfileState.profile = data.profile || userProfileState.profile;
        renderUserProfile(userProfileState);
        setUserProfileStatus('Email verified.');
    } catch (err) {
        setUserProfileStatus(err instanceof Error ? err.message : String(err), true);
    }
}

async function resendPanelEmailVerification() {
    if (!userProfileState.editable) return;
    const button = document.getElementById('resend-panel-email-verification');
    if (button) button.disabled = true;
    setUserProfileStatus('Sending verification email...');
    try {
        const res = await fetch(`${API_BASE}/session/resend-verification`, { method: 'POST' });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || 'Verification resend failed');
        setUserProfileStatus(
            data.email_verification_sent
                ? 'Verification code sent. Enter it in Email Code.'
                : data.already_verified
                    ? 'Email is already verified.'
                    : 'Verification code could not be sent.',
            !data.email_verification_sent && !data.already_verified,
        );
        await loadUserProfile();
    } catch (err) {
        setUserProfileStatus(err instanceof Error ? err.message : String(err), true);
    } finally {
        if (button) button.disabled = false;
    }
}

async function saveUserProfile() {
    if (!userProfileState.editable) return;
    const button = document.getElementById('save-user-profile');
    if (button) button.disabled = true;
    setUserProfileStatus('Saving...');
    const payload = {
        display_name: document.getElementById('profile-display-name')?.value || null,
        avatar_url: document.getElementById('profile-avatar-url')?.value || null,
        server_name: document.getElementById('profile-server-name')?.value || null,
        server_icon_url: document.getElementById('profile-server-icon-url')?.value || null,
        server_invite_url: document.getElementById('profile-server-invite-url')?.value || null,
        favorite_bot: document.getElementById('profile-favorite-bot')?.value || null,
        theme_accent: document.getElementById('profile-theme-accent')?.value || '#89b4fa',
        public_profile: Boolean(document.getElementById('profile-public-profile')?.checked),
        bio: document.getElementById('profile-bio')?.value || null,
    };

    try {
        const res = await fetch(`${API_BASE}/users/me`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'Profile save failed');
        }
        userProfileState.profile = data.profile || userProfileState.profile;
        renderUserProfile(userProfileState);
        setUserProfileStatus('Profile saved.');
        loadUserDirectory();
    } catch (err) {
        setUserProfileStatus(err instanceof Error ? err.message : String(err), true);
    } finally {
        if (button) button.disabled = false;
    }
}

function renderUserDirectory(users = userDirectoryState) {
    const grid = document.getElementById('user-directory-grid');
    if (!grid) return;
    if (!users.length) {
        grid.innerHTML = '<div class="control-context-empty">No public users found yet.</div>';
        return;
    }

    grid.innerHTML = users.map(profile => {
        const activity = profile.activity || {};
        const topTracks = Array.isArray(activity.top_tracks) ? activity.top_tracks : [];
        const topBots = Array.isArray(activity.top_bots) ? activity.top_bots : [];
        const activeSessions = Array.isArray(activity.active_sessions) ? activity.active_sessions : [];
        const serverIcon = safePublicUrl(profile.server_icon_url);
        const inviteUrl = safePublicUrl(profile.server_invite_url);
        const accent = profile.theme_accent || '#89b4fa';
        const serverName = profile.server_name || (profile.guild_id ? `Guild ${profile.guild_id}` : 'Linked server');
        return `
            <article class="user-card" style="--user-accent: ${escapeHtml(accent)};">
                <div class="user-card-head">
                    ${renderCircularAvatar(profile)}
                    <div class="user-card-title">
                        <h3>${escapeHtml(profile.display_name || profile.username)}</h3>
                        <span>@${escapeHtml(profile.username)}</span>
                    </div>
                    ${inviteUrl ? `<a class="user-invite-link" href="${escapeHtml(inviteUrl)}" target="_blank" rel="noopener noreferrer">Invite</a>` : ''}
                </div>
                <div class="user-server-row">
                    ${serverIcon ? `<img src="${escapeHtml(serverIcon)}" alt="${escapeHtml(serverName)} icon" loading="lazy" />` : `<span>${escapeHtml(serverName.slice(0, 1).toUpperCase())}</span>`}
                    <div>
                        <strong>${escapeHtml(serverName)}</strong>
                        <small>${escapeHtml(profile.bio || 'No public bio yet.')}</small>
                    </div>
                </div>
                <div class="user-chip-row">
                    ${profile.favorite_bot ? `<span>Favorite ${escapeHtml(profile.favorite_bot)}</span>` : ''}
                    <span>${Number(activity.total_plays || 0)} plays tracked</span>
                    ${activeSessions.length ? '<span>Listening now</span>' : ''}
                </div>
                <div class="user-activity-block">
                    <div class="user-activity-title">Most Played</div>
                    ${topTracks.length ? topTracks.map(track => `<div class="user-activity-row"><span>${escapeHtml(track.title)}</span><strong>${Number(track.plays || 0)}</strong></div>`).join('') : '<div class="user-empty-line">No track history yet.</div>'}
                </div>
                <div class="user-activity-block">
                    <div class="user-activity-title">Top Bots</div>
                    ${topBots.length ? topBots.map(bot => `<div class="user-activity-row"><span>${escapeHtml(bot.bot_display || bot.bot_key)}</span><strong>${Number(bot.plays || 0)}</strong></div>`).join('') : '<div class="user-empty-line">No bot plays yet.</div>'}
                </div>
                ${activeSessions.length ? `<div class="user-activity-block"><div class="user-activity-title">Now</div>${activeSessions.map(session => `<div class="user-activity-row"><span>${escapeHtml(session.bot_display || session.bot_key)}: ${escapeHtml(session.title || 'Unknown Track')}</span><strong>${session.is_paused ? 'Paused' : 'Live'}</strong></div>`).join('')}</div>` : ''}
            </article>
        `;
    }).join('');
}

async function loadUserDirectory(query = null) {
    const requestId = ++userDirectoryRequestId;
    const searchValue = query ?? document.getElementById('user-search-input')?.value ?? '';
    const grid = document.getElementById('user-directory-grid');
    if (grid) grid.innerHTML = '<div class="control-context-empty">Loading public users...</div>';
    try {
        const params = new URLSearchParams();
        if (String(searchValue || '').trim()) params.set('q', String(searchValue).trim());
        const res = await fetch(`${API_BASE}/users/search${params.toString() ? `?${params.toString()}` : ''}`);
        if (handle401(res)) return;
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || 'User search failed');
        }
        if (requestId !== userDirectoryRequestId) return;
        userDirectoryState = Array.isArray(data.users) ? data.users : [];
        renderUserDirectory(userDirectoryState);
    } catch (err) {
        if (grid) grid.innerHTML = `<div class="control-context-empty">User directory failed: ${escapeHtml(err instanceof Error ? err.message : String(err))}</div>`;
    }
}

// ================================
// 📡 FETCH DASHBOARD DATA
// ================================
async function fetchDashboard() {
    try {
        const res = await fetch(`${API_BASE}/dashboard`);
        lastDashboardFetchAt = Date.now();
        if (handle401(res)) return;
        const data = await res.json();

        const bots = data.bots || [];
        dashboardBotsState = bots;
        updateInviteOnlyMode();

        renderOverview(bots, data.generated_at);
        renderBots(bots);

        const sessionNow = Date.now();
        const sessionMap = new Map();
        const rememberSession = (session, fallbackBot = null) => {
            if (!session) return;
            const botKey = String(session.bot_key || fallbackBot?.key || '');
            const guildKey = String(session.guild_id ?? '0');
            if (!botKey) return;
            const key = `${botKey}:${guildKey}`;
            sessionMap.set(key, normalizeLiveSession({
                ...sessionMap.get(key),
                ...session,
                bot_key: botKey,
                bot_display: session.bot_display || fallbackBot?.display_name || session.bot_name || botKey,
            }, sessionNow));
        };

        bots.forEach(bot => {
            if (Array.isArray(bot.sessions)) {
                bot.sessions.forEach(session => rememberSession(session, bot));
            }
        });
        if (Array.isArray(data.sessions)) {
            data.sessions.forEach(session => rememberSession(session, getDashboardBot(session.bot_key) || null));
        }
        const allSessions = Array.from(sessionMap.values());

        liveSessionState = allSessions;
        pruneLiveSessionPositionCache(new Set(allSessions.map(getSessionRuntimeKey)), sessionNow);
        renderSessions(allSessions.filter(session => isRuntimeSession(session)));
        renderNowPlaying(allSessions);
        syncControlSelectionsFromDashboard();
        renderControlContext();
        renderAriaCommandGuide();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        if (document.getElementById('control-guild-select')?.value && !controlInventoryLoading) {
            fetchSelectedControlState({ silent: true });
            fetchControlMatrix({ silent: true });
        }

        const meta = document.getElementById('dashboard-meta');
        if (meta && data.generated_at) {
            meta.textContent = `Last updated: ${formatCentralTimestamp(data.generated_at)}`;
        }

        updateLivePositionCounters();

    } catch (err) {
        console.error("❌ Dashboard fetch failed:", err);
    }
}

async function fetchDiagnostics(force = false) {
    try {
        const res = await fetch(`${API_BASE}/system-diagnostics${force ? '?force=true' : ''}`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.detail || 'Diagnostics request failed');
        }

        systemDiagnosticsState = data;
        renderDiagnosticsSummary(data);
        renderSharedRuntimeCard(data);
        renderAriaDiagnosticCard(data);
        renderWorkerDiagnosticGrid(data);
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        fetchMetrics();
    } catch (err) {
        systemDiagnosticsState = null;
        renderControlContext();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        console.error('❌ Diagnostics fetch failed:', err);
    }
}


async function fetchMetrics() {
    try {
        const res = await fetch(`${API_BASE}/metrics`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.detail || data.db_error || 'Metrics request failed');
        }
        metricsSnapshotState = data;
        renderMetricsDashboard(data);
    } catch (err) {
        metricsSnapshotState = null;
        renderMetricsDashboard({ error: String(err), totals: {}, bots: [] });
        console.error('❌ Metrics fetch failed:', err);
    }
}

function renderMetricsDashboard(data = metricsSnapshotState) {
    const container = document.getElementById('metrics-dashboard');
    if (!container) return;

    if (!data) {
        container.innerHTML = '<div class="control-context-empty">Metrics have not loaded yet.</div>';
        return;
    }

    if (data.error || data.db_error) {
        container.innerHTML = `<div class="diag-item diag-item-error"><div class="diag-item-title">Metrics unavailable</div><div class="diagnostic-item-body">${escapeHtml(data.error || data.db_error)}</div></div>`;
        return;
    }

    const totals = data.totals || {};
    const botCards = (data.bots || []).map(bot => {
        const metrics = Array.isArray(bot.metrics) ? bot.metrics : [];
        const staleCount = metrics.filter(m => m.stale).length;
        const recovering = metrics.filter(m => m.recovery_pending).length;
        const playing = metrics.filter(m => m.player_playing).length;
        const paused = metrics.filter(m => m.player_paused).length;
        const queued = metrics.reduce((acc, m) => acc + Number(m.queue_count || 0), 0);
        const voiceConnected = metrics.filter(m => m.voice_connected).length;
        const status = bot.error ? 'error' : (staleCount ? 'warning' : 'ok');
        const detail = bot.error
            ? bot.error
            : metrics.length
                ? `guilds=${metrics.length} • voice=${voiceConnected} • playing=${playing} • paused=${paused} • queued=${queued} • recovering=${recovering} • stale=${staleCount}`
                : 'No metrics rows yet; bot may still be warming up or has not joined a guild since metrics were added.';
        const latestErrors = metrics
            .filter(m => m.last_error)
            .slice(0, 3)
            .map(m => `<div class="diag-list-item">guild ${escapeHtml(m.guild_id)}: ${escapeHtml(m.last_error)}</div>`)
            .join('');
        return `<div class="diagnostic-card diagnostic-${escapeHtml(status)}">
            <div class="diagnostic-card-title">${escapeHtml(bot.display_name || bot.key)}</div>
            <div class="diagnostic-card-value">${escapeHtml(String(bot.status || status).toUpperCase())}</div>
            <div class="diagnostic-card-detail">${escapeHtml(detail)}</div>
            ${latestErrors ? `<div class="diag-list">${latestErrors}</div>` : ''}
        </div>`;
    }).join('');

    container.innerHTML = `<div class="diagnostic-card diagnostic-ok">
        <div class="diagnostic-card-title">Swarm Totals</div>
        <div class="diagnostic-card-value">${Number(totals.bots || 0)} bots / ${Number(totals.guilds || 0)} guild rows</div>
        <div class="diagnostic-card-detail">voice=${Number(totals.voice_connected || 0)} • playing=${Number(totals.playing || 0)} • paused=${Number(totals.paused || 0)} • queued=${Number(totals.queued_tracks || 0)} • backup=${Number(totals.backup_tracks || 0)} • stale=${Number(totals.stale_metrics || 0)}</div>
    </div>${botCards || '<div class="control-context-empty">No bot metrics available yet.</div>'}`;
}

function summarizeDbDetails(details) {
    const rowCounts = details?.row_counts || {};
    const extras = details?.extras || {};
    const parts = [];
    const queueTables = Object.entries(rowCounts).filter(([name]) => /queue(?!_backup)/.test(name));
    const backupTables = Object.entries(rowCounts).filter(([name]) => /queue_backup/.test(name));
    const directTables = Object.entries(rowCounts).filter(([name]) => /direct_orders/.test(name));
    const overrideTables = Object.entries(rowCounts).filter(([name]) => /overrides/.test(name));
    const playbackTables = Object.entries(rowCounts).filter(([name]) => /playback_state/.test(name));
    const sum = pairs => pairs.reduce((acc, [, value]) => acc + (Number.isFinite(value) ? Number(value) : 0), 0);
    if (playbackTables.length) parts.push(`Playback rows ${sum(playbackTables)}`);
    if (queueTables.length) parts.push(`Queue rows ${sum(queueTables)}`);
    if (backupTables.length) parts.push(`Backup rows ${sum(backupTables)}`);
    if (directTables.length) parts.push(`Direct orders ${sum(directTables)}`);
    if (overrideTables.length) parts.push(`Overrides ${sum(overrideTables)}`);
    if (extras.active_playback != null) parts.push(`Active ${extras.active_playback}`);
    if (extras.paused_playback != null) parts.push(`Paused ${extras.paused_playback}`);
    return parts.join(' • ');
}

function renderDiagnosticsSummary(data) {

    const container = document.getElementById('diagnostics-summary');
    if (!container) return;

    const bots = Array.isArray(data?.bots) ? data.bots : [];
    const botsDbOnline = bots.filter(bot => bot.db?.reachable).length;
    const botsDiscordReady = bots.filter(bot => bot.discord?.reachable).length;
    const ariaGemini = data?.aria?.gemini;
    const ariaDb = data?.aria?.db;
    const sharedEnv = data?.shared_env;

    const totalPendingDirect = bots.reduce((acc, bot) => acc + Number(bot?.db_details?.row_counts?.[`${bot.key}_swarm_direct_orders`] || 0), 0);
    const totalPendingOverrides = bots.reduce((acc, bot) => acc + Number(bot?.db_details?.row_counts?.[`${bot.key}_swarm_overrides`] || 0), 0);
    const activePlayback = bots.reduce((acc, bot) => acc + Number(bot?.db_details?.extras?.active_playback || 0), 0);
    const cards = [
        {
            label: 'Shared Music Env',
            value: describeDiagnosticState(sharedEnv?.status).label,
            detail: sharedEnv?.message || 'No diagnostics data yet.',
            tone: describeDiagnosticState(sharedEnv?.status).tone,
        },
        {
            label: 'Worker DB Links',
            value: `${botsDbOnline}/${bots.length}`,
            detail: `Active playback rows ${activePlayback} • Pending direct orders ${totalPendingDirect}`,
            tone: botsDbOnline === bots.length && bots.length ? 'online' : botsDbOnline ? 'stale' : 'offline',
        },
        {
            label: 'Discord Inventory',
            value: `${botsDiscordReady}/${bots.length}`,
            detail: `Panel-side Discord token readiness. Pending overrides ${totalPendingOverrides}.`,
            tone: botsDiscordReady === bots.length && bots.length ? 'online' : botsDiscordReady ? 'stale' : 'offline',
        },
        {
            label: 'Aria Intelligence',
            value: describeDiagnosticState(ariaGemini?.status).label,
            detail: ariaDb?.reachable ? (`${ariaGemini?.message || 'Aria DB online and Gemini diagnostics complete.'}`) : (ariaDb?.message || 'Aria DB not reachable.'),
            tone: describeDiagnosticState(ariaGemini?.status).tone,
        },
        {
            label: 'Panel DB Writes',
            value: data?.panel?.db?.write_ok ? 'Healthy' : 'Needs Attention',
            detail: data?.panel?.db?.write_message || 'No panel DB write probe available.',
            tone: data?.panel?.db?.write_ok ? 'online' : 'stale',
        },
    ];

    container.innerHTML = cards.map(card => `
        <article class="overview-card overview-card-${card.tone}">
            <div class="overview-card-label">${escapeHtml(card.label)}</div>
            <div class="overview-card-value">${escapeHtml(card.value)}</div>
            <div class="overview-card-detail">${escapeHtml(card.detail)}</div>
        </article>
    `).join('');
}

function renderSharedRuntimeCard(data) {
    const container = document.getElementById('shared-runtime-card');
    if (!container) return;

    const sharedEnv = data?.shared_env || {};
    const panelDb = data?.panel?.db || {};

    container.innerHTML = `
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Music/.env discovery</span>
                ${diagnosticBadge(sharedEnv.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(sharedEnv.message || 'No shared env status available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(sharedEnv.path || 'Unknown path')}</div>
            ${sharedEnv.last_modified ? `<div class="diagnostic-item-meta">Updated ${escapeHtml(formatCentralTimestamp(sharedEnv.last_modified, true))}</div>` : ''}
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Panel database</span>
                ${diagnosticBadge(panelDb.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(panelDb.message || 'No panel DB probe available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(panelDb.database || 'Unknown schema')} on ${escapeHtml(panelDb.host || 'unknown host')}</div>
            <div class="diagnostic-item-meta">Write probe: ${panelDb.write_ok ? 'ok' : escapeHtml(panelDb.write_message || 'not tested')}</div>
            <div class="diagnostic-item-meta">Schema detail: ${escapeHtml(summarizeDbDetails(data?.panel?.db_details) || 'No panel table summary available yet.')}</div>
        </div>
    `;
}

function renderAriaDiagnosticCard(data) {
    const container = document.getElementById('aria-diagnostic-card');
    if (!container) return;

    const aria = data?.aria || {};
    const env = aria.env || {};
    const db = aria.db || {};
    const swarmDb = aria.swarm_db || {};
    const discord = aria.discord || {};
    const gemini = aria.gemini || {};
    const dbDetails = aria.db_details || {};
    const operatorActions = Array.isArray(aria.operator_actions) ? aria.operator_actions : [];
    const medic = aria.medic_summary || {};
    const medicEvents = Array.isArray(medic.recent_swarm_events) ? medic.recent_swarm_events : [];
    const repairCount = Number(medic.pending_repairs || 0);
    const infraCount = Number(medic.pending_infra || 0);
    const criticalCount = Number(medic.critical_health || 0);
    const recoverableCount = Number(medic.recoverable_health || 0);
    const operatorDecisions = Array.isArray(medic.recent_operator_decisions) ? medic.recent_operator_decisions : [];

    container.innerHTML = `
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria primary database</span>
                ${diagnosticBadge(db.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(db.message || 'No DB diagnostics available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(db.database || env.db_name || 'discord_aria')} on ${escapeHtml(db.host || env.db_host || 'unknown host')}</div>
            <div class="diagnostic-item-meta">Write probe: ${db.write_ok ? 'ok' : escapeHtml(db.write_message || 'not tested')} | DB password: ${escapeHtml(maskSecret(env.masked_db_password))}</div>
            <div class="diagnostic-item-meta">Schema detail: ${escapeHtml(summarizeDbDetails(dbDetails) || 'No Aria DB table summary available yet.')}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria swarm bridge database</span>
                ${diagnosticBadge(swarmDb.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(swarmDb.message || 'No swarm DB diagnostics available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(swarmDb.database || env.swarm_db_name || 'unknown schema')} on ${escapeHtml(swarmDb.host || env.swarm_db_host || 'unknown host')}</div>
            <div class="diagnostic-item-meta">Write probe: ${swarmDb.write_ok ? 'ok' : escapeHtml(swarmDb.write_message || 'not tested')} | Swarm DB password: ${escapeHtml(maskSecret(env.masked_swarm_db_password))}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria Gemini key</span>
                ${diagnosticBadge(gemini.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(gemini.message || 'No Gemini diagnostics available.')}</div>
            <div class="diagnostic-item-meta">Model: ${escapeHtml(gemini.model || 'unknown')} | SDK installed: ${gemini.sdk_installed ? 'yes' : 'no'} | Key: ${escapeHtml(maskSecret(env.masked_gemini_key))}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria Discord inventory access</span>
                ${diagnosticBadge(discord.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(discord.message || 'No Discord diagnostics available.')}</div>
            <div class="diagnostic-item-meta">Shared token: ${env.shared_token_present ? 'present' : 'missing'} | Panel token: ${env.panel_token_present ? 'present' : 'missing'}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria operator scope</span>
                <span class="diag-inline-count">${operatorActions.length} actions</span>
            </div>
            <div class="diagnostic-bullet-list">
                ${operatorActions.map(item => `<span class="diagnostic-bullet">${escapeHtml(item)}</span>`).join('')}
            </div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria medic state</span>
                ${diagnosticBadge(criticalCount > 0 ? 'critical' : (repairCount > 0 || infraCount > 0 || recoverableCount > 0 ? 'warning' : 'ok'))}
            </div>
            <div class="diagnostic-item-body">Pending repairs: ${repairCount} | Pending infra: ${infraCount} | Critical health: ${criticalCount} | Recoverable/degraded: ${recoverableCount}</div>
            <div class="diagnostic-item-meta">This reflects Aria's new medic/event system, not just heartbeat status.</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Recent operator decisions</span>
                <span class="diag-inline-count">${operatorDecisions.length}</span>
            </div>
            <div class="diagnostic-bullet-list">
                ${operatorDecisions.length ? operatorDecisions.map(item => `<span class="diagnostic-bullet">${escapeHtml(String(item.issue_type || 'issue'))} • ${escapeHtml(String(item.bot_name || 'swarm'))} • ${escapeHtml(String(item.urgency_label || 'normal'))}</span>`).join('') : '<span class="diagnostic-bullet">No recent operator decisions recorded yet.</span>'}
            </div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Recent medic events</span>
                <span class="diag-inline-count">${medicEvents.length}</span>
            </div>
            <div class="diagnostic-bullet-list">
                ${medicEvents.length ? medicEvents.slice(0, 5).map(item => `<span class="diagnostic-bullet">${escapeHtml(String(item.event_type || 'event'))} • ${escapeHtml(String(item.bot_name || 'swarm'))} • ${escapeHtml(String(item.severity || 'info'))}</span>`).join('') : '<span class="diagnostic-bullet">No recent medic events recorded yet.</span>'}
            </div>
        </div>
    `;
}

function renderWorkerDiagnosticGrid(data) {
    const container = document.getElementById('worker-diagnostic-grid');
    if (!container) return;

    const bots = Array.isArray(data?.bots) ? data.bots : [];
    if (!bots.length) {
        container.innerHTML = '<div class="control-context-empty">No worker diagnostics available yet.</div>';
        return;
    }

    container.innerHTML = bots.map(bot => {
        const detail = summarizeDbDetails(bot.db_details);
        const missingTables = Array.isArray(bot.db_details?.missing_tables) ? bot.db_details.missing_tables : [];
        return `
        <article class="worker-diagnostic-card">
            <div class="worker-diagnostic-head">
                <div>
                    <div class="worker-diagnostic-name">${escapeHtml(bot.display_name || bot.key)}</div>
                    <div class="worker-diagnostic-meta">Schema ${escapeHtml(bot.env?.db_name || 'unknown')} on ${escapeHtml(bot.env?.db_host || 'unknown host')}</div>
                </div>
                ${diagnosticBadge(bot.db?.status)}
            </div>
            <div class="worker-diagnostic-row">
                <span>Database</span>
                <span>${escapeHtml(bot.db?.message || 'No DB result')}</span>
            </div>
            <div class="worker-diagnostic-row">
                <span>Discord inventory</span>
                <span>${escapeHtml(bot.discord?.message || 'No Discord result')}</span>
            </div>
            <div class="worker-diagnostic-row">
                <span>Schema detail</span>
                <span>${escapeHtml(detail || 'No queue/playback table data available yet.')}</span>
            </div>
            <div class="worker-diagnostic-row">
                <span>Missing tables</span>
                <span>${escapeHtml(missingTables.length ? missingTables.join(', ') : 'none detected')}</span>
            </div>
            <div class="worker-diagnostic-chip-row">
                <span class="worker-chip ${bot.env?.shared_db_password_present ? 'worker-chip-ok' : 'worker-chip-bad'}">DB secret ${bot.env?.shared_db_password_present ? 'present' : 'missing'}</span>
                <span class="worker-chip ${bot.env?.shared_lavalink_password_present ? 'worker-chip-ok' : 'worker-chip-bad'}">Lavalink ${bot.env?.shared_lavalink_password_present ? 'present' : 'missing'}</span>
                <span class="worker-chip ${bot.env?.panel_token_present ? 'worker-chip-ok' : 'worker-chip-bad'}">Panel token ${bot.env?.panel_token_present ? 'present' : 'missing'}</span>
            </div>
        </article>`;
    }).join('');
}

// ================================
// 🎵 RENDER NOW-PLAYING CARDS
// ================================

function formatProgressPercent(positionSeconds, durationSeconds) {
    const duration = Number(durationSeconds || 0);
    const position = Number(positionSeconds || 0);
    if (!duration || duration <= 0) return null;
    return Math.max(0, Math.min(100, Math.round((position / duration) * 100)));
}

function summarizeRecoveryState(session) {
    const backupCount = Number(session?.backup_queue_count || 0);
    const pendingDirect = Number(session?.pending_direct_orders || 0);
    const pendingOverrides = Number(session?.pending_overrides || 0);
    const homeChannel = session?.home_channel_id || session?.saved_home_channel_id || null;
    const recovering = Boolean(session?.recovering || session?.recovery_pending || session?.recovery_state === 'pending');
    const parts = [];
    if (backupCount > 0) parts.push(`backup:${backupCount}`);
    if (homeChannel) parts.push('home saved');
    if (pendingDirect > 0) parts.push(`direct:${pendingDirect}`);
    if (pendingOverrides > 0) parts.push(`override:${pendingOverrides}`);
    if (recovering) parts.push('recovering');
    return parts.length ? parts.join(' · ') : 'No recovery pressure';
}

function summarizeSessionSignals(session) {
    const signals = [];
    const queue = Number(session?.queue_count || 0);
    const backup = Number(session?.backup_queue_count || 0);
    const state = describeSessionState(session).key;
    if (queue > 0) signals.push(`${queue} queued`);
    if (backup > 0) signals.push(`${backup} backup`);
    if (session?.loop_mode && session.loop_mode !== 'off') signals.push(`loop:${session.loop_mode}`);
    if (session?.filter_mode && session.filter_mode !== 'none') signals.push(`fx:${session.filter_mode}`);
    if (state === 'recovering' || state === 'queued') signals.push('watching auto-restore');
    return signals.length ? signals.join(' · ') : 'steady';
}

function renderNowPlaying(sessions) {
    const container = document.getElementById("now-playing-cards");
    if (!container) return;

    const playing = sessions.filter(s => s.is_playing);

    if (playing.length === 0) {
        container.innerHTML = `
            <div class="np-empty">
                <span class="np-empty-icon">🎧</span>
                <p>No active streams right now</p>
            </div>`;
        return;
    }

    container.innerHTML = playing.map(s => {
        const positionSeconds = getDisplayPositionSeconds(s);
        const pos = formatDuration(positionSeconds);
        const durationLabel = formatDuration(s.duration_seconds || s.length_seconds || s.track_length_seconds || 0);
        const progressPercent = formatProgressPercent(positionSeconds, s.duration_seconds || s.length_seconds || s.track_length_seconds);
        const positionKey = escapeHtml(getSessionRuntimeKey(s));
        const recoverySummary = summarizeRecoveryState(s);
        const thumb = s.thumbnail || null;
        const sourceBadge = s.media_source_label
            ? `<span class="np-stat np-source np-source-${s.media_source || 'unknown'}">${s.media_source_label}</span>`
            : '';
        const botColors = {
            gws: '#cba6f7', harmonic: '#89b4fa', maestro: '#a6e3a1',
            melodic: '#fab387', nexus: '#f38ba8', rhythm: '#94e2d5',
            symphony: '#f9e2af', tunestream: '#b4befe'
        };
        const accentColor = botColors[s.bot_key] || '#cba6f7';

        const thumbBg = `linear-gradient(135deg, ${accentColor}33, #101727)`;

        return `
        <div class="np-card" style="--accent: ${accentColor};" data-bot="${s.bot_key}" data-guild="${s.guild_id}">
            <div class="np-thumb-wrap">
                ${thumb
                    ? `<img class="np-thumb-img" src="${thumb}" alt="Track thumbnail" loading="lazy" />`
                    : `<div class="np-thumb-placeholder" style="background: ${thumbBg};">
                           <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="${accentColor}" stroke-width="1.5">
                               <path d="M9 18V5l12-2v13"/>
                               <circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>
                           </svg>
                       </div>`
                }
                <div class="np-live-pill">
                    <span class="np-live-dot"></span>LIVE
                </div>
            </div>
            <div class="np-body">
                <div class="np-bot-tag" style="color: var(--accent);">${s.bot_display}</div>
                <div class="np-title" title="${s.title || ''}">${s.title || 'Unknown Track'}</div>
                <div class="np-location">
                    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
                    ${s.guild_name || s.guild_id}
                    <span class="np-dot">·</span>
                    ${s.channel_name || 'Unknown Channel'}
                </div>
                <div class="np-stats-row">
                    <span class="np-stat">
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                        <span data-position-key="${positionKey}">${pos}</span>
                    </span>
                    ${sourceBadge}
                    ${s.filter_mode && s.filter_mode !== 'none' ? `<span class="np-stat np-filter">${s.filter_mode}</span>` : ''}
                    ${s.loop_mode && s.loop_mode !== 'off' ? `<span class="np-stat np-loop">loop:${s.loop_mode}</span>` : ''}
                    ${s.queue_count > 0 ? `<span class="np-stat">+${s.queue_count} queued</span>` : ''}
                    ${Number(s?.backup_queue_count || 0) > 0 ? `<span class="np-stat np-loop">backup:${Number(s.backup_queue_count)}</span>` : ''}
                </div>
                <div class="np-stats-row" style="margin-top:6px;">
                    <span class="np-stat" data-position-key="${positionKey}" data-position-full="1">${pos}${durationLabel !== '0:00' ? ` / ${durationLabel}` : ''}</span>
                    <span class="np-stat">${escapeHtml(recoverySummary)}</span>
                </div>
                ${progressPercent !== null ? `<div class="np-progress"><span data-progress-key="${positionKey}" style="width:${progressPercent}%"></span></div>` : ''}
                <div class="np-controls">
                    <button class="np-btn" data-action="PAUSE" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Pause">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>
                    </button>
                    <button class="np-btn" data-action="RESUME" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Resume">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><polygon points="5,3 19,12 5,21"/></svg>
                    </button>
                    <button class="np-btn" data-action="SKIP" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Skip">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><polygon points="5,4 15,12 5,20"/><rect x="16" y="4" width="3" height="16"/></svg>
                    </button>
                    <button class="np-btn np-btn-stop" data-action="STOP" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Stop">
                        <svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>
                    </button>
                    <button class="np-btn" data-action="SHUFFLE" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Shuffle queue">
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>
                    </button>
                    <button class="np-btn np-btn-clear" data-action="CLEAR" data-bot="${s.bot_key}" data-guild="${s.guild_id}" title="Clear queue and current track">
                        <span class="np-btn-label">Clear</span>
                    </button>
                </div>
            </div>
        </div>`;
    }).join('');
}

// ================================
// 🤖 RENDER BOT CARDS
// ================================
function renderBots(bots) {
    const container = document.getElementById("bot-cards");
    const ariaContainer = document.getElementById("aria-card-container");

    if (!container) return;
    container.innerHTML = "";
    if (ariaContainer) ariaContainer.innerHTML = "";

    bots.forEach(bot => {
        const statusMeta = describeBotStatus(bot.status);
        const heartbeatLabel = formatHeartbeatAge(bot.heartbeat_age_seconds);
        const sessions = Array.isArray(bot.sessions) ? bot.sessions : [];
        const activeSession = sessions.find(session => describeSessionState(session).key === 'playing')
            || sessions.find(session => describeSessionState(session).key === 'paused')
            || sessions.find(session => describeSessionState(session).key === 'queued')
            || null;
        const activeState = activeSession ? describeSessionState(activeSession) : null;
        const activePositionText = activeSession
            ? formatDuration(currentLivePositionSeconds(activeSession.position_seconds || 0, Boolean(activeSession.is_playing)))
            : null;

        const botColors = {
            gws: '#cba6f7', harmonic: '#89b4fa', maestro: '#a6e3a1',
            melodic: '#fab387', nexus: '#f38ba8', rhythm: '#94e2d5',
            symphony: '#f9e2af', tunestream: '#b4befe', aria: '#cba6f7'
        };
        const accent = botColors[bot.key] || '#89b4fa';

        const card = document.createElement("div");
        card.className = "bot-card";
        card.style.setProperty('--bot-accent', accent);

        const initial = bot.display_name.charAt(0).toUpperCase();
        const rawHeartbeatStatus = String(bot.heartbeat_status || 'unknown').toLowerCase();
        const heartbeatStatusLabel = rawHeartbeatStatus === 'healthy' ? 'Healthy'
            : rawHeartbeatStatus === 'online' ? 'Online'
            : rawHeartbeatStatus === 'stale' ? 'Stale'
            : rawHeartbeatStatus === 'offline' ? 'Offline'
            : rawHeartbeatStatus === 'restart' ? 'Restarting'
            : rawHeartbeatStatus === 'n/a' ? 'N/A'
            : bot.heartbeat_status || 'Unknown';

        card.innerHTML = `
            <div class="bot-card-header">
                <div class="bot-avatar" style="background: linear-gradient(135deg, ${accent}44, ${accent}11); border-color: ${accent}55;">
                    <span style="color: ${accent};">${initial}</span>
                </div>
                <div class="bot-info">
                    <div class="bot-name">${bot.display_name}</div>
                    <div class="bot-status-row">
                        <span class="bot-status-dot" style="background:${statusMeta.color}; box-shadow: 0 0 6px ${statusMeta.color};"></span>
                        <span class="bot-status-label" style="color:${statusMeta.color};">${statusMeta.label}</span>
                    </div>
                </div>
            </div>
            <div class="bot-stats">
                <div class="bot-stat-item">
                    <span class="bot-stat-val">${bot.known_guild_count || 0}</span>
                    <span class="bot-stat-lbl">Guilds</span>
                </div>
                <div class="bot-stat-divider"></div>
                <div class="bot-stat-item">
                    <span class="bot-stat-val" style="color: ${bot.active_playing_count > 0 ? '#a6e3a1' : 'inherit'}">${bot.active_playing_count || 0}</span>
                    <span class="bot-stat-lbl">Playing</span>
                </div>
            </div>
            <div class="bot-meta-row">
                <span class="bot-meta-pill">Heartbeat ${heartbeatLabel}</span>
                <span class="bot-meta-pill">${heartbeatStatusLabel}</span>
                ${activeSession ? `<span class="bot-meta-pill">${activeState?.icon || '•'} ${activeState?.label || 'Active'} · <span data-live-position="true" data-base-seconds="${Number(activeSession.position_seconds || 0)}" data-playing="${Boolean(activeSession.is_playing)}">${activePositionText}</span></span>` : ''}
            </div>
            ${bot.kind === 'orchestrator' ? `
            <div class="bot-meta-row" style="margin-top: 10px; flex-direction: column; align-items: stretch; gap: 8px;">
                <span class="bot-meta-pill">Interactions ${bot.recent_interaction_count || 0}</span>
                <span class="bot-meta-pill">Repairs ${Number(bot.medic_summary?.pending_repairs || 0)} • Infra ${Number(bot.medic_summary?.pending_infra || 0)} • Critical ${Number(bot.medic_summary?.critical_health || 0)}</span>
                ${(Array.isArray(bot.recent_interactions) && bot.recent_interactions.length)
                    ? bot.recent_interactions.slice(0, 2).map(entry => {
                        const prompt = escapeHtml(String(entry?.prompt_text || '—')).slice(0, 120);
                        const response = escapeHtml(String(entry?.response_text || '—')).slice(0, 180);
                        const kind = escapeHtml(String(entry?.interaction_type || 'chat'));
                        return `<div class="bot-meta-pill" style="display:block; white-space:normal; line-height:1.35;">
                            <div style="font-weight:600; color:${accent}; margin-bottom:4px;">${kind}</div>
                            <div><strong>Q:</strong> ${prompt}</div>
                            <div style="margin-top:4px;"><strong>A:</strong> ${response}</div>
                        </div>`;
                    }).join('')
                    : `<span class="bot-meta-pill">No recent Aria prompts captured yet.</span>`}
            </div>` : ''}
            ${bot.kind !== 'orchestrator' ? `
            <div class="bot-actions">
                <button class="bot-action-btn bot-btn-restart admin-only"
                    data-action="RESTART" data-bot="${bot.key}" data-guild="0">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-4.5"/></svg>
                    Restart Node
                </button>
            </div>` : `
            <div class="bot-actions">
                <span class="bot-orchestrator-badge">Orchestrator</span>
            </div>`}
        `;

        if (bot.kind === "orchestrator" && ariaContainer) {
            ariaContainer.appendChild(card);
        } else {
            container.appendChild(card);
        }
    });
}

// ================================
// 🎵 RENDER SESSIONS TABLE
// ================================
function renderSessions(sessions) {
    const table = document.getElementById("sessions-body");
    if (!table) return;

    table.innerHTML = "";

    if (!sessions.length) {
        table.innerHTML = '<tr><td colspan="12">No live or queued worker sessions right now.</td></tr>';
        return;
    }

    sessions.forEach(session => {
        const stateMeta = describeSessionState(session);
        const channelLabel = getSessionChannelLabel(session);
        const trackLabel = session.title || (stateMeta.key === 'queued' ? 'Queued media awaiting worker pickup' : '—');
        const row = document.createElement("tr");
        const recoverySummary = summarizeRecoveryState(session);
        const signalSummary = summarizeSessionSignals(session);
        const durationLabel = formatDuration(session.duration_seconds || session.length_seconds || session.track_length_seconds || 0);
        const positionKey = escapeHtml(getSessionRuntimeKey(session));
        row.innerHTML = `
            <td data-label="Bot">${session.bot_display}</td>
            <td data-label="Guild">${session.guild_name || session.guild_id}</td>
            <td data-label="Channel">${channelLabel}${session.home_channel_id ? `<div class="muted" style="font-size:11px; margin-top:4px;">home:${session.home_channel_id}</div>` : ''}</td>
            <td data-label="Status">${stateMeta.icon} ${stateMeta.label}</td>
            <td data-label="Track">${trackLabel}${session.media_source_label ? ` <span class="tbl-source-badge tbl-source-${session.media_source || 'unknown'}">${session.media_source_label}</span>` : ""}${durationLabel !== '0:00' ? `<div class="muted" style="font-size:11px; margin-top:4px;">dur ${durationLabel}</div>` : ''}</td>
            <td data-label="Filter">${session.filter_mode || "none"}</td>
            <td data-label="Loop">${normalizeLoopMode(session.loop_mode)}</td>
            <td data-label="Queue">${session.queue_count || 0}</td>
            <td data-label="Recovery">${escapeHtml(recoverySummary)}</td>
            <td data-label="Signals">${escapeHtml(signalSummary)}</td>
            <td data-label="Position" data-position-key="${positionKey}">${formatDuration(getDisplayPositionSeconds(session))}</td>
            <td data-label="Actions">
                <button class="tbl-btn" data-action="PAUSE"  data-bot="${session.bot_key}" data-guild="${session.guild_id}">Pause</button>
                <button class="tbl-btn" data-action="RESUME" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Resume</button>
                <button class="tbl-btn" data-action="RECOVER" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Recover</button>
                <button class="tbl-btn" data-action="SKIP"   data-bot="${session.bot_key}" data-guild="${session.guild_id}">Skip</button>
                <button class="tbl-btn tbl-btn-stop" data-action="STOP" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Stop</button>
                <button class="tbl-btn" data-action="SHUFFLE" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Shuffle</button>
                <button class="tbl-btn" data-action="CLEAR" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Clear Queue + Track</button>
                <button class="tbl-btn" data-action="LOOP" data-payload="off" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Loop Off</button>
                <button class="tbl-btn" data-action="LOOP" data-payload="song" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Loop Song</button>
                <button class="tbl-btn" data-action="LOOP" data-payload="queue" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Loop Queue</button>
            </td>
        `;
        table.appendChild(row);
    });
}

// ================================
// 🎮 SEND COMMAND (event delegation)
// ================================
async function sendCommand(bot_key, guild_id, action, payload = null, options = {}) {
    if (controlCooldown) {
        return { ok: false, error: 'Another panel command is already in flight. Try again in a moment.' };
    }
    controlCooldown = true;
    const { refresh = true, delayedRefreshMs = 0 } = options;

    try {
        const res = await fetch(`${API_BASE}/bots/control`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ bot_key, guild_id, action, payload })
        });

        if (handle401(res)) return;

        const data = await res.json().catch(() => ({}));

        if (!res.ok) {
            const detail = data.detail || "Unknown control error";
            console.error("❌ Backend rejected request:", detail);
            return { ok: false, error: detail, data };
        }

        if (refresh) {
            fetchDashboard();
            if (delayedRefreshMs > 0) {
                scheduleDashboardRefresh(delayedRefreshMs);
            }
            setTimeout(() => {
                fetchSelectedControlState({ silent: true });
                fetchControlMatrix({ silent: true });
            }, Math.max(700, delayedRefreshMs || 700));
        }
        return { ok: true, data };

    } catch (err) {
        console.error("❌ Command failed:", err);
        return { ok: false, error: String(err) };
    } finally {
        setTimeout(() => (controlCooldown = false), 500);
    }
}

// Delegated click handler for all command buttons
document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-action][data-bot]');
    if (!btn) return;
    const action = btn.dataset.action;
    const bot = btn.dataset.bot;
    const guild = btn.dataset.guild || '0';
    let payload = btn.dataset.payload || null;
    if (payload && ((payload.startsWith("{") && payload.endsWith("}")) || (payload.startsWith("[") && payload.endsWith("]")))) {
        try { payload = JSON.parse(payload); } catch (_) {}
    }
    if (action === 'CLEAR' && !confirm('Clear the queue and stop the current track for this guild?')) return;
    if (action === 'RESTART' && !confirm('Restart this bot node? Active playback may pause briefly.')) return;
    if (action && bot) {
        sendCommand(bot, guild, action, payload, { delayedRefreshMs: 2200 }).then((result) => {
            if (!result?.ok) {
                setControlStatus(result?.error || `Failed to run ${action}.`, true);
                return;
            }
            setControlStatus(result.data?.message || `${action} sent.`);
            setTimeout(() => {
                fetchSelectedControlState({ silent: true });
                fetchControlMatrix({ silent: true });
            }, 2200);
        });
    }
});

// ================================
// 🤖 BOT SELECT (INVENTORY)
// ================================
async function loadBotSelect() {
    const sel = document.getElementById('bot-select');
    const controlSel = document.getElementById('control-bot-select');
    if (!sel) return;
    try {
        const res = await fetch(`${API_BASE}/bots`);
        if (handle401(res)) return;
        const data = await res.json();
        const visibleBots = data.bots || [];
        inviteCatalogState = Array.isArray(data.invite_bots) ? data.invite_bots : visibleBots;
        botCatalogState = visibleBots.filter(b => b.kind === 'music');
        renderInviteCatalog();
        updateInviteOnlyMode();

        const inventoryOptionsHtml = visibleBots
            .map(b => `<option value="${b.key}">${b.display_name}</option>`)
            .join('');
        const controlOptionsHtml = botCatalogState
            .map(b => `<option value="${b.key}">${b.display_name}</option>`)
            .join('');
        sel.innerHTML = inventoryOptionsHtml || '<option value="">No connected bots</option>';
        if (controlSel) controlSel.innerHTML = controlOptionsHtml || '<option value="">No connected music bots</option>';
        if (sel.value) {
            loadInventory();
        }
        if (controlSel?.value) {
            loadControlInventory(controlSel.value);
        }
        renderControlContext();
        renderAriaCommandGuide();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
    } catch (err) {
        console.error("❌ Failed to load bot list:", err);
        renderInviteCatalog();
    }
}

async function loadInventory() {
    const sel = document.getElementById('bot-select');
    const out = document.getElementById('inventory-output');
    if (!sel || !out) return;
    out.innerHTML = '<div class="control-context-empty">Loading live guild and channel inventory...</div>';
    try {
        const res = await fetch(`${API_BASE}/bots/${sel.value}/inventory`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            out.innerHTML = `<div class="control-context-empty">Inventory load failed: ${escapeHtml(data.detail || 'Unknown error')}</div>`;
            return;
        }
        inventoryBrowserState = {
            bot: data.bot || null,
            identity: data.identity || null,
            guilds: Array.isArray(data.guilds) ? data.guilds : [],
            errors: Array.isArray(data.errors) ? data.errors : [],
        };
        renderInventoryBrowser();
    } catch (err) {
        out.innerHTML = `<div class="control-context-empty">Error: ${escapeHtml(String(err))}</div>`;
    }
}

function getFilteredSortedInventoryGuilds() {
    const search = (document.getElementById('inventory-search')?.value || '').trim().toLowerCase();
    const sortMode = document.getElementById('inventory-sort')?.value || 'name';
    const guilds = [...(inventoryBrowserState.guilds || [])].map(guild => {
        const channels = Array.isArray(guild.channels) ? guild.channels : [];
        return {
            ...guild,
            channels,
            voice_count: channels.filter(ch => ch.type === 2 || ch.type === 13).length,
            text_count: channels.filter(ch => ch.type === 0 || ch.type === 5 || ch.type === 15).length,
        };
    }).filter(guild => {
        if (!search) return true;
        if (String(guild.name || '').toLowerCase().includes(search)) return true;
        return guild.channels.some(ch => String(ch.name || '').toLowerCase().includes(search));
    });

    guilds.sort((a, b) => {
        if (sortMode === 'channels_desc') return (b.channels.length - a.channels.length) || String(a.name || '').localeCompare(String(b.name || ''));
        if (sortMode === 'voice_desc') return (b.voice_count - a.voice_count) || String(a.name || '').localeCompare(String(b.name || ''));
        return String(a.name || '').localeCompare(String(b.name || ''));
    });
    return guilds;
}

function renderInventoryBrowser() {
    const out = document.getElementById('inventory-output');
    if (!out) return;
    const botName = inventoryBrowserState.bot?.display_name || inventoryBrowserState.bot?.key || 'Unknown bot';
    const identity = inventoryBrowserState.identity || {};
    const errors = Array.isArray(inventoryBrowserState.errors) ? inventoryBrowserState.errors : [];
    const guilds = getFilteredSortedInventoryGuilds();

    const summary = `
        <div class="inventory-summary-grid">
            <div class="capability-item"><div class="capability-item-head"><span>Bot identity</span></div><div class="capability-item-body">${escapeHtml(identity.username ? `${identity.username}${identity.global_name ? ` (${identity.global_name})` : ''}` : 'Unavailable')}</div></div>
            <div class="capability-item"><div class="capability-item-head"><span>Guilds visible</span></div><div class="capability-item-body">${escapeHtml(String(guilds.length))} guilds available to ${escapeHtml(botName)}</div></div>
            <div class="capability-item"><div class="capability-item-head"><span>Why this exists</span></div><div class="capability-item-body">Use this to confirm which guilds/channels each bot can actually route commands to before you issue play, home-channel, or leave actions.</div></div>
        </div>
    `;

    const warnings = errors.length ? `<div class="diag-list">${errors.map(error => `<div class="diag-item diag-item-error">${escapeHtml(error)}</div>`).join('')}</div>` : '';

    if (!guilds.length) {
        out.innerHTML = `${summary}${warnings}<div class="control-context-empty">No guilds matched the current filter.</div>`;
        return;
    }

    out.innerHTML = `${summary}${warnings}<div class="inventory-guild-grid">${guilds.map(guild => {
        const channelGroups = [
            { label: 'Voice / Stage', items: guild.channels.filter(ch => ch.type === 2 || ch.type === 13) },
            { label: 'Text / Announcement / Forum', items: guild.channels.filter(ch => ch.type === 0 || ch.type === 5 || ch.type === 15) },
            { label: 'Categories / Other', items: guild.channels.filter(ch => ![0,2,5,13,15].includes(ch.type)) },
        ].filter(group => group.items.length);
        return `<article class="inventory-guild-card">
            <div class="swarm-guild-card-head">
                <div>
                    <div class="worker-diagnostic-name">${escapeHtml(guild.name || `Guild ${guild.id}`)}</div>
                    <div class="worker-diagnostic-meta">${escapeHtml(String(guild.channels.length))} channels • ${escapeHtml(String(guild.voice_count))} voice/stage • ${escapeHtml(String(guild.text_count))} text/forum</div>
                </div>
                <span class="control-context-status control-context-status-${guild.owner ? 'online' : 'idle'}">${guild.owner ? 'Owner access' : 'Member access'}</span>
            </div>
            ${guild.channels_error ? `<div class="diag-item diag-item-error">${escapeHtml(guild.channels_error)}</div>` : ''}
            <div class="inventory-channel-groups">${channelGroups.map(group => `<div class="inventory-channel-group"><div class="worker-diagnostic-meta">${escapeHtml(group.label)}</div><div class="inventory-channel-list">${group.items.map(ch => `<span class="inventory-chip">${escapeHtml(ch.name || ch.id)} <small>${escapeHtml(ch.type_name || String(ch.type))}</small></span>`).join('')}</div></div>`).join('')}</div>
        </article>`;
    }).join('')}</div>`;
}


function normalizeEventFeedEntry(payload) {
    if (!payload || typeof payload !== 'object') return null;

    if (payload.type === 'error' && payload.data) {
        return {
            type: 'feed_event',
            level: 'error',
            title: payload.data.title || 'Error',
            description: payload.data.description || 'Unknown error',
            timestamp: payload.data.timestamp || new Date().toISOString(),
            source: 'worker',
        };
    }

    if (payload.type === 'command_ack' && payload.data) {
        return {
            type: 'command_ack',
            level: 'info',
            title: 'Command Acknowledged',
            description: `${payload.data.action || 'Command'} accepted for ${payload.data.bot_key || 'bot'} in guild ${payload.data.guild_id || 'unknown'}.`,
            timestamp: payload.data.timestamp || new Date().toISOString(),
            source: 'worker',
        };
    }

    const title = payload.title || (payload.type === 'command_ack' ? 'Command Acknowledged' : 'Event');
    const description = payload.description || '';
    if (!title && !description) return null;

    return {
        type: payload.type || 'feed_event',
        level: payload.level || 'info',
        title,
        description,
        timestamp: payload.timestamp || new Date().toISOString(),
        source: payload.source || 'panel',
        bot_key: payload.bot_key || null,
        guild_id: payload.guild_id || null,
        error_type: payload.error_type || null,
    };
}

function addEventFeedEntry(entry) {
    const normalized = normalizeEventFeedEntry(entry);
    if (!normalized) return;
    eventFeedEntries.push(normalized);
    if (eventFeedEntries.length > MAX_EVENT_FEED_ENTRIES) {
        eventFeedEntries = eventFeedEntries.slice(-MAX_EVENT_FEED_ENTRIES);
    }
    renderEventFeed();
}

function renderEventFeed() {
    const out = document.getElementById('error-feed');
    if (!out) return;

    const connectionLabel = { connecting: 'connecting', online: 'live', offline: 'offline' }[eventFeedConnectionState] || eventFeedConnectionState;
    const levelFilter = document.getElementById('error-feed-level')?.value || 'all';
    const sourceFilter = document.getElementById('error-feed-source')?.value || 'all';
    const sortMode = document.getElementById('error-feed-sort')?.value || 'newest';

    let items = eventFeedEntries.slice();
    if (levelFilter !== 'all') items = items.filter(entry => String(entry.level || '').toLowerCase() === levelFilter);
    if (sourceFilter !== 'all') {
        items = items.filter(entry => {
            const source = String(entry.source || '').toLowerCase();
            if (sourceFilter === 'bot_error') return entry.type === 'bot_error' || source.includes('bot');
            return source.includes(sourceFilter);
        });
    }
    if (sortMode === 'oldest') {
        items.sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
    } else if (sortMode === 'level') {
        const rank = { error: 0, warning: 1, info: 2 };
        items.sort((a, b) => (rank[a.level] ?? 9) - (rank[b.level] ?? 9) || new Date(b.timestamp || 0) - new Date(a.timestamp || 0));
    } else {
        items.sort((a, b) => new Date(b.timestamp || 0) - new Date(a.timestamp || 0));
    }

    if (!items.length) {
        out.innerHTML = `<div class="error-feed-empty">Feed: ${escapeHtml(connectionLabel)}<br><br>No matching live events yet.</div>`;
        renderOverview(dashboardBotsState);
        return;
    }

    out.innerHTML = `<div class="error-feed-status">Feed: ${escapeHtml(connectionLabel)} • Timezone: CST/CDT</div>` + items.map(entry => {
        const ts = formatCentralTimestamp(entry.timestamp, true);
        const level = String(entry.level || 'info').toLowerCase();
        const source = entry.source || 'panel';
        const meta = [source, entry.bot_key ? `bot=${entry.bot_key}` : null, entry.guild_id ? `guild=${entry.guild_id}` : null, entry.error_type ? `type=${entry.error_type}` : null].filter(Boolean).join(' • ');
        return `<article class="error-entry error-entry-${escapeHtml(level)}">
            <div class="error-entry-head">
                <span class="error-entry-level">${escapeHtml(level.toUpperCase())}</span>
                <span class="error-entry-time">${escapeHtml(ts)}</span>
            </div>
            <div class="error-entry-title">${escapeHtml(entry.title || 'Untitled event')}</div>
            <div class="error-entry-meta">${escapeHtml(meta)}</div>
            <div class="error-entry-body">${escapeHtml(entry.description || '')}</div>
        </article>`;
    }).join('');
    renderOverview(dashboardBotsState);
}

async function loadEventFeedHistory() {
    try {
        const res = await fetch(`${API_BASE}/events?limit=${MAX_EVENT_FEED_ENTRIES}`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            eventFeedEntries = [{
                level: 'error',
                title: 'Event Feed Failed',
                description: data.detail || 'Unable to load recent events.',
                timestamp: new Date().toISOString(),
                source: 'api',
            }];
            renderEventFeed();
            return;
        }

        eventFeedEntries = (data.events || [])
            .map(normalizeEventFeedEntry)
            .filter(Boolean)
            .slice(-MAX_EVENT_FEED_ENTRIES);
        renderEventFeed();
    } catch (err) {
        eventFeedEntries = [{
            level: 'error',
            title: 'Event Feed Failed',
            description: String(err),
            timestamp: new Date().toISOString(),
            source: 'client',
        }];
        renderEventFeed();
    }
}

function startEventFeedPolling() {
    if (eventFeedPollTimer) return;
    eventFeedPollTimer = setInterval(() => {
        loadEventFeedHistory();
    }, 8000);
}

function stopEventFeedPolling() {
    if (!eventFeedPollTimer) return;
    clearInterval(eventFeedPollTimer);
    eventFeedPollTimer = null;
}

function disconnectEventFeed() {
    if (eventFeedReconnectTimer) {
        clearTimeout(eventFeedReconnectTimer);
        eventFeedReconnectTimer = null;
    }
    stopEventFeedPolling();
    if (eventFeedSocket) {
        try {
            eventFeedSocket.onclose = null;
            eventFeedSocket.close();
        } catch (err) {
            console.warn('⚠️ Failed to close event feed socket:', err);
        }
        eventFeedSocket = null;
    }
    eventFeedConnectionState = 'offline';
}

function updateLiveSyncStatus() {
    const el = document.getElementById('live-sync-status');
    if (!el) return;
    if (eventFeedConnectionState === 'online') {
        el.textContent = 'Live Sync: WebSocket';
        el.style.color = '#a6e3a1';
        el.style.borderColor = '#a6e3a1';
    } else if (eventFeedConnectionState === 'connecting') {
        el.textContent = 'Live Sync: Connecting...';
        el.style.color = '#fab387';
        el.style.borderColor = '#fab387';
    } else {
        el.textContent = 'Live Sync: Polling 5s';
        el.style.color = '#89b4fa';
        el.style.borderColor = '#89b4fa';
    }
}

function connectEventFeed() {
    const wsUrl = buildWebSocketUrl('/ws');
    if (!wsUrl) {
        eventFeedConnectionState = 'offline';
        startEventFeedPolling();
        renderEventFeed();
        return;
    }
    if (eventFeedSocket && (eventFeedSocket.readyState === WebSocket.OPEN || eventFeedSocket.readyState === WebSocket.CONNECTING)) {
        return;
    }
    if (eventFeedReconnectTimer) {
        clearTimeout(eventFeedReconnectTimer);
        eventFeedReconnectTimer = null;
    }
    stopEventFeedPolling();

    eventFeedConnectionState = 'connecting';
    renderEventFeed();
    updateLiveSyncStatus();

    eventFeedSocket = new WebSocket(wsUrl);

    eventFeedSocket.onopen = () => {
        eventFeedConnectionState = 'online';
        stopEventFeedPolling();
        loadEventFeedHistory();
        renderEventFeed();
        updateLiveSyncStatus();
    };

    eventFeedSocket.onmessage = (event) => {
        try {
            const payload = JSON.parse(event.data);
            addEventFeedEntry(payload);
        } catch (err) {
            addEventFeedEntry({
                level: 'warning',
                title: 'Unreadable Feed Event',
                description: String(err),
                timestamp: new Date().toISOString(),
                source: 'client',
            });
        }
    };

    eventFeedSocket.onerror = () => {
        eventFeedConnectionState = 'offline';
        startEventFeedPolling();
        renderEventFeed();
        updateLiveSyncStatus();
    };

    eventFeedSocket.onclose = (event) => {
        eventFeedSocket = null;
        eventFeedConnectionState = 'offline';
        startEventFeedPolling();
        renderEventFeed();
        updateLiveSyncStatus();
        if (event?.code === 4401) {
            panelSessionChecked = false;
            panelAppStarted = false;
            setRemoteToken('');
            showRemoteAuthShell('The live event feed needs a fresh login token.');
            return;
        }
        if (!panelAppStarted || (REMOTE_MODE && !remotePanelToken)) {
            return;
        }
        eventFeedReconnectTimer = setTimeout(connectEventFeed, 3000);
    };
}

function setControlStatus(message, isError = false) {
    const status = document.getElementById('control-status');
    if (!status) return;
    status.textContent = message || '';
    status.classList.toggle('error', Boolean(isError));
}

function getDashboardSession(botKey, guildId) {
    const guildKey = String(guildId);
    for (const bot of dashboardBotsState) {
        if (bot.key !== botKey || !Array.isArray(bot.sessions)) continue;
        const session = bot.sessions.find(item => String(item.guild_id) === guildKey);
        if (session) return session;
    }
    return null;
}

function rankSessionForGuildPreference(session) {
    const state = describeSessionState(session).key;
    if (state === 'playing') return 5;
    if (state === 'paused') return 4;
    if (state === 'queued') return 3;
    if (session?.backup_restore_ready) return 2;
    if (session?.home_channel_id) return 1;
    return 0;
}

function getPreferredGuildIdForBot(botKey, guilds, previousValue = '', preservePrevious = true) {
    const validGuildIds = new Set((guilds || []).map(guild => String(guild.id)));
    if (!validGuildIds.size) return '';

    if (preservePrevious && previousValue && validGuildIds.has(String(previousValue))) {
        return String(previousValue);
    }

    const sessions = [];
    const selectedBot = getSelectedControlBot(botKey);
    if (selectedBot?.session) {
        sessions.push(selectedBot.session);
    }

    const liveBot = getLiveControlBot(botKey);
    if (liveBot?.session) {
        sessions.push(liveBot.session);
    }

    const dashboardBot = getDashboardBot(botKey);
    if (Array.isArray(dashboardBot?.sessions)) {
        sessions.push(...dashboardBot.sessions);
    }

    const ranked = sessions
        .filter(session => session?.guild_id && validGuildIds.has(String(session.guild_id)))
        .sort((left, right) => {
            const priorityDiff = rankSessionForGuildPreference(right) - rankSessionForGuildPreference(left);
            if (priorityDiff) return priorityDiff;
            return Number(right?.queue_count || 0) - Number(left?.queue_count || 0);
        });

    if (ranked.length) {
        return String(ranked[0].guild_id);
    }

    if (previousValue && validGuildIds.has(String(previousValue))) {
        return String(previousValue);
    }

    return String(guilds[0]?.id || '');
}

function getSelectedControlGuild() {
    const guildId = document.getElementById('control-guild-select')?.value;
    return controlInventoryState?.guilds?.find(guild => String(guild.id) === String(guildId)) || null;
}

function populateControlGuilds(options = {}) {
    const { preservePrevious = true } = options;
    const guildSel = document.getElementById('control-guild-select');
    if (!guildSel) return;

    const guilds = controlInventoryState?.guilds || [];
    const previousValue = guildSel.value;

    if (!guilds.length) {
        resetControlInventorySelectors(controlInventoryLoading ? 'Loading guilds...' : 'No guilds found');
        populateControlChannels();
        return;
    }

    guildSel.innerHTML = guilds
        .map(guild => `<option value="${guild.id}">${guild.name}</option>`)
        .join('');

    guildSel.value = getPreferredGuildIdForBot(
        document.getElementById('control-bot-select')?.value,
        guilds,
        previousValue,
        preservePrevious,
    );

    populateControlChannels();
}

function populateControlChannels() {
    const voiceSel = document.getElementById('control-voice-select');
    const botKey = document.getElementById('control-bot-select')?.value;
    const guild = getSelectedControlGuild();
    if (!voiceSel) return;

    const previousVoice = voiceSel.value;
    const channels = guild?.channels || [];
    const voiceChannels = channels.filter(channel => channel.type === 2 || channel.type === 13);
    const session = botKey && guild ? getBestControlSession(botKey, guild.id) : null;

    voiceSel.innerHTML = voiceChannels.length
        ? voiceChannels.map(channel => `<option value="${channel.id}">${channel.name}</option>`).join('')
        : '<option value="">No voice channels found</option>';

    if (previousVoice && voiceChannels.some(channel => String(channel.id) === previousVoice)) {
        voiceSel.value = previousVoice;
    } else if (session?.channel_id && voiceChannels.some(channel => String(channel.id) === String(session.channel_id))) {
        voiceSel.value = String(session.channel_id);
    } else if (session?.home_channel_id && voiceChannels.some(channel => String(channel.id) === String(session.home_channel_id))) {
        voiceSel.value = String(session.home_channel_id);
    }

    syncControlSelectionsFromDashboard();
}

function syncControlSelectionsFromDashboard() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const loopSel = document.getElementById('control-loop-select');
    const filterSel = document.getElementById('control-filter-select');
    const voiceSel = document.getElementById('control-voice-select');
    if (!botKey || !guildId) return;

    const session = getBestControlSession(botKey, guildId);
    if (loopSel) {
        loopSel.value = normalizeLoopMode(session?.loop_mode);
    }
    if (filterSel && session?.filter_mode) {
        filterSel.value = session.filter_mode;
    }

    if (voiceSel && session?.channel_id) {
        const voiceOption = Array.from(voiceSel.options).find(option => option.value === String(session.channel_id));
        if (voiceOption) voiceSel.value = String(session.channel_id);
    }

    renderControlContext();
    renderAriaCommandGuide();
    renderSelectedBotCapabilities();
    renderSelectedGuildMatrix();
}

function getDiagnosticsBot(botKey) {
    return systemDiagnosticsState?.bots?.find(bot => bot.key === botKey) || null;
}

function getDashboardBot(botKey) {
    return dashboardBotsState.find(bot => bot.key === botKey) || null;
}

function getCatalogBot(botKey) {
    return botCatalogState.find(bot => bot.key === botKey) || null;
}

function deriveBotDbAccess(botKey, guildId = null) {
    const selectedBot = getSelectedControlBot(botKey, guildId);
    const liveBot = getBestLiveControlBot(botKey, guildId);
    if (liveBot?.db) {
        return { ...liveBot.db, source: selectedBot ? 'selected-control' : 'control-matrix' };
    }

    const diagnostics = getDiagnosticsBot(botKey);
    if (diagnostics?.db) {
        return { ...diagnostics.db, source: 'diagnostics' };
    }

    const bot = getDashboardBot(botKey);
    if (bot) {
        if (bot.status === 'db-unavailable' || bot.status === 'error') {
            return {
                status: 'error',
                reachable: false,
                message: bot.error || 'Dashboard could not query this bot schema.',
                source: 'dashboard',
            };
        }

        return {
            status: 'online',
            reachable: true,
            message: 'Dashboard is receiving live database state for this bot.',
            source: 'dashboard',
        };
    }

    return {
        status: 'unknown',
        reachable: false,
        message: 'No database diagnostics or dashboard snapshot are available yet.',
        source: 'unknown',
    };
}

function deriveBotDiscordAccess(botKey, guildId = null) {
    const selectedBot = getSelectedControlBot(botKey, guildId);
    const liveBot = getBestLiveControlBot(botKey, guildId);
    if (liveBot?.discord) {
        return { ...liveBot.discord, source: selectedBot ? 'selected-control' : 'control-matrix' };
    }

    const diagnostics = getDiagnosticsBot(botKey);
    if (diagnostics?.discord) {
        return { ...diagnostics.discord, source: 'diagnostics' };
    }

    if (controlInventoryState?.loaded && controlInventoryState?.bot?.key === botKey && Array.isArray(controlInventoryState.guilds)) {
        const guildCount = controlInventoryState.guilds.length;
        const selectedGuildLoaded = !guildId || controlInventoryState.guilds.some(guild => String(guild.id) === String(guildId));
        if (!selectedGuildLoaded) {
            return {
                status: 'error',
                reachable: false,
                message: `The selected guild is not present in ${botKey}'s live Discord inventory.`,
                source: 'inventory',
            };
        }
        return {
            status: 'online',
            reachable: true,
            message: `Live Discord inventory is loaded for ${guildCount} guild${guildCount === 1 ? '' : 's'}.`,
            source: 'inventory',
        };
    }

    const bot = getDashboardBot(botKey);
    if (bot?.discord?.identity) {
        const identity = bot.discord.identity;
        const label = identity.username || identity.global_name || identity.id || bot.display_name || botKey;
        return {
            status: 'online',
            reachable: true,
            message: `Dashboard resolved Discord identity as ${label}.`,
            source: 'dashboard',
        };
    }

    if (bot?.discord?.error) {
        return {
            status: 'error',
            reachable: false,
            message: bot.discord.error,
            source: 'dashboard',
        };
    }

    const catalogBot = getCatalogBot(botKey);
    const tokenConfigured = bot?.discord?.token_configured ?? catalogBot?.token_configured;
    if (tokenConfigured === false) {
        return {
            status: 'missing',
            reachable: false,
            message: 'Panel token is not configured for Discord inventory access.',
            source: 'catalog',
        };
    }
    if (tokenConfigured === true) {
        return {
            status: 'online',
            reachable: true,
            message: 'Panel token is configured for Discord inventory access.',
            source: 'catalog',
        };
    }

    return {
        status: 'unknown',
        reachable: false,
        message: 'No Discord diagnostics or inventory state are available yet.',
        source: 'unknown',
    };
}

function formatAccessLabel(access, okLabel = 'ready') {
    if (access?.reachable) return okLabel;
    const status = String(access?.status || 'unknown').toLowerCase();
    if (status === 'missing') return 'missing';
    if (status === 'error') return 'error';
    return 'unknown';
}

function renderControlContext() {
    const container = document.getElementById('control-context-card');
    if (!container) return;

    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const bot = getBestLiveControlBot(botKey, guildId) || getDashboardBot(botKey) || null;
    const session = botKey && guildId ? getBestControlSession(botKey, guildId) : null;
    const dbAccess = deriveBotDbAccess(botKey, guildId);
    const discordAccess = deriveBotDiscordAccess(botKey, guildId);
    const selectedGuild = getSelectedControlGuild();
    const statusMeta = getControlBotStatus(botKey, guildId);
    const sessionState = describeSessionState(session);
    const liveMatrixReady = Boolean(controlMatrixState.loaded && String(controlMatrixState.guildId) === String(guildId || ''));
    const guildName = session?.guild_name || selectedOptionText('control-guild-select') || 'No guild selected';
    const voiceName = session?.channel_name || selectedOptionText('control-voice-select') || 'No voice channel selected';
    const homeName = session?.home_channel_name || (session?.home_channel_id ? `Home channel ${session.home_channel_id}` : 'Not set');
    const feedbackName = session?.feedback_channel_name || (session?.feedback_channel_id ? `Feedback ${session.feedback_channel_id}` : 'Not set');
    const backupQueueCount = Number(session?.backup_queue_count || 0);
    const pendingOrders = Number(session?.pending_direct_orders || 0);
    const heartbeatAge = getControlHeartbeatAge(botKey, guildId);
    const trackLabel = session?.title
        || (sessionState.key === 'queued'
            ? 'Queued media is waiting for the worker to pick it up.'
            : session?.backup_restore_ready
                ? 'Idle right now, but the backup queue is armed for auto-restore.'
            : sessionState.key === 'configured'
                ? 'This bot is configured for the guild but not actively playing.'
                : selectedGuild && dbAccess.reachable
                    ? 'No live playback record yet, but direct controls are ready for this bot and guild.'
                : controlInventoryLoading
                    ? 'Syncing guild/channel inventory for the selected bot.'
                    : 'Idle. No live playback in the selected guild.');

    if (!botKey) {
        container.innerHTML = `
            <div class="control-context-empty">
                Pick a bot and guild to inspect the live control target.
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div class="control-context-top">
            <div>
                <div class="control-context-bot">${escapeHtml(bot?.display_name || botKey)}</div>
                <div class="control-context-guild">${escapeHtml(guildName)}</div>
            </div>
            <span class="control-context-status control-context-status-${statusMeta.tone}">${escapeHtml(statusMeta.label)}</span>
        </div>
        <div class="control-context-track">${escapeHtml(trackLabel)}</div>
        <div class="control-context-meta">
            <span>State: ${escapeHtml(sessionState.label)}</span>
            <span>Voice: ${escapeHtml(voiceName)}</span>
            <span>Home: ${escapeHtml(homeName)}</span>
            <span>Feedback: ${escapeHtml(feedbackName)}</span>
            <span>Queue: ${escapeHtml(String(session?.queue_count ?? 0))}</span>
            <span>Backup: ${escapeHtml(String(backupQueueCount))}</span>
            <span>Pending orders: ${escapeHtml(String(pendingOrders))}</span>
            <span>Loop: ${escapeHtml(normalizeLoopMode(session?.loop_mode))}</span>
            <span>Filter: ${escapeHtml(session?.filter_mode || 'none')}</span>
            <span>Heartbeat: ${escapeHtml(formatHeartbeatAge(heartbeatAge))}</span>
            <span>DB link: ${escapeHtml(formatAccessLabel(dbAccess))}</span>
            <span>Discord inventory: ${escapeHtml(formatAccessLabel(discordAccess))}</span>
            <span>Live sync: ${escapeHtml(liveMatrixReady ? 'connected' : 'fallback')}</span>
        </div>
    `;
}

function renderAriaCommandGuide() {
    const container = document.getElementById('aria-command-guide');
    if (!container) return;

    const botKey = document.getElementById('control-bot-select')?.value || 'melodic';
    const voiceChannelId = document.getElementById('control-voice-select')?.value || 'VOICE_CHANNEL_ID';
    const sourceInput = document.getElementById('control-source-input')?.value?.trim();
    const playTarget = sourceInput || 'daft punk around the world';

    const examples = [
        `aria play ${playTarget} via ${botKey}`,
        `aria pause ${botKey}`,
        `aria filter nightcore on ${botKey}`,
        `aria home ${botKey} <#${voiceChannelId}>`,
        `aria leave ${botKey}`,
    ];

    container.innerHTML = examples.map(example => `<code class="aria-command-line">${escapeHtml(example)}</code>`).join('');
}

function renderSelectedBotCapabilities() {
    const container = document.getElementById('selected-bot-capabilities');
    if (!container) return;

    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const voiceChannelId = document.getElementById('control-voice-select')?.value;
    if (!botKey) {
        container.innerHTML = '<div class="control-context-empty">Pick a bot to see what actions are currently ready.</div>';
        return;
    }

    const bot = getBestLiveControlBot(botKey, guildId) || getDashboardBot(botKey) || null;
    const dbAccess = deriveBotDbAccess(botKey, guildId);
    const discordAccess = deriveBotDiscordAccess(botKey, guildId);
    const session = guildId ? getBestControlSession(botKey, guildId) : null;
    const selectedGuild = getSelectedControlGuild();
    const dbReady = Boolean(dbAccess.reachable);
    const discordReady = Boolean(discordAccess.reachable);
    const inventoryReady = !controlInventoryLoading && Boolean(controlInventoryState?.loaded) && controlInventoryState?.bot?.key === botKey;
    const hasGuild = Boolean(guildId && selectedGuild);
    const guildInventoryReady = inventoryReady && hasGuild;
    const hasVoice = Boolean(
        voiceChannelId
        && selectedGuild?.channels?.some(channel =>
            String(channel.id) === String(voiceChannelId) && (channel.type === 2 || channel.type === 13)
        )
    );
    const hasHome = Boolean(session?.home_channel_id);
    const inventoryReason = controlInventoryLoading
        ? 'Guild/channel inventory is still syncing for this bot.'
        : !inventoryReady
            ? 'Reload this bot inventory before sending routed commands.'
            : !hasGuild
                ? 'Choose a guild that exists in this bot inventory first.'
                : 'Live guild/channel inventory is loaded for this bot.'

    const items = [
        {
            label: 'Queue media',
            ready: dbReady && guildInventoryReady && hasVoice,
            reason: !dbReady ? dbAccess.message
                : !guildInventoryReady ? inventoryReason
                : hasVoice ? 'Voice route is selected and ready for a direct play order.'
                : 'Select a guild and voice channel first.',
        },
        {
            label: 'Set home channel',
            ready: dbReady && guildInventoryReady && hasVoice,
            reason: !dbReady ? dbAccess.message
                : !guildInventoryReady ? inventoryReason
                : hasHome ? 'A home channel already exists and can be overwritten.'
                : 'Choose a voice channel to anchor this bot.',
        },
        {
            label: 'Pause / resume / skip / stop',
            ready: dbReady && hasGuild,
            reason: !dbReady ? dbAccess.message
                : hasGuild ? `Guild-scoped control path is available${session?.title ? ` for ${session.title}.` : '.'}`
                : 'Choose a guild before sending transport controls.',
        },
        {
            label: 'Leave voice',
            ready: dbReady && hasGuild,
            reason: !dbReady ? dbAccess.message
                : hasGuild ? 'Direct leave order can be injected now.'
                : 'Choose a guild before issuing leave.',
        },
        {
            label: 'Shuffle / clear queue',
            ready: dbReady && hasGuild,
            reason: !dbReady ? dbAccess.message
                : hasGuild ? 'Queue mutation path is ready.'
                : 'Choose a guild to mutate its queue.',
        },
        {
            label: 'Loop / filter changes',
            ready: dbReady && hasGuild,
            reason: !dbReady ? dbAccess.message
                : hasGuild ? 'Guild settings table can be updated.'
                : 'Choose a guild to update loop or filter state.',
        },
        {
            label: 'Backup queue auto-restore',
            ready: dbReady && hasGuild && Boolean(session?.backup_restore_ready),
            reason: !dbReady ? dbAccess.message
                : !hasGuild ? 'Choose a guild before checking backup recovery state.'
                : Number(session?.backup_queue_count || 0) === 0 ? 'No backup queue entries exist for this bot and guild right now.'
                : session?.backup_restore_ready ? (session?.backup_restore_reason || 'Backup queue is armed and should repopulate the live queue when playback goes idle.')
                : (session?.backup_restore_reason || 'Backup queue exists, but the live queue is not empty yet.'),
        },
        {
            label: 'Restart node',
            ready: dbReady,
            reason: dbReady ? 'Database control path is ready for restart orders.' : dbAccess.message,
        },
        {
            label: 'Discord inventory / channel explorer',
            ready: discordReady && inventoryReady,
            reason: !discordReady ? discordAccess.message
                : !inventoryReady ? inventoryReason
                : discordAccess.message,
        },
    ];

    container.innerHTML = `
        <div class="capability-header">
            <div class="worker-diagnostic-name">${escapeHtml(bot?.display_name || botKey)}</div>
            <div class="worker-diagnostic-meta">${escapeHtml(guildId ? `Guild ${guildId}` : 'No guild selected')}</div>
        </div>
        ${items.map(item => `
            <div class="capability-item">
                <div class="capability-item-head">
                    <span>${escapeHtml(item.label)}</span>
                    ${diagnosticBadge(item.ready ? 'online' : 'missing')}
                </div>
                <div class="capability-item-body">${escapeHtml(item.reason)}</div>
            </div>
        `).join('')}
    `;
}

function renderSelectedGuildMatrix() {
    const container = document.getElementById('selected-guild-matrix');
    if (!container) return;

    const guildId = document.getElementById('control-guild-select')?.value;
    if (!guildId) {
        container.innerHTML = '<div class="control-context-empty">Choose a guild to see how the full swarm is positioned there.</div>';
        return;
    }

    const liveMatrixReady = Boolean(controlMatrixState.loaded && String(controlMatrixState.guildId) === String(guildId));
    const workerBots = liveMatrixReady
        ? controlMatrixState.bots
        : dashboardBotsState.filter(bot => bot.kind === 'music');

    container.innerHTML = workerBots.map(bot => {
        const session = getBestControlSession(bot.key, guildId);
        const dbAccess = bot.db || deriveBotDbAccess(bot.key, guildId);
        const discordAccess = bot.discord || deriveBotDiscordAccess(bot.key, guildId);
        const status = getControlBotStatus(bot.key, guildId);
        const homeLabel = session?.home_channel_name || (session?.home_channel_id ? `Home ${session.home_channel_id}` : 'No home channel');
        const sessionState = describeSessionState(session);
        const activityLabel = session
            ? sessionState.key === 'playing'
                ? `Playing ${session.title || 'track'}`
                : sessionState.key === 'paused'
                    ? `Paused on ${session.title || 'track'}`
                    : sessionState.key === 'queued'
                        ? `${session.queue_count || 0} queued`
                        : session?.backup_restore_ready
                            ? 'Idle, backup restore armed'
                        : 'Configured standby'
            : 'No runtime state in this guild';
        return `
            <article class="swarm-guild-card">
                <div class="swarm-guild-card-head">
                    <div>
                        <div class="worker-diagnostic-name">${escapeHtml(bot.display_name)}</div>
                        <div class="worker-diagnostic-meta">${escapeHtml(activityLabel)}</div>
                    </div>
                    <span class="control-context-status control-context-status-${status.tone}">${escapeHtml(status.label)}</span>
                </div>
                <div class="swarm-guild-card-meta">
                    <span>Home: ${escapeHtml(homeLabel)}</span>
                    <span>Queue: ${escapeHtml(String(session?.queue_count ?? 0))}</span>
                    <span>Backup: ${escapeHtml(String(session?.backup_queue_count ?? 0))}</span>
                    <span>Pending: ${escapeHtml(String(session?.pending_direct_orders ?? 0))}</span>
                    <span>DB: ${escapeHtml(describeDiagnosticState(dbAccess.status).label)}</span>
                    <span>Discord: ${escapeHtml(describeDiagnosticState(discordAccess.status).label)}</span>
                </div>
            </article>
        `;
    }).join('');
}

async function fetchSelectedControlState(options = {}) {
    const { silent = false } = options;
    let botKey = document.getElementById('control-bot-select')?.value;
    let guildId = document.getElementById('control-guild-select')?.value;
    const requestId = ++selectedControlRequestId;

    if (!botKey || !guildId) {
        clearSelectedControlState(botKey, guildId);
        renderControlContext();
        renderSelectedBotCapabilities();
        return;
    }

    if (
        controlInventoryState?.loaded
        && controlInventoryState?.bot?.key === botKey
        && Array.isArray(controlInventoryState.guilds)
        && !controlInventoryState.guilds.some(guild => String(guild.id) === String(guildId))
    ) {
        populateControlGuilds({ preservePrevious: false });
        guildId = document.getElementById('control-guild-select')?.value;
    }

    if (!botKey || !guildId) {
        clearSelectedControlState(botKey, guildId);
        renderControlContext();
        renderSelectedBotCapabilities();
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/bots/${encodeURIComponent(botKey)}/control-state?guild_id=${encodeURIComponent(guildId)}`);
        if (handle401(res)) return;
        const data = await res.json();
        if (requestId !== selectedControlRequestId) return;
        if (!res.ok) {
            throw new Error(data.detail || 'Selected control state request failed');
        }

        selectedControlState = {
            botKey: String(botKey),
            guildId: String(guildId),
            loaded: true,
            data,
        };
        renderControlContext();
        renderSelectedBotCapabilities();

        if (!silent) {
            const guildLabel = data?.session?.guild_name || selectedOptionText('control-guild-select') || guildId;
            setControlStatus(`Live control state synced for ${data?.display_name || botKey} in ${guildLabel}.`);
        }
    } catch (err) {
        if (requestId !== selectedControlRequestId) return;
        clearSelectedControlState(botKey, guildId);
        renderControlContext();
        renderSelectedBotCapabilities();
        if (!silent) {
            setControlStatus(`Failed to load selected control state: ${err}`, true);
        }
        console.error('❌ Selected control state fetch failed:', err);
    }
}

async function loadControlInventory(botKey) {
    const controlSel = document.getElementById('control-bot-select');
    if (!botKey && controlSel) botKey = controlSel.value;
    if (!botKey) return;

    const previousBotKey = controlInventoryState?.bot?.key || null;
    const requestId = ++controlInventoryRequestId;
    controlInventoryLoading = true;
    controlInventoryState = { bot: { key: botKey }, guilds: [], loaded: false };
    clearSelectedControlState(botKey, null);
    resetControlInventorySelectors('Loading guilds...');
    setDirectControlsDisabled(true);
    renderControlContext();
    renderAriaCommandGuide();
    renderSelectedBotCapabilities();
    renderSelectedGuildMatrix();
    setControlStatus('Loading bot servers and channels...');
    try {
        const res = await fetch(`${API_BASE}/bots/${encodeURIComponent(botKey)}/inventory`);
        if (handle401(res)) return;
        const data = await res.json();
        if (requestId !== controlInventoryRequestId) return;
        if (!res.ok) {
            controlInventoryState = { bot: { key: botKey }, guilds: [], loaded: false };
            resetControlInventorySelectors('No guilds available');
            setControlStatus(data.detail || 'Failed to load bot inventory.', true);
            return;
        }
        controlInventoryState = { ...data, loaded: true };
        populateControlGuilds({ preservePrevious: previousBotKey === botKey });
        await fetchSelectedControlState({ silent: true });
        await fetchControlMatrix({ silent: true });
        renderControlContext();
        renderAriaCommandGuide();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        setControlStatus(`Loaded ${data.guilds?.length || 0} guilds for ${data.bot?.display_name || botKey}.`);
    } catch (err) {
        if (requestId !== controlInventoryRequestId) return;
        console.error("❌ Failed to load control inventory:", err);
        controlInventoryState = { bot: { key: botKey }, guilds: [], loaded: false };
        resetControlInventorySelectors('Inventory load failed');
        setControlStatus(`Failed to load bot inventory: ${err}`, true);
    } finally {
        if (requestId === controlInventoryRequestId) {
            controlInventoryLoading = false;
            setDirectControlsDisabled(false);
            renderControlContext();
            renderSelectedBotCapabilities();
            renderSelectedGuildMatrix();
        }
    }
}

async function fetchControlMatrix(options = {}) {
    const { silent = false } = options;
    const requestId = ++controlMatrixRequestId;
    let guildId = document.getElementById('control-guild-select')?.value;

    if (
        guildId
        && controlInventoryState?.loaded
        && Array.isArray(controlInventoryState.guilds)
        && !controlInventoryState.guilds.some(guild => String(guild.id) === String(guildId))
    ) {
        populateControlGuilds({ preservePrevious: false });
        guildId = document.getElementById('control-guild-select')?.value;
    }

    if (!guildId) {
        clearControlMatrixState();
        renderControlContext();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/guilds/${encodeURIComponent(guildId)}/control-matrix`);
        if (handle401(res)) return;
        const data = await res.json();
        if (requestId !== controlMatrixRequestId) return;
        if (!res.ok) {
            throw new Error(data.detail || 'Live control matrix request failed');
        }

        controlMatrixState = {
            guildId: String(guildId),
            bots: Array.isArray(data.bots) ? data.bots : [],
            loaded: true,
            generatedAt: data.generated_at || null,
        };
        renderControlContext();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();

        if (!silent) {
            setControlStatus(`Live control state synced for ${selectedOptionText('control-guild-select') || guildId}.`);
        }
    } catch (err) {
        if (requestId !== controlMatrixRequestId) return;
        clearControlMatrixState(guildId);
        renderControlContext();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        if (!silent) {
            setControlStatus(`Failed to load live control state: ${err}`, true);
        }
        console.error('❌ Control matrix fetch failed:', err);
    }
}

async function queueFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const voiceChannelId = document.getElementById('control-voice-select')?.value;
    const sourceInput = document.getElementById('control-source-input');
    const sourceUrl = sourceInput?.value?.trim();

    if (controlInventoryLoading) {
        setControlStatus('Wait for the selected bot inventory to finish loading before queueing media.', true);
        return;
    }
    if (!botKey || !guildId) {
        setControlStatus('Select a bot and guild first.', true);
        return;
    }
    if (!voiceChannelId) {
        setControlStatus('Choose the voice channel that bot should use.', true);
        return;
    }
    if (!sourceUrl) {
        setControlStatus('Paste a track, playlist URL, livestream, or search text first.', true);
        return;
    }

    setControlStatus('Sending play request to the selected bot...');
    const result = await sendCommand(botKey, guildId, 'PLAY', {
        source_url: sourceUrl,
        voice_channel_id: voiceChannelId,
    }, { refresh: false });

    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to queue media on the selected bot.', true);
        return;
    }

    setControlStatus(result.data?.message || 'Play request sent.');
    if (sourceInput) sourceInput.value = '';
    scheduleDashboardRefresh(2200);
    setTimeout(() => {
        fetchSelectedControlState({ silent: true });
        fetchControlMatrix({ silent: true });
    }, 2200);
    renderAriaCommandGuide();
}

async function applyLoopModeFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const loopMode = normalizeLoopMode(document.getElementById('control-loop-select')?.value);

    if (controlInventoryLoading) {
        setControlStatus('Wait for the selected bot inventory to finish loading before changing loop mode.', true);
        return;
    }
    if (!botKey || !guildId) {
        setControlStatus('Select a bot and guild before changing loop mode.', true);
        return;
    }

    setControlStatus('Applying loop mode...');
    const result = await sendCommand(botKey, guildId, 'LOOP', loopMode, { delayedRefreshMs: 900 });
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update loop mode.', true);
        return;
    }

    setControlStatus(result.data?.message || `Loop mode set to ${loopMode}.`);
    setTimeout(() => {
        fetchSelectedControlState({ silent: true });
        fetchControlMatrix({ silent: true });
    }, 900);
}

async function applyFilterModeFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const filterMode = document.getElementById('control-filter-select')?.value || 'none';

    if (controlInventoryLoading) {
        setControlStatus('Wait for the selected bot inventory to finish loading before changing filter mode.', true);
        return;
    }
    if (!botKey || !guildId) {
        setControlStatus('Select a bot and guild before changing filter mode.', true);
        return;
    }

    setControlStatus('Applying audio filter...');
    const result = await sendCommand(botKey, guildId, 'FILTER', filterMode, { delayedRefreshMs: 900 });
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update filter mode.', true);
        return;
    }

    setControlStatus(result.data?.message || `Filter mode set to ${filterMode}.`);
    setTimeout(() => {
        fetchSelectedControlState({ silent: true });
        fetchControlMatrix({ silent: true });
    }, 900);
}

async function setHomeChannelFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const voiceChannelId = document.getElementById('control-voice-select')?.value;

    if (controlInventoryLoading) {
        setControlStatus('Wait for the selected bot inventory to finish loading before setting a home channel.', true);
        return;
    }
    if (!botKey || !guildId) {
        setControlStatus('Select a bot and guild before setting a home channel.', true);
        return;
    }
    if (!voiceChannelId) {
        setControlStatus('Select a voice channel to store as the bot home channel.', true);
        return;
    }

    setControlStatus('Writing home channel...');
    const result = await sendCommand(botKey, guildId, 'SET_HOME', {
        voice_channel_id: voiceChannelId,
    }, { delayedRefreshMs: 900 });
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update the home channel.', true);
        return;
    }

    setControlStatus(result.data?.message || 'Home channel updated.');
    setTimeout(() => {
        fetchSelectedControlState({ silent: true });
        fetchControlMatrix({ silent: true });
    }, 900);
}

function getDirectControlSelection(action) {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const needsGuild = action !== 'RESTART';

    if (controlInventoryLoading && action !== 'RESTART') {
        setControlStatus('Wait for the selected bot inventory to finish loading before sending guild-scoped commands.', true);
        return null;
    }
    if (!botKey) {
        setControlStatus('Select a bot first.', true);
        return null;
    }
    if (needsGuild && !guildId) {
        setControlStatus('Select a guild first.', true);
        return null;
    }

    return { botKey, guildId: guildId || '0' };
}

async function sendPanelAction(action) {
    const selection = getDirectControlSelection(action);
    if (!selection) return;

    if (action === 'CLEAR' && !confirm('Clear the queue and stop the current track for the selected guild?')) {
        return;
    }

    if (action === 'RESTART' && !confirm('Restart the selected bot? Active playback may pause briefly.')) {
        return;
    }

    const statusLabels = {
        PAUSE: 'Sending pause command...',
        RESUME: 'Sending resume command...',
        RECOVER: 'Requesting voice reconnect and queue recovery...',
        SKIP: 'Sending skip command...',
        STOP: 'Sending stop command...',
        LEAVE: 'Sending leave command...',
        SHUFFLE: 'Shuffling the queue...',
        CLEAR: 'Clearing the queue and current playback...',
        RESTART: 'Requesting bot restart...',
    };

    setControlStatus(statusLabels[action] || 'Sending command...');
    const result = await sendCommand(selection.botKey, selection.guildId, action, null, { delayedRefreshMs: 2200 });
    if (!result?.ok) {
        setControlStatus(result?.error || `Failed to run ${action}.`, true);
        return;
    }

    setControlStatus(result.data?.message || `${action} sent.`);
    setTimeout(() => {
        fetchSelectedControlState({ silent: true });
        fetchControlMatrix({ silent: true });
    }, 900);
}

// ================================
// 🗄️ DATABASE CONTROLS
// ================================
let dbSchemas = [];
let imageGalleryTables = [];

function galleryFormatBytes(bytes) {
    const value = Number(bytes || 0);
    if (value >= 1024 * 1024 * 1024) return `${(value / 1024 / 1024 / 1024).toFixed(1)} GB`;
    if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
    if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
    return `${value} B`;
}

async function loadImageGalleryAdmin() {
    if (!isImageGalleryOwnerSession()) return;
    const status = document.getElementById('image-gallery-admin-status');
    const summary = document.getElementById('image-gallery-summary');
    const usersBody = document.getElementById('image-gallery-users-body');
    const reportsBody = document.getElementById('image-gallery-reports-body');
    const commentsBody = document.getElementById('image-gallery-comments-body');
    const mediaBody = document.getElementById('image-gallery-media-body');
    const taxonomyBody = document.getElementById('image-gallery-taxonomy-body');
    if (!usersBody || !commentsBody || !mediaBody) return;
    if (status) status.textContent = 'Loading Image Gallery data...';
    try {
        const res = await fetch(`${API_BASE}/image-gallery/admin?limit=100`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Image Gallery request failed');
        const payload = data.data || {};
        const totals = payload.summary || {};
        const users = payload.users || [];
        const reports = payload.reports || [];
        const comments = payload.comments || [];
        const media = payload.media || [];
        const categories = payload.categories || [];
        const collections = payload.collections || [];
        if (summary) {
            summary.innerHTML = [
                ['Schema', payload.schema || 'image_gallery'],
                ['Users', totals.users ?? users.length],
                ['Media', totals.media ?? media.length],
                ['Comments', totals.comments ?? comments.length],
                ['Open Reports', totals.reports_open ?? reports.filter(report => report.status === 'open').length],
                ['Collections', totals.collections ?? collections.length],
            ].map(([label, value]) => `<div class="diagnostic-item"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(String(value))}</span></div>`).join('');
        }
        usersBody.innerHTML = users.length ? users.map(user => `
            <tr>
                <td>${escapeHtml(user.id)}</td>
                <td>${escapeHtml(user.display_name || user.username)}<div class="muted">@${escapeHtml(user.username)}</div></td>
                <td>${escapeHtml(user.email || 'No email')}<div class="muted">${user.email_verified_at ? 'verified' : user.email ? 'pending' : 'no email'}</div></td>
                <td>
                    <div>${user.public_profile ? 'public' : 'private'} profile</div>
                    <div class="muted">${user.age_verified_at ? 'age verified' : 'age unverified'} | ${user.adult_content_consent ? 'adult consent' : 'no adult consent'}</div>
                </td>
                <td>
                    <div>${escapeHtml(user.media_count || 0)} media | ${escapeHtml(user.comment_count || 0)} comments</div>
                    <div class="muted">${escapeHtml(user.bookmark_count || 0)} bookmarks | ${escapeHtml(user.collection_count || 0)} collections</div>
                </td>
                <td>${escapeHtml(user.last_login_at || 'Never')}</td>
                <td>
                    <button class="tbl-btn" data-gallery-reset-password="${escapeHtml(user.id)}">Reset Password</button>
                    <button class="tbl-btn" data-gallery-edit-username="${escapeHtml(user.id)}" data-gallery-current-username="${escapeHtml(user.username || '')}">Edit Username</button>
                    <button class="tbl-btn" data-gallery-edit-display="${escapeHtml(user.id)}" data-gallery-current-display="${escapeHtml(user.display_name || '')}">Edit Display</button>
                    <button class="tbl-btn" data-gallery-edit-email="${escapeHtml(user.id)}" data-gallery-current-email="${escapeHtml(user.email || '')}">Edit Email</button>
                    <button class="tbl-btn" data-gallery-resend-email="${escapeHtml(user.id)}" ${user.email && !user.email_verified_at ? '' : 'disabled'}>Resend Email</button>
                    <button class="tbl-btn" data-gallery-email-verified="${escapeHtml(user.id)}" data-gallery-verified="${user.email_verified_at ? '0' : '1'}">${user.email_verified_at ? 'Unverify Email' : 'Verify Email'}</button>
                    <button class="tbl-btn" data-gallery-age-verified="${escapeHtml(user.id)}" data-gallery-verified="${user.age_verified_at ? '0' : '1'}">${user.age_verified_at ? 'Revoke Age' : 'Verify Age'}</button>
                    <button class="tbl-btn" data-gallery-public-profile="${escapeHtml(user.id)}" data-gallery-public="${user.public_profile ? '0' : '1'}">${user.public_profile ? 'Make Private' : 'Make Public'}</button>
                    <button class="tbl-btn tbl-btn-stop" data-gallery-delete-user="${escapeHtml(user.id)}">Delete User</button>
                </td>
            </tr>
        `).join('') : '<tr><td colspan="7">No Image Gallery users found.</td></tr>';
        if (reportsBody) {
            reportsBody.innerHTML = reports.length ? reports.map(report => `
                <tr>
                    <td>${escapeHtml(report.id)}</td>
                    <td>${escapeHtml(report.display_name || report.username)}</td>
                    <td>${escapeHtml(report.media_title || `Media ${report.media_id}`)}</td>
                    <td>${escapeHtml(report.reason || '')}<div class="muted">${escapeHtml(report.details || '')}</div></td>
                    <td>${escapeHtml(report.status || 'open')}</td>
                    <td>${escapeHtml(report.created_at || '')}</td>
                    <td>
                        <button class="tbl-btn" data-gallery-report-status="${escapeHtml(report.id)}" data-gallery-status="open">Open</button>
                        <button class="tbl-btn" data-gallery-report-status="${escapeHtml(report.id)}" data-gallery-status="reviewed">Reviewed</button>
                        <button class="tbl-btn" data-gallery-report-status="${escapeHtml(report.id)}" data-gallery-status="dismissed">Dismiss</button>
                    </td>
                </tr>
            `).join('') : '<tr><td colspan="7">No Image Gallery reports found.</td></tr>';
        }
        commentsBody.innerHTML = comments.length ? comments.map(comment => `
            <tr>
                <td>${escapeHtml(comment.id)}</td>
                <td>${escapeHtml(comment.display_name || comment.username)}</td>
                <td>${escapeHtml(comment.media_title || `Media ${comment.media_id}`)}</td>
                <td>${escapeHtml(comment.body || '')}</td>
                <td>${escapeHtml(comment.created_at || '')}</td>
                <td><button class="tbl-btn tbl-btn-stop" data-gallery-delete-comment="${escapeHtml(comment.id)}">Delete Comment</button></td>
            </tr>
        `).join('') : '<tr><td colspan="6">No Image Gallery comments found.</td></tr>';
        mediaBody.innerHTML = media.length ? media.map(item => `
            <tr>
                <td>${escapeHtml(item.id)}</td>
                <td>${escapeHtml(item.username || item.user_id)}</td>
                <td>${escapeHtml(item.title || '')}</td>
                <td>${escapeHtml(item.media_kind || '')}</td>
                <td>${item.is_adult ? '18+' : 'clear'}<div class="muted">${escapeHtml(item.moderation_status || 'clear')} | ${galleryFormatBytes(item.file_size)}</div></td>
                <td>${escapeHtml(item.views || 0)}</td>
                <td>${escapeHtml(item.created_at || '')}</td>
                <td>
                    <button class="tbl-btn" data-gallery-media-adult="${escapeHtml(item.id)}" data-gallery-adult="${item.is_adult ? '0' : '1'}">${item.is_adult ? 'Clear 18+' : 'Mark 18+'}</button>
                    <button class="tbl-btn" data-gallery-media-title="${escapeHtml(item.id)}" data-gallery-current-title="${escapeHtml(item.title || '')}">Rename</button>
                    <button class="tbl-btn tbl-btn-stop" data-gallery-delete-media="${escapeHtml(item.id)}">Delete Media</button>
                </td>
            </tr>
        `).join('') : '<tr><td colspan="8">No Image Gallery media found.</td></tr>';
        if (taxonomyBody) {
            const categoryRows = categories.map(category => `
                <tr>
                    <td>Category</td>
                    <td>${escapeHtml(category.name || '')}<div class="muted">${escapeHtml(category.slug || '')}</div></td>
                    <td>${escapeHtml(category.media_kind || '')}</td>
                    <td>${escapeHtml(category.media_count || 0)}</td>
                    <td>${escapeHtml(category.created_at || '')}</td>
                </tr>
            `);
            const collectionRows = collections.map(collection => `
                <tr>
                    <td>Collection</td>
                    <td>${escapeHtml(collection.name || '')}<div class="muted">${collection.is_public ? 'public' : 'private'}</div></td>
                    <td>${escapeHtml(collection.username || collection.user_id || '')}</td>
                    <td>${escapeHtml(collection.item_count || 0)}</td>
                    <td>${escapeHtml(collection.created_at || '')}</td>
                </tr>
            `);
            taxonomyBody.innerHTML = [...categoryRows, ...collectionRows].join('') || '<tr><td colspan="5">No categories or collections found.</td></tr>';
        }
        if (status) status.textContent = 'Image Gallery data loaded.';
        await loadImageGalleryTables();
    } catch (err) {
        if (status) status.textContent = `Image Gallery Error: ${err instanceof Error ? err.message : String(err)}`;
    }
}

async function loadImageGalleryTables() {
    if (!isImageGalleryOwnerSession()) return;
    const tableSelect = document.getElementById('image-gallery-table-select');
    const status = document.getElementById('image-gallery-table-status');
    if (!tableSelect) return;
    if (status) status.textContent = 'Loading Image Gallery tables...';
    try {
        const res = await fetch(`${API_BASE}/image-gallery/tables`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Image Gallery table request failed');
        imageGalleryTables = data.tables || [];
        tableSelect.innerHTML = imageGalleryTables.length
            ? imageGalleryTables.map(table => `<option value="${escapeHtml(table.table_name)}">${escapeHtml(table.table_name)} (~${escapeHtml(table.estimated_rows ?? 0)} rows)</option>`).join('')
            : '<option value="">No tables found</option>';
        if (status) status.textContent = `${imageGalleryTables.length} Image Gallery tables loaded from ${data.schema || 'image_gallery'}.`;
    } catch (err) {
        if (status) status.textContent = `Image Gallery Tables Error: ${err instanceof Error ? err.message : String(err)}`;
    }
}

async function viewImageGalleryTableData() {
    if (!isImageGalleryOwnerSession()) return;
    const tableName = document.getElementById('image-gallery-table-select')?.value;
    const viewer = document.getElementById('image-gallery-table-viewer');
    const title = document.getElementById('image-gallery-table-title');
    const head = document.getElementById('image-gallery-table-head');
    const body = document.getElementById('image-gallery-table-body');
    const status = document.getElementById('image-gallery-table-status');
    if (!tableName) return;
    if (status) status.textContent = `Loading ${tableName} rows...`;
    try {
        const res = await fetch(`${API_BASE}/image-gallery/table-data?table_name=${encodeURIComponent(tableName)}&limit=100`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Image Gallery table data request failed');
        const rows = data.data?.rows || [];
        if (viewer) viewer.hidden = false;
        if (title) title.textContent = `${data.data?.schema || 'image_gallery'}.${tableName} - ${rows.length} rows`;
        if (!rows.length) {
            if (head) head.innerHTML = '';
            if (body) body.innerHTML = '<tr><td colspan="99">No rows found.</td></tr>';
            if (status) status.textContent = 'Table loaded with no rows.';
            return;
        }
        const columns = Object.keys(rows[0]);
        if (head) head.innerHTML = `<tr>${columns.map(column => `<th>${escapeHtml(column)}</th>`).join('')}</tr>`;
        if (body) {
            body.innerHTML = rows.map(row => `<tr>${columns.map(column => `<td>${escapeHtml(row[column] ?? '')}</td>`).join('')}</tr>`).join('');
        }
        if (status) status.textContent = `Loaded ${rows.length} rows from ${tableName}.`;
    } catch (err) {
        if (status) status.textContent = `Image Gallery Table Error: ${err instanceof Error ? err.message : String(err)}`;
    }
}

async function postImageGalleryAdminAction(path, payload) {
    if (!isImageGalleryOwnerSession()) return false;
    const res = await fetch(`${API_BASE}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    if (handle401(res)) return false;
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || 'Image Gallery action failed');
    await loadImageGalleryAdmin();
    return true;
}

async function loadDbSchemas() {
    const schemaSel = document.getElementById('schema-select');
    const status = document.getElementById('db-status');
    if (!schemaSel) return;
    try {
        const res = await fetch(`${API_BASE}/databases?include_tables=true`);
        if (handle401(res)) return;
        const data = await res.json();
        dbSchemas = data.schemas || [];
        schemaSel.innerHTML = dbSchemas
            .map(s => `<option value="${s.schema}">${s.schema}</option>`)
            .join('');
        updateTableSelect();
    } catch (err) {
        if (status) status.textContent = `DB Error: ${err}`;
        console.error("❌ Failed to load DB schemas:", err);
    }
}

function updateTableSelect() {
    const schemaSel = document.getElementById('schema-select');
    const tableSel = document.getElementById('table-select');
    if (!schemaSel || !tableSel) return;
    const schemaObj = dbSchemas.find(s => s.schema === schemaSel.value);
    const tables = schemaObj ? schemaObj.tables : [];
    tableSel.innerHTML = tables
        .map(t => `<option value="${t.table_name}">${t.table_name} (~${t.estimated_rows} rows)</option>`)
        .join('');
    updateConfirmText();
}

function updateConfirmText() {
    const schema = document.getElementById('schema-select')?.value;
    const table = document.getElementById('table-select')?.value;
    const tableConfirmEl = document.getElementById('confirm-table-text');
    const schemaConfirmEl = document.getElementById('confirm-schema-text');
    if (tableConfirmEl) tableConfirmEl.textContent = schema && table ? `TRUNCATE ${schema}.${table}` : '—';
    if (schemaConfirmEl) schemaConfirmEl.textContent = schema ? `TRUNCATE ALL ${schema}` : '—';
}

async function viewTableData() {
    const schema = document.getElementById('schema-select')?.value;
    const table = document.getElementById('table-select')?.value;
    const container = document.getElementById('data-viewer-container');
    const titleEl = document.getElementById('data-viewer-title');
    const head = document.getElementById('data-table-head');
    const body = document.getElementById('data-table-body');
    const status = document.getElementById('db-status');
    if (!schema || !table) return;

    try {
        const res = await fetch(`${API_BASE}/database/data?schema_name=${encodeURIComponent(schema)}&table_name=${encodeURIComponent(table)}&limit=100`);
        if (handle401(res)) return;
        const data = await res.json();
        const rows = data.data?.rows || [];

        if (container) container.style.display = 'block';
        if (titleEl) titleEl.textContent = `${schema}.${table} — ${rows.length} rows`;

        if (rows.length === 0) {
            if (head) head.innerHTML = '';
            if (body) body.innerHTML = '<tr><td colspan="99">No rows found.</td></tr>';
            return;
        }

        const cols = Object.keys(rows[0]);
        if (head) head.innerHTML = `<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>`;
        if (body) body.innerHTML = rows.map(row =>
            `<tr>${cols.map(c => `<td>${row[c] ?? ''}</td>`).join('')}</tr>`
        ).join('');

    } catch (err) {
        if (status) status.textContent = `Error loading data: ${err}`;
    }
}

async function truncateTable() {
    const schema = document.getElementById('schema-select')?.value;
    const table = document.getElementById('table-select')?.value;
    const status = document.getElementById('db-status');
    if (!schema || !table) return;

    const confirmText = `TRUNCATE ${schema}.${table}`;
    const input = prompt(`⚠️ This is irreversible. Type exactly to confirm:\n\n${confirmText}`);
    if (input !== confirmText) {
        if (status) status.textContent = 'Cancelled — confirmation did not match.';
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/database/truncate-table`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ schema_name: schema, table_name: table, confirm_text: input })
        });
        if (handle401(res)) return;
        const data = await res.json();
        if (status) status.textContent = data.message || (data.ok ? '✅ Done.' : '❌ Failed.');
        loadDbSchemas();
    } catch (err) {
        if (status) status.textContent = `Error: ${err}`;
    }
}

async function truncateSchema() {
    const schema = document.getElementById('schema-select')?.value;
    const status = document.getElementById('db-status');
    if (!schema) return;

    const confirmText = `TRUNCATE ALL ${schema}`;
    const input = prompt(`⚠️ This will wipe ALL tables in ${schema}. Type exactly to confirm:\n\n${confirmText}`);
    if (input !== confirmText) {
        if (status) status.textContent = 'Cancelled — confirmation did not match.';
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/database/truncate-schema`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ schema_name: schema, confirm_text: input })
        });
        if (handle401(res)) return;
        const data = await res.json();
        if (status) status.textContent = data.ok
            ? `✅ Truncated ${data.truncated_tables} tables in ${schema}.`
            : '❌ Failed.';
        loadDbSchemas();
    } catch (err) {
        if (status) status.textContent = `Error: ${err}`;
    }
}

// ================================
// 🔌 WIRE UP ALL BUTTONS
// ================================
document.addEventListener('DOMContentLoaded', () => {
    if (REMOTE_MODE) {
        document.body.classList.add('remote-pages-body');
        updateRemoteConnectionStatus('GitHub Pages remote mode', 'idle');
    }

    document.getElementById('refresh-dashboard')
        ?.addEventListener('click', fetchDashboard);

    document.getElementById('refresh-invite-catalog')
        ?.addEventListener('click', () => {
            loadBotSelect();
            fetchDashboard();
        });

    document.getElementById('refresh-user-directory')
        ?.addEventListener('click', () => loadUserDirectory());

    document.getElementById('save-user-profile')
        ?.addEventListener('click', saveUserProfile);
    document.getElementById('save-panel-email')
        ?.addEventListener('click', savePanelEmailAndSendCode);
    document.getElementById('verify-panel-email-code')
        ?.addEventListener('click', verifyPanelEmailCode);
    document.getElementById('resend-panel-email-verification')
        ?.addEventListener('click', resendPanelEmailVerification);

    document.getElementById('save-panel-preferences')
        ?.addEventListener('click', () => savePanelPreferences());

    document.getElementById('reset-panel-preferences')
        ?.addEventListener('click', resetPanelPreferences);

    document.getElementById('user-search-button')
        ?.addEventListener('click', () => loadUserDirectory());

    document.getElementById('user-search-input')
        ?.addEventListener('keydown', event => {
            if (event.key === 'Enter') loadUserDirectory();
        });

    [
        'profile-display-name',
        'profile-avatar-url',
        'profile-server-name',
        'profile-server-icon-url',
        'profile-server-invite-url',
        'profile-favorite-bot',
        'profile-theme-accent',
        'profile-public-profile',
        'profile-bio',
    ].forEach(id => {
        document.getElementById(id)?.addEventListener('input', () => {
            const profile = {
                ...(userProfileState.profile || {}),
                display_name: document.getElementById('profile-display-name')?.value || '',
                avatar_url: document.getElementById('profile-avatar-url')?.value || '',
                server_name: document.getElementById('profile-server-name')?.value || '',
                server_icon_url: document.getElementById('profile-server-icon-url')?.value || '',
                server_invite_url: document.getElementById('profile-server-invite-url')?.value || '',
                favorite_bot: document.getElementById('profile-favorite-bot')?.value || '',
                theme_accent: document.getElementById('profile-theme-accent')?.value || '#89b4fa',
                public_profile: Boolean(document.getElementById('profile-public-profile')?.checked),
                bio: document.getElementById('profile-bio')?.value || '',
            };
            renderUserProfile({ ...userProfileState, profile });
        });
    });

    [
        'panel-accent-color',
        'panel-background-mode',
        'panel-background-color',
        'panel-background-image-url',
        'panel-layout-mode',
        'panel-density',
        'panel-card-shape',
        'panel-font-scale',
        'panel-motion',
    ].forEach(id => {
        const eventName = id === 'panel-background-image-url' ? 'input' : 'change';
        document.getElementById(id)?.addEventListener(eventName, () => {
            renderPanelPreferenceInputs(getPanelPreferencesFromInputs());
            setPanelPreferencesStatus('Previewing changes. Save to keep them.');
        });
    });

    document.getElementById('refresh-diagnostics')
        ?.addEventListener('click', () => fetchDiagnostics(true));

    document.getElementById('refresh-event-feed')
        ?.addEventListener('click', loadEventFeedHistory);

    document.getElementById('refresh-metrics')
        ?.addEventListener('click', fetchMetrics);

    document.getElementById('load-inventory')
        ?.addEventListener('click', loadInventory);
    document.getElementById('inventory-search')
        ?.addEventListener('input', renderInventoryBrowser);
    document.getElementById('inventory-sort')
        ?.addEventListener('change', renderInventoryBrowser);

    document.getElementById('control-bot-select')
        ?.addEventListener('change', (event) => loadControlInventory(event.target.value));

    document.getElementById('control-guild-select')
        ?.addEventListener('change', () => {
            populateControlChannels();
            fetchSelectedControlState({ silent: true });
            fetchControlMatrix({ silent: true });
        });

    document.getElementById('control-voice-select')
        ?.addEventListener('change', () => {
            renderControlContext();
            renderAriaCommandGuide();
            renderSelectedBotCapabilities();
        });

    document.getElementById('control-source-input')
        ?.addEventListener('input', renderAriaCommandGuide);

    document.getElementById('queue-from-panel')
        ?.addEventListener('click', queueFromPanel);

    document.getElementById('apply-loop-mode')
        ?.addEventListener('click', applyLoopModeFromPanel);

    document.getElementById('apply-filter-mode')
        ?.addEventListener('click', applyFilterModeFromPanel);

    document.getElementById('set-home-channel')
        ?.addEventListener('click', setHomeChannelFromPanel);

    document.querySelectorAll('[data-panel-action]')
        .forEach(button => button.addEventListener('click', () => sendPanelAction(button.dataset.panelAction)));

    document.getElementById('refresh-db')
        ?.addEventListener('click', loadDbSchemas);

    document.getElementById('refresh-image-gallery-admin')
        ?.addEventListener('click', loadImageGalleryAdmin);
    document.getElementById('refresh-image-gallery-tables')
        ?.addEventListener('click', loadImageGalleryTables);
    document.getElementById('view-image-gallery-table')
        ?.addEventListener('click', viewImageGalleryTableData);

    document.getElementById('image-gallery-tab')
        ?.addEventListener('click', async (event) => {
            if (!isImageGalleryOwnerSession()) return;
            const deleteUser = event.target.closest('[data-gallery-delete-user]');
            const deleteComment = event.target.closest('[data-gallery-delete-comment]');
            const resetPassword = event.target.closest('[data-gallery-reset-password]');
            const editUsername = event.target.closest('[data-gallery-edit-username]');
            const editDisplay = event.target.closest('[data-gallery-edit-display]');
            const editEmail = event.target.closest('[data-gallery-edit-email]');
            const resendEmail = event.target.closest('[data-gallery-resend-email]');
            const emailVerified = event.target.closest('[data-gallery-email-verified]');
            const ageVerified = event.target.closest('[data-gallery-age-verified]');
            const publicProfile = event.target.closest('[data-gallery-public-profile]');
            const deleteMedia = event.target.closest('[data-gallery-delete-media]');
            const mediaAdult = event.target.closest('[data-gallery-media-adult]');
            const mediaTitle = event.target.closest('[data-gallery-media-title]');
            const reportStatus = event.target.closest('[data-gallery-report-status]');
            try {
                if (deleteUser) {
                    const userId = deleteUser.dataset.galleryDeleteUser;
                    if (confirm(`Delete Image Gallery user ${userId}? This removes their media, comments, likes, and collections.`)) {
                        await postImageGalleryAdminAction('/image-gallery/users/delete', { user_id: Number(userId) });
                    }
                }
                if (deleteComment) {
                    const commentId = deleteComment.dataset.galleryDeleteComment;
                    if (confirm(`Delete Image Gallery comment ${commentId}?`)) {
                        await postImageGalleryAdminAction('/image-gallery/comments/delete', { comment_id: Number(commentId) });
                    }
                }
                if (resetPassword) {
                    const userId = resetPassword.dataset.galleryResetPassword;
                    const password = prompt(`Enter a new Image Gallery password for user ${userId}. Minimum 8 characters.`);
                    if (password) {
                        await postImageGalleryAdminAction('/image-gallery/users/reset-password', { user_id: Number(userId), new_password: password });
                    }
                }
                if (editUsername) {
                    const userId = editUsername.dataset.galleryEditUsername;
                    const current = editUsername.dataset.galleryCurrentUsername || '';
                    const username = prompt(`Set Image Gallery username for user ${userId}.`, current);
                    if (username) {
                        await postImageGalleryAdminAction('/image-gallery/users/update', { user_id: Number(userId), username: username.trim() });
                    }
                }
                if (editDisplay) {
                    const userId = editDisplay.dataset.galleryEditDisplay;
                    const current = editDisplay.dataset.galleryCurrentDisplay || '';
                    const displayName = prompt(`Set Image Gallery display name for user ${userId}. Leave blank to clear it.`, current);
                    if (displayName !== null) {
                        await postImageGalleryAdminAction('/image-gallery/users/update', { user_id: Number(userId), display_name: displayName.trim() || null });
                    }
                }
                if (editEmail) {
                    const userId = editEmail.dataset.galleryEditEmail;
                    const current = editEmail.dataset.galleryCurrentEmail || '';
                    const email = prompt(`Set Image Gallery email for user ${userId}. Leave blank to clear it.`, current);
                    if (email !== null) {
                        await postImageGalleryAdminAction('/image-gallery/users/update', { user_id: Number(userId), email: email.trim() || null });
                    }
                }
                if (resendEmail) {
                    const userId = resendEmail.dataset.galleryResendEmail;
                    await postImageGalleryAdminAction('/image-gallery/users/resend-verification', { user_id: Number(userId) });
                }
                if (emailVerified) {
                    const userId = emailVerified.dataset.galleryEmailVerified;
                    const verified = emailVerified.dataset.galleryVerified === '1';
                    await postImageGalleryAdminAction('/image-gallery/users/email-verified', { user_id: Number(userId), verified });
                }
                if (ageVerified) {
                    const userId = ageVerified.dataset.galleryAgeVerified;
                    const verified = ageVerified.dataset.galleryVerified === '1';
                    await postImageGalleryAdminAction('/image-gallery/users/age-verified', { user_id: Number(userId), verified });
                }
                if (publicProfile) {
                    const userId = publicProfile.dataset.galleryPublicProfile;
                    const nextPublic = publicProfile.dataset.galleryPublic === '1';
                    await postImageGalleryAdminAction('/image-gallery/users/update', { user_id: Number(userId), public_profile: nextPublic });
                }
                if (deleteMedia) {
                    const mediaId = deleteMedia.dataset.galleryDeleteMedia;
                    if (confirm(`Delete Image Gallery media ${mediaId}? This removes the database row and related likes/comments/reports.`)) {
                        await postImageGalleryAdminAction('/image-gallery/media/delete', { media_id: Number(mediaId) });
                    }
                }
                if (mediaAdult) {
                    const mediaId = mediaAdult.dataset.galleryMediaAdult;
                    const isAdult = mediaAdult.dataset.galleryAdult === '1';
                    await postImageGalleryAdminAction('/image-gallery/media/update', { media_id: Number(mediaId), is_adult: isAdult, moderation_status: isAdult ? 'review' : 'clear' });
                }
                if (mediaTitle) {
                    const mediaId = mediaTitle.dataset.galleryMediaTitle;
                    const current = mediaTitle.dataset.galleryCurrentTitle || '';
                    const title = prompt(`Rename Image Gallery media ${mediaId}.`, current);
                    if (title) {
                        await postImageGalleryAdminAction('/image-gallery/media/update', { media_id: Number(mediaId), title });
                    }
                }
                if (reportStatus) {
                    await postImageGalleryAdminAction('/image-gallery/reports/status', {
                        report_id: Number(reportStatus.dataset.galleryReportStatus),
                        status: reportStatus.dataset.galleryStatus,
                    });
                }
            } catch (err) {
                const status = document.getElementById('image-gallery-admin-status');
                if (status) status.textContent = err instanceof Error ? err.message : String(err);
            }
        });

    document.getElementById('view-table-data')
        ?.addEventListener('click', viewTableData);

    document.getElementById('truncate-table')
        ?.addEventListener('click', truncateTable);

    document.getElementById('truncate-schema')
        ?.addEventListener('click', truncateSchema);

    document.getElementById('schema-select')
        ?.addEventListener('change', () => { updateTableSelect(); updateConfirmText(); });

    document.getElementById('table-select')
        ?.addEventListener('change', updateConfirmText);

    document.getElementById('topbar-profile-button')
        ?.addEventListener('click', (event) => {
            event.stopPropagation();
            toggleAccountDropdown();
        });

    document.getElementById('admin-mode-toggle')
        ?.addEventListener('change', (event) => setAdminMode(event.target.checked));

    document.querySelectorAll('[data-account-tab]')
        .forEach(button => button.addEventListener('click', () => {
            activateSwarmTab(button.dataset.accountTab);
            setAccountDropdownOpen(false);
        }));

    document.addEventListener('click', (event) => {
        const menu = document.getElementById('account-menu');
        if (menu && !menu.contains(event.target)) setAccountDropdownOpen(false);
    });

    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') setAccountDropdownOpen(false);
    });

    document.getElementById('remote-login-button')
        ?.addEventListener('click', loginRemotePanel);

    document.getElementById('remote-register-button')
        ?.addEventListener('click', registerRemotePanel);

    document.getElementById('logout-button')
        ?.addEventListener('click', logoutPanel);

    updateTopbarAccount();
    ensureLivePositionTicker();
    bootstrapPanelApplication();
});

// ================================
// 🔄 AUTO REFRESH
// ================================
async function dashboardRefreshLoop() {
    if (panelAppStarted) await fetchDashboard();
    dashboardRefreshTimer = setTimeout(dashboardRefreshLoop, 5000);
}

async function diagnosticsRefreshLoop() {
    if (panelAppStarted && isAdminSession()) await fetchDiagnostics();
    diagnosticsRefreshTimer = setTimeout(diagnosticsRefreshLoop, 60000);
}

async function metricsRefreshLoop() {
    if (panelAppStarted && isAdminSession()) await fetchMetrics();
    metricsRefreshTimer = setTimeout(metricsRefreshLoop, 15000);
}

dashboardRefreshLoop();
diagnosticsRefreshLoop();
metricsRefreshLoop();
setInterval(() => {
    renderLivePositionTick();
}, 1000);
