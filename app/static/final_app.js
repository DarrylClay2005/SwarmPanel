// ================================
// 🔧 CONFIG
// ================================
const API_BASE = "/api";
let controlCooldown = false;
let dashboardBotsState = [];
let botCatalogState = [];
let controlInventoryState = null;
let controlMatrixState = { guildId: null, bots: [], loaded: false, generatedAt: null };
let controlInventoryLoading = false;
let controlInventoryRequestId = 0;
let controlMatrixRequestId = 0;
let eventFeedEntries = [];
let eventFeedSocket = null;
let eventFeedReconnectTimer = null;
let eventFeedConnectionState = 'offline';
let systemDiagnosticsState = null;
let controlRefreshTimer = null;
const MAX_EVENT_FEED_ENTRIES = 80;
const RUNTIME_SESSION_STATES = new Set(['playing', 'paused', 'queued']);

// ================================
// 🔒 AUTH HELPER
// ================================
function handle401(res) {
    if (res.status === 401) {
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

function clearControlMatrixState(guildId = null) {
    controlMatrixState = { guildId: guildId ? String(guildId) : null, bots: [], loaded: false, generatedAt: null };
}

function getLiveControlBot(botKey, guildId = null) {
    const selectedGuildId = guildId ?? document.getElementById('control-guild-select')?.value;
    if (!botKey || !selectedGuildId) return null;
    if (!controlMatrixState.loaded || String(controlMatrixState.guildId) !== String(selectedGuildId)) return null;
    return controlMatrixState.bots.find(bot => bot.key === botKey) || null;
}

function getBestControlSession(botKey, guildId = null) {
    const liveBot = getLiveControlBot(botKey, guildId);
    if (liveBot?.session) return liveBot.session;
    return getDashboardSession(botKey, guildId);
}

function getControlBotStatus(botKey, guildId = null) {
    const dashboardBot = getDashboardBot(botKey);
    if (dashboardBot) return describeBotStatus(dashboardBot.status);

    const heartbeatStatus = String(getLiveControlBot(botKey, guildId)?.heartbeat?.status || '').toLowerCase();
    if (heartbeatStatus === 'online') return describeBotStatus('online');
    if (heartbeatStatus === 'stale') return describeBotStatus('stale');
    if (heartbeatStatus === 'offline' || heartbeatStatus === 'error') return describeBotStatus('offline');
    return describeBotStatus('unknown');
}

function getControlHeartbeatAge(botKey, guildId = null) {
    const liveBot = getLiveControlBot(botKey, guildId);
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

// ================================
// 📡 FETCH DASHBOARD DATA
// ================================
async function fetchDashboard() {
    try {
        const res = await fetch(`${API_BASE}/dashboard`);
        if (handle401(res)) return;
        const data = await res.json();

        const bots = data.bots || [];
        dashboardBotsState = bots;

        renderOverview(bots, data.generated_at);
        renderBots(bots);

        let allSessions = [];
        bots.forEach(bot => {
            if (Array.isArray(bot.sessions)) {
                bot.sessions.forEach(session => {
                    allSessions.push({
                        ...session,
                        bot_key: bot.key,
                        bot_display: bot.display_name
                    });
                });
            }
        });

        renderSessions(allSessions.filter(session => isRuntimeSession(session)));
        renderNowPlaying(allSessions);
        syncControlSelectionsFromDashboard();
        renderControlContext();
        renderAriaCommandGuide();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        if (document.getElementById('control-guild-select')?.value && !controlInventoryLoading) {
            fetchControlMatrix({ silent: true });
        }

        const meta = document.getElementById('dashboard-meta');
        if (meta && data.generated_at) {
            meta.textContent = `Last updated: ${new Date(data.generated_at).toLocaleTimeString()}`;
        }

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
    } catch (err) {
        systemDiagnosticsState = null;
        renderControlContext();
        renderSelectedBotCapabilities();
        renderSelectedGuildMatrix();
        console.error('❌ Diagnostics fetch failed:', err);
    }
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
            detail: 'Database reachability using each bot runtime credential set.',
            tone: botsDbOnline === bots.length && bots.length ? 'online' : botsDbOnline ? 'stale' : 'offline',
        },
        {
            label: 'Discord Inventory',
            value: `${botsDiscordReady}/${bots.length}`,
            detail: 'Panel-side Discord token readiness for live inventory and name resolution.',
            tone: botsDiscordReady === bots.length && bots.length ? 'online' : botsDiscordReady ? 'stale' : 'offline',
        },
        {
            label: 'Aria Intelligence',
            value: describeDiagnosticState(ariaGemini?.status).label,
            detail: ariaDb?.reachable
                ? (ariaGemini?.message || 'Aria DB online and Gemini diagnostics complete.')
                : (ariaDb?.message || 'Aria DB not reachable.'),
            tone: describeDiagnosticState(ariaGemini?.status).tone,
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
            ${sharedEnv.last_modified ? `<div class="diagnostic-item-meta">Updated ${escapeHtml(new Date(sharedEnv.last_modified).toLocaleString())}</div>` : ''}
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Panel database</span>
                ${diagnosticBadge(panelDb.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(panelDb.message || 'No panel DB probe available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(panelDb.database || 'Unknown schema')} on ${escapeHtml(panelDb.host || 'unknown host')}</div>
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
    const operatorActions = Array.isArray(aria.operator_actions) ? aria.operator_actions : [];

    container.innerHTML = `
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria primary database</span>
                ${diagnosticBadge(db.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(db.message || 'No DB diagnostics available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(db.database || 'discord_aria')} on ${escapeHtml(db.host || 'unknown host')}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria swarm bridge database</span>
                ${diagnosticBadge(swarmDb.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(swarmDb.message || 'No swarm DB diagnostics available.')}</div>
            <div class="diagnostic-item-meta">${escapeHtml(swarmDb.database || 'unknown schema')} on ${escapeHtml(swarmDb.host || 'unknown host')}</div>
        </div>
        <div class="diagnostic-item">
            <div class="diagnostic-item-head">
                <span>Aria Gemini key</span>
                ${diagnosticBadge(gemini.status)}
            </div>
            <div class="diagnostic-item-body">${escapeHtml(gemini.message || 'No Gemini diagnostics available.')}</div>
            <div class="diagnostic-item-meta">Model: ${escapeHtml(gemini.model || 'unknown')} | SDK installed: ${gemini.sdk_installed ? 'yes' : 'no'} | Key present: ${env.gemini_key_present ? 'yes' : 'no'}</div>
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

    container.innerHTML = bots.map(bot => `
        <article class="worker-diagnostic-card">
            <div class="worker-diagnostic-head">
                <div>
                    <div class="worker-diagnostic-name">${escapeHtml(bot.display_name || bot.key)}</div>
                    <div class="worker-diagnostic-meta">Shared env token ${bot.env?.shared_token_present ? 'present' : 'missing'}</div>
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
            <div class="worker-diagnostic-chip-row">
                <span class="worker-chip ${bot.env?.shared_db_password_present ? 'worker-chip-ok' : 'worker-chip-bad'}">DB secret ${bot.env?.shared_db_password_present ? 'present' : 'missing'}</span>
                <span class="worker-chip ${bot.env?.shared_lavalink_password_present ? 'worker-chip-ok' : 'worker-chip-bad'}">Lavalink ${bot.env?.shared_lavalink_password_present ? 'present' : 'missing'}</span>
                <span class="worker-chip ${bot.env?.panel_token_present ? 'worker-chip-ok' : 'worker-chip-bad'}">Panel token ${bot.env?.panel_token_present ? 'present' : 'missing'}</span>
            </div>
        </article>
    `).join('');
}

// ================================
// 🎵 RENDER NOW-PLAYING CARDS
// ================================
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
        const pos = formatDuration(s.position_seconds);
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
                        ${pos}
                    </span>
                    ${sourceBadge}
                    ${s.filter_mode && s.filter_mode !== 'none' ? `<span class="np-stat np-filter">${s.filter_mode}</span>` : ''}
                    ${s.loop_mode && s.loop_mode !== 'off' ? `<span class="np-stat np-loop">loop:${s.loop_mode}</span>` : ''}
                    ${s.queue_count > 0 ? `<span class="np-stat">+${s.queue_count} queued</span>` : ''}
                </div>
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
                <span class="bot-meta-pill">${bot.heartbeat_status || 'unknown'}</span>
            </div>
            ${bot.kind !== 'orchestrator' ? `
            <div class="bot-actions">
                <button class="bot-action-btn bot-btn-restart"
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
        table.innerHTML = '<tr><td colspan="10">No live or queued worker sessions right now.</td></tr>';
        return;
    }

    sessions.forEach(session => {
        const stateMeta = describeSessionState(session);
        const channelLabel = getSessionChannelLabel(session);
        const trackLabel = session.title || (stateMeta.key === 'queued' ? 'Queued media awaiting worker pickup' : '—');
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${session.bot_display}</td>
            <td>${session.guild_name || session.guild_id}</td>
            <td>${channelLabel}</td>
            <td>${stateMeta.icon} ${stateMeta.label}</td>
            <td>${trackLabel}${session.media_source_label ? ` <span class="tbl-source-badge tbl-source-${session.media_source || 'unknown'}">${session.media_source_label}</span>` : ""}</td>
            <td>${session.filter_mode || "none"}</td>
            <td>${normalizeLoopMode(session.loop_mode)}</td>
            <td>${session.queue_count || 0}</td>
            <td>${formatDuration(session.position_seconds)}</td>
            <td>
                <button class="tbl-btn" data-action="PAUSE"  data-bot="${session.bot_key}" data-guild="${session.guild_id}">Pause</button>
                <button class="tbl-btn" data-action="RESUME" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Resume</button>
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
    if (controlCooldown) return;
    controlCooldown = true;
    const { refresh = true } = options;

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

        if (refresh) fetchDashboard();
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
    const payload = btn.dataset.payload || null;
    if (action === 'CLEAR' && !confirm('Clear the queue and stop the current track for this guild?')) return;
    if (action === 'RESTART' && !confirm('Restart this bot node? Active playback may pause briefly.')) return;
    if (action && bot) sendCommand(bot, guild, action, payload);
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
        const allBots = data.bots || [];
        botCatalogState = allBots.filter(b => b.kind === 'music');
        const inventoryOptionsHtml = allBots
            .map(b => `<option value="${b.key}">${b.display_name}</option>`)
            .join('');
        const controlOptionsHtml = botCatalogState
            .map(b => `<option value="${b.key}">${b.display_name}</option>`)
            .join('');
        sel.innerHTML = inventoryOptionsHtml;
        if (controlSel) controlSel.innerHTML = controlOptionsHtml;
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
    }
}

async function loadInventory() {
    const sel = document.getElementById('bot-select');
    const out = document.getElementById('inventory-output');
    if (!sel || !out) return;
    out.textContent = 'Loading...';
    try {
        const res = await fetch(`${API_BASE}/bots/${sel.value}/inventory`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            out.textContent = `Inventory load failed: ${data.detail || 'Unknown error'}`;
            return;
        }
        out.textContent = formatInventoryOutput(data);
    } catch (err) {
        out.textContent = `Error: ${err}`;
    }
}

function formatInventoryOutput(data) {
    const lines = [];
    const botName = data.bot?.display_name || data.bot?.key || 'Unknown Bot';
    const identity = data.identity || {};
    const guilds = Array.isArray(data.guilds) ? [...data.guilds] : [];
    const errors = Array.isArray(data.errors) ? data.errors : [];

    lines.push(`Bot: ${botName}`);
    if (identity.username) {
        lines.push(`Identity: ${identity.username}${identity.global_name ? ` (${identity.global_name})` : ''} [${identity.id || 'unknown id'}]`);
    } else {
        lines.push('Identity: unavailable');
    }
    lines.push(`Guilds visible: ${guilds.length}`);

    if (errors.length) {
        lines.push('');
        lines.push('API warnings:');
        errors.forEach(error => lines.push(`- ${error}`));
    }

    if (!guilds.length) {
        lines.push('');
        lines.push('No guilds were returned for this bot.');
        return lines.join('\n');
    }

    guilds
        .sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')))
        .forEach(guild => {
            const channels = Array.isArray(guild.channels) ? [...guild.channels] : [];
            const sortedChannels = channels.sort((a, b) => {
                const typeCompare = String(a.type_name || '').localeCompare(String(b.type_name || ''));
                return typeCompare || String(a.name || '').localeCompare(String(b.name || ''));
            });

            const textCount = sortedChannels.filter(channel => channel.type === 0 || channel.type === 5).length;
            const voiceCount = sortedChannels.filter(channel => channel.type === 2 || channel.type === 13).length;

            lines.push('');
            lines.push(`${guild.name} (${guild.id})`);
            lines.push(`  Text: ${textCount} | Voice/Stage: ${voiceCount} | Total Channels: ${sortedChannels.length}`);

            if (guild.channels_error) {
                lines.push(`  Channel lookup warning: ${guild.channels_error}`);
            }

            if (!sortedChannels.length) {
                lines.push('  No channels returned.');
                return;
            }

            sortedChannels.forEach(channel => {
                const parentSuffix = channel.parent_id ? ` | parent ${channel.parent_id}` : '';
                lines.push(`  - [${channel.type_name || channel.type}] ${channel.name} (${channel.id})${parentSuffix}`);
            });
        });

    return lines.join('\n');
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

    const connectionLabel = {
        connecting: 'connecting',
        online: 'live',
        offline: 'offline',
    }[eventFeedConnectionState] || eventFeedConnectionState;

    const header = `Feed: ${connectionLabel}`;
    if (!eventFeedEntries.length) {
        out.textContent = `${header}\n\nNo live events yet.`;
        renderOverview(dashboardBotsState);
        return;
    }

    const lines = eventFeedEntries
        .slice()
        .reverse()
        .map(entry => {
            const ts = entry.timestamp ? new Date(entry.timestamp).toLocaleTimeString() : '--:--:--';
            const level = String(entry.level || 'info').toUpperCase();
            const source = entry.source ? ` | ${entry.source}` : '';
            const description = entry.description ? `\n${entry.description}` : '';
            return `[${ts}] ${level}${source} :: ${entry.title}${description}`;
        });

    out.textContent = `${header}\n\n${lines.join('\n\n')}`;
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

function connectEventFeed() {
    if (eventFeedSocket && (eventFeedSocket.readyState === WebSocket.OPEN || eventFeedSocket.readyState === WebSocket.CONNECTING)) {
        return;
    }
    if (eventFeedReconnectTimer) {
        clearTimeout(eventFeedReconnectTimer);
        eventFeedReconnectTimer = null;
    }

    eventFeedConnectionState = 'connecting';
    renderEventFeed();

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    eventFeedSocket = new WebSocket(`${protocol}//${window.location.host}/ws`);

    eventFeedSocket.onopen = () => {
        eventFeedConnectionState = 'online';
        renderEventFeed();
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
        renderEventFeed();
    };

    eventFeedSocket.onclose = () => {
        eventFeedConnectionState = 'offline';
        renderEventFeed();
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

function getSelectedControlGuild() {
    const guildId = document.getElementById('control-guild-select')?.value;
    return controlInventoryState?.guilds?.find(guild => String(guild.id) === String(guildId)) || null;
}

function populateControlGuilds() {
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

    if (previousValue && guilds.some(guild => String(guild.id) === previousValue)) {
        guildSel.value = previousValue;
    }

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

function deriveBotDbAccess(botKey) {
    const liveBot = getLiveControlBot(botKey);
    if (liveBot?.db) {
        return { ...liveBot.db, source: 'control-matrix' };
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

function deriveBotDiscordAccess(botKey) {
    const liveBot = getLiveControlBot(botKey);
    if (liveBot?.discord) {
        return { ...liveBot.discord, source: 'control-matrix' };
    }

    const diagnostics = getDiagnosticsBot(botKey);
    if (diagnostics?.discord) {
        return { ...diagnostics.discord, source: 'diagnostics' };
    }

    if (controlInventoryState?.loaded && controlInventoryState?.bot?.key === botKey && Array.isArray(controlInventoryState.guilds)) {
        const guildCount = controlInventoryState.guilds.length;
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
    const bot = getDashboardBot(botKey) || getLiveControlBot(botKey, guildId) || null;
    const session = botKey && guildId ? getBestControlSession(botKey, guildId) : null;
    const dbAccess = deriveBotDbAccess(botKey);
    const discordAccess = deriveBotDiscordAccess(botKey);
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

    const bot = getDashboardBot(botKey) || getLiveControlBot(botKey, guildId) || null;
    const dbAccess = deriveBotDbAccess(botKey);
    const discordAccess = deriveBotDiscordAccess(botKey);
    const session = guildId ? getBestControlSession(botKey, guildId) : null;
    const selectedGuild = getSelectedControlGuild();
    const dbReady = Boolean(dbAccess.reachable);
    const discordReady = Boolean(discordAccess.reachable);
    const inventoryReady = !controlInventoryLoading && Boolean(controlInventoryState?.loaded) && controlInventoryState?.bot?.key === botKey;
    const hasGuild = Boolean(guildId && selectedGuild);
    const hasVoice = Boolean(
        voiceChannelId
        && selectedGuild?.channels?.some(channel =>
            String(channel.id) === String(voiceChannelId) && (channel.type === 2 || channel.type === 13)
        )
    );
    const hasHome = Boolean(session?.home_channel_id);
    const inventoryReason = controlInventoryLoading
        ? 'Guild/channel inventory is still syncing for this bot.'
        : inventoryReady
            ? 'Live guild/channel inventory is loaded for this bot.'
            : 'Reload this bot inventory before sending routed commands.';

    const items = [
        {
            label: 'Queue media',
            ready: dbReady && inventoryReady && hasGuild && hasVoice,
            reason: !dbReady ? dbAccess.message
                : !inventoryReady ? inventoryReason
                : hasVoice ? 'Voice route is selected and ready for a direct play order.'
                : 'Select a guild and voice channel first.',
        },
        {
            label: 'Set home channel',
            ready: dbReady && inventoryReady && hasGuild && hasVoice,
            reason: !dbReady ? dbAccess.message
                : !inventoryReady ? inventoryReason
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
                : session?.backup_restore_ready ? 'Backup queue is armed and should repopulate the live queue when playback goes idle.'
                : 'Backup queue exists, but the live queue is not empty yet.',
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
        : dashboardBotsState
            .filter(bot => bot.kind === 'music')
            .map(bot => ({ key: bot.key, display_name: bot.display_name }));

    container.innerHTML = workerBots.map(bot => {
        const session = getBestControlSession(bot.key, guildId);
        const dbAccess = bot.db || deriveBotDbAccess(bot.key);
        const discordAccess = bot.discord || deriveBotDiscordAccess(bot.key);
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
                            ? 'Idle, backup queue armed'
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

async function loadControlInventory(botKey) {
    const controlSel = document.getElementById('control-bot-select');
    if (!botKey && controlSel) botKey = controlSel.value;
    if (!botKey) return;

    const requestId = ++controlInventoryRequestId;
    controlInventoryLoading = true;
    controlInventoryState = { bot: { key: botKey }, guilds: [], loaded: false };
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
        populateControlGuilds();
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
    const guildId = document.getElementById('control-guild-select')?.value;
    const requestId = ++controlMatrixRequestId;

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
    const result = await sendCommand(botKey, guildId, 'LOOP', loopMode);
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update loop mode.', true);
        return;
    }

    setControlStatus(result.data?.message || `Loop mode set to ${loopMode}.`);
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
    const result = await sendCommand(botKey, guildId, 'FILTER', filterMode);
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update filter mode.', true);
        return;
    }

    setControlStatus(result.data?.message || `Filter mode set to ${filterMode}.`);
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
    });
    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to update the home channel.', true);
        return;
    }

    setControlStatus(result.data?.message || 'Home channel updated.');
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
        SKIP: 'Sending skip command...',
        STOP: 'Sending stop command...',
        LEAVE: 'Sending leave command...',
        SHUFFLE: 'Shuffling the queue...',
        CLEAR: 'Clearing the queue and current playback...',
        RESTART: 'Requesting bot restart...',
    };

    setControlStatus(statusLabels[action] || 'Sending command...');
    const result = await sendCommand(selection.botKey, selection.guildId, action);
    if (!result?.ok) {
        setControlStatus(result?.error || `Failed to run ${action}.`, true);
        return;
    }

    setControlStatus(result.data?.message || `${action} sent.`);
}

// ================================
// 🗄️ DATABASE CONTROLS
// ================================
let dbSchemas = [];

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
    document.getElementById('refresh-dashboard')
        ?.addEventListener('click', fetchDashboard);

    document.getElementById('refresh-diagnostics')
        ?.addEventListener('click', () => fetchDiagnostics(true));

    document.getElementById('load-inventory')
        ?.addEventListener('click', loadInventory);

    document.getElementById('control-bot-select')
        ?.addEventListener('change', (event) => loadControlInventory(event.target.value));

    document.getElementById('control-guild-select')
        ?.addEventListener('change', () => {
            populateControlChannels();
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

    // Initial page load
    fetchDashboard();
    fetchDiagnostics();
    loadBotSelect();
    loadDbSchemas();
    loadEventFeedHistory();
    connectEventFeed();
});

// ================================
// 🔄 AUTO REFRESH (5s)
// ================================
setInterval(fetchDashboard, 5000);
setInterval(fetchDiagnostics, 60000);
