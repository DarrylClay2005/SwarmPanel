// ================================
// 🔧 CONFIG
// ================================
const API_BASE = "/api";
let controlCooldown = false;
let dashboardBotsState = [];
let botCatalogState = [];
let controlInventoryState = null;

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

        renderSessions(allSessions);
        renderNowPlaying(allSessions);
        syncControlSelectionsFromDashboard();

        const meta = document.getElementById('dashboard-meta');
        if (meta && data.generated_at) {
            meta.textContent = `Last updated: ${new Date(data.generated_at).toLocaleTimeString()}`;
        }

    } catch (err) {
        console.error("❌ Dashboard fetch failed:", err);
    }
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
        const isOnline = bot.status === 'online' || bot.status === 'ONLINE';
        const isError = bot.status === 'error' || bot.status === 'OFFLINE';
        const statusColor = isOnline ? '#a6e3a1' : isError ? '#f38ba8' : '#fab387';
        const statusLabel = isOnline ? 'Online' : isError ? 'Offline' : 'Idle';

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
                        <span class="bot-status-dot" style="background:${statusColor}; box-shadow: 0 0 6px ${statusColor};"></span>
                        <span class="bot-status-label" style="color:${statusColor};">${statusLabel}</span>
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

    sessions.forEach(session => {
        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${session.bot_display}</td>
            <td>${session.guild_name || session.guild_id}</td>
            <td>${session.channel_name || "Unknown"}</td>
            <td>${session.is_playing ? "▶️ Playing" : "⏸️ Paused"}</td>
            <td>${session.title || "—"}</td>
            <td>${session.filter_mode || "none"}</td>
            <td>${session.loop_mode || "off"}</td>
            <td>${session.queue_count || 0}</td>
            <td>${formatDuration(session.position_seconds)}</td>
            <td>
                <button class="tbl-btn" data-action="PAUSE"  data-bot="${session.bot_key}" data-guild="${session.guild_id}">Pause</button>
                <button class="tbl-btn" data-action="RESUME" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Resume</button>
                <button class="tbl-btn" data-action="SKIP"   data-bot="${session.bot_key}" data-guild="${session.guild_id}">Skip</button>
                <button class="tbl-btn tbl-btn-stop" data-action="STOP" data-bot="${session.bot_key}" data-guild="${session.guild_id}">Stop</button>
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
        if (controlSel?.value) {
            loadControlInventory(controlSel.value);
        }
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
        out.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
        out.textContent = `Error: ${err}`;
    }
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
    const textSel = document.getElementById('control-text-select');
    const botKey = document.getElementById('control-bot-select')?.value;
    const guild = getSelectedControlGuild();
    if (!voiceSel || !textSel) return;

    const previousVoice = voiceSel.value;
    const previousText = textSel.value;
    const channels = guild?.channels || [];
    const voiceChannels = channels.filter(channel => channel.type === 2 || channel.type === 13);
    const textChannels = channels.filter(channel => channel.type === 0 || channel.type === 5);
    const session = botKey && guild ? getDashboardSession(botKey, guild.id) : null;

    voiceSel.innerHTML = voiceChannels.length
        ? voiceChannels.map(channel => `<option value="${channel.id}">${channel.name}</option>`).join('')
        : '<option value="">No voice channels found</option>';

    textSel.innerHTML = [
        '<option value="0">No text channel</option>',
        ...textChannels.map(channel => `<option value="${channel.id}">${channel.name}</option>`),
    ].join('');

    if (previousVoice && voiceChannels.some(channel => String(channel.id) === previousVoice)) {
        voiceSel.value = previousVoice;
    } else if (session?.channel_id && voiceChannels.some(channel => String(channel.id) === String(session.channel_id))) {
        voiceSel.value = String(session.channel_id);
    }

    if (previousText && (previousText === '0' || textChannels.some(channel => String(channel.id) === previousText))) {
        textSel.value = previousText;
    }

    syncControlSelectionsFromDashboard();
}

function syncControlSelectionsFromDashboard() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const loopSel = document.getElementById('control-loop-select');
    const voiceSel = document.getElementById('control-voice-select');
    if (!botKey || !guildId) return;

    const session = getDashboardSession(botKey, guildId);
    if (loopSel && session?.loop_mode) {
        loopSel.value = session.loop_mode;
    }

    if (voiceSel && session?.channel_id) {
        const voiceOption = Array.from(voiceSel.options).find(option => option.value === String(session.channel_id));
        if (voiceOption) voiceSel.value = String(session.channel_id);
    }
}

async function loadControlInventory(botKey) {
    const controlSel = document.getElementById('control-bot-select');
    if (!botKey && controlSel) botKey = controlSel.value;
    if (!botKey) return;

    setControlStatus('Loading bot servers and channels...');
    try {
        const res = await fetch(`${API_BASE}/bots/${encodeURIComponent(botKey)}/inventory`);
        if (handle401(res)) return;
        const data = await res.json();
        if (!res.ok) {
            setControlStatus(data.detail || 'Failed to load bot inventory.', true);
            return;
        }
        controlInventoryState = data;
        populateControlGuilds();
        setControlStatus(`Loaded ${data.guilds?.length || 0} guilds for ${data.bot?.display_name || botKey}.`);
    } catch (err) {
        console.error("❌ Failed to load control inventory:", err);
        setControlStatus(`Failed to load bot inventory: ${err}`, true);
    }
}

async function queueFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const voiceChannelId = document.getElementById('control-voice-select')?.value;
    const textChannelId = document.getElementById('control-text-select')?.value || '0';
    const sourceInput = document.getElementById('control-source-input');
    const sourceUrl = sourceInput?.value?.trim();

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
        text_channel_id: textChannelId,
    });

    if (!result?.ok) {
        setControlStatus(result?.error || 'Failed to queue media on the selected bot.', true);
        return;
    }

    setControlStatus(result.data?.message || 'Play request sent.');
    if (sourceInput) sourceInput.value = '';
}

async function applyLoopModeFromPanel() {
    const botKey = document.getElementById('control-bot-select')?.value;
    const guildId = document.getElementById('control-guild-select')?.value;
    const loopMode = document.getElementById('control-loop-select')?.value || 'off';

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

    document.getElementById('load-inventory')
        ?.addEventListener('click', loadInventory);

    document.getElementById('control-bot-select')
        ?.addEventListener('change', (event) => loadControlInventory(event.target.value));

    document.getElementById('control-guild-select')
        ?.addEventListener('change', populateControlChannels);

    document.getElementById('queue-from-panel')
        ?.addEventListener('click', queueFromPanel);

    document.getElementById('apply-loop-mode')
        ?.addEventListener('click', applyLoopModeFromPanel);

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
    loadBotSelect();
    loadDbSchemas();
});

// ================================
// 🔄 AUTO REFRESH (5s)
// ================================
setInterval(fetchDashboard, 5000);
