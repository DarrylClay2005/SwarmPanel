// ================================
// 🔧 CONFIG
// ================================
const API_BASE = "/api";
let controlCooldown = false;

// ================================
// 📡 FETCH DASHBOARD DATA
// ================================
async function fetchDashboard() {
    try {
        const res = await fetch(`${API_BASE}/dashboard`);
        const data = await res.json();

        const bots = data.bots || [];

        renderBots(bots);

        // 🔥 Flatten nested sessions
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

    } catch (err) {
        console.error("❌ Dashboard fetch failed:", err);
    }
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
        const card = document.createElement("div");
        card.className = "card";

        card.innerHTML = `
            <h3>${bot.display_name}</h3>
            <p>Status: <strong>${bot.status}</strong></p>
            <p>Guilds: ${bot.known_guild_count || 0}</p>

            <button onclick="sendCommand('${bot.key}', '0', 'RESTART')">
                Restart Node
            </button>
        `;

        if (bot.kind === "orchestrator" && ariaContainer) {
            ariaContainer.appendChild(card);
        } else {
            container.appendChild(card);
        }
    });
}

// ================================
// 🎵 RENDER SESSIONS
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
            <td>${session.queue_count || 0}</td>
            <td>${session.position_seconds || 0}s</td>

            <td>
                <button onclick="sendCommand('${session.bot_key}', '${session.guild_id}', 'PAUSE')">Pause</button>
                <button onclick="sendCommand('${session.bot_key}', '${session.guild_id}', 'RESUME')">Resume</button>
                <button onclick="sendCommand('${session.bot_key}', '${session.guild_id}', 'SKIP')">Skip</button>
                <button onclick="sendCommand('${session.bot_key}', '${session.guild_id}', 'STOP')">Stop</button>
            </td>
        `;

        table.appendChild(row);
    });
}

// ================================
// 🎮 SEND COMMAND
// ================================
async function sendCommand(bot_key, guild_id, action) {
    if (controlCooldown) return;
    controlCooldown = true;

    try {
        const res = await fetch(`${API_BASE}/bots/control`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                bot_key: bot_key,
                guild_id: guild_id,
                action: action
            })
        });

        if (!res.ok) {
            console.error("❌ Backend rejected request:", await res.text());
        }

        fetchDashboard();

    } catch (err) {
        console.error("❌ Command failed:", err);
    } finally {
        setTimeout(() => (controlCooldown = false), 500);
    }
}

// ================================
// 🔄 AUTO REFRESH
// ================================
setInterval(fetchDashboard, 5000);

// Initial load
fetchDashboard();