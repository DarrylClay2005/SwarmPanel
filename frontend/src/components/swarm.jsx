import { memo } from "react";
import { Music2, PlugZap } from "lucide-react";
import { EmptyState, Notice } from "./ui.jsx";
import { formatCell, formatTime, initials, pick, unique } from "../utils/format.js";

function bestSession(bot) {
  const sessions = bot.sessions || [];
  return sessions.find((session) => session.is_playing) || sessions[0] || null;
}

export function BotCard({ bot }) {
  const sessions = bot.sessions || [];
  const session = bestSession(bot);
  const accent = bot.accent || "#89b4fa";
  const thumbnail = session?.thumbnail || session?.thumbnail_url || "";
  return (
    <article className="bot-card" style={{ "--card-accent": accent }}>
      <div className="bot-head"><span className="bot-dot" /><h3>{bot.display_name || bot.name || bot.key}</h3><small>{bot.heartbeat_status || bot.status || "unknown"}</small></div>
      <div className="bot-now">
        {thumbnail ? <img className="bot-thumb" src={thumbnail} alt="" loading="lazy" decoding="async" /> : <div className="bot-thumb bot-thumb-empty"><Music2 size={22} /></div>}
        <div>
          <p>{session?.title || bot.db_error || bot.schema || "Waiting for live playback."}</p>
          <small>{session?.guild_name || session?.channel_name || "Live state will fill in automatically."}</small>
        </div>
      </div>
      <div className="chip-row">
        <span>{bot.active_playing_count || sessions.filter((session) => session.is_playing).length} live</span>
        <span>{bot.known_guild_count || bot.guild_count || 0} guilds</span>
        <span>{bot.queue_depth || sessions.reduce((sum, session) => sum + Number(session.queue_count || 0), 0)} queued</span>
        <span>{bot.backup_queue_depth || sessions.reduce((sum, session) => sum + Number(session.backup_queue_count || 0), 0)} backup</span>
      </div>
    </article>
  );
}

export function SessionTable({ sessions }) {
  if (!sessions.length) return <EmptyState title="No active sessions" />;
  return <DataTable rows={sessions.map((session) => pick(session, ["bot_name", "guild_name", "guild_id", "channel_name", "title", "is_playing", "queue_count", "backup_queue_count", "filter_mode", "loop_mode"]))} />;
}

export function IntelligenceView({ data }) {
  if (!data) return <EmptyState title="No intelligence snapshot" />;
  if (Array.isArray(data)) return <DataTable rows={data} />;
  const rows = data.recommendations || data.guilds || data.bots || data.rows || [];
  return rows.length ? <DataTable rows={rows} /> : <JsonPanel data={data} />;
}

export function ControlState({ state, compact = false }) {
  if (!state) return <EmptyState title="No state loaded" compact />;
  if (state.error) return <Notice tone="error">{state.error}</Notice>;
  const session = state.session || {};
  const backupPreview = session.backup_queue_preview || [];
  return (
    <article className={`control-state ${compact ? "compact" : ""}`}>
      <div><strong>{state.display_name || state.key}</strong><small>{state.discord?.status || state.db?.status || "unknown"}</small></div>
      <p>{session.title || session.session_state_label || state.discord?.message || "Idle"}</p>
      <div className="chip-row">
        <span>{session.guild_name || state.guild_id}</span>
        <span>{session.channel_name || "No channel"}</span>
        <span>{session.queue_count || 0} queued</span>
        <span>{session.backup_queue_count || 0} backup</span>
      </div>
      {!compact && backupPreview.length ? (
        <div className="backup-preview">
          {backupPreview.map((track, index) => (
            <span key={`${track.video_url || track.title || "track"}-${index}`}>{track.title || track.video_url || "Untitled backup track"}</span>
          ))}
        </div>
      ) : null}
    </article>
  );
}

export function InviteCard({ bot }) {
  return (
    <article className="invite-card">
      <div className="bot-head"><span className="bot-dot" /><h3>{bot.display_name}</h3><small>{bot.token_configured ? "token ready" : "missing token"}</small></div>
      <p>{bot.capability_summary}</p>
      <div className="chip-row">{(bot.permissions || []).slice(0, 6).map((permission) => <span key={permission}>{permission}</span>)}</div>
      {bot.invite_url ? <a className="button-link primary" href={bot.invite_url} target="_blank" rel="noreferrer"><PlugZap size={16} />Invite</a> : <button disabled>Invite unavailable</button>}
    </article>
  );
}

export function UserCard({ user }) {
  const imageUrl = user.avatar_url || user.server_icon_url || "";
  return (
    <article className="user-card">
      <div className="avatar">{imageUrl ? <img src={imageUrl} alt="" loading="lazy" decoding="async" /> : initials(user.display_name || user.username)}</div>
      <div>
        <h3>{user.display_name || user.username}</h3>
        <p>@{user.username} / {user.server_name || `Guild ${user.guild_id}`}</p>
        <div className="chip-row"><span>{user.favorite_bot || "no favorite"}</span><span>{user.public_profile === false ? "private" : "public"}</span></div>
      </div>
    </article>
  );
}

export function EventList({ events }) {
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

export function ChannelSelect({ value, channels, onChange, optional = false }) {
  return (
    <select value={value} onChange={(event) => onChange(event.target.value)}>
      <option value="">{optional ? "None" : "Choose channel"}</option>
      {channels.map((channel) => <option value={channel.id} key={channel.id}>{channel.name || channel.id}</option>)}
    </select>
  );
}

export function DataTable({ rows = [], actions }) {
  if (!rows?.length) return <EmptyState title="No rows" compact />;
  const columns = unique(rows.flatMap((row) => Object.keys(row))).filter((column) => !String(column).toLowerCase().includes("password_hash")).slice(0, 9);
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

export function JsonPanel({ data }) {
  if (!data) return <EmptyState title="No data loaded" compact />;
  return <pre className="json-panel">{JSON.stringify(data, null, 2)}</pre>;
}


export const MemoBotCard = memo(BotCard);
export const MemoUserCard = memo(UserCard);
export const MemoDataTable = memo(DataTable);
