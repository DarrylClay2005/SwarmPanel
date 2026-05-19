import { Link, NavLink, useLocation } from "react-router-dom";
import {
  Database,
  HeartPulse,
  Image as ImageIcon,
  LayoutDashboard,
  LogIn,
  LogOut,
  MessageCircle,
  Palette,
  Play,
  PlugZap,
  Shield,
  Siren,
  Sparkles,
  UserRound,
  Users,
} from "lucide-react";

export function Shell({ ctx, children }) {
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
            <NavItem to="/friends" icon={UserRound} label="Friends" />
            <NavItem to="/messages" icon={MessageCircle} label="Messages" />
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
