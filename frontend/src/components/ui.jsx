import { Lock, Sparkles } from "lucide-react";
import { number, titleCase } from "../utils/format.js";

export function Choice({ label, value, values, onChange }) {
  return (
    <label className="field">
      <span>{label}</span>
      <select value={value || values[0]} onChange={(event) => onChange(event.target.value)}>
        {values.map((item) => <option key={item} value={item}>{titleCase(item)}</option>)}
      </select>
    </label>
  );
}

export function Page({ title, eyebrow, actions, children }) {
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

export function SectionHead({ title, count }) {
  return <div className="section-head"><h2>{title}</h2>{count !== undefined ? <span>{count}</span> : null}</div>;
}

export function MetricGrid({ children }) {
  return <section className="metric-grid">{children}</section>;
}

export function Metric({ icon: Icon, label, value }) {
  return <article className="metric"><Icon size={19} /><div><strong>{number(value)}</strong><span>{label}</span></div></article>;
}

export function Notice({ tone = "info", children }) {
  return <div className={`notice notice-${tone}`}>{children}</div>;
}

export function EmptyState({ title, compact = false }) {
  return <div className={`empty-state ${compact ? "compact" : ""}`}><Sparkles size={22} /><h2>{title}</h2></div>;
}

export function Denied({ message }) {
  return <Page title="Access Locked" eyebrow="Permissions"><div className="empty-state"><Lock size={28} /><h2>{message}</h2></div></Page>;
}

export function NotFound() {
  return <Page title="Not Found" eyebrow="404"><EmptyState title="That panel page is not available" /></Page>;
}

export function SkeletonGrid({ count = 6 }) {
  return <div className="skeleton-grid">{Array.from({ length: count }, (_, index) => <div className="skeleton-card" key={index} />)}</div>;
}

export function Segmented({ value, onChange, options }) {
  return <div className="segmented">{options.map(([key, label]) => <button className={value === key ? "active" : ""} type="button" onClick={() => onChange(key)} key={key}>{label}</button>)}</div>;
}
