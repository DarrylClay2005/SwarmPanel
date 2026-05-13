export function safeHex(value, fallback) {
  return /^#[0-9a-f]{6}$/i.test(value || "") ? value : fallback;
}
export function pick(row, keys) {
  return Object.fromEntries(keys.map((key) => [key, row?.[key]]).filter(([, value]) => value !== undefined));
}
export function unique(values) {
  return Array.from(new Set(values));
}
export function uniqueBy(rows, key) {
  const seen = new Set();
  return rows.filter((row) => {
    const value = String(row?.[key] || "");
    if (!value || seen.has(value)) return false;
    seen.add(value);
    return true;
  });
}
export function formatCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "object") return JSON.stringify(value).slice(0, 220);
  return String(value);
}
export function number(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}
export function initials(value) {
  return String(value || "SP").trim().split(/\s+/).slice(0, 2).map((part) => part[0]).join("").toUpperCase() || "SP";
}
export function titleCase(value) {
  return String(value || "").replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
export function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}
