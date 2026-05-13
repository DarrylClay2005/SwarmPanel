const TOKEN_KEY = "swarm_panel_remote_token";
const USER_KEY = "swarm_panel_remote_username";
const CACHE_TTL = 12_000;
const cache = new Map();

export function readToken() {
  try {
    return localStorage.getItem(TOKEN_KEY) || "";
  } catch (_error) {
    return "";
  }
}

export function writeToken(token) {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch (_error) {
    // Some browser contexts block storage.
  }
}

export function writeUsername(username) {
  try {
    if (username) localStorage.setItem(USER_KEY, username);
    else localStorage.removeItem(USER_KEY);
  } catch (_error) {
    // Some browser contexts block storage.
  }
}

export function clearCache(prefix = "") {
  for (const key of Array.from(cache.keys())) {
    if (!prefix || key.startsWith(prefix)) cache.delete(key);
  }
}

export function query(params) {
  const values = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    values.set(key, String(value));
  });
  const text = values.toString();
  return text ? `?${text}` : "";
}

export async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  const token = options.token ?? readToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (options.body && !(options.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, { ...options, headers });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json().catch(() => null)
    : await response.text();
  if (!response.ok) {
    const error = new Error(payload?.detail || payload?.message || payload || `Request failed (${response.status})`);
    error.status = response.status;
    error.payload = payload;
    throw error;
  }
  return payload;
}

export async function cachedFetch(path, options = {}) {
  const ttl = options.ttl ?? CACHE_TTL;
  const token = options.token ?? readToken();
  const key = `${path}|${token ? "auth" : "anon"}`;
  const hit = cache.get(key);
  const now = Date.now();
  if (hit && hit.expires > now) return hit.value;
  const value = await apiFetch(path, options);
  if ((!options.method || options.method === "GET") && ttl > 0) {
    cache.set(key, { value, expires: now + ttl });
  }
  return value;
}
