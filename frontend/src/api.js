const TOKEN_KEY = "swarm_panel_remote_token";
const USER_KEY = "swarm_panel_remote_username";
const CACHE_TTL = 12_000;
const CACHE_STALE_TTL = 90_000;
const CACHE_VERSION = "v2";
const CACHE_STORE_PREFIX = "swarm_panel_api_cache:";
const MAX_STORED_CACHE_BYTES = 450_000;

const cache = new Map();
const inFlightFetches = new Map();

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
    if (!prefix || key.includes(`|${prefix}`)) cache.delete(key);
  }
  for (const storage of [safeStorage(sessionStorage), safeStorage(localStorage)]) {
    if (!storage) continue;
    for (let index = storage.length - 1; index >= 0; index -= 1) {
      const key = storage.key(index);
      if (!key?.startsWith(CACHE_STORE_PREFIX)) continue;
      if (!prefix || key.includes(`|${prefix}`)) storage.removeItem(key);
    }
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
  if (!headers.has("Accept")) headers.set("Accept", "application/json");
  if (options.body && !(options.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(apiUrl(path), { ...options, headers });
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
  const method = String(options.method || "GET").toUpperCase();
  if (method !== "GET") return apiFetch(path, options);
  const ttl = options.ttl ?? CACHE_TTL;
  const staleTtl = options.staleTtl ?? CACHE_STALE_TTL;
  const token = options.token ?? readToken();
  const storage = options.storage === "local" ? safeStorage(localStorage) : safeStorage(sessionStorage);
  const key = `${CACHE_VERSION}|${path}|${token ? "auth" : "anon"}`;
  const hit = cache.get(key);
  const now = Date.now();
  const stored = hit || readStoredCache(storage, key);
  if (stored) {
    cache.set(key, stored);
    if (stored.expires > now) return stored.value;
    if (stored.staleUntil > now && options.allowStale !== false) {
      revalidateCache(key, path, options, ttl, staleTtl, storage);
      return stored.value;
    }
  }
  return revalidateCache(key, path, options, ttl, staleTtl, storage);
}

export function prefetchFetch(path, options = {}) {
  cachedFetch(path, { ...options, allowStale: false }).catch(() => {});
}

export function apiUrl(path) {
  if (/^https?:\/\//i.test(path)) return path;
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${window.SWARM_PANEL_API_ORIGIN || ""}${normalized}`;
}

function revalidateCache(key, path, options, ttl, staleTtl, storage) {
  if (inFlightFetches.has(key)) return inFlightFetches.get(key);
  const promise = apiFetch(path, options)
    .then((value) => {
      if (ttl > 0) {
        const entry = {
          value,
          expires: Date.now() + ttl,
          staleUntil: Date.now() + ttl + Math.max(0, staleTtl),
        };
        cache.set(key, entry);
        writeStoredCache(storage, key, entry);
      }
      return value;
    })
    .finally(() => inFlightFetches.delete(key));
  inFlightFetches.set(key, promise);
  return promise;
}

function safeStorage(storage) {
  try {
    const probe = "__swarm_panel_cache_probe__";
    storage.setItem(probe, "1");
    storage.removeItem(probe);
    return storage;
  } catch (_error) {
    return null;
  }
}

function readStoredCache(storage, key) {
  if (!storage) return null;
  try {
    const raw = storage.getItem(CACHE_STORE_PREFIX + key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.staleUntil <= Date.now()) {
      storage.removeItem(CACHE_STORE_PREFIX + key);
      return null;
    }
    return parsed;
  } catch (_error) {
    return null;
  }
}

function writeStoredCache(storage, key, entry) {
  if (!storage) return;
  try {
    const raw = JSON.stringify(entry);
    if (raw.length > MAX_STORED_CACHE_BYTES) return;
    storage.setItem(CACHE_STORE_PREFIX + key, raw);
  } catch (_error) {
    pruneStoredCache(storage);
  }
}

function pruneStoredCache(storage) {
  try {
    const keys = [];
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key?.startsWith(CACHE_STORE_PREFIX)) keys.push(key);
    }
    keys.slice(0, Math.ceil(keys.length / 3)).forEach((key) => storage.removeItem(key));
  } catch (_error) {
    // Storage cleanup is best-effort.
  }
}
