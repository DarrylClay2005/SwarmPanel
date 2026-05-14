const TOKEN_KEY = "swarm_panel_remote_token";
const USER_KEY = "swarm_panel_remote_username";
const CACHE_TTL = 12_000;
const CACHE_STALE_TTL = 90_000;
const CACHE_VERSION = "v3";
const CACHE_STORE_PREFIX = "swarm_panel_api_cache:";
const MAX_STORED_CACHE_BYTES = 450_000;
const REMOTE_ORIGIN_KEY = "swarm_panel_remote_origin";
const REMOTE_ORIGIN_TTL = 60_000;
const REMOTE_ORIGIN_STALE_TTL = 15 * 60_000;

const cache = new Map();
const inFlightFetches = new Map();
let remoteOriginPromise = null;
let remoteOriginRefreshAfter = 0;
let remoteConfig = null;

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
  const response = await fetchWithRemoteRetry(path, options, headers);
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
  return `${currentApiOrigin()}${normalized}`;
}

export async function resolveApiUrl(path) {
  if (/^https?:\/\//i.test(path)) return path;
  await ensureRemoteOrigin();
  return apiUrl(path);
}

function currentApiOrigin() {
  return String(window.SWARM_PANEL_API_ORIGIN || "").replace(/\/+$/, "");
}

function isRemoteStaticHost() {
  return Boolean(window.SWARM_PANEL_REMOTE_MODE) || window.location.hostname.endsWith("github.io");
}

function remoteConfigUrl() {
  if (window.SWARM_PANEL_CONFIG_URL) return window.SWARM_PANEL_CONFIG_URL;
  const basename = String(window.SWARM_PANEL_BASENAME || "").replace(/\/+$/, "");
  if (basename) return `${basename}/live-config.json`;
  return "live-config.json";
}

async function ensureRemoteOrigin() {
  if (!isRemoteStaticHost()) return currentApiOrigin();
  if (currentApiOrigin() && remoteOriginRefreshAfter > Date.now()) return currentApiOrigin();
  if (!remoteOriginPromise) {
    remoteOriginPromise = loadRemoteOrigin().finally(() => {
      remoteOriginPromise = null;
    });
  }
  return remoteOriginPromise;
}

async function refreshRemoteOrigin() {
  if (!isRemoteStaticHost()) return currentApiOrigin();
  remoteOriginPromise = loadRemoteOrigin({ force: true }).finally(() => {
    remoteOriginPromise = null;
  });
  return remoteOriginPromise;
}

async function loadRemoteOrigin({ force = false } = {}) {
  const cached = readRemoteOriginCache();
  if (!force && cached?.origin && cached.updatedAt && Date.now() - cached.updatedAt < REMOTE_ORIGIN_TTL) {
    applyRemoteOrigin(cached);
    return cached.origin;
  }
  try {
    const response = await fetch(`${remoteConfigUrl()}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Remote config failed (${response.status})`);
    const config = await response.json();
    const origin = String(config.panel_url || config.api_url || "").replace(/\/+$/, "");
    if (origin) {
      const entry = { origin, localUrls: normalizeLocalUrls(config.local_urls), updatedAt: Date.now() };
      applyRemoteOrigin(entry);
      writeRemoteOriginCache(entry);
      return origin;
    }
    remoteConfig = config || null;
  } catch (_error) {
    if (cached?.origin && (!cached.updatedAt || Date.now() - cached.updatedAt < REMOTE_ORIGIN_STALE_TTL)) {
      applyRemoteOrigin(cached);
      return cached.origin;
    }
  }
  clearRemoteOriginCache();
  return "";
}

async function fetchWithRemoteRetry(path, options, headers) {
  const target = await resolveApiUrl(path);
  try {
    const response = await fetch(target, { ...options, headers });
    if (shouldRefreshRemoteAfterStatus(response.status, options)) {
      const retryTarget = await retryTargetFor(path, target, options);
      if (retryTarget && retryTarget !== target) {
        return fetch(retryTarget, { ...options, headers });
      }
    }
    return response;
  } catch (error) {
    const retryTarget = await retryTargetFor(path, target, options);
    if (retryTarget && retryTarget !== target) {
      return fetch(retryTarget, { ...options, headers });
    }
    throw error;
  }
}

async function retryTargetFor(path, failedTarget, options) {
  if (!canRetryWithFreshRemote(options)) return "";
  const failedOrigin = originFromUrl(failedTarget) || currentApiOrigin();
  await refreshRemoteOrigin();
  const refreshed = apiUrl(path);
  if (refreshed && refreshed !== failedTarget) return refreshed;
  return fallbackApiUrl(path, failedOrigin);
}

function shouldRefreshRemoteAfterStatus(status, options) {
  return canRetryWithFreshRemote(options) && (status === 502 || status === 503 || status === 504 || (status >= 520 && status <= 526));
}

function canRetryWithFreshRemote(options) {
  const method = String(options.method || "GET").toUpperCase();
  return isRemoteStaticHost() && !options.body && (method === "GET" || method === "HEAD");
}

function fallbackApiUrl(path, failedOrigin) {
  const origin = localOriginFallback(failedOrigin);
  if (!origin) return "";
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${origin}${normalized}`;
}

function localOriginFallback(failedOrigin) {
  const localUrls = normalizeLocalUrls(remoteConfig?.local_urls);
  for (const origin of localUrls) {
    if (!origin || origin === failedOrigin) continue;
    if (window.location.protocol === "https:" && !/^http:\/\/(localhost|127\.0\.0\.1|\[::1\])(?::|$)/i.test(origin)) continue;
    return origin;
  }
  return "";
}

function applyRemoteOrigin(entry) {
  const origin = String(entry?.origin || "").replace(/\/+$/, "");
  const current = currentApiOrigin();
  window.SWARM_PANEL_API_ORIGIN = origin;
  remoteOriginRefreshAfter = Date.now() + REMOTE_ORIGIN_TTL;
  remoteConfig = { ...(remoteConfig || {}), local_urls: normalizeLocalUrls(entry?.localUrls || entry?.local_urls) };
  if (origin && current && current !== origin) clearCache();
}

function readRemoteOriginCache() {
  const raw = readStorageValue(REMOTE_ORIGIN_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.origin) {
      return {
        origin: String(parsed.origin).replace(/\/+$/, ""),
        localUrls: normalizeLocalUrls(parsed.localUrls || parsed.local_urls),
        updatedAt: Number(parsed.updatedAt || 0),
      };
    }
  } catch (_error) {
    return { origin: raw.replace(/\/+$/, ""), localUrls: [], updatedAt: 0 };
  }
  return null;
}

function writeRemoteOriginCache(entry) {
  try {
    localStorage.setItem(REMOTE_ORIGIN_KEY, JSON.stringify(entry));
  } catch (_error) {
    // Storage can be unavailable in hardened browser contexts.
  }
}

function clearRemoteOriginCache() {
  window.SWARM_PANEL_API_ORIGIN = "";
  remoteOriginRefreshAfter = 0;
  try {
    localStorage.removeItem(REMOTE_ORIGIN_KEY);
  } catch (_error) {
    // Storage can be unavailable in hardened browser contexts.
  }
}

function normalizeLocalUrls(urls) {
  if (!Array.isArray(urls)) return [];
  return urls
    .map((url) => String(url || "").replace(/\/+$/, ""))
    .filter(Boolean);
}

function originFromUrl(url) {
  try {
    return new URL(url, window.location.href).origin.replace(/\/+$/, "");
  } catch (_error) {
    return "";
  }
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

function readStorageValue(key) {
  try {
    return localStorage.getItem(key) || "";
  } catch (_error) {
    return "";
  }
}

function writeStorageValue(key, value) {
  try {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  } catch (_error) {
    // Storage can be unavailable in hardened browser contexts.
  }
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
