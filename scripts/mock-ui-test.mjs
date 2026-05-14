import { chromium } from "playwright";

const BASE_URL = process.env.SWARM_PANEL_TEST_URL || "http://127.0.0.1:8000";
const TOKEN = "mock-swarm-token";
const OWNER_SESSION = {
  authenticated: true,
  mode: "token",
  token: TOKEN,
  username: "MockAdmin",
  role: "admin",
  guild_id: "1247394560007471134",
  account_guild_id: "1247394560007471134",
  site_owner: true,
  admin_mode: true,
  image_gallery_owner: true,
  pages_public_url: "https://example.test/SwarmPanel/",
};

const preferences = {
  accent_color: "#89b4fa",
  background_mode: "default",
  background_color: "#0b0e18",
  layout_mode: "standard",
  density: "comfortable",
  card_shape: "soft",
  font_scale: "normal",
  motion: "standard",
  tab_style: "pills",
  surface_opacity: 0.92,
  surface_blur: 18,
};

const musicBots = [
  { key: "gws", display_name: "GWS", name: "GWS", kind: "music", schema: "discord_music_gws", token_configured: true },
  { key: "lockhart", display_name: "Lockhart", name: "Lockhart", kind: "music", schema: "discord_music", token_configured: true },
  { key: "strife", display_name: "Strife", name: "Strife", kind: "music", schema: "discord_music", token_configured: true },
];
const allBots = [...musicBots, { key: "aria", display_name: "Aria", name: "Aria", kind: "orchestrator", schema: null, token_configured: true }];

const channels = [
  { id: "111", name: "General Voice", type: 2 },
  { id: "222", name: "Stage", type: 13 },
  { id: "333", name: "bot-commands", type: 0 },
];

function dashboardPayload() {
  return {
    generated_at: new Date().toISOString(),
    bots: musicBots.map((bot, index) => ({
      ...bot,
      accent: index === 1 ? "#f9a8d4" : "#89b4fa",
      status: "online",
      heartbeat_status: "fresh",
      active_playing_count: index === 0 ? 1 : 0,
      known_guild_count: 1,
      queue_depth: index + 1,
      sessions: [{
        bot_key: bot.key,
        bot_name: bot.display_name,
        guild_id: "1247394560007471134",
        guild_name: "Mock Guild",
        channel_name: "General Voice",
        title: `${bot.display_name} mock track`,
        is_playing: index === 0,
        queue_count: index + 1,
        filter_mode: "none",
        loop_mode: "off",
        session_state: index === 0 ? "playing" : "idle",
      }],
    })),
    sessions: musicBots.map((bot, index) => ({
      bot_key: bot.key,
      bot_name: bot.display_name,
      guild_id: "1247394560007471134",
      guild_name: "Mock Guild",
      channel_name: "General Voice",
      title: `${bot.display_name} mock track`,
      is_playing: index === 0,
      queue_count: index + 1,
      filter_mode: "none",
      loop_mode: "off",
      session_state: index === 0 ? "playing" : "idle",
    })),
  };
}

function botsPayload() {
  return {
    bots: allBots,
    invite_bots: allBots.map((bot) => ({
      ...bot,
      client_id: bot.key === "lockhart" ? "111111111111111111" : "222222222222222222",
      invite_url: `https://discord.com/oauth2/authorize?client_id=${bot.key === "lockhart" ? "111111111111111111" : "222222222222222222"}&permissions=8&scope=bot%20applications.commands`,
      permission_integer: "8",
      permissions: ["Send Messages", "Connect", "Speak", "Use Slash Commands"],
      capability_summary: `${bot.display_name} mock invite coverage.`,
      identity_name: bot.display_name,
      token_configured: true,
    })),
    scoped_guild_id: "1247394560007471134",
  };
}

function controlState(botKey = "lockhart") {
  const bot = allBots.find((item) => item.key === botKey) || allBots[0];
  return {
    key: bot.key,
    display_name: bot.display_name,
    guild_id: "1247394560007471134",
    discord: { status: "online", message: "Ready" },
    db: { status: "online" },
    database: { status: "online" },
    session: {
      guild_id: "1247394560007471134",
      guild_name: "Mock Guild",
      channel_name: "General Voice",
      title: `${bot.display_name} readiness track`,
      queue_count: 2,
      session_state_label: "Playing",
    },
  };
}

function profilePayload() {
  return {
    editable: true,
    profile: {
      username: "MockAdmin",
      display_name: "Mock Admin",
      server_name: "Mock Guild",
      avatar_url: "https://example.test/avatar.png",
      server_icon_url: "https://example.test/server.png",
      bio: "Mock profile bio",
      favorite_bot: "lockhart",
      theme_accent: "#89b4fa",
      public_profile: true,
      server_invite_url: "https://discord.gg/mock",
    },
    favorite_bot_options: musicBots,
  };
}

function galleryAdminPayload() {
  return {
    data: {
      counts: { users: 1, media: 2, reports: 1 },
      users: [{ id: 1, username: "gallery-admin", email_verified_at: null, age_verified_at: null, created_at: "2026-05-14" }],
      reports: [{ id: 8, media_id: 14, reason: "mock report", status: "open" }],
      media: [{ id: 14, title: "Mock media" }],
    },
  };
}

function json(payload, status = 200) {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify(payload),
    headers: { "Cache-Control": "no-store" },
  };
}

function text(payload, status = 200) {
  return {
    status,
    contentType: "text/plain",
    body: String(payload),
  };
}

async function installMocks(context, options = {}) {
  const requests = [];
  const unhandled = [];
  let unauthenticated = Boolean(options.unauthenticated);

  await context.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    requests.push({ method, path, search: url.search });

    if (path === "/api/session" && method === "GET") {
      return route.fulfill(json(unauthenticated ? { authenticated: false, pages_public_url: OWNER_SESSION.pages_public_url } : OWNER_SESSION));
    }
    if (path === "/api/session/login" && method === "POST") {
      unauthenticated = false;
      return route.fulfill(json({ ok: true, ...OWNER_SESSION }));
    }
    if (path === "/api/session/register" && method === "POST") {
      unauthenticated = false;
      return route.fulfill(json({ ok: true, ...OWNER_SESSION, username: "NewMockUser", role: "account" }));
    }
    if (path === "/api/session/admin-mode" && method === "POST") return route.fulfill(json({ ...OWNER_SESSION, admin_mode: true }));
    if (path === "/api/session/logout" && method === "POST") {
      unauthenticated = true;
      return route.fulfill(json({ ok: true }));
    }
    if (path === "/api/session/email" && method === "POST") return route.fulfill(json({ ok: true }));
    if (path === "/api/session/email/verify" && method === "POST") return route.fulfill(json({ ok: true }));
    if (path === "/api/session/password" && method === "POST") return route.fulfill(json({ ok: true }));

    if (path === "/api/users/preferences" && method === "GET") return route.fulfill(json({ preferences }));
    if (path === "/api/users/preferences" && method === "POST") return route.fulfill(json({ preferences: { ...preferences, accent_color: "#b4befe" } }));
    if (path === "/api/users/me" && method === "GET") return route.fulfill(json(profilePayload()));
    if (path === "/api/users/me" && method === "POST") return route.fulfill(json({ profile: profilePayload().profile }));
    if (path === "/api/users/directory" && method === "GET") {
      return route.fulfill(json({ users: [{ username: "MockAdmin", display_name: "Mock Admin", guild_id: "1247394560007471134", server_name: "Mock Guild", favorite_bot: "lockhart", public_profile: true }] }));
    }

    if (path === "/api/bots" && method === "GET") return route.fulfill(json(botsPayload()));
    if (path === "/api/dashboard" && method === "GET") return route.fulfill(json(dashboardPayload()));
    if (path === "/api/music-intelligence" && method === "GET") {
      return route.fulfill(json({ recommendations: [{ bot: "lockhart", title: "Mock recommendation", score: 0.91 }] }));
    }
    if (/^\/api\/bots\/[^/]+\/inventory$/.test(path) && method === "GET") {
      return route.fulfill(json({ guilds: [{ id: "1247394560007471134", name: "Mock Guild", channels }], channels }));
    }
    if (/^\/api\/bots\/[^/]+\/control-state$/.test(path) && method === "GET") {
      return route.fulfill(json(controlState(path.split("/")[3])));
    }
    if (/^\/api\/guilds\/[^/]+\/control-matrix$/.test(path) && method === "GET") {
      return route.fulfill(json({ bots: musicBots.map((bot) => controlState(bot.key)) }));
    }
    if (path === "/api/bots/control" && method === "POST") return route.fulfill(json({ ok: true, message: "Mock control accepted." }));

    if (path === "/api/system-diagnostics" && method === "GET") {
      return route.fulfill(json({ ok: true, docker: { swarmpanel: "running" }, checks: ["mock"] }));
    }
    if (path === "/api/swarm-accounts/admin" && method === "GET") {
      return route.fulfill(json({ data: { accounts: [{ id: 1, username: "MockAdmin", guild_id: "1247394560007471134", email_verified_at: null }] } }));
    }
    if (path.startsWith("/api/swarm-accounts/") && method === "POST") return route.fulfill(json({ ok: true }));
    if (path === "/api/databases" && method === "GET") {
      return route.fulfill(json({ schemas: [{ schema: "discord_music", tables: [{ name: "lockhart_queue" }, { name: "strife_queue" }] }] }));
    }
    if (path === "/api/database/data" && method === "GET") return route.fulfill(json({ data: [{ id: 1, title: "Mock row", bot: "lockhart" }] }));
    if (path === "/api/image-gallery/admin" && method === "GET") return route.fulfill(json(galleryAdminPayload()));
    if (path === "/api/image-gallery/tables" && method === "GET") return route.fulfill(json({ tables: [{ name: "media" }, { name: "users" }] }));
    if (path === "/api/image-gallery/table-data" && method === "GET") return route.fulfill(json({ data: [{ id: 14, title: "Mock gallery row" }] }));
    if (path.startsWith("/api/image-gallery/") && method === "POST") return route.fulfill(json({ ok: true }));
    if (path === "/api/events" && method === "GET") return route.fulfill(json({ events: [{ timestamp: new Date().toISOString(), level: "info", title: "Mock event", source: "test", message: "Event ready" }] }));
    if (path === "/api/metrics" && method === "GET") return route.fulfill(json({ ok: true, counters: { controls: 3 } }));
    if (path === "/api/stability" && method === "GET") return route.fulfill(json({ ok: true, status: "stable" }));

    unhandled.push(`${method} ${path}${url.search}`);
    return route.fulfill(json({ detail: `Unhandled mock route: ${method} ${path}` }, 599));
  });

  return { requests, unhandled };
}

function watchPage(page, failures) {
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", (request) => failures.push(`request failed: ${request.method()} ${request.url()} ${request.failure()?.errorText}`));
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/api/") && response.status() >= 500) failures.push(`api ${response.status()}: ${url}`);
  });
}

async function click(page, role, name) {
  await page.getByRole(role, { name }).first().click();
}

async function expectVisible(page, text) {
  await page.getByText(text, { exact: false }).first().waitFor({ state: "visible", timeout: 10_000 });
}

async function runAuthMock(browser, failures) {
  const loginContext = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const loginMock = await installMocks(loginContext, { unauthenticated: true });
  const loginPage = await loginContext.newPage();
  watchPage(loginPage, failures);
  await loginPage.goto(`${BASE_URL}/login`, { waitUntil: "networkidle" });
  await expectVisible(loginPage, "Login");
  await loginPage.getByLabel("Username").fill("MockAdmin");
  await loginPage.getByLabel("Password").fill("mock-password");
  await loginPage.locator(".auth-card button.primary").click();
  await expectVisible(loginPage, "Swarm Command Deck");
  if (!loginMock.requests.some((request) => request.path === "/api/session/login" && request.method === "POST")) failures.push("login form did not POST");
  failures.push(...loginMock.unhandled);
  await loginContext.close();

  const registerContext = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const registerMock = await installMocks(registerContext, { unauthenticated: true });
  const registerPage = await registerContext.newPage();
  watchPage(registerPage, failures);
  await registerPage.goto(`${BASE_URL}/login`, { waitUntil: "networkidle" });
  await click(registerPage, "button", "Register");
  await registerPage.getByLabel("Username").fill("NewMockUser");
  await registerPage.getByLabel("Password").fill("mock-password");
  await registerPage.getByLabel("Guild ID").fill("1247394560007471134");
  await registerPage.getByLabel("Email").fill("new@example.test");
  await click(registerPage, "button", "Create Account");
  await expectVisible(registerPage, "Swarm Command Deck");
  if (!registerMock.requests.some((request) => request.path === "/api/session/register" && request.method === "POST")) failures.push("register form did not POST");
  failures.push(...registerMock.unhandled);
  await registerContext.close();
}

async function runAuthenticatedMock(browser, failures) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 980 } });
  await context.addInitScript((token) => {
    localStorage.setItem("swarm_panel_remote_token", token);
    localStorage.setItem("swarm_panel_remote_username", "MockAdmin");
  }, TOKEN);
  const mock = await installMocks(context);
  const page = await context.newPage();
  watchPage(page, failures);

  await page.goto(`${BASE_URL}/`, { waitUntil: "networkidle" });
  await expectVisible(page, "Swarm Command Deck");
  await expectVisible(page, "Lockhart");
  await click(page, "button", /Refresh|Updating/);

  await page.getByLabel("Admin").click();
  await expectVisible(page, "Admin");

  await page.getByRole("link", { name: "Controls" }).click();
  await expectVisible(page, "Direct Playback Control");
  const controlForm = page.locator("form.form-panel");
  await controlForm.locator("select").nth(0).selectOption("lockhart");
  await controlForm.locator("input[list='known-guilds']").fill("1247394560007471134");
  await controlForm.locator("select").nth(1).selectOption("PLAY");
  await page.getByPlaceholder("https://youtube.com/... or search terms").fill("mock search");
  await controlForm.locator("select").nth(2).selectOption("111");
  await controlForm.locator("select").nth(3).selectOption("333");
  await click(page, "button", "Send Control");
  await expectVisible(page, "Mock control accepted.");
  await click(page, "button", "Smart Rec");
  await click(page, "button", "Send Control");
  await controlForm.locator("select").nth(1).selectOption("LOOP");
  await controlForm.locator("select").nth(2).selectOption("track");
  await click(page, "button", "Send Control");
  await controlForm.locator("select").nth(1).selectOption("FILTER");
  await controlForm.locator("select").nth(2).selectOption("nightcore");
  await click(page, "button", "Send Control");
  await controlForm.locator("select").nth(1).selectOption("SET_HOME");
  await controlForm.locator("select").nth(2).selectOption("222");
  await click(page, "button", "Send Control");
  await controlForm.locator("select").nth(1).selectOption("RESTART");
  await click(page, "button", "Send Control");

  await page.getByRole("link", { name: "Invites" }).click();
  await expectVisible(page, "Bot Access");
  await click(page, "button", "Refresh");
  await expectVisible(page, "Lockhart");
  const lockhartInvite = await page.locator(".invite-card", { hasText: "Lockhart" }).locator("a[href*='discord.com/oauth2/authorize']").count();
  if (lockhartInvite < 1) failures.push("Lockhart invite link was not rendered");

  await page.getByRole("link", { name: "Users" }).click();
  await expectVisible(page, "Swarm Directory");
  await page.getByPlaceholder("Search users, servers, favorite bots").fill("mock");
  await expectVisible(page, "Mock Admin");

  await page.getByRole("link", { name: "Profile" }).click();
  await expectVisible(page, "Server Identity");
  await page.getByLabel("Display Name").fill("Mock Admin Updated");
  await click(page, "button", "Save");
  await expectVisible(page, "Profile saved.");
  await page.getByLabel("Email").fill("mock@example.test");
  await click(page, "button", "Save Email");
  await page.getByLabel("Code").fill("123456");
  await click(page, "button", "Verify");
  await page.getByLabel("Current").fill("old-password");
  await page.getByLabel("New").fill("new-password");
  await click(page, "button", "Change");

  await page.getByRole("link", { name: "Look" }).click();
  await expectVisible(page, "Panel Look");
  await page.getByLabel("Layout").selectOption("wide");
  await page.getByLabel("Density").selectOption("compact");
  await page.getByLabel("Background Image URL").fill("https://example.test/bg.png");
  await click(page, "button", "Save");
  await expectVisible(page, "Appearance saved.");

  await page.getByRole("link", { name: "Diagnostics" }).click();
  await expectVisible(page, "System Runtime");
  await click(page, "button", "Force");

  await page.getByRole("link", { name: "Accounts" }).click();
  await expectVisible(page, "SwarmPanel Recovery");
  await page.getByPlaceholder("Search accounts").fill("mock");
  await click(page, "button", "Refresh");
  await page.locator(".table-actions").first().getByRole("button", { name: "Email" }).click();
  await page.locator(".mini-input").first().fill("new-password");
  await page.locator(".table-actions").first().getByRole("button", { name: "Reset" }).click();
  await page.locator(".table-actions").first().getByRole("button", { name: "Resend" }).click();
  await page.locator(".table-actions").first().getByRole("button", { name: "Delete" }).click();

  await page.getByRole("link", { name: "Data" }).click();
  await expectVisible(page, "Database Viewer");
  await click(page, "button", "Refresh Schemas");
  await page.getByRole("button", { name: "Load" }).click();
  await expectVisible(page, "Mock row");

  await page.getByRole("link", { name: "Gallery" }).click();
  await expectVisible(page, "Image Gallery Admin");
  await click(page, "button", "Refresh");
  await page.locator(".table-actions").first().getByRole("button", { name: "Email" }).click();
  await page.locator(".table-actions").first().getByRole("button", { name: "Age" }).click();
  await page.locator(".mini-input").first().fill("gallery-password");
  await page.locator(".table-actions").first().getByRole("button", { name: "Reset" }).click();
  await page.locator(".table-actions").first().getByRole("button", { name: "Delete" }).click();
  await page.getByRole("button", { name: "Resolve" }).click();
  await page.locator(".panel.wide").last().locator("select").selectOption("media");
  await page.locator(".panel.wide").last().getByRole("button", { name: "Load" }).click();
  await expectVisible(page, "Mock gallery row");

  await page.getByRole("link", { name: "Intel" }).click();
  await expectVisible(page, "Errors And Metrics");
  await click(page, "button", "Refresh");
  await expectVisible(page, "Mock event");

  await page.getByTitle("Logout").click();
  await expectVisible(page, "Login");

  const controlPosts = mock.requests.filter((request) => request.path === "/api/bots/control" && request.method === "POST").length;
  if (controlPosts < 5) failures.push(`expected multiple control POSTs, saw ${controlPosts}`);
  failures.push(...mock.unhandled);
  await context.close();
}

async function runMobileMock(browser, failures) {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    isMobile: true,
    hasTouch: true,
  });
  await context.addInitScript((token) => {
    localStorage.setItem("swarm_panel_remote_token", token);
    localStorage.setItem("swarm_panel_remote_username", "MockAdmin");
  }, TOKEN);
  const mock = await installMocks(context);
  const page = await context.newPage();
  watchPage(page, failures);

  await page.goto(`${BASE_URL}/`, { waitUntil: "networkidle" });
  await expectVisible(page, "Swarm Command Deck");
  await page.getByRole("link", { name: "Controls" }).click();
  await expectVisible(page, "Direct Playback Control");
  await page.locator("form.form-panel").locator("select").nth(0).selectOption("lockhart");
  await click(page, "button", "Smart Rec");
  await click(page, "button", "Send Control");

  await page.getByRole("link", { name: "Invites" }).click();
  await expectVisible(page, "Bot Access");
  await page.getByRole("link", { name: "Users" }).click();
  await expectVisible(page, "Swarm Directory");
  await page.getByRole("link", { name: "Profile" }).click();
  await expectVisible(page, "Server Identity");
  await page.getByRole("link", { name: "Look" }).click();
  await expectVisible(page, "Panel Look");
  await page.getByRole("link", { name: "Gallery" }).click();
  await expectVisible(page, "Image Gallery Admin");
  await page.getByRole("link", { name: "Intel" }).click();
  await expectVisible(page, "Errors And Metrics");
  await page.getByTitle("Logout").click();
  await expectVisible(page, "Login");

  if (!mock.requests.some((request) => request.path === "/api/bots/control" && request.method === "POST")) {
    failures.push("mobile control flow did not POST");
  }
  failures.push(...mock.unhandled);
  await context.close();
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const failures = [];
  try {
    await runAuthMock(browser, failures);
    await runAuthenticatedMock(browser, failures);
    await runMobileMock(browser, failures);
  } finally {
    await browser.close();
  }
  if (failures.length) {
    console.error(failures.join("\n"));
    process.exit(1);
  }
  console.log("swarmpanel_mock_ui=passed");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
