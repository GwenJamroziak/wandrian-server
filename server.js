"use strict";
/**
 * Evergrind backend -- Phase 1 (accounts + persistent saves + graveyard + leaderboard + chat).
 *
 * Deliberately minimal dependencies: Express for HTTP, ws for WebSocket chat, and Node's
 * built-in node:sqlite + node:crypto for storage/auth, so there is nothing here that needs
 * native compilation (no better-sqlite3/bcrypt) -- easier to deploy on any host.
 *
 * Requires Node.js 22.5+ (for node:sqlite). If your host is stuck on an older Node, swap the
 * DB layer for better-sqlite3 (same API shape) -- everything else is unaffected.
 *
 * IMPORTANT: this phase moves saves/accounts/chat/leaderboard server-side, which is what lets
 * friends log in from anywhere and keeps chat/leaderboard trustworthy. It does NOT yet stop a
 * player from editing their own client and calling PUT /api/characters/:slot with fabricated
 * numbers -- that requires moving combat/loot resolution server-side too (see combat.js / the
 * next phase). Treat this phase as "shared state," not yet "cheat-proof."
 */

const express = require("express");
const http = require("http");
const crypto = require("crypto");
const path = require("path");
const { WebSocketServer } = require("ws");
const { DatabaseSync } = require("node:sqlite");

const PORT = process.env.PORT || 8787;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "evergrind.db");
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*"; // lock this down to your real domain in production
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const MAX_CHARACTER_SLOTS = 6;
const CHAT_HISTORY_LIMIT = 50;

/* ---------------- DB setup ---------------- */

const db = new DatabaseSync(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    passcode_hash TEXT NOT NULL,
    passcode_salt TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    account_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS characters (
    account_id INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, slot)
  );
  CREATE TABLE IF NOT EXISTS graveyard (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    name TEXT NOT NULL,
    class_name TEXT NOT NULL,
    level INTEGER NOT NULL,
    cause TEXT,
    died_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS leaderboard_bests (
    account_id INTEGER NOT NULL,
    character_name TEXT NOT NULL,
    class_name TEXT NOT NULL,
    level INTEGER NOT NULL,
    highest_tier_reached INTEGER NOT NULL,
    gold INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, character_name)
  );
  CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
`);

/* ---------------- auth helpers ---------------- */

function hashPasscode(passcode, salt) {
  return crypto.scryptSync(passcode, salt, 64).toString("hex");
}
function newToken() {
  return crypto.randomBytes(24).toString("hex");
}
function nowIso() {
  return new Date().toISOString();
}

function createAccount(username, passcode) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = hashPasscode(passcode, salt);
  const stmt = db.prepare(
    "INSERT INTO accounts (username, passcode_hash, passcode_salt, created_at) VALUES (?, ?, ?, ?)"
  );
  const info = stmt.run(username, hash, salt, nowIso());
  return Number(info.lastInsertRowid);
}

function findAccountByUsername(username) {
  return db.prepare("SELECT * FROM accounts WHERE username = ?").get(username);
}

function verifyPasscode(account, passcode) {
  const candidate = hashPasscode(passcode, account.passcode_salt);
  const a = Buffer.from(candidate, "hex");
  const b = Buffer.from(account.passcode_hash, "hex");
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function createSession(accountId) {
  const token = newToken();
  const created = nowIso();
  const expires = new Date(Date.now() + SESSION_TTL_MS).toISOString();
  db.prepare("INSERT INTO sessions (token, account_id, created_at, expires_at) VALUES (?, ?, ?, ?)").run(
    token,
    accountId,
    created,
    expires
  );
  return token;
}

function accountForToken(token) {
  if (!token) return null;
  const row = db.prepare("SELECT * FROM sessions WHERE token = ?").get(token);
  if (!row) return null;
  if (new Date(row.expires_at).getTime() < Date.now()) {
    db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
    return null;
  }
  return db.prepare("SELECT * FROM accounts WHERE id = ?").get(row.account_id);
}

function requireAuth(req, res, next) {
  const header = req.headers["authorization"] || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  const account = accountForToken(token);
  if (!account) return res.status(401).json({ error: "Not authenticated." });
  req.account = account;
  next();
}

/* ---------------- app ---------------- */

const app = express();
app.use(express.json({ limit: "256kb" }));
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", CORS_ORIGIN);
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// Serve the game client itself, so the whole thing (client + API) is one deployable
// unit on your domain -- put evergrind_web.html (renamed to index.html) in ./public.
app.use(express.static(path.join(__dirname, "public")));

function isValidUsername(u) {
  return typeof u === "string" && /^[A-Za-z0-9_\-]{3,20}$/.test(u);
}

app.post("/api/register", (req, res) => {
  const { username, passcode } = req.body || {};
  if (!isValidUsername(username)) {
    return res.status(400).json({ error: "Username must be 3-20 letters/numbers/_/- ." });
  }
  if (typeof passcode !== "string" || passcode.length < 4) {
    return res.status(400).json({ error: "Passcode must be at least 4 characters." });
  }
  if (findAccountByUsername(username)) {
    return res.status(409).json({ error: "That username is already taken." });
  }
  const accountId = createAccount(username, passcode);
  const token = createSession(accountId);
  res.json({ token, username });
});

app.post("/api/login", (req, res) => {
  const { username, passcode } = req.body || {};
  const account = findAccountByUsername(username || "");
  if (!account || !verifyPasscode(account, passcode || "")) {
    return res.status(401).json({ error: "Wrong username or passcode." });
  }
  const token = createSession(account.id);
  res.json({ token, username: account.username });
});

app.get("/api/characters", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT slot, data, updated_at FROM characters WHERE account_id = ?").all(req.account.id);
  const bySlot = {};
  for (const row of rows) bySlot[row.slot] = { data: JSON.parse(row.data), updated_at: row.updated_at };
  const slots = [];
  for (let i = 0; i < MAX_CHARACTER_SLOTS; i++) slots.push(bySlot[i] ? { slot: i, ...bySlot[i] } : { slot: i, empty: true });
  res.json({ slots });
});

app.put("/api/characters/:slot", requireAuth, (req, res) => {
  const slot = Number(req.params.slot);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid character data." });
  const json = JSON.stringify(data);
  db.prepare(
    `INSERT INTO characters (account_id, slot, data, updated_at) VALUES (?, ?, ?, ?)
     ON CONFLICT(account_id, slot) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at`
  ).run(req.account.id, slot, json, nowIso());

  // Track this character's personal best for the leaderboard, independent of live deletion.
  if (data.character_name && data.class_display_name) {
    db.prepare(
      `INSERT INTO leaderboard_bests (account_id, character_name, class_name, level, highest_tier_reached, gold, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(account_id, character_name) DO UPDATE SET
         class_name=excluded.class_name,
         level=MAX(leaderboard_bests.level, excluded.level),
         highest_tier_reached=MAX(leaderboard_bests.highest_tier_reached, excluded.highest_tier_reached),
         gold=MAX(leaderboard_bests.gold, excluded.gold),
         updated_at=excluded.updated_at`
    ).run(
      req.account.id,
      data.character_name,
      data.class_display_name,
      data.level || 1,
      data.highest_tier_reached || 1,
      data.gold || 0,
      nowIso()
    );
  }
  res.json({ ok: true });
});

app.delete("/api/characters/:slot", requireAuth, (req, res) => {
  const slot = Number(req.params.slot);
  const { hardcore_death } = req.body || {};
  if (hardcore_death) {
    db.prepare(
      "INSERT INTO graveyard (account_id, username, name, class_name, level, cause, died_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(
      req.account.id,
      req.account.username,
      hardcore_death.name || "Hero",
      hardcore_death.class_name || "",
      hardcore_death.level || 1,
      hardcore_death.cause || "Died in the wilds",
      nowIso()
    );
  }
  db.prepare("DELETE FROM characters WHERE account_id = ? AND slot = ?").run(req.account.id, slot);
  res.json({ ok: true });
});

app.get("/api/graveyard", requireAuth, (req, res) => {
  const rows = db
    .prepare("SELECT name, class_name, level, cause, died_at FROM graveyard WHERE account_id = ? ORDER BY died_at DESC LIMIT 100")
    .all(req.account.id);
  res.json({ entries: rows });
});

app.get("/api/leaderboard", (req, res) => {
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, level, highest_tier_reached, gold
       FROM leaderboard_bests ORDER BY level DESC, highest_tier_reached DESC, gold DESC LIMIT 50`
    )
    .all();
  // join usernames without leaking passcode data
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return { player: acc ? acc.username : "?", character_name: r.character_name, class_name: r.class_name, level: r.level, highest_tier_reached: r.highest_tier_reached, gold: r.gold };
  });
  res.json({ entries: withNames });
});

app.get("/api/health", (req, res) => res.json({ ok: true, time: nowIso() }));

const server = http.createServer(app);

/* ---------------- chat over WebSocket ---------------- */

const wss = new WebSocketServer({ server, path: "/ws/chat" });
const chatClients = new Set();

function loadRecentChat() {
  return db
    .prepare("SELECT username, message, created_at FROM chat_messages ORDER BY id DESC LIMIT ?")
    .all(CHAT_HISTORY_LIMIT)
    .reverse();
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://localhost");
  const token = url.searchParams.get("token");
  const account = accountForToken(token);
  if (!account) {
    ws.send(JSON.stringify({ type: "error", message: "Not authenticated." }));
    ws.close();
    return;
  }
  ws.username = account.username;
  chatClients.add(ws);
  ws.send(JSON.stringify({ type: "history", messages: loadRecentChat() }));

  ws.on("message", (raw) => {
    let parsed;
    try {
      parsed = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }
    if (parsed.type !== "chat" || typeof parsed.message !== "string") return;
    const message = parsed.message.slice(0, 300).trim();
    if (!message) return;
    const created_at = nowIso();
    db.prepare("INSERT INTO chat_messages (username, message, created_at) VALUES (?, ?, ?)").run(
      ws.username,
      message,
      created_at
    );
    const payload = JSON.stringify({ type: "chat", username: ws.username, message, created_at });
    for (const client of chatClients) {
      if (client.readyState === client.OPEN) client.send(payload);
    }
  });

  ws.on("close", () => chatClients.delete(ws));
});

server.listen(PORT, () => {
  console.log(`Evergrind server listening on port ${PORT} (db: ${DB_PATH})`);
});
