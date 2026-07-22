"use strict";
/**
 * Wandrian backend, Phase 1 (accounts + persistent saves + graveyard + leaderboard + chat).
 *
 * Deliberately minimal dependencies: Express for HTTP, ws for WebSocket chat, and Node's
 * built-in node:sqlite + node:crypto for storage/auth, so there is nothing here that needs
 * native compilation (no better-sqlite3/bcrypt), easier to deploy on any host.
 *
 * Requires Node.js 22.5+ (for node:sqlite). If your host is stuck on an older Node, swap the
 * DB layer for better-sqlite3 (same API shape), everything else is unaffected.
 *
 * IMPORTANT: this phase moves saves/accounts/chat/leaderboard server-side, which is what lets
 * friends log in from anywhere and keeps chat/leaderboard trustworthy. It does NOT yet stop a
 * player from editing their own client and calling PUT /api/characters/:slot with fabricated
 * numbers, that requires moving combat/loot resolution server-side too (see combat.js / the
 * next phase). Treat this phase as "shared state," not yet "cheat-proof."
 */

const express = require("express");
const http = require("http");
const crypto = require("crypto");
const path = require("path");
const { WebSocketServer } = require("ws");
const { DatabaseSync } = require("node:sqlite");

const PORT = process.env.PORT || 8787;
// Filename kept as evergrind.db intentionally: your already-deployed server's real
// database is at this path, and changing the default here would make a fresh restart
// silently start a brand-new empty database instead of loading existing player data.
// Rename the actual file (and set DB_PATH) yourself if you ever want it renamed.
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data", "evergrind.db");
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*"; // lock this down to your real domain in production
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const MAX_CHARACTER_SLOTS = 6;
const CHAT_HISTORY_LIMIT = 50;
// Storage Vault (v0.11): a 20x10 (200-slot) shared chest, one per ACCOUNT rather than
// per character, so every character on the same account can pull from the same stash.
const VAULT_CAPACITY = 200;

// Leaderboard-moderation token (v0.10.1): deliberately NOT the client's Dev Tools
// "atldp0" password -- that one ships inside index.html, so anyone who views page
// source knows it. This token is a real server-side secret: set it as an env var
// before starting the server, then paste the same value into the Dev Tools screen's
// "Admin Token" field in-game. Without it set, the admin endpoints below refuse to
// run at all (rather than silently accepting an empty/guessable token).
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || null;
if (!ADMIN_TOKEN) {
  console.warn("ADMIN_TOKEN is not set -- leaderboard moderation endpoints are disabled until you set it.");
}

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
  CREATE TABLE IF NOT EXISTS auction_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_account_id INTEGER NOT NULL,
    seller_username TEXT NOT NULL,
    seller_character_name TEXT NOT NULL,
    seller_slot INTEGER NOT NULL,
    type TEXT NOT NULL,
    item_key TEXT,
    item_json TEXT,
    display_name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price INTEGER NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS mailbox_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS vaults (
    account_id INTEGER PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
`);

// Migrations for columns added after the table already existed on a live deployment --
// SQLite's ALTER TABLE ADD COLUMN fails if the column is already there, so these are
// wrapped individually and ignored if they've already been applied.
for (const stmt of [
  "ALTER TABLE leaderboard_bests ADD COLUMN hardcore INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE leaderboard_bests ADD COLUMN lifetime_xp INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE leaderboard_bests ADD COLUMN is_dead INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE accounts ADD COLUMN email TEXT",
]) {
  try { db.exec(stmt); } catch (e) { /* column already exists, fine */ }
}

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

function createAccount(username, passcode, email) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = hashPasscode(passcode, salt);
  const stmt = db.prepare(
    "INSERT INTO accounts (username, passcode_hash, passcode_salt, email, created_at) VALUES (?, ?, ?, ?, ?)"
  );
  const info = stmt.run(username, hash, salt, email || null, nowIso());
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

// Leaderboard moderation is gated behind requireAuth (must be logged in) AND this
// separate admin token, checked with a timing-safe comparison. Always run requireAuth
// first on any route using this.
function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ error: "Admin moderation is not configured on this server (ADMIN_TOKEN not set)." });
  const supplied = req.headers["x-admin-token"] || "";
  const a = Buffer.from(String(supplied));
  const b = Buffer.from(ADMIN_TOKEN);
  const ok = a.length === b.length && crypto.timingSafeEqual(a, b);
  if (!ok) return res.status(403).json({ error: "Invalid admin token." });
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
// unit on your domain, put index.html (the game client) in ./public.
app.use(express.static(path.join(__dirname, "public")));

function isValidUsername(u) {
  return typeof u === "string" && /^[A-Za-z0-9_\-]{3,20}$/.test(u);
}

// email is optional (v0.10): early testers can sign up with just a username and
// password, no onboarding friction, if given, it must at least look like an email,
// but a missing/blank email is never a reason to reject registration.
function isValidOptionalEmail(e) {
  if (e === undefined || e === null || e === "") return true;
  return typeof e === "string" && e.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e);
}

app.post("/api/register", (req, res) => {
  const { username, passcode, email } = req.body || {};
  if (!isValidUsername(username)) {
    return res.status(400).json({ error: "Username must be 3-20 letters/numbers/_/- ." });
  }
  if (typeof passcode !== "string" || passcode.length < 4) {
    return res.status(400).json({ error: "Password must be at least 4 characters." });
  }
  if (!isValidOptionalEmail(email)) {
    return res.status(400).json({ error: "That doesn't look like a valid email (or leave it blank)." });
  }
  if (findAccountByUsername(username)) {
    return res.status(409).json({ error: "That username is already taken." });
  }
  const accountId = createAccount(username, passcode, email);
  const token = createSession(accountId);
  res.json({ token, username });
});

app.post("/api/login", (req, res) => {
  const { username, passcode } = req.body || {};
  const account = findAccountByUsername(username || "");
  if (!account || !verifyPasscode(account, passcode || "")) {
    return res.status(401).json({ error: "Wrong username or password." });
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
      `INSERT INTO leaderboard_bests (account_id, character_name, class_name, level, highest_tier_reached, gold, updated_at, hardcore, lifetime_xp, is_dead)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
       ON CONFLICT(account_id, character_name) DO UPDATE SET
         class_name=excluded.class_name,
         level=MAX(leaderboard_bests.level, excluded.level),
         highest_tier_reached=MAX(leaderboard_bests.highest_tier_reached, excluded.highest_tier_reached),
         gold=MAX(leaderboard_bests.gold, excluded.gold),
         updated_at=excluded.updated_at,
         hardcore=excluded.hardcore,
         lifetime_xp=MAX(leaderboard_bests.lifetime_xp, excluded.lifetime_xp),
         is_dead=0`
    ).run(
      req.account.id,
      data.character_name,
      data.class_display_name,
      data.level || 1,
      data.highest_tier_reached || 1,
      data.gold || 0,
      nowIso(),
      data.hardcore ? 1 : 0,
      data.lifetime_xp || 0
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
    // Keep the leaderboard entry (don't delete it) but mark it dead so the client can
    // render a tombstone / strikethrough instead of pretending they're still active.
    db.prepare("UPDATE leaderboard_bests SET is_dead = 1 WHERE account_id = ? AND character_name = ?").run(
      req.account.id,
      hardcore_death.name || "Hero"
    );
  }
  db.prepare("DELETE FROM characters WHERE account_id = ? AND slot = ?").run(req.account.id, slot);
  res.json({ ok: true });
});

/* Storage Vault (v0.11): one shared 200-slot chest per ACCOUNT (not per character), so
   every character slot on the same account can deposit into and withdraw from the same
   stash. Scoped strictly by req.account.id (set by requireAuth from the caller's own
   session token) -- there is no accountId taken from the request body or URL anywhere
   here, so there is no way to address another account's vault, logged-in or not.
   Like character saves, this is a client-trusted full-replace blob for now (same caveat
   as the rest of the game: combat/loot/inventory aren't server-verified yet). */
app.get("/api/vault", requireAuth, (req, res) => {
  const row = db.prepare("SELECT data FROM vaults WHERE account_id = ?").get(req.account.id);
  let items = [];
  if (row) {
    try {
      items = JSON.parse(row.data);
    } catch (e) {
      items = [];
    }
  }
  res.json({ items, capacity: VAULT_CAPACITY });
});

app.put("/api/vault", requireAuth, (req, res) => {
  const items = req.body && req.body.items;
  if (!Array.isArray(items)) return res.status(400).json({ error: "items must be an array." });
  if (items.length > VAULT_CAPACITY) {
    return res.status(400).json({ error: `The vault only holds ${VAULT_CAPACITY} items.` });
  }
  const json = JSON.stringify(items);
  db.prepare(
    `INSERT INTO vaults (account_id, data, updated_at) VALUES (?, ?, ?)
     ON CONFLICT(account_id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at`
  ).run(req.account.id, json, nowIso());
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
      `SELECT account_id, character_name, class_name, level, highest_tier_reached, gold, lifetime_xp, hardcore, is_dead
       FROM leaderboard_bests ORDER BY highest_tier_reached DESC, level DESC, lifetime_xp DESC, gold DESC LIMIT 50`
    )
    .all();
  // join usernames without leaking passcode data
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return {
      player: acc ? acc.username : "?",
      character_name: r.character_name,
      class_name: r.class_name,
      level: r.level,
      highest_tier_reached: r.highest_tier_reached,
      gold: r.gold,
      lifetime_xp: r.lifetime_xp || 0,
      hardcore: !!r.hardcore,
      is_dead: !!r.is_dead,
    };
  });
  res.json({ entries: withNames });
});

/* ---------------- admin: leaderboard moderation ----------------
   For cleaning up entries that look buggy or clearly cheated -- e.g. a record that
   doesn't correspond to anything in your own accessible characters, or stats far
   outside what normal play could produce. This only removes the leaderboard_bests
   row (the ranking entry itself), not the underlying character save, graveyard
   history, or account -- it just takes the entry off the ladder. */

// Lets the client confirm a typed-in token is the real ADMIN_TOKEN before unlocking
// Dev Tools, without ever embedding the secret itself in the client code.
app.get("/api/admin/verify", requireAuth, requireAdmin, (req, res) => {
  res.json({ ok: true });
});

app.get("/api/admin/leaderboard", requireAuth, requireAdmin, (req, res) => {
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, level, highest_tier_reached, gold, lifetime_xp, hardcore, is_dead, updated_at
       FROM leaderboard_bests ORDER BY highest_tier_reached DESC, level DESC, lifetime_xp DESC, gold DESC`
    )
    .all();
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return {
      account_id: r.account_id,
      player: acc ? acc.username : "?",
      character_name: r.character_name,
      class_name: r.class_name,
      level: r.level,
      highest_tier_reached: r.highest_tier_reached,
      gold: r.gold,
      lifetime_xp: r.lifetime_xp || 0,
      hardcore: !!r.hardcore,
      is_dead: !!r.is_dead,
      updated_at: r.updated_at,
    };
  });
  res.json({ entries: withNames });
});

app.delete("/api/admin/leaderboard/:accountId/:characterName", requireAuth, requireAdmin, (req, res) => {
  const accountId = Number(req.params.accountId);
  const characterName = decodeURIComponent(req.params.characterName);
  if (!Number.isInteger(accountId)) return res.status(400).json({ error: "Invalid account id." });
  const info = db
    .prepare("DELETE FROM leaderboard_bests WHERE account_id = ? AND character_name = ?")
    .run(accountId, characterName);
  console.log(
    `[admin] ${req.account.username} removed leaderboard entry account_id=${accountId} character_name="${characterName}" (${info.changes} row(s) affected)`
  );
  if (info.changes === 0) return res.status(404).json({ error: "No matching leaderboard entry found." });
  res.json({ ok: true, removed: info.changes });
});

app.get("/api/health", (req, res) => res.json({ ok: true, time: nowIso() }));

/* ---------------- system chat announcements (trial results, hardcore deaths) ---------------- */
// These are self-reported by the client (consistent with this phase's "not cheat-proof yet"
// trust model, see the top-of-file note) but are flavor-only chat text, no economy impact.

function broadcastSystemMessage(message) {
  const created_at = nowIso();
  db.prepare("INSERT INTO chat_messages (username, message, created_at) VALUES (?, ?, ?)").run("System", message, created_at);
  const payload = JSON.stringify({ type: "chat", username: "System", message, created_at });
  for (const client of chatClients) {
    if (client.readyState === client.OPEN) client.send(payload);
  }
}

app.post("/api/announce/trial", requireAuth, (req, res) => {
  const { character_name, level, class_name, result, new_class_name, failed_step } = req.body || {};
  if (!character_name || !class_name || (result !== "passed" && result !== "failed")) {
    return res.status(400).json({ error: "Invalid announcement." });
  }
  let suffix = "";
  if (result === "passed" && new_class_name) suffix = ` and is now a ${new_class_name}`;
  else if (result === "failed" && failed_step) suffix = ` at step ${failed_step}`;
  broadcastSystemMessage(`${character_name} (Lv ${level || 1} ${class_name}) has ${result} the broken bridge trial${suffix}.`);
  res.json({ ok: true });
});

app.post("/api/announce/death", requireAuth, (req, res) => {
  const { character_name, level, class_name, cause } = req.body || {};
  if (!character_name || !class_name || !cause) {
    return res.status(400).json({ error: "Invalid announcement." });
  }
  broadcastSystemMessage(`${character_name} (Lv ${level || 1} ${class_name}) has ${cause}.`);
  res.json({ ok: true });
});

/* ---------------- private mailbox (auction sale notifications) ---------------- */

function sendPrivateMessage(accountId, message) {
  const created_at = nowIso();
  let delivered = 0;
  for (const client of chatClients) {
    if (client.accountId === accountId && client.readyState === client.OPEN) {
      client.send(JSON.stringify({ type: "private", message, created_at }));
      delivered = 1;
    }
  }
  db.prepare("INSERT INTO mailbox_messages (account_id, message, created_at, delivered) VALUES (?, ?, ?, ?)").run(
    accountId,
    message,
    created_at,
    delivered
  );
}

/* ---------------- auction house ---------------- */

app.get("/api/auction", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT * FROM auction_listings ORDER BY created_at DESC LIMIT 100").all();
  const out = rows.map((r) => ({
    id: r.id,
    seller_username: r.seller_username,
    seller_character_name: r.seller_character_name,
    type: r.type,
    item_key: r.item_key,
    item: r.item_json ? JSON.parse(r.item_json) : null,
    display_name: r.display_name,
    quantity: r.quantity,
    price: r.price,
  }));
  res.json({ listings: out });
});

app.post("/api/auction", requireAuth, (req, res) => {
  const { type, price, character_name, item, item_key, quantity, display_name, seller_slot } = req.body || {};
  if (!["gear", "consumable", "herb"].includes(type)) return res.status(400).json({ error: "Invalid item type." });
  const numPrice = Number(price);
  if (!Number.isFinite(numPrice) || numPrice < 1) return res.status(400).json({ error: "Invalid price." });
  if (!character_name || !display_name) return res.status(400).json({ error: "Missing listing details." });
  const numQty = type === "gear" ? 1 : Math.max(1, Number(quantity) || 1);
  const itemJson = type === "gear" ? JSON.stringify(item || {}) : null;
  const info = db
    .prepare(
      `INSERT INTO auction_listings
       (seller_account_id, seller_username, seller_character_name, seller_slot, type, item_key, item_json, display_name, quantity, price, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(
      req.account.id,
      req.account.username,
      character_name,
      Number.isInteger(seller_slot) ? seller_slot : -1,
      type,
      type === "gear" ? null : String(item_key || ""),
      itemJson,
      display_name,
      numQty,
      Math.round(numPrice),
      nowIso()
    );
  res.json({ ok: true, id: Number(info.lastInsertRowid) });
});

app.delete("/api/auction/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const listing = db.prepare("SELECT * FROM auction_listings WHERE id = ?").get(id);
  if (!listing) return res.status(404).json({ error: "Listing not found." });
  if (listing.seller_account_id !== req.account.id) return res.status(403).json({ error: "Not your listing." });
  db.prepare("DELETE FROM auction_listings WHERE id = ?").run(id);
  res.json({ ok: true, refund: { type: listing.type, item_key: listing.item_key, item: listing.item_json ? JSON.parse(listing.item_json) : null, quantity: listing.quantity } });
});

app.post("/api/auction/:id/buy", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const { character_name } = req.body || {};
  const listing = db.prepare("SELECT * FROM auction_listings WHERE id = ?").get(id);
  if (!listing) return res.status(404).json({ error: "That listing is no longer available." });
  if (listing.seller_account_id === req.account.id) return res.status(400).json({ error: "You can't buy your own listing." });

  // Best-effort gold check against the buyer's own persisted characters (defense in depth --
  // the client is still trusted for its own post-purchase save, per this phase's model).
  const buyerChars = db.prepare("SELECT data FROM characters WHERE account_id = ?").all(req.account.id);
  const hasEnoughGold = buyerChars.some((row) => {
    try {
      const d = JSON.parse(row.data);
      return (d.gold || 0) >= listing.price;
    } catch (e) {
      return false;
    }
  });
  if (!hasEnoughGold) return res.status(400).json({ error: "Not enough gold." });

  // Remove the listing first (best-effort race protection against double-buy).
  const del = db.prepare("DELETE FROM auction_listings WHERE id = ?").run(id);
  if (del.changes === 0) return res.status(409).json({ error: "Someone already bought that." });

  // Credit the seller directly, since the seller's own client isn't present for this request.
  if (listing.seller_slot >= 0) {
    const sellerRow = db
      .prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?")
      .get(listing.seller_account_id, listing.seller_slot);
    if (sellerRow) {
      try {
        const d = JSON.parse(sellerRow.data);
        d.gold = (d.gold || 0) + listing.price;
        db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
          JSON.stringify(d),
          nowIso(),
          listing.seller_account_id,
          listing.seller_slot
        );
      } catch (e) {
        /* if the seller's save is somehow malformed, skip the credit rather than crash the sale */
      }
    }
  }

  const label = listing.quantity > 1 ? `${listing.display_name} x${listing.quantity}` : listing.display_name;
  sendPrivateMessage(req.account.id, `You bought ${label} for ${listing.price} gold from ${listing.seller_character_name}.`);
  sendPrivateMessage(listing.seller_account_id, `Your ${label} has been sold to ${character_name || req.account.username} for ${listing.price} gold.`);

  res.json({
    ok: true,
    type: listing.type,
    item_key: listing.item_key,
    item: listing.item_json ? JSON.parse(listing.item_json) : null,
    quantity: listing.quantity,
  });
});

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
  ws.accountId = account.id;
  chatClients.add(ws);
  ws.send(JSON.stringify({ type: "history", messages: loadRecentChat() }));

  const pending = db
    .prepare("SELECT message, created_at FROM mailbox_messages WHERE account_id = ? AND delivered = 0 ORDER BY id ASC")
    .all(account.id);
  if (pending.length > 0) {
    ws.send(JSON.stringify({ type: "private_history", messages: pending }));
    db.prepare("UPDATE mailbox_messages SET delivered = 1 WHERE account_id = ? AND delivered = 0").run(account.id);
  }

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
  console.log(`Wandrian server listening on port ${PORT} (db: ${DB_PATH})`);
});
