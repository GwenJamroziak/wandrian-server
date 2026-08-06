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
const fs = require("fs");
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
// v0.24.2 / v0.25.1: the known-issues banner's length cap. Raised from 300 to 500 at Gwen's
// request so a short multi-issue summary fits. Newlines inside the message are deliberately
// PRESERVED (only leading/trailing whitespace is trimmed at the route), so an admin can write a
// few bulleted lines; the client renders the banner with white-space:pre-line to match.
const ADMIN_ANNOUNCEMENT_MAX_LEN = 500;
if (!ADMIN_TOKEN) {
  console.warn("ADMIN_TOKEN is not set -- leaderboard moderation endpoints are disabled until you set it.");
}

/* ---------------- DB setup ---------------- */

// v0.23.6: node:sqlite does not create missing parent directories on its own -- a fresh
// checkout/clone that doesn't carry over the (often empty, so not always preserved by zip/
// copy/git) data/ folder used to crash here with a cryptic "unable to open database file"
// (ERR_SQLITE_ERROR) instead of just working. Guard against that once, up front.
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
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
  -- v0.16 (#334): Graveyard flower tribute -- one row per (grave, account) pair, so a
  -- given logged-in account can leave exactly one tribute on any given grave (the UNIQUE
  -- constraint below is what enforces that server-side; the client can retry all it wants,
  -- it'll just get the same 400 back). username is copied from the account at insert time
  -- (not client-supplied) purely so the public GET /api/graveyard response doesn't need to
  -- join against accounts every time it's read.
  CREATE TABLE IF NOT EXISTS graveyard_tributes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grave_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(grave_id, account_id)
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
  CREATE TABLE IF NOT EXISTS admin_announcement (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    message TEXT NOT NULL DEFAULT '',
    updated_at TEXT
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
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0
  );
  -- v0.22.3 (#10): single-row table recording whether this ladder season's champion (first
  -- tier-5 character to reach Level 100) has already been declared -- guards against a
  -- second "winner" being announced by a race between two players leveling up at once, and
  -- lets the admin's manual ladder reset (see /api/admin/reset-ladder) clear it back to 0 to
  -- start a fresh season.
  CREATE TABLE IF NOT EXISTS season_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    winner_declared INTEGER NOT NULL DEFAULT 0,
    winner_account_id INTEGER,
    winner_character_name TEXT,
    winner_class_name TEXT,
    won_at TEXT
  );
  -- Endgame choice (Keeper of the Emerald): permanent log of every Tier-5, Level-100
  -- character that has stood before the Keeper at Ashveil Sanctum and made their choice.
  -- This is a pure append-only record for the "Hall of Healers" / "Hall of Victors" pages --
  -- it does NOT drive any reset logic. Deliberately, choosing "crown" here does not touch
  -- any other character's data or trigger a mass reset; for this first season the only way
  -- to reset a character's ladder progress is the existing manual, one-at-a-time
  -- /api/admin/reset-ladder/:accountId/:slot endpoint. See maybeResolveKeeperChoice()'s
  -- comment for why an automatic global reset is intentionally NOT wired up yet.
  CREATE TABLE IF NOT EXISTS hall_of_heroes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    character_name TEXT NOT NULL,
    class_name TEXT NOT NULL,
    choice TEXT NOT NULL CHECK (choice IN ('emerald', 'crown')),
    chosen_at TEXT NOT NULL
  );
`);
db.prepare("INSERT OR IGNORE INTO season_state (id, winner_declared) VALUES (1, 0)").run();
db.prepare("INSERT OR IGNORE INTO admin_announcement (id, message, updated_at) VALUES (1, '', NULL)").run();

// Migrations for columns added after the table already existed on a live deployment --
// SQLite's ALTER TABLE ADD COLUMN fails if the column is already there, so these are
// wrapped individually and ignored if they've already been applied.
for (const stmt of [
  "ALTER TABLE leaderboard_bests ADD COLUMN hardcore INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE leaderboard_bests ADD COLUMN lifetime_xp INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE leaderboard_bests ADD COLUMN is_dead INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE accounts ADD COLUMN email TEXT",
  "ALTER TABLE vaults ADD COLUMN version INTEGER NOT NULL DEFAULT 0",
  // v0.17 (#1): new leaderboard sort tiebreakers -- "Class rank (highest_tier_reached) >
  // bridge steps > level > current-level XP". last_bridge_steps and xp are both CURRENT
  // (non-monotonic) snapshots, same semantics as `level`, not lifetime maxes like
  // highest_tier_reached/lifetime_xp -- see the ON CONFLICT clause below.
  "ALTER TABLE leaderboard_bests ADD COLUMN last_bridge_steps INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE leaderboard_bests ADD COLUMN xp INTEGER NOT NULL DEFAULT 0",
  // v0.17 (#29): gold moves from being a field inside each character's own `data` JSON
  // blob to a single ACCOUNT-BOUND column, shared by every character on the account. See
  // the one-time migration block right after this ALTER TABLE loop, which sums each
  // existing account's characters' gold into this column exactly once (gold_migrated
  // guards against re-summing on every server restart).
  "ALTER TABLE accounts ADD COLUMN gold INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE accounts ADD COLUMN gold_migrated INTEGER NOT NULL DEFAULT 0",
  // v0.17.3 (#13): denormalized copies of each character's weapon-skill hit counters and
  // herbalism points, kept in step with the same upsert that already tracks level/xp/tier
  // on every save -- lets the "Skill Progression" leaderboard tab rank every character by
  // these without touching the private per-character `data` JSON blob at read time. Both
  // are plain CURRENT snapshots (excluded.value, no MAX()) same as level/gold/xp -- see the
  // ON CONFLICT clause below for why that's correct even though the values themselves only
  // ever go up while a given character is alive (a deleted-and-recreated character gets a
  // brand new leaderboard_bests row from scratch, not a stale MAX()'d one).
  "ALTER TABLE leaderboard_bests ADD COLUMN weapon_skills TEXT NOT NULL DEFAULT '{}'",
  "ALTER TABLE leaderboard_bests ADD COLUMN herbalism_points INTEGER NOT NULL DEFAULT 0",
  // v0.23.0 (Part B9): Spellcasting proficiency's raw hit counter, ridden along on the same
  // upsert as weapon_skills/herbalism_points -- feeds the Leaderboard's "Skill Progression"
  // tab alongside the weapon-type skills.
  "ALTER TABLE leaderboard_bests ADD COLUMN spellcasting_hits INTEGER NOT NULL DEFAULT 0",
  // v0.18 (#18): guard column for the one-time stale-bridge-steps cleanup below -- see
  // migrateStaleBridgeSteps() for what it's cleaning up and why a blanket reset (rather than
  // trying to guess which rows are actually corrupt) is the safe choice here.
  "ALTER TABLE leaderboard_bests ADD COLUMN bridge_steps_migrated_v18 INTEGER NOT NULL DEFAULT 0",
  // v0.18.2 (#8): lifetime Kill Streak record per character, for the new "Max Killstreak"
  // leaderboard tab -- see index.html's PS.max_kill_streak declaration. DEFAULT 0 means every
  // existing row on a live deployment just starts showing 0 until that character's next
  // save (harmless -- nothing else reads or depends on this column, so there's no data to
  // lose or backfill here, unlike the gold/bridge-steps migrations above).
  "ALTER TABLE leaderboard_bests ADD COLUMN max_kill_streak INTEGER NOT NULL DEFAULT 0",
  // v0.21 (#17): lifetime mob-kill counter per character, for the new "Monsters Killed"
  // leaderboard tab. Unlike kill_streak (resets to 0 on every town return/reload -- see
  // resetKillStreak()) this NEVER resets while the character is alive, and is MAX()'d
  // against the existing row on every save exactly like max_kill_streak above, for the
  // same reason (an out-of-order/retried save can never stomp a higher count back down).
  "ALTER TABLE leaderboard_bests ADD COLUMN total_kills INTEGER NOT NULL DEFAULT 0",
  // v0.24.2: chat lines now carry a machine-readable kind so the client can style them
  // without pattern matching their text. "chat" = a real player message, "system" = anything
  // broadcastSystemMessage() emits, "death" = a character death notice, "admin" = an admin
  // broadcast. Existing rows default to "chat"/"system" via the backfill right below.
  "ALTER TABLE chat_messages ADD COLUMN kind TEXT NOT NULL DEFAULT 'chat'",
  // v0.24.2: set exactly once by whichever path first announces a given character's death
  // (the server-authoritative combat permadeath, or the client's own courtesy announce for
  // deaths the server can't see, e.g. a lethal trap) so the same death can never be
  // broadcast to global chat twice.
  "ALTER TABLE leaderboard_bests ADD COLUMN death_announced INTEGER NOT NULL DEFAULT 0",
]) {
  try { db.exec(stmt); } catch (e) { /* column already exists, fine */ }
}

// v0.24.2 ONE-TIME BACKFILL: every chat line already in history predates the `kind` column
// and got the "chat" default, which would render an old System announcement as if a player
// had said it. Anything stored under the reserved "System" username is retroactively tagged
// "system" (the generic style) -- old death/admin lines stay generic rather than being
// guessed at from their wording, which is fine: only new ones need the richer styling.
try { db.prepare("UPDATE chat_messages SET kind = 'system' WHERE username = 'System' AND kind = 'chat'").run(); } catch (e) { /* pre-migration schema, nothing to backfill */ }

// v0.17 (#29) ONE-TIME MIGRATION: gold used to live per-character (inside each row's own
// `data` JSON blob); it's now account-bound and shared. For every account that hasn't been
// migrated yet, sum the `gold` field out of every one of its existing characters and store
// that total as the account's new single gold balance -- "keeping it fair for players with
// current progression" per Gwen's explicit instruction, and critically, LOSSLESS: the sum
// of the parts becomes the whole, nothing is discarded. Guarded by `gold_migrated` so this
// only ever runs once per account, no matter how many times the server restarts (a brand
// new account created after this code shipped just has zero characters to sum, so it's a
// harmless no-op that immediately marks itself migrated).
(function migrateAccountGold() {
  const unmigrated = db.prepare("SELECT id FROM accounts WHERE gold_migrated = 0").all();
  if (unmigrated.length === 0) return;
  const sumStmt = db.prepare("SELECT data FROM characters WHERE account_id = ?");
  const applyStmt = db.prepare("UPDATE accounts SET gold = ?, gold_migrated = 1 WHERE id = ?");
  // NOTE: this project uses node's built-in node:sqlite (DatabaseSync), not better-sqlite3 --
  // DatabaseSync has no higher-order transaction-wrapper method, so atomicity is done by hand
  // with a raw BEGIN/COMMIT (and ROLLBACK on failure) around the whole batch instead.
  db.exec("BEGIN");
  try {
    for (const acc of unmigrated) {
      let total = 0;
      for (const row of sumStmt.all(acc.id)) {
        try {
          const d = JSON.parse(row.data);
          total += Number(d.gold) || 0;
        } catch (e) {
          /* a corrupt row contributes 0 rather than aborting the whole account's migration */
        }
      }
      applyStmt.run(total, acc.id);
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
  console.log(`[migration] account gold: migrated ${unmigrated.length} account(s) to the shared account-level gold column.`);
})();

// v0.18 (#18) ONE-TIME MIGRATION: `last_bridge_steps` ("how far you got on your last Broken
// Bridge Trial attempt") had a bug, fixed across v0.17.1 (client no longer forgets to reset
// it to 0 the moment a promotion happens) and v0.17.2 (server-side ON CONFLICT clause now
// forces it back to 0 the instant it sees highest_tier_reached increase, as a safety net).
// Both fixes are self-healing for any promotion that happens FROM NOW ON, but neither one
// retroactively touches a row that was already promoted before the fix shipped -- ordinary
// gameplay (walking, fighting, shopping) never writes to this field, only a bridge-trial
// attempt does, so a player who was promoted pre-fix and simply hasn't attempted another
// trial since is still stuck showing a stale, leftover step count from their OLD tier next
// to their CURRENT (higher) tier on the public leaderboard.
//
// There's no reliable way to tell, from the data alone, which nonzero values are this stale
// leftover vs. a legitimate "failed my last attempt at my current tier and haven't played
// since" -- both look identical (a plain 0-9 number). But `last_bridge_steps` is explicitly
// just a "how far did you get on your LAST attempt" display/tiebreaker stat, not real
// progression -- it's never load-bearing for level, gold, gear, or tier, all of which are
// left completely untouched here. So the safe call is a blanket one-time reset to 0 for every
// row that still has a nonzero value: worst case, a handful of players see their footstep
// tiebreaker number reset to 0 instead of a legitimate small number, which costs them
// nothing and re-populates itself accurately the moment they next attempt the trial (pass or
// fail). Guarded by bridge_steps_migrated_v18 so, like the gold migration above, this only
// ever touches each row once, no matter how many times the server restarts.
(function migrateStaleBridgeSteps() {
  const unmigrated = db.prepare("SELECT account_id, character_name, last_bridge_steps FROM leaderboard_bests WHERE bridge_steps_migrated_v18 = 0").all();
  if (unmigrated.length === 0) return;
  const resetStmt = db.prepare("UPDATE leaderboard_bests SET last_bridge_steps = 0, bridge_steps_migrated_v18 = 1 WHERE account_id = ? AND character_name = ?");
  const markStmt = db.prepare("UPDATE leaderboard_bests SET bridge_steps_migrated_v18 = 1 WHERE account_id = ? AND character_name = ?");
  let resetCount = 0;
  db.exec("BEGIN");
  try {
    for (const row of unmigrated) {
      if (row.last_bridge_steps > 0) {
        resetStmt.run(row.account_id, row.character_name);
        resetCount++;
      } else {
        markStmt.run(row.account_id, row.character_name);
      }
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
  console.log(`[migration] stale bridge steps: reviewed ${unmigrated.length} leaderboard row(s), reset ${resetCount} stale/ambiguous last_bridge_steps value(s) to 0. No level, gold, gear, or tier data was touched.`);
})();

// v0.17 (#29): small helpers so every gold-touching route (character save, Auction House
// listing fee, Auction House buy/sell credit) reads and writes the same single source of
// truth the same way.
function getAccountGold(accountId) {
  const row = db.prepare("SELECT gold FROM accounts WHERE id = ?").get(accountId);
  return row ? row.gold : 0;
}
function setAccountGold(accountId, newGold) {
  db.prepare("UPDATE accounts SET gold = ? WHERE id = ?").run(Math.max(0, Math.round(newGold)), accountId);
}
function creditAccountGold(accountId, amount) {
  const current = getAccountGold(accountId);
  const next = Math.max(0, Math.round(current + amount));
  setAccountGold(accountId, next);
  return next;
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
// v0.16: Auction House listing fee -- Gwen's spec is "1g for items priced 1-99g, 2g for
// v0.21.1 (#3): simplified to a flat 1% commission on the asking price (was a tiered "+1g
// per 100g bracket" fee), floored at 1g so a listing is never effectively free. Charged
// server-side (not just checked/deducted on the client) so a modified client can't post
// free listings -- see its use in POST /api/auction below.
function auctionListingFee(price) {
  return Math.max(1, Math.ceil(price * 0.01));
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
  // v0.24.2: X-Admin-Token added -- the admin routes have always read it, but it was never
  // on this allowlist, so any deployment serving the client from a different origin than the
  // API would have had every Dev Tools request fail its CORS preflight.
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Admin-Token");
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
  // v0.12: a real (server-verified, not self-reported) global chat ping so the rest of
  // the woods knows someone just signed in. broadcastSystemMessage is defined further
  // down the file but hoisted as a function declaration, so it's callable here.
  broadcastSystemMessage(`${account.username} has entered the woods.`);
});

app.get("/api/characters", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT slot, data, updated_at FROM characters WHERE account_id = ?").all(req.account.id);
  const accountGold = getAccountGold(req.account.id);
  const bySlot = {};
  for (const row of rows) {
    const data = JSON.parse(row.data);
    // v0.17 (#29): gold is account-bound now -- always hand back the CURRENT authoritative
    // account total here, regardless of whatever (now-stale) gold figure happens to be
    // sitting in this character's own stored JSON blob. This is what lets a player catch
    // up on gold credited by an Auction House sale that completed while they were offline
    // or playing a different character -- see Net.refreshCharacters() client-side.
    data.gold = accountGold;
    // v0.24: lazily migrate the old flat owned_spells array into owned_spell_tiers (Tier I,
    // free) here too, so a character who hasn't touched the Magician since before v0.24 still
    // sees correct tier ownership the moment they load into the game.
    migrateSpellTiers(data);
    bySlot[row.slot] = { data, updated_at: row.updated_at };
  }
  const slots = [];
  for (let i = 0; i < MAX_CHARACTER_SLOTS; i++) slots.push(bySlot[i] ? { slot: i, ...bySlot[i] } : { slot: i, empty: true });
  res.json({ slots, account_gold: accountGold });
});

// v0.22.3 (#1): lightweight live-feedback endpoint for the character-creation name field --
// same case-insensitive "does any OTHER live character already have this name" rule the real
// enforcement (the PUT route below) applies, just read-only and with no slot to exclude (a
// brand-new character has no slot of its own yet). This is purely a UX nicety; the PUT route's
// 409 is what actually stops a duplicate name from being saved even if this check is bypassed.
app.get("/api/character-name-available", requireAuth, (req, res) => {
  const name = (req.query.name || "").toString().trim();
  if (!name) return res.json({ available: false });
  const conflict = db
    .prepare(`SELECT 1 FROM characters WHERE LOWER(json_extract(data,'$.character_name')) = LOWER(?)`)
    .get(name);
  res.json({ available: !conflict });
});

app.put("/api/characters/:slot", requireAuth, (req, res) => {
  const slot = Number(req.params.slot);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid character data." });

  // HOTFIX (CRITICAL): a hardcore character that was just permadeath'd by
  // combatHandleHardcoreDeath() (see the combat section above) has had its `characters` row
  // DELETED -- but an autosave already in flight when the fatal hit landed (or a client that
  // fires one more autosave before it notices the character is gone) would otherwise just
  // re-INSERT it right back via the ON CONFLICT upsert below, resurrecting a character that
  // was already announced dead in global chat and struck off as dead on the leaderboard. The
  // characters row itself is gone by now, so it can't be checked directly -- instead this
  // checks leaderboard_bests, which combatHandleHardcoreDeath() deliberately does NOT delete
  // (it only sets is_dead=1) for exactly this reason: it's the one durable record that
  // survives the characters-row deletion and can positively identify "this exact named
  // character on this account was hardcore-killed, not just any is_dead row" -- is_dead=1
  // alone isn't enough since a plain leaderboard_bests row could theoretically be marked dead
  // by other means in the future, so both columns are checked together. A voluntary manual
  // delete of a live (non-hardcore-death) character does NOT set is_dead (see the `else`
  // branch of DELETE /api/characters/:slot below, which deletes the leaderboard_bests row
  // entirely instead) -- so freely re-creating a character with a freed-up name after an
  // ordinary deletion is untouched by this check, exactly like before. Refuses outright
  // (410 Gone) rather than silently no-oping, so a bypassed/buggy client gets a clear signal
  // instead of a save that mysteriously never sticks.
  const incomingNameForDeathCheck = (data.character_name || "").toString().trim();
  if (incomingNameForDeathCheck) {
    const deathRecord = db
      .prepare("SELECT 1 FROM leaderboard_bests WHERE account_id = ? AND character_name = ? AND is_dead = 1 AND hardcore = 1")
      .get(req.account.id, incomingNameForDeathCheck);
    if (deathRecord) {
      return res.status(410).json({ error: "This hardcore character has already been slain -- its slot is permanently locked." });
    }
  }

  // v0.22.3 (#1): character names must be globally unique (case-insensitive) across every
  // LIVE character on every account -- names show up on the leaderboard, graveyard, global
  // chat and Auction House, so duplicates are confusing. A client-side check alone is
  // bypassable, so this is the real enforcement point. Ordinary autosaves (the name hasn't
  // changed since what's already stored for this slot) always pass -- this only bites when
  // the incoming name is actually NEW for this (account, slot), i.e. character creation or
  // an explicit rename. Existing duplicate names already sitting in the DB are grandfathered
  // (nothing retroactively breaks); a name freed up by a DELETEd character is fair game again
  // since this only ever looks at the live `characters` table, never `graveyard`.
  const incomingName = (data.character_name || "").toString().trim();
  if (incomingName) {
    const existingRow = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
    const existingName = existingRow ? (JSON.parse(existingRow.data).character_name || "").toString().trim() : null;
    if (existingName === null || existingName.toLowerCase() !== incomingName.toLowerCase()) {
      const conflict = db
        .prepare(
          `SELECT 1 FROM characters WHERE LOWER(json_extract(data,'$.character_name')) = LOWER(?) AND NOT (account_id = ? AND slot = ?)`
        )
        .get(incomingName, req.account.id, slot);
      if (conflict) {
        return res.status(409).json({ error: "That name is already taken -- choose another." });
      }
    }
  }

  // v0.24.2 BUG FIX (credit: Gwen -- "The Unbroken Chain stays 0/8", "Thin the Wanderers stays
  // 0/10", "Ascendant Hunt step 1 stays 0/20"). Every one of those had the same cause, and it
  // was here. Quest progress is mutated EXCLUSIVELY server-side: combatFinalizeMonsterKill()
  // runs the kill trackers, the four report-* routes handle the client-attested quests, and the
  // claim route grants rewards. But this route stored whatever `quests` blob the autosave sent
  // along, and the client's own PS.quests was never refreshed from the kill response -- so the
  // sequence was: kill a monster, server records 1/8, autosave fires a second later carrying
  // the client's stale 0/8, and the server's own progress is erased. The board could never
  // climb above zero no matter how many monsters died.
  //
  // The fix is to make this route stop accepting quest state at all. Whatever is already stored
  // wins, unconditionally, for the five quest-owned fields. A brand new character (no stored row
  // yet) is the one case where the incoming payload is genuinely the only source, so it is used
  // as-is there; from the second save onward the server's copy is authoritative. The client
  // still sends these fields (harmless, and it keeps older clients working unchanged) -- they
  // are simply overwritten here before anything is persisted.
  const storedRowForQuests = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (storedRowForQuests) {
    let stored = null;
    try { stored = JSON.parse(storedRowForQuests.data); } catch (e) { stored = null; }
    if (stored) {
      for (const field of ["quests", "quest_attr_bonus", "quest_bonus_hp", "unspent_quest_stat_points"]) {
        if (stored[field] !== undefined) data[field] = stored[field];
        else delete data[field];
      }
    }
  }

  const json = JSON.stringify(data);

  // v0.15.1 BUG FIX (progression/loot/trial setbacks on browser close, etc.): autosave
  // fires very frequently (every meaningful action, plus once a second while regen is
  // active) and each call used to be an independent, un-awaited PUT with zero ordering
  // guarantee -- ordinary network jitter could let an OLDER save's request arrive here
  // AFTER a NEWER one that had already landed, and this route would just blindly overwrite
  // the newer state with the older one. That's a real, silent rollback with no error on
  // either end, and it explains reports of "random" progress/loot/trial setbacks that have
  // nothing to do with the browser Back button specifically (see the bfcache/pageshow fix
  // above this route in a previous patch, which only covers that one distinct cause).
  //
  // Each save now carries a strictly-increasing `_save_seq` (see PS._nextSaveSeq() and
  // Net.saveCharacter()'s client-side request-coalescing, which already makes this race far
  // less likely on its own). This WHERE clause is the server-side backstop: the UPDATE
  // becomes a no-op (0 rows changed, NOT an error) whenever the incoming sequence isn't
  // actually newer than what's already stored, so a late-arriving stale write can never
  // clobber a fresher one. Saves with no _save_seq at all (older cached clients, or this
  // being the very first save for a fresh slot) are always let through.
  const result = db
    .prepare(
      `INSERT INTO characters (account_id, slot, data, updated_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(account_id, slot) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
       WHERE json_extract(excluded.data, '$._save_seq') IS NULL
          OR json_extract(characters.data, '$._save_seq') IS NULL
          OR json_extract(excluded.data, '$._save_seq') > json_extract(characters.data, '$._save_seq')`
    )
    .run(req.account.id, slot, json, nowIso());

  if (result.changes === 0) {
    // A newer (or equal-sequence) save is already stored -- tell the caller so a future
    // client could react to it, without treating this as an error (autosave doesn't need
    // conflict-resolution UX the way the Vault's user-facing deposit/withdraw does). Gold
    // is deliberately NOT synced here either -- a stale save's gold figure is just as
    // untrustworthy as the rest of its (rejected) payload.
    return res.json({ ok: true, ignored: true, account_gold: getAccountGold(req.account.id) });
  }

  // v0.22.1 (Part B2, credit: Gwen's cross-tab dupe report): this USED to mirror whatever
  // absolute `data.gold` figure the save reported straight onto the account's shared
  // column -- the exact bug Gwen reproduced: open the same account in two tabs, spend gold
  // in tab A (correctly deducted via the atomic reroll/reforge/refine/auction routes), then
  // ANY autosave from tab B (which never saw that spend, so its own local total is still
  // the old, higher figure) would silently overwrite the correct post-spend balance right
  // back up -- effectively un-spending it, and letting the same gold be spent again.
  // The fix: this route no longer trusts an ABSOLUTE client-reported total at all. The
  // client instead reports a DELTA (`gold_delta`, see Net._sendCharacterSave() in
  // index.html) -- how much ITS OWN local total has moved since it last heard back from
  // the server -- and that delta is applied ATOMICALLY on top of whatever the account's
  // CURRENT balance actually is (creditAccountGold() reads-then-writes in one synchronous,
  // uninterruptible step). Two tabs each reporting a real, distinct delta now both land
  // correctly instead of one clobbering the other -- there is no longer any absolute figure
  // for a stale tab to overwrite the truth with.
  if (typeof data.gold_delta === "number" && Number.isFinite(data.gold_delta) && data.gold_delta !== 0) {
    creditAccountGold(req.account.id, Math.round(data.gold_delta));
  }

  // Track this character on the leaderboard. `level` and `gold` reflect the character's
  // CURRENT state (a Broken Bridge Trial failure resets level to 1, and that must be
  // visible on the leaderboard, not masked by a frozen historic peak -- see Gwen's v0.13
  // bug report: a demoted-and-reground level 2 character was still showing as level 12 /
  // rank 1). `highest_tier_reached` and `lifetime_xp` are genuinely monotonic lifetime
  // stats (tier never decreases, lifetime_xp only ever accumulates), so those two keep
  // their MAX() semantics. v0.17 (#1): `last_bridge_steps` and `xp` (current in-level XP,
  // distinct from lifetime_xp) are new leaderboard-sort tiebreakers and are CURRENT
  // snapshots like level/gold, not maxed -- last_bridge_steps only changes client-side on a
  // trial failure (see attemptStep() in index.html), and xp resets every level-up, so
  // MAX()-ing either here would just freeze them at a stale high point.
  // v0.17.3 (#12) HOTFIX: Gwen reported a character freshly promoted to Tier 2 still
  // showing their old Tier 1 step count (e.g. "Tier 2, 8 steps") on the leaderboard,
  // wrongly outranking players with genuine Tier 2 progress. The client already resets
  // PS.last_bridge_steps to 0 the moment a promotion resolves (see resolveMasterTrial()
  // in index.html) and saves it right after -- but that promotion-completing save is the
  // SECOND of two autosave() calls fired in quick succession from attemptStep()'s pass
  // branch (the first fires just after the final plank is recorded, still carrying
  // whatever last_bridge_steps was left over from a previous failed run at the OLD tier).
  // If those two requests ever land out of order (slow/retried request, multiple tabs,
  // etc.) the stale one could stomp the correct 0 right back to a stale value, since this
  // upsert used to always trust whatever last_bridge_steps the request carried. Closing
  // the loophole at the source of truth instead of only trusting client ordering: any
  // save that raises this row's highest_tier_reached (i.e. represents a promotion) now
  // forces last_bridge_steps to 0 here regardless of what value rode along with it -- a
  // just-promoted character has, by definition, taken 0 steps on their new tier's bridge
  // yet. (The client-side double-autosave on the crossing step was also collapsed into a
  // single save to remove the race at its root -- see attemptStep() in index.html.)
  // v0.18.3: this upsert used to be inline here -- extracted to upsertLeaderboardBests()
  // (defined below, just above the new Broken Bridge Trial endpoint) so the trial endpoint
  // can share the exact same logic instead of duplicating/risking drift on this SQL. See
  // that function's own comment for the full column-by-column rationale (MAX() vs
  // overwrite semantics for each field) -- unchanged by the extraction, just relocated.
  upsertLeaderboardBests(req.account.id, data);
  res.json({ ok: true, account_gold: getAccountGold(req.account.id) });
});

app.delete("/api/characters/:slot", requireAuth, (req, res) => {
  const slot = Number(req.params.slot);
  const { hardcore_death } = req.body || {};
  if (hardcore_death) {
    // HOTFIX: this client-driven path is now a defensive courtesy layer, not the source of
    // truth for hardcore permadeath -- see combatHandleHardcoreDeath() (in the combat
    // section above), which now performs the SAME graveyard/leaderboard/delete sequence
    // atomically, server-side, the instant a fatal hit is resolved for a hardcore character.
    // Made idempotent here so a client that still fires this (its pre-existing behavior,
    // left in place on purpose -- see PS.handleDeath/Net.deleteCharacter in index.html)
    // after the server already processed the kill doesn't create a SECOND, duplicate
    // graveyard/tombstone entry for the same death. is_dead=1 AND hardcore=1 on this
    // character's leaderboard_bests row is exactly the signal combatHandleHardcoreDeath()
    // leaves behind (see that function's own comment) -- if it's already set, this request
    // is a late/duplicate courtesy call and only needs to make sure the characters row is
    // gone (harmless no-op if it already is), not write a second graveyard row.
    const alreadyHandled = db
      .prepare("SELECT 1 FROM leaderboard_bests WHERE account_id = ? AND character_name = ? AND is_dead = 1 AND hardcore = 1")
      .get(req.account.id, hardcore_death.name || "Hero");
    if (!alreadyHandled) {
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
  } else {
    // v0.17.1 (#16) BUG FIX: a player manually deleting a (non-hardcore-death) character
    // slot used to leave its leaderboard_bests row sitting there forever -- the public
    // leaderboard kept showing a character that, as far as the player could tell, no longer
    // existed, until an admin noticed and removed it by hand via Dev Tools. Look up the
    // character's own name from its stored row BEFORE deleting it (server-side, not trusting
    // anything from the request body) and remove the matching leaderboard row too, so the
    // deletion is reflected immediately. Only reached for an ordinary manual delete -- a
    // hardcore death (handled above) deliberately KEEPS its leaderboard row, just marked dead.
    const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
    if (row) {
      try {
        const data = JSON.parse(row.data);
        if (data.character_name) {
          db.prepare("DELETE FROM leaderboard_bests WHERE account_id = ? AND character_name = ?").run(req.account.id, data.character_name);
        }
      } catch (e) { /* corrupt row -- nothing sensible to look up, just fall through and delete the character row itself */ }
    }
  }
  db.prepare("DELETE FROM characters WHERE account_id = ? AND slot = ?").run(req.account.id, slot);
  res.json({ ok: true });
});

/* ---------------- shared leaderboard upsert helper ----------------
   v0.18.3: extracted out of PUT /api/characters/:slot (where this logic used to live
   inline) so the new server-authoritative Broken Bridge Trial endpoint below can call
   the exact same upsert after IT resolves a promotion/failure, instead of duplicating
   this SQL block and risking the two copies drifting apart in a future patch. Nothing
   about the actual column semantics changed -- see each line's own comment for why
   level/gold/xp/last_bridge_steps/weapon_skills/herbalism_points are current-snapshot
   overwrites while highest_tier_reached/lifetime_xp/max_kill_streak are MAX()'d lifetime
   records, and why a promotion (highest_tier_reached rising) forces last_bridge_steps
   back to 0 regardless of what value rode along with this particular save. */
function upsertLeaderboardBests(accountId, data) {
  if (!data.character_name || !data.class_display_name) return;
  // v0.17.3 (#13): weapon_skills/herbalism_points ride along on the same upsert as
  // everything else here, stored as excluded.value (current snapshot, not MAX()'d) --
  // see the ALTER TABLE comment (search "weapon_skills TEXT") for why that's correct.
  const weaponSkillsJson = JSON.stringify(data.weapon_skills || {});
  // v0.23.0 (Part B9): rides along the same upsert, current-snapshot (not MAX()'d), exactly
  // like weapon_skills/herbalism_points above.
  const spellcastingHits = (data.spellcasting_skill && data.spellcasting_skill.hits) || 0;
  // v0.18.2 (#8): max_kill_streak is a genuine lifetime record (only ever grows for a
  // given character, exactly like highest_tier_reached/lifetime_xp above) -- MAX()'d
  // against the existing row rather than overwritten with excluded.value, so an
  // out-of-order/retried save can never stomp a higher previously-recorded streak back
  // down to a smaller one.
  db.prepare(
    `INSERT INTO leaderboard_bests (account_id, character_name, class_name, level, highest_tier_reached, gold, updated_at, hardcore, lifetime_xp, is_dead, last_bridge_steps, xp, weapon_skills, herbalism_points, max_kill_streak, total_kills, spellcasting_hits)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(account_id, character_name) DO UPDATE SET
       class_name=excluded.class_name,
       level=excluded.level,
       highest_tier_reached=MAX(leaderboard_bests.highest_tier_reached, excluded.highest_tier_reached),
       gold=excluded.gold,
       updated_at=excluded.updated_at,
       hardcore=excluded.hardcore,
       lifetime_xp=MAX(leaderboard_bests.lifetime_xp, excluded.lifetime_xp),
       is_dead=0,
       last_bridge_steps=CASE WHEN excluded.highest_tier_reached > leaderboard_bests.highest_tier_reached THEN 0 ELSE excluded.last_bridge_steps END,
       xp=excluded.xp,
       weapon_skills=excluded.weapon_skills,
       herbalism_points=excluded.herbalism_points,
       max_kill_streak=MAX(leaderboard_bests.max_kill_streak, excluded.max_kill_streak),
       total_kills=MAX(leaderboard_bests.total_kills, excluded.total_kills),
       spellcasting_hits=excluded.spellcasting_hits`
  ).run(
    accountId,
    data.character_name,
    data.class_display_name,
    data.level || 1,
    data.highest_tier_reached || 1,
    data.gold || 0,
    nowIso(),
    data.hardcore ? 1 : 0,
    data.lifetime_xp || 0,
    data.last_bridge_steps || 0,
    data.xp || 0,
    weaponSkillsJson,
    data.herbalism_points || 0,
    data.max_kill_streak || 0,
    data.total_kills || 0,
    spellcastingHits
  );
}

/* ---------------- Broken Bridge Trial (server-authoritative) ----------------
   v0.18.3: this is the first concrete piece of the expanded security-hardening scope
   Gwen asked for after reading server_authoritative_combat_design.md -- explicitly
   called out as the HIGHEST priority of that whole request ("this is a crucial
   progression step that I do not want to see get hacked"), ahead of even combat. Every
   roll that decides whether a plank holds, and every consequence of a pass, a fail, or
   a full crossing (level/xp/attribute reset, tier promotion, HP/Stamina, fail-streak,
   max_maze_depth_reached), now happens HERE, server-side, against the server's own
   stored copy of the character row. A modified client can still ask the server for a
   ruling, but it can no longer fabricate the ruling itself, invent a "safe" plank, or
   skip straight to a promotion.

   Deliberately a small, self-contained mirror of the handful of CLASSES/Balance facts
   this ONE mechanic actually needs (tier, next_class, display_name, base attributes,
   base_hp/hp_per_level, and the bridge/level/xp formulas) -- see index.html's own
   CLASSES table and Balance object for the authoritative client-side copies these are
   kept in sync with by hand. A server test (v0183_trial_server_test.js) pins the exact
   numbers below so any future drift between the two copies is caught immediately
   instead of silently shipping a mismatched trial.

   v0.18.3 SCOPE NOTE (HP/Stamina after a resolution): computed here with the SIMPLE
   base formula (class.base_hp, TRIAL_STAMINA_MAX_BASE) rather than porting the full
   gearBonus()/canEquipGear()/affix-total dependency chain -- omitting gear bonuses can
   only ever UNDERSHOOT the true max, never oversell an advantage, and the client's very
   next autosave (still flowing through the existing, untightened PUT
   /api/characters/:slot) corrects the displayed/stored HP/Stamina to the fully accurate
   figure moments later regardless. A deliberate scope-narrowing decision to keep this
   port focused on the RNG-sensitive, progression-critical logic. */

// Tier / next-class / base-attribute subset of index.html's CLASSES table -- only what
// a trial resolution needs (nothing about role/description/damage/crit/armor/regen).
// v0.22.3 (#10): each entry now also carries `chain` (mirroring index.html's CLASSES table)
// so a ladder reset can look up "what is this character's chain's tier-1 class" without a
// separate table -- see CHAIN_TIER1_CLASS / applyLadderReset() below.
const TRIAL_CLASSES = {
  wizard:      { tier: 1, chain: "wizard",     next_class: "sorcerer",    display_name: "Wizard",      base_hp: 60,  hp_per_level: 8,  base_str: 2,  base_dex: 6,  base_vit: 16, base_int: 16 },
  thornguard:  { tier: 1, chain: "thornguard", next_class: "stonewarden", display_name: "Thornguard",  base_hp: 100, hp_per_level: 14, base_str: 16, base_dex: 6,  base_vit: 16, base_int: 2 },
  windrider:   { tier: 1, chain: "windrider",  next_class: "galestrider", display_name: "Windrider",   base_hp: 75,  hp_per_level: 10, base_str: 4,  base_dex: 16, base_vit: 16, base_int: 4 },
  sorcerer:    { tier: 2, chain: "wizard",     next_class: "warlock",     display_name: "Sorcerer",    base_hp: 80,  hp_per_level: 10, base_str: 3,  base_dex: 8,  base_vit: 20, base_int: 21 },
  stonewarden: { tier: 2, chain: "thornguard", next_class: "treesinger",  display_name: "Stonewarden", base_hp: 140, hp_per_level: 18, base_str: 20, base_dex: 8,  base_vit: 21, base_int: 3 },
  galestrider: { tier: 2, chain: "windrider",  next_class: "shadowbloom", display_name: "Galestrider", base_hp: 100, hp_per_level: 13, base_str: 5,  base_dex: 21, base_vit: 21, base_int: 5 },
  warlock:     { tier: 3, chain: "wizard",     next_class: "necromancer", display_name: "Warlock",     base_hp: 105, hp_per_level: 13, base_str: 3,  base_dex: 10, base_vit: 27, base_int: 26 },
  treesinger:  { tier: 3, chain: "thornguard", next_class: "rootbinder",  display_name: "Treesinger",  base_hp: 160, hp_per_level: 20, base_str: 27, base_dex: 10, base_vit: 26, base_int: 3 },
  shadowbloom: { tier: 3, chain: "windrider",  next_class: "druid",       display_name: "Shadowbloom", base_hp: 125, hp_per_level: 16, base_str: 7,  base_dex: 26, base_vit: 26, base_int: 7 },
  necromancer: { tier: 4, chain: "wizard",     next_class: "archmage",    display_name: "Summoner",    base_hp: 135, hp_per_level: 17, base_str: 4,  base_dex: 12, base_vit: 33, base_int: 33 },
  rootbinder:  { tier: 4, chain: "thornguard", next_class: "emberpriest", display_name: "Rootbinder",  base_hp: 210, hp_per_level: 26, base_str: 33, base_dex: 12, base_vit: 33, base_int: 4 },
  druid:       { tier: 4, chain: "windrider",  next_class: "galeshaper",  display_name: "Druid",       base_hp: 160, hp_per_level: 20, base_str: 8,  base_dex: 33, base_vit: 33, base_int: 8 },
  emberpriest: { tier: 5, chain: "thornguard", next_class: null,          display_name: "Emberpriest", base_hp: 190, hp_per_level: 24, base_str: 40, base_dex: 15, base_vit: 40, base_int: 5 },
  archmage:    { tier: 5, chain: "wizard",     next_class: null,          display_name: "Archmage",    base_hp: 165, hp_per_level: 21, base_str: 5,  base_dex: 15, base_vit: 40, base_int: 40 },
  galeshaper:  { tier: 5, chain: "windrider",  next_class: null,          display_name: "Galeshaper",  base_hp: 195, hp_per_level: 25, base_str: 10, base_dex: 40, base_vit: 40, base_int: 10 },
};
// v0.22.3 (#10): the tier-1 class id for each chain -- a ladder reset drops a character back
// to this class regardless of which tier it was at.
const CHAIN_TIER1_CLASS = { wizard: "wizard", thornguard: "thornguard", windrider: "windrider" };

// Balance-equivalent constants/formulas, ported verbatim from index.html's Balance
// object -- see that file's own comments (search "BRIDGE_ROWS", "STARTING_STAT_POINTS")
// for the full rationale behind these exact numbers.
const TRIAL_BRIDGE_ROWS = 10, TRIAL_BRIDGE_MIN_PLANKS = 2, TRIAL_BRIDGE_MAX_PLANKS = 5;
const TRIAL_LEVEL_REQUIREMENT_PER_TIER = 10;
const TRIAL_STARTING_STAT_POINTS = 3;
// v0.23.0 (A2): kept in sync by hand with CB.STAMINA_MAX_BASE's own 100 -> 175 change below.
const TRIAL_STAMINA_MAX_BASE = 175.0;
const TRIAL_KILLS_PER_LEVEL_TARGET = 20, TRIAL_MONSTER_BASE_XP = 8.0, TRIAL_LEVEL_XP_GROWTH = 0.11;

function trialClampi(n, lo, hi) { return Math.max(lo, Math.min(hi, Math.round(n))); }
function trialBridgePlankCount(tier) { return trialClampi((tier || 1) + 1, TRIAL_BRIDGE_MIN_PLANKS, TRIAL_BRIDGE_MAX_PLANKS); }
function trialLevelRequirement(tier) { return trialClampi(tier || 1, 1, 4) * TRIAL_LEVEL_REQUIREMENT_PER_TIER; }
// v0.20 (#9.4): must stay in sync with Balance.xpRequiredForLevel() in index.html and
// combatXpRequiredForLevel() above.
function trialXpRequiredForLevel(level) {
  return Math.round((TRIAL_KILLS_PER_LEVEL_TARGET + (level - 1)) * TRIAL_MONSTER_BASE_XP * Math.pow(1.0 + TRIAL_LEVEL_XP_GROWTH, level));
}

// Mirrors PS._resetAttributesToClassBase() + the surrounding reset lines in
// PS.resolveMasterTrial() (index.html): level/xp back to the very start, attributes
// reset to the (possibly newly-promoted) class's own base stats plus `startingPoints`
// unspent points, HP/Stamina maxed out, max_maze_depth_reached back to 1. Mutates
// `data` (the character's parsed JSON blob) in place.
// v-quest: optional 4th param `newTownTier` -- when provided, ALSO rebuilds the Quest
// System's board for that town (see questRebuildBoardForTown() below). This function is
// called on every trial resolution, including a plain FAILURE (same class/tier, just more
// starting stat points) -- a failure must NOT touch the quest board, only a genuine tier
// change (a promotion, or a ladder reset back to tier 1) should, so only the two call
// sites that actually change tier pass this, instead of the quest board getting its own
// parallel reset trigger. See app.post("/api/trial/attempt", ...)'s promotion branch and
// applyLadderReset() below for the two call sites that pass this.
function applyTrialResolutionReset(data, classId, startingPoints, newTownTier) {
  const c = TRIAL_CLASSES[classId];
  data.level = 1;
  data.xp = 0;
  data.xp_to_next = trialXpRequiredForLevel(1);
  data.attributes = { str: c.base_str || 5, dex: c.base_dex || 5, vit: c.base_vit || 5, int: c.base_int || 5 };
  data.unspent_stat_points = startingPoints;
  data.bonus_hp_from_attributes = 0;
  data.bonus_stamina_from_attributes = 0;
  // v0.24.2 (credit: Gwen): the quest board is now rebuilt on EVERY trial resolution, not just
  // on a genuine tier change. A failed attempt drops the character back to level 1 with fresh
  // attributes exactly like a promotion does, so leaving a half-finished board (and its earned
  // quest_attr_bonus/quest_bonus_hp) standing meant a failure kept quest rewards a promotion
  // would have cleared. When no new tier is given (a plain failure) the board is rebuilt for
  // the tier the character is already in.
  questRebuildBoardForTown(data, newTownTier != null ? newTownTier : (c && c.tier) || 1);
  // getMaxHp()'s (level-1)*hp_per_level term is 0 at level 1, and gear/attribute bonuses
  // are both freshly zeroed above -- so base_hp alone is the exact, correct level-1 max
  // (see this section's top-of-file SCOPE NOTE for why gear is deliberately excluded).
  data.current_hp = c.base_hp || 50;
  data.current_stamina = TRIAL_STAMINA_MAX_BASE;
  data.max_maze_depth_reached = 1;
}

// v0.22.3 (#10 section B): a manual ladder reset touches ONLY class rank + level + current
// XP -- gear, gold, vault, auction listings, materials, consumables, herbs, weapon_skills,
// and herbalism_points are all deliberately left untouched (see the spec's "Keep untouched"
// list). Drops the character back to the tier-1 class of its OWN chain (wizard/thornguard/
// windrider stays whichever chain it already was), via the same applyTrialResolutionReset()
// a bridge-trial promotion/failure already uses, then clears the season-specific fields a
// trial resolution doesn't touch (highest_tier_reached, season_won, trial_progress, the
// bridge-step/fail-streak counters) and forest_reputation (back to tier 1 = fresh rep).
function applyLadderReset(data) {
  const currentClass = TRIAL_CLASSES[data.class_id];
  const chain = (currentClass && currentClass.chain) || "wizard";
  const tier1ClassId = CHAIN_TIER1_CLASS[chain] || "wizard";
  data.class_id = tier1ClassId;
  applyTrialResolutionReset(data, tier1ClassId, TRIAL_STARTING_STAT_POINTS, 1);
  data.class_display_name = TRIAL_CLASSES[tier1ClassId].display_name;
  data.highest_tier_reached = 1;
  data.season_won = false;
  data.bridge_fail_streak = 0;
  data.last_bridge_steps = 0;
  data.trial_progress = {};
  data.forest_reputation = {};
}

/* ---------------- Quest System (new) ----------------
   A per-town (per class tier) board of 8 quests, keyed off the character's CURRENT tier
   (data.quests.town, 1-5, mirrors TRIAL_CLASSES[data.class_id].tier). Completing a quest's
   objective flips it "ready"; the player must POST /api/quests/:questId/claim to actually
   collect the reward (never auto-applied). Crossing the Broken Bridge (a genuine tier
   promotion) or a ladder reset rebuilds the WHOLE board for the new town -- see
   questRebuildBoardForTown() and its two call sites inside applyTrialResolutionReset() above
   (the 4th `newTownTier` param) and applyLadderReset() just above this comment.

   index.html's Balance.QUEST_DEFS is a display-only copy of QUEST_DEFS below and must stay
   byte-identical (mirrors how Balance.SPELLS/SPELLS are kept in sync, see that section's own
   comment further down this file).

   DATA-MODEL NOTE on stat points (see the feature spec's own "trickiest data-integrity part"
   callout): this project already has a general-purpose unspent-stat-points pool
   (data.unspent_stat_points, from the original v0.11 Attributes system) that the CLIENT spends
   directly into data.attributes with no dedicated server endpoint of its own (spend-time
   validation for that pool doesn't exist yet -- see the top-of-file Phase 1 scope note). Quest
   stat-point rewards do NOT merge into that pool. Reusing it would mean quest-granted points
   become indistinguishable from level-derived ones the instant they're spent, so a later
   promotion could never subtract back out exactly what quests granted without risking either
   double-clearing manually-earned points or leaving quest points behind. Instead, quest stat
   points get their OWN pool (data.unspent_quest_stat_points) and their OWN spend endpoint
   (POST /api/quests/spend-stat-point) that adds straight into data.quest_attr_bonus -- a
   SEPARATE additive layer, never folded into data.attributes itself. combatGetTotalAttr()/
   combatGetMaxHp() below add quest_attr_bonus/quest_bonus_hp on top of the normal totals, so
   the points still do something in combat, but questRebuildBoardForTown() can wipe
   quest_attr_bonus/quest_bonus_hp/unspent_quest_stat_points to zero on a town change with zero
   ambiguity about which points were whose -- no shared-pool bookkeeping needed at all. */

// v0.24.2 (credit: Gwen): quest requirements and rewards no longer scale with the town/class
// tier at all. Two players on the same quest were seeing different numbers (one asked for 4
// stronghold guardians, another for 8) purely because they stood in different towns, which
// reads as a bug rather than as progression. A quest's demands now depend only on its own
// position in its own questline. The old per-town arrays (QUEST_TOWN_REWARD_MULT and every
// [town1..town5] TARGETS/REWARDS array) are gone; each value below is a single fixed number,
// taken from what town 1 used to ask for. Escalation now lives exclusively in the multi-step
// questlines (WARDEN_TRIAL_STEPS and ASCENDANT_HUNT_STEPS), where the step number is shown to
// the player so a rising target is legible as progress instead of as drift.

// Single source of truth for the whole quest board -- every requirement and reward is tunable
// purely by editing the values below, nothing scattered in per-town `if` branches anywhere
// else in this file.
const QUEST_DEFS = {
  // v0.25: two new questlines, both 5-step like "wardens"/"elder". "trader" gets trade moving
  // by rewarding Auction House sales; "cleanse" is the season-long lifetime-kill chase.
  IDS: ["explore", "wardens", "gatherer", "wanderers", "streak", "cauldron", "elder", "palisade", "pacifist", "trader", "cleanse"],
  NAMES: {
    explore: "Every Root and Hollow", wardens: "The Warden Trials", gatherer: "The Gatherer's Dozen",
    wanderers: "Thin the Wanderers", streak: "The Unbroken Chain", cauldron: "The Simmering Cauldron",
    // v0.24.1 (B4): renamed from "Elder of the Thicket" -- now a 5-step questline through every
    // band, not a single Elder-band kill. See ASCENDANT_HUNT_STEPS below.
    elder: "The Ascendant Hunt", palisade: "Behind the Palisade",
    // v0.24.1 (B5): new. Deliberately named/worded without ever mentioning Thorns/reflect
    // mechanics -- see questObjectiveText's "pacifist" branch in index.html for the exact
    // player-facing copy, per Gwen's explicit instruction not to spell out the mechanism.
    pacifist: "The Pacifist",
    trader: "The Long Road to Market", cleanse: "Cleanse the Forest",
  },
  // Level requirement (within the CURRENT town's own level arc) for a quest to flip from
  // "locked" to "active". Kept IDENTICAL across all 5 towns rather than scaled per town --
  // a Broken Bridge Trial resolution always resets data.level back to 1 (see
  // applyTrialResolutionReset() above), so "must be level 8" is already a meaningfully
  // mid-late gate in every town's own arc (town 1 runs levels 1-10 before trial eligibility,
  // town 2 runs 1-20, town 3+ runs 1-30/40 -- see trialLevelRequirement()), not just town 1's.
  // v0.29 (credit: Gwen): every quest is available from level 1 for now. The gating machinery is
  // left intact -- questInitialStatus() treats a requirement of 1 as "no gate" -- so restoring a
  // gate later is a matter of changing a number back. Must stay in sync with index.html.
  LEVEL_REQ: { explore: 1, wardens: 1, gatherer: 1, wanderers: 1, streak: 1, cauldron: 1, elder: 1, palisade: 1, pacifist: 1, trader: 1, cleanse: 1 },
  // Fixed, town-independent. See the de-scaling note above this object.
  TARGETS: {
    gatherer_herbs: 12,
    wanderers_roamers: 10,
    streak_kills: 8,
    // v-quest: "in the town" = a running total across the WHOLE town's play, never reset
    // between delves (unlike wardens/streak/explore, which require completion within one
    // continuous delve) -- see questTrackStrongholdChestReport() below.
    palisade_chests: 1,
  },
  // v0.24.2 / v0.25.2 (credit: Gwen): "The Warden Trials" (renamed from "The Four Wardens",
  // which only ever described its first step) is a 5-step questline, mirroring
  // ASCENDANT_HUNT_STEPS' shape exactly -- each step is claimed at the board before
  // the next unlocks, and the card shows "Step N/5" so a rising target reads as progression.
  // Two changes to how a guardian kill counts, both per Gwen:
  //   1. The old "within the first N area levels" band gate is gone entirely. A stronghold
  //      guardian counts at ANY area level.
  //   2. Every step must still be completed within ONE delve -- a single continuous run out of
  //      town, across as many area levels as the player can chain, ending only when they
  //      return to town or die (this is exactly the kill-streak run: see
  //      questTrackGuardianKill()'s comment for how the delve boundary is detected). A
  //      stronghold holds 4 guardians, so step 5's 20 guardians means roughly five strongholds
  //      without going home. That difficulty is intentional; the rewards are sized for it.
  // Total across all 5 steps: +20 stat points, 24,700 XP, 41,000 gold.
  WARDEN_TRIAL_STEPS: [
    { count: 4, stat_points: 2, xp: 300, gold: 500 },
    { count: 8, stat_points: 3, xp: 900, gold: 1500 },
    { count: 12, stat_points: 4, xp: 2500, gold: 4000 },
    { count: 16, stat_points: 5, xp: 6000, gold: 10000 },
    { count: 20, stat_points: 6, xp: 15000, gold: 25000 },
  ],
  // v0.24.1 (B4): "elder" is now a 5-step questline through every band in order, replacing the
  // old single "kill N of band X" target -- each step must be individually claimed at the board
  // before the next one unlocks (see the claim endpoint's "elder" branch, which advances
  // entry.step/progress/target in place instead of setting a terminal "claimed" status until
  // step 5). Deliberately NOT scaled by town/QUEST_TOWN_REWARD_MULT like every other quest here
  // -- per Gwen's exact spec this is a fixed reward table tied to band difficulty, which already
  // is its own escalation axis. Band keys are the internal COMBAT_MONSTERS tier keys (Common/
  // Uncommon/Rare/Epic/Legendary), displayed via index.html's MONSTER_BAND_INFO[...].label as
  // Newborn/Young/Mature/Elder/Ancient. Total across all 5 steps: +25 stat points, +19,700 XP.
  ASCENDANT_HUNT_STEPS: [
    { band: "Common", count: 20, stat_points: 5, xp: 200 },
    { band: "Uncommon", count: 10, stat_points: 5, xp: 500 },
    { band: "Rare", count: 5, stat_points: 5, xp: 1000 },
    { band: "Epic", count: 3, stat_points: 5, xp: 3000 },
    { band: "Legendary", count: 1, stat_points: 5, xp: 15000 },
  ],
  // v0.24.1 (B6): "The Simmering Cauldron" now asks for 7 DISTINCT potion recipes brewed while
  // the quest is active, not 7 (or N) brews of anything -- brewing the same potion twice no
  // longer makes any progress past the first time. Fixed across every town (variety, not
  // volume, doesn't get harder just because you're in a later town), replacing the old
  // per-town TARGETS.cauldron_brews count entirely. See questTrackBrewReport() below for how
  // the entry's new `distinct_brewed` array (instead of a plain counter) is tracked.
  // v0.25 (credit: Gwen): "The Long Road to Market" -- 5 steps counting completed Auction
  // House SALES, to get trade moving. Deliberately character-bound, not account-bound: the
  // counter lives in the seller character's own save (data.auction_sales, incremented by
  // POST /api/auction/:id/buy when a listing that character made actually sells), so each
  // character earns its own way through this line even though the gold from a sale is
  // account-shared. Step 1 is a single sale on purpose -- the whole point is getting a player
  // to list something for the first time.
  TRADER_STEPS: [
    { count: 1, stat_points: 1, xp: 50 },
    { count: 5, stat_points: 1, xp: 50 },
    { count: 20, stat_points: 1, xp: 200 },
    { count: 50, stat_points: 1, xp: 500 },
    { count: 100, stat_points: 1, xp: 1000 },
  ],
  // v0.25 (credit: Gwen): "Cleanse the Forest" -- 5 steps against data.total_kills, the LIFETIME
  // monster counter that already survives Broken Bridge promotions and ladder resets. Step 5's
  // 100,000 kills is an intentionally season-long commitment, not something to be finished in a
  // week. Because this reads a lifetime total rather than a per-town counter, its progress is
  // deliberately NOT wiped by questRebuildBoardForTown() the way every other quest's is -- see
  // questTrackTotalKills(), which re-derives progress from total_kills on every kill, so a board
  // rebuild simply recomputes the same true number rather than losing it.
  CLEANSE_STEPS: [
    { count: 100, stat_points: 5, gold: 500 },
    { count: 500, stat_points: 5, gold: 2500 },
    { count: 3000, stat_points: 10, gold: 15000 },
    { count: 10000, stat_points: 25, gold: 50000 },
    { count: 100000, stat_points: 50, gold: 300000 },
  ],
  CAULDRON_DISTINCT_TARGET: 7,
  // v0.24.2 / v0.25.2 (credit: Gwen): this used to be a per-town array
  // ([false, true, true, true, true]) requiring at least one attribute potion among the distinct
  // recipes from town 2 onward. De-scaling briefly took town 1's value (false, i.e. off
  // everywhere); Gwen has since confirmed the intent was the opposite, so it is now ON in every
  // town. That is a genuine de-scaling either way -- the requirement no longer depends on which
  // town you are standing in -- it just resolves to "always" rather than "never". At least one
  // of the distinct recipes brewed toward the target must be a Potion of Might, Swiftness or
  // Intellect (see ATTR_POTION_IDS and questTrackBrewReport()'s attr_potion_brewed flag).
  CAULDRON_ATTR_POTION_REQUIRED: true,
  ATTR_POTION_IDS: ["potion_of_might", "potion_of_swiftness", "potion_of_intellect"],
  // v-quest: every valid brew RESULT item id (mirrors index.html's ALCHEMY_RECIPES result_item
  // list verbatim) -- COMBAT_CONSUMABLES above is deliberately narrower (only what combat's
  // use-item endpoint needs), so report-brew below validates against this list instead.
  BREWABLE_ITEM_IDS: [
    "minor_health_potion", "health_potion", "medium_health_potion", "greater_health_potion", "supreme_health_potion",
    "minor_stamina_potion", "stamina_potion", "medium_stamina_potion", "greater_stamina_potion", "supreme_stamina_potion",
    "minor_mana_potion", "mana_potion", "medium_mana_potion", "greater_mana_potion", "supreme_mana_potion",
    "elixir_of_eyesight", "potion_of_might", "potion_of_swiftness", "potion_of_intellect", "potion_of_wardveil",
    "potion_of_renewal", "antidote", "elixir_of_clarity",
  ],
  // Fixed, town-independent. See the de-scaling note above this object. "wardens" has no
  // entry here at all any more -- its rewards live per-step in WARDEN_TRIAL_STEPS, the same
  // way "elder" reads from ASCENDANT_HUNT_STEPS.
  REWARDS: {
    explore: { hp: 50, gold: 500 },
    gatherer: { elixirs: ["elixir_of_clarity"] },
    wanderers: { hp: 80, gold: 1800, xp: 400 },
    streak: { gold: 600, xp: 250 },
    cauldron: { elixirs: ["elixir_of_eyesight"], stat_points: 1 },
    palisade: { gold: 900 },
  },
  // v0.24.1 (B5 "The Pacifist"): a single fixed reward, deliberately NOT town-scaled (like
  // ASCENDANT_HUNT_STEPS above) -- this is a one-time "did you ever pull off a pure-Thorns
  // kill" achievement, not a repeatable per-town grind, so one flat payout regardless of which
  // town's board it's claimed from. Potion quantities are {item_id: qty} so the claim endpoint
  // can grant multiple stacks of each in one combatAddConsumable() loop.
  PACIFIST_REWARD: { xp: 5000, stat_points: 20, potions: { potion_of_renewal: 3, potion_of_wardveil: 3 } },
  // Mirrors index.html's Balance.SHRINE_XP_BUFF_MULT/SHRINE_XP_BUFF_ENCOUNTERS +
  // PS.applyXpShrineBuff()'s exact effect (multiplier assigned outright, encounters ADDED on
  // top of whatever's already active) -- the gatherer reward calls this directly instead of a
  // fresh reimplementation, per the feature spec.
  SHRINE_XP_BUFF_MULT: 1.5,
  SHRINE_XP_BUFF_ENCOUNTERS: 20,
};

function questTownIndex(town) { return cbClampi(town, 1, 5) - 1; }

// Fresh `entries` for a given town -- level-gated quests start "locked", the rest start
// "active" immediately. Mirrors the exact target numbers in QUEST_DEFS.TARGETS above.
function questBuildEntriesForTown(town) {
  const entries = {};
  for (const qid of QUEST_DEFS.IDS) entries[qid] = questBuildEntry(qid);
  return entries;
}

// v0.24.2: extracted out of questBuildEntriesForTown() so questEnsureState() below can
// BACKFILL a single missing entry without rebuilding (and thereby wiping) the whole board.
// That gap is what made "The Pacifist" show as locked with the nonsensical "Unlocks at Level
// 1" caption for every character created before it existed: questEnsureState() migrated the
// reshaped "elder" and "cauldron" entries but had no path for a quest id that was simply
// absent, so the board fell through to a hardcoded {status:"locked"} placeholder.
// No town parameter any more -- no target here depends on the town (see the de-scaling note
// above QUEST_DEFS).
// v0.29: a freshly built Cleanse entry anchors its baseline to the character's current lifetime
// kills, which is what makes a Broken Bridge Trial reset the quest without touching total_kills.
// questBuildEntry() is called both when a board is first created and whenever
// questRebuildBoardForTown() runs, which is exactly the two moments the count should restart.
function questBuildEntry(qid) {
  let target = 1; // explore is a single boolean "fully explored this delve" report.
  if (qid === "gatherer") target = QUEST_DEFS.TARGETS.gatherer_herbs;
  else if (qid === "wanderers") target = QUEST_DEFS.TARGETS.wanderers_roamers;
  else if (qid === "streak") target = QUEST_DEFS.TARGETS.streak_kills;
  else if (qid === "cauldron") target = QUEST_DEFS.CAULDRON_DISTINCT_TARGET;
  else if (qid === "palisade") target = QUEST_DEFS.TARGETS.palisade_chests;
  let entry;
  if (qid === "elder") {
    entry = { step: 1, progress: 0, target: QUEST_DEFS.ASCENDANT_HUNT_STEPS[0].count, status: questInitialStatus(qid) };
  } else if (qid === "wardens") {
    // v0.24.2: "wardens" is step-based now too, same shape as "elder" -- see WARDEN_TRIAL_STEPS.
    entry = { step: 1, progress: 0, target: QUEST_DEFS.WARDEN_TRIAL_STEPS[0].count, status: questInitialStatus(qid) };
  } else if (qid === "trader") {
    entry = { step: 1, progress: 0, target: QUEST_DEFS.TRADER_STEPS[0].count, status: questInitialStatus(qid) };
  } else if (qid === "cleanse") {
    entry = { step: 1, progress: 0, target: QUEST_DEFS.CLEANSE_STEPS[0].count, status: questInitialStatus(qid), _kill_base: null };
  } else {
    entry = { progress: 0, target, status: questInitialStatus(qid) };
  }
  if (qid === "cauldron") { entry.attr_potion_brewed = false; entry.distinct_brewed = []; }
  if (qid === "wardens") entry._delve_streak_marker = 0; // internal bookkeeping, see questTrackGuardianKill()
  return entry;
}

// v0.24.2: a LEVEL_REQ of 1 is not a gate at all (every character is level 1 or above from the
// moment it exists), so such a quest must always start active. Centralised here rather than
// repeated inline so the board and the unlock pass can never disagree about it.
function questInitialStatus(qid) { return (QUEST_DEFS.LEVEL_REQ[qid] || 1) <= 1 ? "active" : "locked"; }

// Rebuilds the ENTIRE quests object for `town`, and clears every town-scoped quest reward
// pool alongside it (quest_attr_bonus/quest_bonus_hp/unspent_quest_stat_points) -- called
// ONLY from applyTrialResolutionReset() when it's actually given a newTownTier (a genuine
// promotion or ladder reset), never on a plain trial failure. Mutates `data` in place.
function questRebuildBoardForTown(data, town) {
  const t = cbClampi(town, 1, 5);
  data.quests = { town: t, entries: questBuildEntriesForTown(t) };
  // Separate additive layer from data.attributes (see this section's top DATA-MODEL NOTE) --
  // cleared here on every town-scoped reset so a quest-granted attribute point never survives
  // past the town it was earned in, without touching level-derived or manually-earned
  // points living in data.attributes itself.
  data.quest_attr_bonus = { str: 0, dex: 0, vit: 0, int: 0 };
  data.quest_bonus_hp = 0;
  data.unspent_quest_stat_points = 0;
}

// Lazily hydrates data.quests/quest_attr_bonus/quest_bonus_hp/unspent_quest_stat_points for a
// character that predates this feature (or was somehow saved without them), and recomputes
// any locked->active transitions live off the character's CURRENT level -- so a level-up
// unlocks a not-yet-active quest immediately, without a dedicated reset trigger of its own.
// Idempotent; called at the top of every quest-touching route/hook below.
function questEnsureState(data) {
  const tier = (TRIAL_CLASSES[data.class_id] || {}).tier || 1;
  if (!data.quests || typeof data.quests !== "object" || !data.quests.entries) {
    questRebuildBoardForTown(data, tier);
  }
  if (!data.quest_attr_bonus || typeof data.quest_attr_bonus !== "object") data.quest_attr_bonus = { str: 0, dex: 0, vit: 0, int: 0 };
  if (typeof data.quest_bonus_hp !== "number") data.quest_bonus_hp = 0;
  if (typeof data.unspent_quest_stat_points !== "number") data.unspent_quest_stat_points = 0;
  const level = data.level || 1;
  for (const qid of QUEST_DEFS.IDS) {
    // v0.24.2 BUG FIX (credit: Gwen, "The Pacifist" showing as locked): backfill any quest id
    // that simply isn't on this character's board yet. Every quest added after a character was
    // created hit this gap -- the migrations below only reshaped entries that already existed.
    if (!data.quests.entries[qid]) data.quests.entries[qid] = questBuildEntry(qid);
    const entry = data.quests.entries[qid];
    if (entry.status === "locked" && level >= QUEST_DEFS.LEVEL_REQ[qid]) entry.status = "active";
  }
  // v0.24.1 (B4): migrate a legacy "elder" entry (pre-Ascendant-Hunt save, or one that somehow
  // lost its step field) onto the new step-based shape -- fresh start at step 1, since the old
  // single-target progress has no meaningful equivalent in the new 5-step chain.
  const elderEntry = data.quests.entries.elder;
  if (elderEntry && typeof elderEntry.step !== "number") {
    elderEntry.step = 1;
    elderEntry.progress = 0;
    elderEntry.target = QUEST_DEFS.ASCENDANT_HUNT_STEPS[0].count;
    if (elderEntry.status === "ready" || elderEntry.status === "claimed") elderEntry.status = "active";
  }
  // v0.24.1 (B6): migrate a legacy "cauldron" entry (pre-distinct-recipes save, tracking a
  // plain brew count) onto the new distinct_brewed shape -- old progress toward "N brews of
  // anything" has no honest equivalent under "7 DIFFERENT recipes", so this resets progress to
  // 0 rather than carry over a count that may not represent 7 distinct items at all. A
  // "ready"/"claimed" legacy entry is reset to "active" too, same as the elder migration above.
  // v0.24.2: migrate a legacy "wardens" entry (a flat progress/target count, pre-Warden-Trials)
  // onto the new step-based shape. Starts fresh at step 1: the old count was a single one-off
  // target with no meaningful position in the new 5-step chain, and every step must be earned
  // within one delve anyway, so carrying a partial count forward would be inventing progress.
  const wardensEntry = data.quests.entries.wardens;
  if (wardensEntry && typeof wardensEntry.step !== "number") {
    wardensEntry.step = 1;
    wardensEntry.progress = 0;
    wardensEntry.target = QUEST_DEFS.WARDEN_TRIAL_STEPS[0].count;
    wardensEntry._delve_streak_marker = 0;
    if (wardensEntry.status === "ready" || wardensEntry.status === "claimed") wardensEntry.status = "active";
  }
  // v0.25: same step-shape migration as "wardens"/"elder" for the two new questlines. Both
  // are new ids, so questEnsureState()'s backfill above normally builds them correctly -- this
  // only catches an entry that somehow exists without a step field.
  for (const qid of ["trader", "cleanse"]) {
    const e = data.quests.entries[qid];
    const steps = qid === "trader" ? QUEST_DEFS.TRADER_STEPS : QUEST_DEFS.CLEANSE_STEPS;
    if (e && typeof e.step !== "number") {
      e.step = 1; e.progress = 0; e.target = steps[0].count;
      if (e.status === "ready" || e.status === "claimed") e.status = "active";
    }
  }
  // v0.24.2: every entry's target is re-pinned to today's fixed value on load, so a character
  // saved under the old per-town scaling (e.g. a town-4 board asking for 22 roamers) is
  // corrected in place instead of keeping a target no longer offered anywhere. Progress
  // already made is kept, and clamped down if it now overshoots.
  for (const qid of ["gatherer", "wanderers", "streak", "palisade"]) {
    const e = data.quests.entries[qid];
    if (!e) continue;
    const fresh = questBuildEntry(qid);
    if (e.target !== fresh.target) {
      e.target = fresh.target;
      e.progress = Math.min(e.progress || 0, e.target);
      if (e.status === "active" && e.progress >= e.target) e.status = "ready";
    }
  }
  // v0.25: both lifetime-counter questlines re-derive their progress here, on every
  // quest-touching route, so they are correct even when the event that advanced them happened
  // while the player was offline (an Auction House sale) or before the current board existed.
  if (typeof data.auction_sales !== "number") data.auction_sales = 0;
  questTrackTotalKills(data);
  questTrackAuctionSales(data);
  const cauldronEntry = data.quests.entries.cauldron;
  if (cauldronEntry && !Array.isArray(cauldronEntry.distinct_brewed)) {
    cauldronEntry.distinct_brewed = [];
    cauldronEntry.progress = 0;
    cauldronEntry.target = QUEST_DEFS.CAULDRON_DISTINCT_TARGET;
    if (cauldronEntry.status === "ready" || cauldronEntry.status === "claimed") cauldronEntry.status = "active";
  }
}

// v-quest (Q5 "streak"): a "watch a value cross a threshold" quest, not an incrementing
// counter -- reads the ALREADY-server-tracked data.kill_streak (combatIncrementKillStreak()
// below) at each kill resolution. Once ready, status never gets un-set by a later streak
// reset (we simply never re-check a "ready" entry down), but the quest can also never
// complete without a genuine unbroken streak reaching the target in one go, since progress is
// only ever raised to (never speculatively past) the CURRENT streak value.
function questTrackKillStreak(data) {
  const entry = data.quests.entries.streak;
  if (!entry || entry.status !== "active") return;
  const streak = data.kill_streak || 0;
  entry.progress = Math.min(entry.target, Math.max(entry.progress || 0, streak));
  if (streak >= entry.target) entry.status = "ready";
}

// v-quest (Q4 "wanderers"): a genuine persistent per-town running total -- every is_roamer
// kill counts, no delve-boundary logic needed (unlike wardens/streak/explore below).
function questTrackRoamerKill(data) {
  const entry = data.quests.entries.wanderers;
  if (!entry || entry.status !== "active") return;
  entry.progress = Math.min(entry.target, (entry.progress || 0) + 1);
  if (entry.progress >= entry.target) entry.status = "ready";
}

// v-quest / v0.24.2 (Q2 "wardens" -- "The Warden Trials"): guardian kills ARE genuinely
// server-resolved (is_guardian kills only ever happen through the validated combat attack
// endpoint), but WHICH stronghold a given guardian belongs to is NOT known server-side --
// maze/stronghold generation is still entirely client-side (see the top-of-file BACKLOG note
// on server-authoritative maze generation), so there's no real per-stronghold identity to
// count guardian kills against. That is why every step of this line counts GUARDIANS rather
// than strongholds cleared.
//
// "Within one delve" is detected via data.kill_streak, which the client resets to 0 on every
// town return, death, and fresh venture, and combatIncrementKillStreak() bumps on every kill
// (guardians included) -- so a genuinely continuous delve has a kill_streak that never drops
// across all of that delve's guardian kills, no matter how many area levels it spans. We
// remember the kill_streak value at the guardian kill that last advanced progress
// (_delve_streak_marker); if a LATER guardian kill's kill_streak is lower than that marker,
// the streak must have reset in between (town return, death) -- a new delve -- so progress
// restarts at 1 for this kill instead of accumulating further.
//
// v0.24.2 (credit: Gwen): the old area-level band gate is REMOVED. This used to return early
// unless areaLevel was within that town's explore_area_level target, which both hid progress
// for no visible reason and was itself a form of the town scaling Gwen asked to be rid of.
// Wardens count anywhere now.
function questTrackGuardianKill(data, areaLevel) {
  const entry = data.quests.entries.wardens;
  if (!entry || entry.status !== "active") return;
  const streak = data.kill_streak || 0;
  if (streak < (entry._delve_streak_marker || 0)) entry.progress = 0;
  entry._delve_streak_marker = streak;
  entry.progress = Math.min(entry.target, (entry.progress || 0) + 1);
  if (entry.progress >= entry.target) entry.status = "ready";
}

// v0.24.1 (B4 -- "The Ascendant Hunt"): monster band is already known server-side at kill
// resolution (COMBAT_MONSTERS[...].tier, the SAME field combatRollRarityForBand() already
// reads). Unlike the old "band-or-higher" rule, each step requires an EXACT band match against
// ASCENDANT_HUNT_STEPS[entry.step-1].band -- a Legendary kill does NOT count toward the
// Common-band step 1, it has to be worked through in order.
function questTrackEliteKill(data, monsterBand) {
  const entry = data.quests.entries.elder;
  if (!entry || entry.status !== "active") return;
  const step = QUEST_DEFS.ASCENDANT_HUNT_STEPS[(entry.step || 1) - 1];
  if (!step || monsterBand !== step.band) return;
  entry.progress = Math.min(entry.target, (entry.progress || 0) + 1);
  if (entry.progress >= entry.target) entry.status = "ready";
}

// v0.25 ("Cleanse the Forest"): reads the LIFETIME data.total_kills rather than counting up
// from zero, so progress is always the true number no matter how many town/board resets have
// happened since. That also makes it self-healing: a fresh board recomputes the same figure on
// the very next kill instead of starting the player over on a 100,000-kill commitment.
// v0.29 BUG FIX (credit: Gwen): this read data.total_kills directly, which is the LIFETIME
// counter and deliberately survives a Broken Bridge Trial (the leaderboard needs it to). So a
// character that fell at the bridge with a few thousand kills behind it came back at level 1 and
// instantly had steps 1 and 2 of Cleanse the Forest complete -- questRebuildBoardForTown() did
// reset the entry's progress to 0, and this function put it straight back on the next kill.
//
// The quest now counts kills made SINCE its own baseline rather than kills ever made. The
// baseline is the character's total_kills at the moment the board was last rebuilt, so a trial
// attempt (pass or fall) starts the count again from zero while total_kills itself is untouched
// and the leaderboard is unaffected. Claiming a step also advances the baseline, so each step
// counts its own kills rather than inheriting the previous step's.
function questCleanseKillsSinceBaseline(data, entry) {
  const base = entry._kill_base || 0;
  return Math.max(0, (data.total_kills || 0) - base);
}
function questTrackTotalKills(data) {
  const entry = data.quests.entries.cleanse;
  if (!entry || entry.status !== "active") return;
  const step = QUEST_DEFS.CLEANSE_STEPS[(entry.step || 1) - 1];
  if (!step) return;
  // A character created before this baseline existed has no _kill_base. Anchoring it to their
  // CURRENT total (rather than to 0) is the honest migration: it neither wipes progress they can
  // no longer prove nor hands them credit for kills made under the old rule.
  if (typeof entry._kill_base !== "number") entry._kill_base = data.total_kills || 0;
  entry.target = step.count;
  entry.progress = Math.min(step.count, questCleanseKillsSinceBaseline(data, entry));
  if (entry.progress >= step.count) entry.status = "ready";
}

// v0.25 ("The Long Road to Market"): same lifetime-counter shape as questTrackTotalKills()
// above, against data.auction_sales -- incremented by POST /api/auction/:id/buy on the SELLER's
// character row when one of its listings actually sells. Called from questEnsureState() rather
// than from a kill hook, because a sale can complete while the seller is offline entirely: the
// counter is already correct by the time they next load, and this simply reflects it onto the
// board the next time any quest-touching route runs.
function questTrackAuctionSales(data) {
  const entry = data.quests.entries.trader;
  if (!entry || entry.status !== "active") return;
  const step = QUEST_DEFS.TRADER_STEPS[(entry.step || 1) - 1];
  if (!step) return;
  entry.target = step.count;
  entry.progress = Math.min(step.count, data.auction_sales || 0);
  if (entry.progress >= step.count) entry.status = "ready";
}

// v0.24.1 (B5 "The Pacifist"): called from combatFinalizeMonsterKill() exactly once, only when
// THIS fight's session.player_dealt_direct_damage flag never got set -- i.e. the monster died
// without the player ever landing a swing or a spell-DOT tick, which (now that C2 lets Thorns
// land the killing blow) is a genuinely achievable "tank it to death on reflect damage alone"
// challenge. A single boolean completion, not a counter -- one qualifying kill is enough.
function questTrackPacifist(data) {
  const entry = data.quests.entries.pacifist;
  if (!entry || entry.status !== "active") return;
  entry.progress = entry.target;
  entry.status = "ready";
}

// v-quest: explore (Q1)/gatherer (Q3)/cauldron (Q6)/palisade (Q8) all live behind systems
// that are STILL entirely client-resolved today (maze generation, herb/chest encounters, and
// Alchemist brewing all predate any server-authoritative endpoint for them -- see the
// top-of-file BACKLOG notes on server-authoritative maze generation and chest/herb/critter
// encounters; Alchemist brewing has never had a server route at all). These 4 quests are
// therefore CLIENT-ATTESTED, acceptable only until that server-authoritative work lands, per
// the feature spec's own explicit acknowledgment of this limitation. Each report route below
// applies the same shape of defense: a minimum interval between reports (questReportRateLimitOk)
// and a proportionate plausibility check against server-known state (max_maze_depth_reached,
// per-action yield caps, known item ids) -- not full verification, just closing the most
// obvious "one call, huge jump" abuse vectors.
const QUEST_REPORT_MIN_INTERVAL_MS = 3000;
function questReportRateLimitOk(entry) {
  const now = Date.now();
  if (entry._last_report_at && now - entry._last_report_at < QUEST_REPORT_MIN_INTERVAL_MS) return false;
  entry._last_report_at = now;
  return true;
}

// v0.22.3 (#10 section A): declares the season's first tier-5 character to reach Level 100
// as champion -- guarded by season_state.winner_declared so only the very first character to
// cross the line wins, no matter how many others hit the same milestone afterward (or in the
// same instant, since this whole function runs inside the request handler synchronously with
// respect to node:sqlite's single-threaded execution -- no race window between the read and
// the write below). Broadcasts a global-chat announcement exactly once, on the winning call.
function maybeDeclareSeasonWinner(accountId, data) {
  const classInfo = TRIAL_CLASSES[data.class_id];
  if (!classInfo || classInfo.tier !== 5) return;
  if ((data.level || 1) < CB.LEVEL_CAP) return;
  const state = db.prepare("SELECT winner_declared FROM season_state WHERE id = 1").get();
  if (state && state.winner_declared) return;
  const nowStr = nowIso();
  db.prepare(
    "UPDATE season_state SET winner_declared = 1, winner_account_id = ?, winner_character_name = ?, winner_class_name = ?, won_at = ? WHERE id = 1"
  ).run(accountId, data.character_name || "A traveler", classInfo.display_name, nowStr);
  broadcastSystemMessage(
    `🏆 ${data.character_name || "A traveler"} the ${classInfo.display_name} has reached Level ${CB.LEVEL_CAP} and won the season! A new champion stands atop the ladder.`
  );
}

// Keeper of the Emerald: the endgame moral choice. Independent of maybeDeclareSeasonWinner()
// above (that's just a "first past the post" bragging-rights record) -- per the design doc,
// EVERY Tier-5, Level-100 character gets to make this choice for themselves, not just the
// first one to reach the cap. A character may only choose once (data.emerald_choice is set
// permanently after this call).
//
// IMPORTANT SAFETY NOTE (first season): choosing "crown" intentionally does NOT trigger any
// reset, mass or otherwise. It only records the choice in hall_of_heroes, pays out gold, and
// broadcasts an announcement. For this first season the game's owner wants sole manual control
// over resets via the existing one-character-at-a-time /api/admin/reset-ladder/:accountId/:slot
// endpoint, specifically to avoid any logic-flaw accidentally wiping every player's progress.
// Do NOT wire this function (or anything it calls) to iterate/reset other characters.
app.post("/api/keeper/choice", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const choice = req.body?.choice;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  if (choice !== "emerald" && choice !== "crown") {
    return res.status(400).json({ error: "choice must be \"emerald\" or \"crown\"." });
  }

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "Corrupt character save." });
  }

  const classInfo = TRIAL_CLASSES[data.class_id];
  if (!classInfo || classInfo.tier !== 5) {
    return res.status(400).json({ error: "Only a Tier-5 character may stand before the Keeper." });
  }
  if ((data.level || 1) < CB.LEVEL_CAP) {
    return res.status(400).json({ error: `Must reach Level ${CB.LEVEL_CAP} before the Keeper will speak with you.` });
  }
  if ((data.highest_tier_reached || 1) < 5) {
    return res.status(400).json({ error: "The Keeper only appears at Ashveil Sanctum." });
  }
  if (data.emerald_choice) {
    return res.status(400).json({ error: "You have already stood before the Emerald and made your choice." });
  }

  const nowStr = nowIso();
  const characterName = data.character_name || "A traveler";
  data.emerald_choice = choice;
  data.emerald_choice_at = nowStr;

  if (choice === "crown") {
    data.gold = (data.gold || 0) + CB.CROWN_GOLD_REWARD;
    data.has_crown_of_victory = true;
  } else {
    data.has_protector_title = true;
  }

  data._save_seq = (data._save_seq || 0) + 1;
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowStr,
    req.account.id,
    slot
  );
  upsertLeaderboardBests(req.account.id, data);

  db.prepare(
    "INSERT INTO hall_of_heroes (account_id, character_name, class_name, choice, chosen_at) VALUES (?, ?, ?, ?, ?)"
  ).run(req.account.id, characterName, classInfo.display_name, choice, nowStr);

  if (choice === "crown") {
    broadcastSystemMessage(
      `⚡ ${characterName} the ${classInfo.display_name} has claimed the Crown of Victory. The corruption seeps back into the forest, but the world does not reset itself; that choice is left to the game's keepers, in their own time.`
    );
  } else {
    broadcastSystemMessage(
      `🌿 ${characterName} the ${classInfo.display_name} chose to heal the world, planting the Emerald at the heart of the restored forest. Their name is carved into the Hall of Healers.`
    );
  }

  res.json({ ok: true, choice, character_name: characterName, gold: data.gold });
});

// Public, read-only log of every Keeper-of-the-Emerald choice ever made -- split into the
// two halls the design doc describes. No auth required, same pattern as /api/leaderboard.
app.get("/api/hall-of-heroes", (req, res) => {
  const rows = db.prepare("SELECT character_name, class_name, choice, chosen_at FROM hall_of_heroes ORDER BY chosen_at DESC").all();
  res.json({
    healers: rows.filter((r) => r.choice === "emerald"),
    victors: rows.filter((r) => r.choice === "crown"),
  });
});

app.post("/api/trial/attempt", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const plankIndex = Number(req.body?.plank_index);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  if (!Number.isInteger(plankIndex) || plankIndex < 0) {
    return res.status(400).json({ error: "Invalid plank_index." });
  }

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "Corrupt character save." });
  }

  const classId = data.class_id;
  const classInfo = TRIAL_CLASSES[classId];
  if (!classInfo) return res.status(400).json({ error: "Unknown class for this character." });
  const nextClassId = classInfo.next_class;
  // Mirrors screenMasterTrial()'s own gate: a character at the final tier of its chain
  // has no further trial to attempt -- the client never even shows plank buttons in this
  // case, so a request reaching here with no next_class is necessarily a bypassed client.
  if (!nextClassId) return res.status(400).json({ error: "This class has no further trial to attempt." });

  const currentTier = classInfo.tier || 1;
  const levelReq = trialLevelRequirement(currentTier);
  const level = data.level || 1;
  if (level < levelReq) {
    return res.status(400).json({ error: `Must reach Level ${levelReq} before attempting this trial.` });
  }

  // Server's own authoritative mirror of PS.trial_progress (getTrialProgress() client-
  // side) -- lives in the same stored JSON blob the client already round-trips as
  // data.trial_progress, keyed per class_id, so there is nothing new to migrate.
  if (!data.trial_progress || typeof data.trial_progress !== "object") data.trial_progress = {};
  if (!data.trial_progress[classId] || typeof data.trial_progress[classId] !== "object") {
    data.trial_progress[classId] = { stage: 0, discoveredSides: [], stepFailedPlanks: {} };
  }
  const progress = data.trial_progress[classId];
  if (!Array.isArray(progress.discoveredSides)) progress.discoveredSides = [];
  if (!progress.stepFailedPlanks || typeof progress.stepFailedPlanks !== "object") progress.stepFailedPlanks = {};

  const plankCount = trialBridgePlankCount(currentTier);
  if (plankIndex >= plankCount) {
    return res.status(400).json({ error: "plank_index out of range for this tier's bridge." });
  }
  const stepIdx = progress.discoveredSides.length;
  if (stepIdx >= TRIAL_BRIDGE_ROWS) {
    return res.status(400).json({ error: "This trial attempt has already been fully resolved." });
  }
  const failedHere = progress.stepFailedPlanks[stepIdx] || [];
  if (failedHere.includes(plankIndex)) {
    return res.status(400).json({ error: "That plank is already known to be broken at this step." });
  }

  // The exact same "the bridge remembers the faulty plank" guarantee the client's local
  // roll used to provide: once every OTHER plank at this step has already failed, the
  // one remaining plank is certain to hold, so a step can never become unwinnable.
  const remaining = plankCount - failedHere.length;
  const passed = remaining <= 1 ? true : Math.random() < 1 / remaining;
  const nowStr = nowIso();

  if (!passed) {
    failedHere.push(plankIndex);
    progress.stepFailedPlanks[stepIdx] = failedHere;
    // v0.17 (#1): stepIdx is exactly "how many steps were already cleared when this one
    // broke" -- the leaderboard's public step count. Only ever updated on a failure, same
    // as the client-side field this mirrors.
    data.last_bridge_steps = stepIdx;
    data.bridge_fail_streak = (data.bridge_fail_streak || 0) + 1;
    applyTrialResolutionReset(data, classId, TRIAL_STARTING_STAT_POINTS + data.bridge_fail_streak);
    data._save_seq = (data._save_seq || 0) + 1;
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data),
      nowStr,
      req.account.id,
      slot
    );
    upsertLeaderboardBests(req.account.id, data);
    broadcastSystemMessage(
      `${data.character_name || "A traveler"} (Lv ${level} ${classInfo.display_name}) has failed the broken bridge trial at step ${stepIdx + 1}.`
    );
    return res.json({
      ok: true,
      outcome: "failed",
      failed_step_index: stepIdx,
      plank_index: plankIndex,
      discovered_sides: progress.discoveredSides,
      step_failed_planks: progress.stepFailedPlanks,
      level: data.level,
      xp: data.xp,
      xp_to_next: data.xp_to_next,
      attributes: data.attributes,
      unspent_stat_points: data.unspent_stat_points,
      current_hp: cbInt(data.current_hp),
      current_stamina: cbInt(data.current_stamina),
      max_maze_depth_reached: data.max_maze_depth_reached,
      last_bridge_steps: data.last_bridge_steps,
      bridge_fail_streak: data.bridge_fail_streak,
      _save_seq: data._save_seq,
    });
  }

  // Passed this step.
  progress.discoveredSides.push(plankIndex);
  const fullyCrossed = progress.discoveredSides.length >= TRIAL_BRIDGE_ROWS;

  if (!fullyCrossed) {
    data._save_seq = (data._save_seq || 0) + 1;
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data),
      nowStr,
      req.account.id,
      slot
    );
    return res.json({
      ok: true,
      outcome: "passed_step",
      plank_index: plankIndex,
      discovered_sides: progress.discoveredSides,
      step_failed_planks: progress.stepFailedPlanks,
      _save_seq: data._save_seq,
    });
  }

  // Full crossing: promotion to the next class in this chain.
  data.highest_tier_reached = Math.max(data.highest_tier_reached || 1, currentTier + 1);
  data.class_id = nextClassId;
  data.class_display_name = TRIAL_CLASSES[nextClassId].display_name;
  data.last_bridge_steps = 0;
  data.bridge_fail_streak = 0;
  // A fresh promotion means 0 steps taken so far on the NEW tier's bridge -- clear (not
  // carry over) this class_id's trial_progress entry, mirroring the client's own
  // `PS.trial_progress[PS.class_id]={...}` reset right after resolveMasterTrial(true).
  data.trial_progress[nextClassId] = { stage: 0, discoveredSides: [], stepFailedPlanks: {} };
  applyTrialResolutionReset(data, nextClassId, TRIAL_STARTING_STAT_POINTS, TRIAL_CLASSES[nextClassId].tier);
  data._save_seq = (data._save_seq || 0) + 1;
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowStr,
    req.account.id,
    slot
  );
  upsertLeaderboardBests(req.account.id, data);
  broadcastSystemMessage(
    `${data.character_name || "A traveler"} (Lv ${level} ${classInfo.display_name}) has passed the broken bridge trial and is now a ${data.class_display_name}.`
  );
  return res.json({
    ok: true,
    outcome: "promoted",
    final_tier: !TRIAL_CLASSES[nextClassId].next_class,
    new_class_id: data.class_id,
    new_class_display_name: data.class_display_name,
    level: data.level,
    xp: data.xp,
    xp_to_next: data.xp_to_next,
    attributes: data.attributes,
    unspent_stat_points: data.unspent_stat_points,
    current_hp: cbInt(data.current_hp),
    current_stamina: cbInt(data.current_stamina),
    max_maze_depth_reached: data.max_maze_depth_reached,
    last_bridge_steps: data.last_bridge_steps,
    bridge_fail_streak: data.bridge_fail_streak,
    _save_seq: data._save_seq,
  });
});

/* ---------------- Quest System: endpoints ----------------
   Claim (POST /api/quests/:questId/claim), the quest-only stat-point spend (POST
   /api/quests/spend-stat-point), and the 4 client-attested progress-report routes for the
   quests whose underlying systems (maze/chest/herb/Alchemist) aren't server-authoritative yet
   -- see the Quest System data/helper section above (right after applyLadderReset()) for
   QUEST_DEFS and every questTrack*()/questEnsureState() helper these routes call. */

// Every grant below reuses the SAME validated function another feature already trusts for
// that exact reward type (creditAccountGold for gold, combatAddXp for XP, combatAddGearAuto
// Equip+combatGenerateGearItem for gear, combatAddConsumable for elixirs/keys) -- nothing here
// is a hand-rolled duplicate of existing loot/reward logic. Like the combat attack/trial
// endpoints, this loads the row, mutates it, and saves it back synchronously within one
// request handler (node:sqlite is synchronous, Express runs on a single JS thread) -- there is
// no `await` between the initial `entry.status !== "ready"` re-check and the final save, so a
// double-submit from the same account+slot can never race past that check twice (same
// no-race-window guarantee maybeDeclareSeasonWinner()'s own comment documents for its
// read-then-write). The `status = "claimed"` write below is what actually prevents a second
// claim from ever seeing "ready" again.
app.post("/api/quests/:questId/claim", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const questId = req.params.questId;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!QUEST_DEFS.IDS.includes(questId)) return res.status(400).json({ error: "Unknown quest." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  const entry = data.quests.entries[questId];
  // Re-derive readiness server-side -- never trust a client-sent status, mirrors every other
  // reward-granting route in this file (trial resolution, combat kills, Blacksmith crafts).
  if (!entry || entry.status !== "ready") return res.status(400).json({ error: "That quest isn't ready to claim." });
  if (questId === "cauldron" && QUEST_DEFS.CAULDRON_ATTR_POTION_REQUIRED && !entry.attr_potion_brewed) {
    return res.status(400).json({ error: "That quest isn't ready to claim." });
  }

  const reward = { quest_id: questId };

  if (questId === "explore") {
    data.quest_bonus_hp = (data.quest_bonus_hp || 0) + QUEST_DEFS.REWARDS.explore.hp;
    creditAccountGold(req.account.id, QUEST_DEFS.REWARDS.explore.gold);
    reward.quest_bonus_hp = QUEST_DEFS.REWARDS.explore.hp;
    reward.gold = QUEST_DEFS.REWARDS.explore.gold;
  } else if (questId === "wardens") {
    // v0.24.2: step-based, exactly like "elder" below -- fixed per-step reward off
    // WARDEN_TRIAL_STEPS, and a mid-chain claim advances to the next step instead of
    // terminating the quest.
    const stepIdx = (entry.step || 1) - 1;
    const stepDef = QUEST_DEFS.WARDEN_TRIAL_STEPS[stepIdx] || QUEST_DEFS.WARDEN_TRIAL_STEPS[0];
    data.unspent_quest_stat_points = (data.unspent_quest_stat_points || 0) + stepDef.stat_points;
    creditAccountGold(req.account.id, stepDef.gold);
    const xpResult = combatAddXp(data, stepDef.xp);
    if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
    reward.stat_points = stepDef.stat_points;
    reward.gold = stepDef.gold;
    reward.xp = xpResult.xpGained;
    reward.step = entry.step || 1;
    reward.step_count = QUEST_DEFS.WARDEN_TRIAL_STEPS.length;
    if (stepIdx + 1 < QUEST_DEFS.WARDEN_TRIAL_STEPS.length) {
      entry.step = stepIdx + 2;
      entry.progress = 0;
      entry.target = QUEST_DEFS.WARDEN_TRIAL_STEPS[stepIdx + 1].count;
      // A fresh step must be earned in its own delve from scratch, so the marker resets too --
      // otherwise the guardian kills already banked this delve would carry into the next step.
      entry._delve_streak_marker = data.kill_streak || 0;
      entry.status = "active";
      reward.done = false;
    } else {
      reward.done = true;
    }
  } else if (questId === "gatherer") {
    for (const itemId of QUEST_DEFS.REWARDS.gatherer.elixirs) combatAddConsumable(data, itemId, 1);
    // Applies the SAME Experience Shrine buff the in-maze shrine grants (see
    // combatGetShrineXpPct()'s own comment for how these two fields are consumed).
    data.xp_buff_multiplier = QUEST_DEFS.SHRINE_XP_BUFF_MULT;
    data.xp_buff_encounters_left = (data.xp_buff_encounters_left || 0) + QUEST_DEFS.SHRINE_XP_BUFF_ENCOUNTERS;
    reward.elixirs = QUEST_DEFS.REWARDS.gatherer.elixirs;
  } else if (questId === "wanderers") {
    data.quest_bonus_hp = (data.quest_bonus_hp || 0) + QUEST_DEFS.REWARDS.wanderers.hp;
    creditAccountGold(req.account.id, QUEST_DEFS.REWARDS.wanderers.gold);
    const xpResult = combatAddXp(data, QUEST_DEFS.REWARDS.wanderers.xp);
    if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
    reward.quest_bonus_hp = QUEST_DEFS.REWARDS.wanderers.hp;
    reward.gold = QUEST_DEFS.REWARDS.wanderers.gold;
    reward.xp = xpResult.xpGained;
  } else if (questId === "streak") {
    creditAccountGold(req.account.id, QUEST_DEFS.REWARDS.streak.gold);
    const xpResult = combatAddXp(data, QUEST_DEFS.REWARDS.streak.xp);
    if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
    reward.gold = QUEST_DEFS.REWARDS.streak.gold;
    reward.xp = xpResult.xpGained;
  } else if (questId === "cauldron") {
    for (const itemId of QUEST_DEFS.REWARDS.cauldron.elixirs) combatAddConsumable(data, itemId, 1);
    data.unspent_quest_stat_points = (data.unspent_quest_stat_points || 0) + QUEST_DEFS.REWARDS.cauldron.stat_points;
    reward.elixirs = QUEST_DEFS.REWARDS.cauldron.elixirs;
    reward.stat_points = QUEST_DEFS.REWARDS.cauldron.stat_points;
  } else if (questId === "elder") {
    // v0.24.1 (B4): fixed per-step reward off ASCENDANT_HUNT_STEPS -- deliberately NOT scaled
    // by town idx (see that constant's own comment). No gear roll; this questline pays in
    // stat points + XP only.
    const stepIdx = (entry.step || 1) - 1;
    const stepDef = QUEST_DEFS.ASCENDANT_HUNT_STEPS[stepIdx] || QUEST_DEFS.ASCENDANT_HUNT_STEPS[0];
    data.unspent_quest_stat_points = (data.unspent_quest_stat_points || 0) + stepDef.stat_points;
    const xpResult = combatAddXp(data, stepDef.xp);
    if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
    reward.stat_points = stepDef.stat_points;
    reward.xp = xpResult.xpGained;
    reward.step = entry.step || 1;
    reward.step_count = QUEST_DEFS.ASCENDANT_HUNT_STEPS.length;
    if (stepIdx + 1 < QUEST_DEFS.ASCENDANT_HUNT_STEPS.length) {
      // Mid-chain: advance to the next band/count and stay "active" -- do NOT fall through to
      // the generic `entry.status = "claimed"` below, this questline isn't done yet.
      entry.step = stepIdx + 2;
      entry.progress = 0;
      entry.target = QUEST_DEFS.ASCENDANT_HUNT_STEPS[stepIdx + 1].count;
      entry.status = "active";
      reward.done = false;
    } else {
      reward.done = true;
    }
  } else if (questId === "trader" || questId === "cleanse") {
    // v0.25: both new questlines share the exact step-advance shape "wardens"/"elder" use.
    const steps = questId === "trader" ? QUEST_DEFS.TRADER_STEPS : QUEST_DEFS.CLEANSE_STEPS;
    const stepIdx = (entry.step || 1) - 1;
    const stepDef = steps[stepIdx] || steps[0];
    data.unspent_quest_stat_points = (data.unspent_quest_stat_points || 0) + stepDef.stat_points;
    reward.stat_points = stepDef.stat_points;
    if (stepDef.gold) { creditAccountGold(req.account.id, stepDef.gold); reward.gold = stepDef.gold; }
    if (stepDef.xp) {
      const xpResult = combatAddXp(data, stepDef.xp);
      if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
      reward.xp = xpResult.xpGained;
    }
    reward.step = entry.step || 1;
    reward.step_count = steps.length;
    if (stepIdx + 1 < steps.length) {
      entry.step = stepIdx + 2;
      entry.target = steps[stepIdx + 1].count;
      // Both count a LIFETIME total, so the next step's progress carries straight over rather
      // than restarting at 0 -- a player at 120 kills who claims step 1 (100) is genuinely
      // already 120/500 toward step 2, and showing 0/500 would be a lie.
      // v0.29: Cleanse counts kills since its own baseline, so claiming a step moves that
      // baseline to now -- each step asks for its own kills rather than inheriting the last
      // step's. Trader still carries its lifetime sales forward, which is correct for it: an
      // Auction House sale is a permanent fact about the character, not something a trial undoes.
      if (questId === "cleanse") {
        entry._kill_base = data.total_kills || 0;
        entry.progress = 0;
      } else {
        entry.progress = Math.min(entry.target, data.auction_sales || 0);
      }
      entry.status = entry.progress >= entry.target ? "ready" : "active";
      reward.done = false;
    } else {
      reward.done = true;
    }
  } else if (questId === "palisade") {
    creditAccountGold(req.account.id, QUEST_DEFS.REWARDS.palisade.gold);
    const keyTier = cbClampi((data.quests.town || 1) + 1, 1, ITEM_TIER_MAX);
    const keyItemId = combatStrongholdKeyItemIdForTier(keyTier);
    combatAddConsumable(data, keyItemId, 1);
    reward.gold = QUEST_DEFS.REWARDS.palisade.gold;
    reward.key_item_id = keyItemId;
  } else if (questId === "pacifist") {
    // v0.24.1 (B5): single fixed non-town-scaled reward -- see PACIFIST_REWARD's own comment.
    const r = QUEST_DEFS.PACIFIST_REWARD;
    data.unspent_quest_stat_points = (data.unspent_quest_stat_points || 0) + r.stat_points;
    const xpResult = combatAddXp(data, r.xp);
    if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
    const potions = {};
    for (const [itemId, qty] of Object.entries(r.potions)) {
      combatAddConsumable(data, itemId, qty);
      potions[itemId] = qty;
    }
    reward.stat_points = r.stat_points;
    reward.xp = xpResult.xpGained;
    reward.potions = potions;
  }

  // v0.24.1 (B4) / v0.24.2: a mid-chain "elder" or "wardens" claim already set entry.status =
  // "active" (with the next step's progress/target) above -- do NOT stomp that back to
  // "claimed" here. Every other quest, and a final-step claim on either questline, still
  // terminate normally.
  if (!(["elder", "wardens", "trader", "cleanse"].includes(questId) && (entry.status === "active" || entry.status === "ready"))) entry.status = "claimed";
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({
    ok: true, reward, quests: data.quests,
    quest_attr_bonus: data.quest_attr_bonus, quest_bonus_hp: data.quest_bonus_hp,
    unspent_quest_stat_points: data.unspent_quest_stat_points,
    level: data.level, xp: data.xp, xp_to_next: data.xp_to_next,
    current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data),
    account_gold: getAccountGold(req.account.id), _save_seq: saveSeq,
  });
});

// v-quest: spends ONE point from the quest-only pool directly into quest_attr_bonus[stat] --
// deliberately a SEPARATE endpoint from the general attribute system (which has no
// spend-validation endpoint of its own yet, see this section's DATA-MODEL NOTE above) so
// there is never any ambiguity about which points are which, and questRebuildBoardForTown()
// can zero both fields out cleanly on a town change.
// v0.25: reading a Scroll of Wisdom. Server-authoritative on purpose, unlike most consumable
// use in this phase: this one hands out raw experience, and the level gate is the entire point
// of the item, so neither the gate nor the grant may live in the client where a player could
// simply skip them. Consumes the scroll and applies the XP in one synchronous step, so a
// failure anywhere means the scroll is still in the bag.
app.post("/api/scroll/read", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const itemId = req.body?.item_id;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  const tier = scrollOfWisdomTierForItemId(itemId);
  if (!tier) return res.status(400).json({ error: "That isn't a Scroll of Wisdom." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  if (((data.consumables || {})[itemId] || 0) < 1) return res.status(400).json({ error: "You don't have that scroll." });
  const requiredLevel = combatTierLevelRequirement(tier);
  if ((data.level || 1) < requiredLevel) {
    return res.status(400).json({ error: `This scroll's words are beyond you until Level ${requiredLevel}.` });
  }
  data.consumables[itemId] -= 1;
  if (data.consumables[itemId] <= 0) delete data.consumables[itemId];
  const xpResult = combatAddXp(data, SCROLL_OF_WISDOM_XP[tier] || 0);
  if (xpResult.leveled) maybeDeclareSeasonWinner(req.account.id, data);
  questEnsureState(data);
  upsertLeaderboardBests(req.account.id, data);
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({
    ok: true, xp_gained: xpResult.xpGained, leveled: xpResult.leveled,
    level: data.level, xp: data.xp, xp_to_next: data.xp_to_next,
    consumables: data.consumables, quests: data.quests,
    current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), _save_seq: saveSeq,
  });
});

app.post("/api/quests/spend-stat-point", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const stat = req.body?.stat;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!["str", "dex", "vit", "int"].includes(stat)) return res.status(400).json({ error: "Invalid stat." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  if ((data.unspent_quest_stat_points || 0) <= 0) return res.status(400).json({ error: "No unspent quest stat points." });
  data.unspent_quest_stat_points -= 1;
  data.quest_attr_bonus[stat] = (data.quest_attr_bonus[stat] || 0) + 1;
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({ ok: true, unspent_quest_stat_points: data.unspent_quest_stat_points, quest_attr_bonus: data.quest_attr_bonus, _save_seq: saveSeq });
});

// v-quest (Q1 "explore"): client-attested (see the Quest System section's report-route
// comment, above questReportRateLimitOk()). Plausibility: the character must have genuinely
// reached the reported area level already (max_maze_depth_reached) -- rejects a report
// claiming a depth the character never reached.
// v0.24.1 (B3): previously ALSO required areaLevel >= this town's explore_area_level target
// (3/6/9/12/15), so "fully explore a maze" only counted at that specific depth or deeper --
// per Gwen's exact spec this quest is meant to be satisfiable at ANY area level (same simple
// "finish exploring a delve" objective everywhere for a given town), so that lower-bound check
// is dropped entirely. The max_maze_depth_reached upper-bound plausibility check stays.
app.post("/api/quests/report-explore", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const areaLevel = Number(req.body?.area_level);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!Number.isInteger(areaLevel) || areaLevel < 1) return res.status(400).json({ error: "Invalid area_level." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  const entry = data.quests.entries.explore;
  if (!entry || entry.status !== "active") return res.json({ ok: true, ignored: true, quests: data.quests });
  if (!questReportRateLimitOk(entry)) return res.status(429).json({ error: "Too soon since your last report." });
  if (areaLevel > (data.max_maze_depth_reached || 1)) {
    return res.status(400).json({ error: "That report isn't plausible for your current progress." });
  }
  entry.progress = entry.target;
  entry.status = "ready";
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({ ok: true, quests: data.quests, _save_seq: saveSeq });
});

// v-quest (Q3 "gatherer"): client-attested. Plausibility: caps how much progress ONE report
// can move (max single herb-node yield is 12, see v0.12's "Boost herb gathering yield to
// 1-12" note) and rate-limits how often a single report can move progress at all.
app.post("/api/quests/report-herb-harvest", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const count = Number(req.body?.count);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!Number.isInteger(count) || count < 1 || count > 12) return res.status(400).json({ error: "Implausible harvest amount." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  const entry = data.quests.entries.gatherer;
  if (!entry || entry.status !== "active") return res.json({ ok: true, ignored: true, quests: data.quests });
  if (!questReportRateLimitOk(entry)) return res.status(429).json({ error: "Too soon since your last report." });
  entry.progress = Math.min(entry.target, (entry.progress || 0) + count);
  if (entry.progress >= entry.target) entry.status = "ready";
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({ ok: true, quests: data.quests, _save_seq: saveSeq });
});

// v-quest (Q6 "cauldron"): client-attested (Alchemist brewing has never had a server route at
// all). Plausibility: item_id must be a real, known brewable consumable, and reports are
// rate-limited. v0.24.1 (B6): now tracks DISTINCT recipe ids brewed (entry.distinct_brewed),
// not a plain brew count -- brewing the same potion again is a no-op for progress (still
// counts toward attr_potion_brewed below if it happens to be an attribute potion, since that
// sub-requirement only needs "at least once", not "as one of the 7 distinct").
app.post("/api/quests/report-brew", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const itemId = req.body?.item_id;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!itemId || !QUEST_DEFS.BREWABLE_ITEM_IDS.includes(itemId)) return res.status(400).json({ error: "Unknown brewed item." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  const entry = data.quests.entries.cauldron;
  if (!entry || entry.status !== "active") return res.json({ ok: true, ignored: true, quests: data.quests });
  if (!questReportRateLimitOk(entry)) return res.status(429).json({ error: "Too soon since your last report." });
  if (!Array.isArray(entry.distinct_brewed)) entry.distinct_brewed = [];
  if (!entry.distinct_brewed.includes(itemId)) entry.distinct_brewed.push(itemId);
  entry.progress = Math.min(entry.target, entry.distinct_brewed.length);
  if (QUEST_DEFS.ATTR_POTION_IDS.includes(itemId)) entry.attr_potion_brewed = true;
  const subReqMet = !QUEST_DEFS.CAULDRON_ATTR_POTION_REQUIRED || entry.attr_potion_brewed;
  if (entry.progress >= entry.target && subReqMet) entry.status = "ready";
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({ ok: true, quests: data.quests, _save_seq: saveSeq });
});

// v-quest (Q8 "palisade"): client-attested (stronghold chest resolution, like all maze
// content, has no server route yet). Plausibility: the reported area level can't exceed the
// character's own max_maze_depth_reached. Persists as a running per-TOWN total, deliberately
// NEVER reset between delves (unlike wardens/streak/explore, see QUEST_DEFS.TARGETS'
// palisade_chests comment).
app.post("/api/quests/report-stronghold-chest", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const areaLevel = Number(req.body?.area_level);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!Number.isInteger(areaLevel) || areaLevel < 1) return res.status(400).json({ error: "Invalid area_level." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  questEnsureState(data);
  const entry = data.quests.entries.palisade;
  if (!entry || entry.status !== "active") return res.json({ ok: true, ignored: true, quests: data.quests });
  if (!questReportRateLimitOk(entry)) return res.status(429).json({ error: "Too soon since your last report." });
  if (areaLevel > (data.max_maze_depth_reached || 1)) return res.status(400).json({ error: "That report isn't plausible for your current progress." });
  entry.progress = Math.min(entry.target, (entry.progress || 0) + 1);
  if (entry.progress >= entry.target) entry.status = "ready";
  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({ ok: true, quests: data.quests, _save_seq: saveSeq });
});

/* ---------------- Gear item authenticity validation (v0.18.4) ----------------
   Shared by the Blacksmith reroll endpoint just below, the Auction House listing route,
   and the Storage Vault PUT route -- anywhere a gear item JSON blob arrives from the
   client and gets persisted or traded, it must pass this bounds check first. Mirrors
   index.html's IF.generate()/Balance.affixMaxForTier() rules just closely enough to
   catch a FABRICATED item (impossible tier, an affix count that doesn't match its own
   rarity, an affix value beyond what that tier could ever legitimately roll, an unknown
   affix stat) without needing full server-side item generation or per-item provenance
   tracking (that's real future work -- see the anti-cheat roadmap discussion at the top
   of this file). This does NOT prove a given item was actually EARNED through combat/
   crafting, only that its numbers are within what the game's own generation rules could
   ever produce -- closing the "list/stash an impossible perfect-rolled item" vector
   specifically, which is the concrete duplication/manipulation risk Gwen flagged for
   the Auction House and Storage Vault. */
const ITEM_TIER_MAX = 5;
const ITEM_AFFIX_POOL = [
  "damage", "hp", "crit", "armor", "regen", "stamina_regen", "gold_find", "xp_find", "stamina_max",
  "strength", "dexterity", "vitality", "intelligence", "poison_resist", "magic_find",
  "block_chance", "flee_chance", "crit_multiplier", "attack_speed",
  // v0.22.4 (#1): 9 new affixes registered so every ITEM_SLOT_AFFIXES entry can reach >=8
  // legal stats (needed for an 8-affix Legendary to roll 8 distinct affixes per v0.22.3's
  // slot-legality rules). Each stat's gameplay EFFECT is deferred -- it rolls/validates like
  // any other affix but has no consumer yet (no mana pool, no spell-damage system, etc.).
  // Must stay byte-identical to index.html's Balance.AFFIX_POOL.
  "life_on_hit", "thorns", "max_health_pct", "trap_ward", "spell_cdr",
  "mana", "mana_regen", "attack_rating", "spell_power",
];
// v0.19.1 (#15): xp_find raised from a T1 max of 1 back up to 5 (5/10/15/20/25% across
// T1-T5) -- mirrors index.html's Balance.AFFIX_TIER1_MAX. Still well within
// ITEM_AFFIX_TIER1_MAX_CEILING's xp_find:10 below, so no ceiling change is needed.
// v0.20 (#11): new "block_chance" gear affix -- must stay in sync with index.html's
// Balance.AFFIX_TIER1_MAX (see that constant's comment). T1 max of 2 (2/4/6/8/10% across
// T1-T5) feeds into combatGetBlockChance() below via combatGearBonus(data,"block_chance").
// v0.20.4: "armor" changed from a PERCENTAGE (20/40/60/80/100% across T1-T5) to a FLAT
// number (2/4/6/8/10 across T1-T5) -- must stay in sync with index.html's
// Balance.AFFIX_TIER1_MAX (see that constant's v0.20.4 comment for the full mechanic
// change). See ITEM_AFFIX_TIER1_MAX_CEILING just below for why the OLD max (20) is
// preserved there instead of being lowered along with this one.
// v0.22 (batch2 #5): "regen" (now displayed as "Health Regeneration") had its Tier-1 max
// nerfed from 5 down to 1 (1/2/3/4/5 across T1-T5, was 5/10/15/20/25), and a NEW sibling
// affix "stamina_regen" ("Stamina Regeneration") was added at the same Tier-1 max of 1 --
// must stay in sync with index.html's Balance.AFFIX_TIER1_MAX. The internal key "regen" is
// NEVER renamed (that would orphan live gear/saves) -- only its Tier-1 max and display label
// changed. See ITEM_AFFIX_TIER1_MAX_CEILING just below for why regen's OLD max (5) is
// preserved there instead of being lowered along with this one.
// v0.24.1 (C6): "crit" (Critical Strike Chance) Tier-1 max nerfed 5 -> 3 (curve becomes
// 3/6/9/12/15% across T1-T5, was 5/10/15/20/25%) -- must stay in sync with index.html's
// Balance.AFFIX_TIER1_MAX. See ITEM_AFFIX_TIER1_MAX_CEILING just below for why crit's OLD max
// (5) is preserved there instead of being lowered along with this one.
const ITEM_AFFIX_TIER1_MAX = {
  damage: 5, hp: 15, crit: 3, armor: 2, regen: 1, stamina_regen: 1, gold_find: 10, xp_find: 5,
  stamina_max: 15, strength: 5, dexterity: 5, vitality: 5, intelligence: 5, poison_resist: 5,
  magic_find: 10, block_chance: 2, flee_chance: 2,
  // v0.21 (#8): new "crit_multiplier" gear affix -- stored/rolled as a plain integer (10-50
  // across T1-T5) through the same machinery as every other affix, then divided by 100 when
  // consumed (see combatGetCritMultiplierBonus() below) to land on Gwen's exact spec of
  // +0.10/0.20/0.30/0.40/0.50. Must stay in sync with index.html's Balance.AFFIX_TIER1_MAX.
  crit_multiplier: 10,
  // v0.21.1 (#11): new "attack_speed" gear affix -- a % increase to how many attacks/sec the
  // player can land (see combatGetAttackSpeed() below), stored as a plain integer 5-25 across
  // T1-T5 (5/10/15/20/25%) through the same machinery as every other affix. Must stay in
  // sync with index.html's Balance.AFFIX_TIER1_MAX.
  attack_speed: 5,
  // v0.22.4 (#1): 9 new affixes, effect deferred to a later version -- registered now purely
  // to fill out ITEM_SLOT_AFFIXES below so every slot reaches >=8 legal stats. Values roll
  // through the same 20-step distribution as every other affix. attack_rating's curve is a
  // placeholder to retune once a hit/accuracy system exists. Must stay in sync with
  // index.html's Balance.AFFIX_TIER1_MAX.
  life_on_hit: 2, thorns: 4, max_health_pct: 3, trap_ward: 10, spell_cdr: 3,
  mana: 15, mana_regen: 1, attack_rating: 25,
  // v0.23.0 (Part B8): "spell_power" (renamed from the inert placeholder "spell_damage" of
  // v0.22.4 (#1)) is now a real PERCENT stat feeding the B5 spell-effectiveness multiplier --
  // T1 max of 2 (2/4/6/8/10% across T1-T5), the same T1-max-of-2 percent pattern Flee Chance/
  // Block Chance already use. Must stay in sync with index.html's Balance.AFFIX_TIER1_MAX and
  // Balance.affixIsPercent (server.js has no equivalent generic percent-affix registry -- see
  // combatGetSpellPowerPct() below for why that's fine).
  spell_power: 2,
};
// index.html's RARITY_TABLE's `slots` field, keyed by the RARITY_TABLE `name` (internal
// name, not the player-facing RARITY_DISPLAY_NAMES wording -- items store the internal
// name, e.g. "Epic" is displayed to players as "Rare").
// v0.22.3 (#16): per-slot legal-affix map -- generation/reforge/validation must only ever
// accept a stat from the list for that item's slot (fixes nonsense like armor-on-weapon).
// Eyesight is handled separately (a fixed rare bonus, not part of this pool) and is gated to
// head/amulet only at its roll site. Must stay byte-identical to index.html's Balance.SLOT_AFFIXES.
// v0.22.4 (#2): expanded so every slot has >=8 legal stats (an 8-affix Legendary needs 8
// distinct legal affixes per slot, and several slots previously came up short) -- the new
// stats from v0.22.4 (#1) exist specifically to fill this map out. Same enforcement sites as
// before; only the map grew.
const ITEM_SLOT_AFFIXES = {
  weapon: ["damage", "crit", "crit_multiplier", "attack_speed", "strength", "dexterity", "intelligence", "attack_rating", "spell_power", "life_on_hit"],
  head: ["armor", "hp", "xp_find", "magic_find", "vitality", "intelligence", "mana", "spell_power", "spell_cdr", "max_health_pct"],
  shoulders: ["armor", "hp", "block_chance", "strength", "poison_resist", "vitality", "thorns", "max_health_pct"],
  armor: ["armor", "hp", "block_chance", "poison_resist", "regen", "strength", "vitality", "thorns", "max_health_pct", "mana"],
  pants: ["armor", "hp", "stamina_regen", "stamina_max", "flee_chance", "vitality", "max_health_pct", "trap_ward"],
  gloves: ["crit", "crit_multiplier", "attack_speed", "armor", "hp", "block_chance", "gold_find", "strength", "dexterity", "attack_rating", "life_on_hit"],
  boots: ["armor", "hp", "poison_resist", "stamina_regen", "stamina_max", "flee_chance", "dexterity", "trap_ward"],
  belt: ["armor", "hp", "block_chance", "poison_resist", "regen", "stamina_regen", "gold_find", "strength", "dexterity", "vitality", "thorns", "max_health_pct", "mana_regen", "trap_ward"],
  ring: ["crit", "crit_multiplier", "attack_speed", "hp", "regen", "gold_find", "xp_find", "magic_find", "strength", "dexterity", "vitality", "intelligence", "mana", "mana_regen", "attack_rating", "spell_power", "life_on_hit", "spell_cdr"],
  amulet: ["crit", "crit_multiplier", "hp", "poison_resist", "regen", "gold_find", "xp_find", "magic_find", "vitality", "intelligence", "mana", "mana_regen", "spell_power", "life_on_hit", "spell_cdr", "max_health_pct"],
};
const ITEM_RARITY_SLOTS = { Common: 1, Uncommon: 2, Rare: 3, Epic: 5, Legendary: 8, Set: 3 };
// v0.22.6 (#11): "offhand" added -- must stay in sync with index.html's IF.GENERATION_SLOTS.
const ITEM_GENERATION_SLOTS = ["weapon", "head", "shoulders", "armor", "pants", "gloves", "boots", "ring", "amulet", "belt", "offhand"];
// v0.22.6 (#14): "offhand" legality is by base-item TYPE (Shield vs Orb) rather than by slot --
// this is a separate type-keyed map consulted instead of ITEM_SLOT_AFFIXES whenever an item's
// slot is "offhand" (see legalAffixesForItem() below). Must stay byte-identical to index.html's
// Balance.OFFHAND_SLOT_AFFIXES.
const ITEM_OFFHAND_SLOT_AFFIXES = {
  shield: ["armor", "hp", "block_chance", "poison_resist", "regen", "vitality", "max_health_pct", "thorns"],
  orb: ["spell_power", "mana", "mana_regen", "spell_cdr", "intelligence", "crit", "crit_multiplier", "magic_find", "hp"],
};
const ITEM_ELEMENT_IDS = ["fire", "wind", "earth", "water"];
const ITEM_WEAPON_DAMAGE_AFFIX_MULT = 1.40;
const ITEM_SALVAGE_MATERIALS_PER_SLOT = 3, ITEM_REROLL_MATERIAL_COST_MULT = 2;
// v0.20.1 (#17/#18): Reroll gains a gold cost on top of its existing material cost, and a
// new "Reforge" feature (changes ONE existing affix's STAT to a fresh random stat, unlike
// Reroll which keeps the same stats and only rerolls their values) is added alongside it.
// Both gold costs key off the same sellValue() figure the Merchant's "Sell" button already
// shows (mirrors index.html's Balance.SELL_GOLD_PER_SLOT/sellValue() exactly, so the
// Blacksmith's cost preview and this server-side charge always agree).
const ITEM_SELL_GOLD_PER_SLOT = 4;
const ITEM_REROLL_GOLD_COST_MULT = 3;
const ITEM_REFORGE_MATERIAL_COST_MULT = 6, ITEM_REFORGE_GOLD_COST_MULT = 12;
// v0.20.2: "Refine" -- final, priciest crafting step. Priced off Reforge's own costs: 1.4x
// Reforge's material cost, 4x Reforge's gold cost. Mirrors index.html's
// Balance.REFINE_MATERIAL_COST_MULT/REFINE_GOLD_COST_MULT exactly.
const ITEM_REFINE_MATERIAL_COST_MULT = 1.4, ITEM_REFINE_GOLD_COST_MULT = 4;

// Upper-bound-only override, used SOLELY by validateGearItem's "is this affix value even
// plausible" check below -- never by the actual reroll/generation math, which always uses
// the live ITEM_AFFIX_TIER1_MAX above. A stat's live-tuning max can be REDUCED later as a
// balance nerf (index.html's own comment on xp_find: its Tier-1 max was 10, hotfixed down
// to 1 in v0.17.2 (#4) because it was far too strong) -- an item legitimately rolled back
// when the max was still 10 must not start failing validation retroactively just because
// today's max is tighter. This map is deliberately the LOOSEST ceiling any stat has ever
// had; update it here (never the live map above) if a future balance pass tightens another
// stat's max.
// v0.20.4: armor's ceiling is pinned at 20 (its old, pre-rework T1 max) rather than
// inheriting the new tightened live max of 2 -- same reasoning as xp_find's override just
// above. A helmet/chestpiece/etc. rolled back when armor was still a 20/40/60/80/100%
// affix must keep passing validateGearItem()'s bounds check (so it can still be reforged,
// listed on the Auction House, deposited to the Vault, etc.) even though FRESH rolls now
// cap at 2/4/6/8/10 -- see legacyGearScan() below for how these old-spec items still get
// surfaced to an admin for optional manual rescale despite remaining valid here.
// v0.22 (batch2 #5): regen's ceiling is pinned at 5 (its old, pre-nerf T1 max) rather than
// inheriting the new tightened live max of 1 -- same reasoning as xp_find/armor's overrides.
// A regen-5 item rolled before this patch must keep passing validateGearItem()'s bounds
// check (reforge/list/deposit) even though fresh rolls now cap at 1/2/3/4/5 -- see
// legacyGearScan() below for how these old-spec items still get surfaced for optional rescale.
const ITEM_AFFIX_TIER1_MAX_CEILING = Object.assign({}, ITEM_AFFIX_TIER1_MAX, { xp_find: 10, armor: 20, regen: 5, crit: 5 });

function itemAffixMaxForTier(stat, tier) {
  const base = ITEM_AFFIX_TIER1_MAX[stat];
  return base != null ? base * tier : 0;
}
function itemAffixCeilingForTier(stat, tier) {
  const base = ITEM_AFFIX_TIER1_MAX_CEILING[stat];
  return base != null ? base * tier : 0;
}
// v0.22 (batch2 #6): every affix now rolls on 20 discrete steps of (max/20) instead of an
// integer [1..max] -- a perfect roll is k=20 (exactly the tier max), a 1-in-20 chance. Mirrors
// index.html's Balance.rollAffixValue() exactly. The weapon-damage +40% bonus is applied AFTER
// this roll (post-multiply), never folded into `max` before rolling -- all four call sites
// below (gear generation, reroll, reforge, refine) and the client's IF.generate()/isAffixMaxRoll()
// diamond-marker check all use this same post-multiply order, so a "perfect" weapon-damage roll
// always resolves to the identical value everywhere and never disagrees on the 💎 marker.
function round2(v) { return Math.round(v * 100) / 100; }
function rollAffixValue(stat, tier) {
  const max = itemAffixMaxForTier(stat, tier);
  const step = max / 20;
  const k = Math.floor(Math.random() * 20) + 1;
  return round2(k * step);
}
function itemSalvageValue(tier, slots) { return Math.round(Math.pow(tier, 1.5) * slots * ITEM_SALVAGE_MATERIALS_PER_SLOT); }
function itemRerollCost(tier, slots) { return itemSalvageValue(tier, slots) * ITEM_REROLL_MATERIAL_COST_MULT; }
// v0.20.1 (#17/#18): mirrors index.html's Balance.sellValue(tier,slots) exactly.
function itemSellValue(tier, slots) { return Math.round(Math.pow(tier, 1.5) * slots * ITEM_SELL_GOLD_PER_SLOT); }
function itemRerollGoldCost(tier, slots) { return itemSellValue(tier, slots) * ITEM_REROLL_GOLD_COST_MULT; }
function itemReforgeMaterialCost(tier, slots) { return itemRerollCost(tier, slots) * ITEM_REFORGE_MATERIAL_COST_MULT; }
function itemReforgeGoldCost(tier, slots) { return itemSellValue(tier, slots) * ITEM_REFORGE_GOLD_COST_MULT; }
// v0.20.2: mirrors index.html's Balance.refineCost()/refineGoldCost() exactly.
function itemRefineMaterialCost(tier, slots) { return Math.round(itemReforgeMaterialCost(tier, slots) * ITEM_REFINE_MATERIAL_COST_MULT); }
function itemRefineGoldCost(tier, slots) { return itemReforgeGoldCost(tier, slots) * ITEM_REFINE_GOLD_COST_MULT; }

// v0.22.6 (#14): shared helper -- "offhand" legality is keyed by base-item TYPE (Shield vs
// Orb) via COMBAT_OFFHAND_TYPE_BY_BASE_NAME (defined further down alongside COMBAT_BASE_NAMES),
// not by slot like every other ITEM_GENERATION_SLOTS entry. Every call site that used to read
// ITEM_SLOT_AFFIXES[item.slot] directly (validateGearItem, the Reforge endpoint, Legacy Gear
// scan/rescale) now goes through this instead, so an offhand item gets the right pool
// everywhere. Mirrors index.html's equivalent special-case in IF.generate().
function legalAffixesForSlotAndBaseName(slot, baseName) {
  if (slot === "offhand") {
    const t = COMBAT_OFFHAND_TYPE_BY_BASE_NAME[baseName] || "";
    return ITEM_OFFHAND_SLOT_AFFIXES[t] || [];
  }
  return ITEM_SLOT_AFFIXES[slot] || [];
}
function legalAffixesForItem(item) { return legalAffixesForSlotAndBaseName(item.slot, item.base_name); }

// Returns null if `item` is a plausible gear item the game's own generation rules could
// have produced, or a short player-facing string describing what's wrong with it.
function validateGearItem(item) {
  if (!item || typeof item !== "object") return "Invalid item.";
  const tier = item.tier;
  if (!Number.isInteger(tier) || tier < 1 || tier > ITEM_TIER_MAX) return "Invalid item tier.";
  if (!ITEM_GENERATION_SLOTS.includes(item.slot)) return "Invalid item slot.";
  const slotCount = ITEM_RARITY_SLOTS[item.rarity];
  if (slotCount == null) return "Invalid item rarity.";
  if (item.element && !ITEM_ELEMENT_IDS.includes(item.element)) return "Invalid item element.";
  if (!Array.isArray(item.affixes)) return "Invalid item affixes.";
  let statCount = 0, eyesightCount = 0;
  for (const a of item.affixes) {
    if (!a || typeof a.stat !== "string" || !Number.isFinite(a.value)) return "Invalid affix entry.";
    if (a.stat === "eyesight") {
      eyesightCount++;
      // EYESIGHT_AFFIX_CHANCE grants a fixed +2 -- it never scales with tier like a normal
      // AFFIX_POOL stat does, so any other value is definitely fabricated.
      if (a.value !== 2) return "Invalid eyesight affix value.";
      continue;
    }
    if (!ITEM_AFFIX_POOL.includes(a.stat)) return "Unknown affix stat.";
    // v0.22.3 (#16): reject any affix illegal for its own slot (e.g. armor rolled onto a
    // weapon) -- closes the client-crafted-illegal-combo vector the same way the existing
    // tier/value bounds checks do.
    // v0.22.6 (#14): routed through legalAffixesForItem() so "offhand" is checked against the
    // Shield/Orb pool for this item's base_name instead of a flat per-slot list.
    const legalForSlot = legalAffixesForItem(item);
    if (!legalForSlot.includes(a.stat)) return `Affix "${a.stat}" isn't legal on this item's slot.`;
    let max = itemAffixCeilingForTier(a.stat, tier);
    if (item.slot === "weapon" && a.stat === "damage") max = round2(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    // v0.22 (batch2 #6): the lower bound used to be a flat `< 1` back when every affix rolled
    // an integer in [1, max] -- but the 20-step grid's smallest possible roll is max/20, which
    // for a low-tier low-max stat (e.g. Tier 1 Health Regen, max=1) is 0.05, well under 1. Using
    // `<= 0` instead still rejects anything actually invalid (zero/negative) without rejecting
    // legitimate low rolls on the new fractional grid.
    if (a.value <= 0 || a.value > max) return `Affix "${a.stat}" value is beyond what a Tier ${tier} item could roll.`;
    statCount++;
  }
  if (eyesightCount > 1) return "An item can only carry one Eyesight affix.";
  // IF.generate() always rolls exactly `slotCount` normal stat affixes (min(slotCount,
  // pool.length), and pool.length=14 is never the limiting factor) -- so a legitimate
  // item's normal-affix count always matches its own rarity's slot count exactly.
  if (statCount !== slotCount) return "Affix count doesn't match this item's rarity.";
  return null;
}

// v0.18.4: the Blacksmith's Reroll button used to run entirely client-side (PS.rerollGear()
// deducted materials and called IF.reroll() locally, then just autosaved whatever the
// client claims the outcome was) -- a modified client could report back a "reroll" that
// actually just directly wrote max-roll affixes onto the item with zero material cost.
// This endpoint moves the whole decision server-side: it validates the request, re-checks
// the item is a plausible one to begin with (see validateGearItem above), verifies+deducts
// the material cost against the server's own stored `materials` figure, and rolls every new
// affix value itself, mirroring IF.reroll()'s exact algorithm (same stats survive, only the
// numbers change). The Blacksmith UI/flow is unchanged -- same button, same cost preview,
// same instant result -- only the RNG and the outcome now come from the server.
app.post("/api/blacksmith/reroll", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const instanceId = req.body?.instance_id;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  if (typeof instanceId !== "string" || !instanceId) {
    return res.status(400).json({ error: "Invalid instance_id." });
  }

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "Corrupt character save." });
  }

  // Rerolling is only ever offered from the Blacksmith's backpack list (equipped gear
  // must be unequipped first) -- mirrors PS.findGearIndex()'s own search scope, which
  // never looks at PS.equipped.
  const gearList = Array.isArray(data.gear_instances) ? data.gear_instances : [];
  const idx = gearList.findIndex((g) => g && g.instance_id === instanceId);
  if (idx === -1) return res.status(404).json({ error: "That item isn't in your backpack." });
  const inst = gearList[idx];

  // Reject outright if the item itself is already implausible -- a modified client
  // shouldn't be able to launder a fabricated item into legitimacy just by asking the
  // server to reroll it (the reroll below only touches affix VALUES for the stats
  // already present, so a fabricated tier/rarity/slot/affix-set would otherwise sail
  // through completely untouched).
  const itemError = validateGearItem(inst);
  if (itemError) return res.status(400).json({ error: `This item can't be rerolled: ${itemError}` });

  // IF.slotCount(inst) counts EVERY affix including a bonus Eyesight roll -- matches the
  // cost the client already previews on the Reroll button.
  const totalSlotCount = (inst.affixes || []).length;
  const cost = itemRerollCost(inst.tier, totalSlotCount);
  const materials = data.materials || 0;
  if (materials < cost) return res.status(400).json({ error: `Not enough materials -- this reroll costs ${cost}.` });
  // v0.20.1 (#18): Reroll now ALSO costs gold (3x the item's sell value), on top of the
  // pre-existing material cost above -- gold is account-bound (see getAccountGold/
  // creditAccountGold), same charge pattern the Auction House listing fee already uses.
  const goldCost = itemRerollGoldCost(inst.tier, totalSlotCount);
  if (getAccountGold(req.account.id) < goldCost) {
    return res.status(400).json({ error: `Not enough gold -- this reroll costs ${goldCost}g.` });
  }

  // The actual roll: the SERVER picks a fresh value for every existing affix (same stats,
  // same count -- only the numbers change), exactly mirroring IF.reroll()'s algorithm.
  // This is the specific fix for the "blacksmith's reroll stat outcome mustn't be
  // manipulable to a perfect roll" concern -- a modified client can still ask for a
  // reroll, but it can no longer dictate what comes back.
  const newAffixes = inst.affixes.map((a) => {
    if (a.stat === "eyesight") return { stat: a.stat, value: a.value };
    let value = rollAffixValue(a.stat, inst.tier);
    if (inst.slot === "weapon" && a.stat === "damage") value = round2(value * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    return { stat: a.stat, value };
  });
  const rerolled = Object.assign({}, inst, { affixes: newAffixes });
  gearList[idx] = rerolled;
  data.gear_instances = gearList;
  data.materials = materials - cost;
  data._save_seq = (data._save_seq || 0) + 1;
  // Gold is account-bound, not part of this character's own `data` blob -- deduct it via
  // the shared accounts.gold column, same as every other gold-touching route.
  const accountGoldAfter = creditAccountGold(req.account.id, -goldCost);

  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowIso(),
    req.account.id,
    slot
  );

  res.json({ ok: true, item: rerolled, materials: data.materials, account_gold: accountGoldAfter, _save_seq: data._save_seq });
});

// v0.20.1 (#17): "Reforge" -- unlike Reroll (keeps every existing affix's STAT, only
// rerolls their numeric values), Reforge changes exactly ONE existing normal affix's STAT
// to a fresh, different stat (with a freshly rolled value), leaving every other affix on
// the item completely untouched. Costs considerably more than Reroll (6x Reroll's material
// cost, 12x the item's sell value in gold) since it can fix a bad-stat roll entirely, not
// just a bad-VALUE roll.
// v0.20.1 CORRECTION: the WHICH-affix decision used to be random too -- Gwen's actual spec
// is that the PLAYER picks which existing affix gets replaced (via the new affix_index
// param, an index into the item's own affixes array), while the server still owns what the
// new stat/value becomes. A modified client can name a target slot but can never dictate
// or predict the reforge's actual outcome.
app.post("/api/blacksmith/reforge", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const instanceId = req.body?.instance_id;
  const affixIndex = Number(req.body?.affix_index);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  if (typeof instanceId !== "string" || !instanceId) {
    return res.status(400).json({ error: "Invalid instance_id." });
  }
  if (!Number.isInteger(affixIndex) || affixIndex < 0) {
    return res.status(400).json({ error: "Invalid affix_index." });
  }

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "Corrupt character save." });
  }

  const gearList = Array.isArray(data.gear_instances) ? data.gear_instances : [];
  const idx = gearList.findIndex((g) => g && g.instance_id === instanceId);
  if (idx === -1) return res.status(404).json({ error: "That item isn't in your backpack." });
  const inst = gearList[idx];

  const itemError = validateGearItem(inst);
  if (itemError) return res.status(400).json({ error: `This item can't be reforged: ${itemError}` });

  const totalSlotCount = (inst.affixes || []).length;
  const materialCost = itemReforgeMaterialCost(inst.tier, totalSlotCount);
  const goldCost = itemReforgeGoldCost(inst.tier, totalSlotCount);
  const materials = data.materials || 0;
  if (materials < materialCost) return res.status(400).json({ error: `Not enough materials -- this reforge costs ${materialCost}.` });
  if (getAccountGold(req.account.id) < goldCost) {
    return res.status(400).json({ error: `Not enough gold -- this reforge costs ${goldCost}g.` });
  }

  // v0.20.1 CORRECTION: the target affix is now the PLAYER's choice (affixIndex), not a
  // random pick -- but it's still validated server-side exactly like every other
  // player-supplied index in this codebase: must be a real position on THIS item, and must
  // not be the rare fixed-value "eyesight" bonus roll (there's nothing to "reforge" about a
  // flat +2 that never varies, same exclusion the old random-pick logic already enforced).
  if (affixIndex >= inst.affixes.length) {
    return res.status(400).json({ error: "That stat slot doesn't exist on this item." });
  }
  if (inst.affixes[affixIndex].stat === "eyesight") {
    return res.status(400).json({ error: "That bonus can't be reforged." });
  }
  const targetPos = affixIndex;

  // The new stat must be one the item doesn't already carry (matches IF.generate()'s own
  // no-duplicate-stat rule -- a legitimate item never has the same stat twice) -- pick
  // uniformly at random from whatever's left in the pool.
  // v0.22.3 (#16): the new stat must ALSO be legal for this item's own slot -- reforge can
  // no longer hand a weapon an armor affix. Pull from the item's legal pool, not the full pool.
  // v0.22.6 (#14): routed through legalAffixesForItem() so an offhand item reforges within its
  // own Shield/Orb pool instead of an undifferentiated "offhand" list.
  const existingStats = new Set(inst.affixes.map((a) => a.stat));
  const legalForSlot = legalAffixesForItem(inst).length ? legalAffixesForItem(inst) : ITEM_AFFIX_POOL;
  const candidatePool = legalForSlot.filter((s) => !existingStats.has(s));
  // Every slot's legal-affix list has more entries than any item's max affix count
  // (Legendary=8 vs. the smallest slot list, shoulders, at 4 -- so a slot with only 4 legal
  // stats could theoretically run dry on a fully-rolled Legendary; this fallback keeps the
  // existing stat rather than erroring in that edge case).
  const newStat = candidatePool.length > 0
    ? candidatePool[Math.floor(Math.random() * candidatePool.length)]
    : inst.affixes[targetPos].stat;

  let newValue = rollAffixValue(newStat, inst.tier);
  if (inst.slot === "weapon" && newStat === "damage") newValue = round2(newValue * ITEM_WEAPON_DAMAGE_AFFIX_MULT);

  const newAffixes = inst.affixes.slice();
  newAffixes[targetPos] = { stat: newStat, value: newValue };
  const reforged = Object.assign({}, inst, { affixes: newAffixes });
  gearList[idx] = reforged;
  data.gear_instances = gearList;
  data.materials = materials - materialCost;
  data._save_seq = (data._save_seq || 0) + 1;
  const accountGoldAfter = creditAccountGold(req.account.id, -goldCost);

  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowIso(),
    req.account.id,
    slot
  );

  res.json({ ok: true, item: reforged, materials: data.materials, account_gold: accountGoldAfter, _save_seq: data._save_seq });
});

// v0.20.2: "Refine" -- the final, priciest crafting step. Nearly identical validation to
// Reforge just above (player-supplied affix_index, must exist on the item, can't target the
// fixed "eyesight" bonus), but the outcome is different: the targeted affix keeps its STAT,
// only its VALUE gets rerolled (same per-affix reroll math the Reroll endpoint above uses
// for every affix, just applied to one chosen affix here instead of all of them). Priced at
// itemRefineMaterialCost/itemRefineGoldCost (1.4x/4x Reforge's own costs).
app.post("/api/blacksmith/refine", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const instanceId = req.body?.instance_id;
  const affixIndex = Number(req.body?.affix_index);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  if (typeof instanceId !== "string" || !instanceId) {
    return res.status(400).json({ error: "Invalid instance_id." });
  }
  if (!Number.isInteger(affixIndex) || affixIndex < 0) {
    return res.status(400).json({ error: "Invalid affix_index." });
  }

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "Corrupt character save." });
  }

  const gearList = Array.isArray(data.gear_instances) ? data.gear_instances : [];
  const idx = gearList.findIndex((g) => g && g.instance_id === instanceId);
  if (idx === -1) return res.status(404).json({ error: "That item isn't in your backpack." });
  const inst = gearList[idx];

  const itemError = validateGearItem(inst);
  if (itemError) return res.status(400).json({ error: `This item can't be refined: ${itemError}` });

  if (affixIndex >= inst.affixes.length) {
    return res.status(400).json({ error: "That stat slot doesn't exist on this item." });
  }
  if (inst.affixes[affixIndex].stat === "eyesight") {
    return res.status(400).json({ error: "That bonus can't be refined." });
  }

  const totalSlotCount = (inst.affixes || []).length;
  const materialCost = itemRefineMaterialCost(inst.tier, totalSlotCount);
  const goldCost = itemRefineGoldCost(inst.tier, totalSlotCount);
  const materials = data.materials || 0;
  if (materials < materialCost) return res.status(400).json({ error: `Not enough materials -- this refine costs ${materialCost}.` });
  if (getAccountGold(req.account.id) < goldCost) {
    return res.status(400).json({ error: `Not enough gold -- this refine costs ${goldCost}g.` });
  }

  // The stat itself never changes here -- only reroll a fresh value for it, exactly like the
  // Reroll endpoint's per-affix math above, just targeted at this one affix instead of all of
  // them.
  const targetStat = inst.affixes[affixIndex].stat;
  let newValue = rollAffixValue(targetStat, inst.tier);
  if (inst.slot === "weapon" && targetStat === "damage") newValue = round2(newValue * ITEM_WEAPON_DAMAGE_AFFIX_MULT);

  const newAffixes = inst.affixes.slice();
  newAffixes[affixIndex] = { stat: targetStat, value: newValue };
  const refined = Object.assign({}, inst, { affixes: newAffixes });
  gearList[idx] = refined;
  data.gear_instances = gearList;
  data.materials = materials - materialCost;
  data._save_seq = (data._save_seq || 0) + 1;
  const accountGoldAfter = creditAccountGold(req.account.id, -goldCost);

  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowIso(),
    req.account.id,
    slot
  );

  res.json({ ok: true, item: refined, materials: data.materials, account_gold: accountGoldAfter, _save_seq: data._save_seq });
});

/* ================= Server-authoritative Combat (v0.19) =================
   Implements server_authoritative_combat_design.md's Phase 1: every roll that decides a
   fight's outcome (monster block/damage, player crit/damage/block, the monster's counter,
   flee, loot, XP, gold, leveling, weapon-skill proficiency) now happens HERE, against the
   server's own stored copy of the character and a server-held combat_sessions row for the
   monster's HP -- not inside screenCombat()'s old Math.random()-driven playerTurn()/
   attemptFlee(). A modified client can still ask for a ruling on each Attack/Flee/potion
   click, but it can no longer fabricate the ruling itself.

   SCOPE NOTE ON DEATH (deliberate, not an oversight): when a round drops the player's HP
   below 1, this code clamps current_hp to 0, persists that, marks the session `lost`, and
   returns `fatal:true` -- but does NOT itself run the softcore gold/XP/item-loss penalty or
   the hardcore permadeath/graveyard flow. Those stay exactly where they already are
   (PS.handleDeath/applyDeathPenalty in index.html, invoked by the client the instant it sees
   fatal:true), for two reasons: (1) porting the FULL death economy -- item-loss random
   selection across equipped+backpack, hardcore character deletion, graveyard insertion --
   is a large, first-time port of delicate, data-destroying logic in its own right, and the
   one thing this phase must never do is rush that under the same change that's meant to
   IMPROVE data safety; (2) the exploitable surface here is winning (fabricating damage/loot/
   XP/gold), not losing -- a manipulated client faking a smaller death PENALTY than deserved
   is the same pre-existing "client-trusted PUT /api/characters/:slot" trust boundary the
   design doc itself explicitly scoped to a later "validate on save" phase, not a new
   regression introduced here. What's actually closed by this file is the ability to fabricate
   a fight's outcome to AVOID ever reaching 0 HP, or to invent kills/loot/XP/gold that never
   happened -- that's the concrete request ("per attack round server validation").

   SCOPE NOTE ON MAZE LEGITIMACY: `is_guardian`/`is_roamer` flags and the monster id/area
   level are still client-asserted at /api/combat/start, same explicitly-flagged gap the
   design doc calls out ("no independent knowledge of maze layout yet"). What IS validated:
   the area level must be one this character has actually reached (max_maze_depth_reached),
   and the monster id must be a real monster whose tier is actually available at that area
   level (mirrors DL.pickRandomMonsterForArea's own gating) -- closing the "claim area level
   1 unlocks an Epic-tier fight" version of this gap, even though "was there really a
   guardian on that tile" isn't provable without server-side maze state (tracked separately,
   see BACKLOG task #483/#484).

   v0.23.2 (Night delve): is_night joins is_guardian/is_roamer/area_level as client-asserted
   at POST /api/combat/start, under this exact same scope note -- there's no server-tracked
   maze/delve session to validate it against yet (that's the same BACKLOG #483/#484 gap), so
   it's trusted the same way the maze content flags already are. The stakes it unlocks
   (tighter sight, more encounters) are purely client-side anyway; the one thing THIS file is
   responsible for keeping honest is the loot/gold bonus below, which only reads is_night off
   the combat_sessions row set at fight-start, not a value re-sent per-request.
*/

db.exec(`
  CREATE TABLE IF NOT EXISTS combat_sessions (
    id TEXT PRIMARY KEY,
    account_id INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    monster_id TEXT NOT NULL,
    name TEXT NOT NULL,
    area_level INTEGER NOT NULL,
    max_hp REAL NOT NULL,
    hp REAL NOT NULL,
    dmg_min REAL NOT NULL,
    dmg_max REAL NOT NULL,
    xp REAL NOT NULL,
    gold_min INTEGER NOT NULL,
    gold_max INTEGER NOT NULL,
    loot_table TEXT NOT NULL,
    is_guardian INTEGER NOT NULL DEFAULT 0,
    is_roamer INTEGER NOT NULL DEFAULT 0,
    is_night INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
`);

// v0.21.1 (#9/#10/#12): attack-speed system -- each combat session freezes the player's and
// monster's own attacks/sec at the moment the fight starts (player_attack_speed from gear,
// monster_attack_speed from area level), and last_monster_hit_at (epoch ms) is the server
// clock combatCatchUpMonsterHits() advances every request so the monster keeps landing hits
// over real elapsed time, not just once per player action. DEFAULT 0 on last_monster_hit_at
// is harmless -- every session that reaches this column already has a real value written at
// creation time (POST /api/combat/start), and no code reads this column for a session created
// before this migration (those sessions are long since resolved/abandoned, since a
// combat_sessions row only ever lives for one active fight). Kept here, right after this
// table's own CREATE TABLE, rather than in the big migration loop up top -- that loop runs
// before combat_sessions exists yet on a fresh database, which would make every ALTER TABLE
// here silently fail against a nonexistent table.
for (const stmt of [
  "ALTER TABLE combat_sessions ADD COLUMN player_attack_speed REAL NOT NULL DEFAULT 1.0",
  "ALTER TABLE combat_sessions ADD COLUMN monster_attack_speed REAL NOT NULL DEFAULT 1.0",
  "ALTER TABLE combat_sessions ADD COLUMN last_monster_hit_at INTEGER NOT NULL DEFAULT 0",
  // v0.23.0 (Part B7): per-session spellcasting state -- spell_cooldowns is a JSON map of
  // {spellId: readyAtEpochMs}, spell_dots is a JSON array of queued Fireflies-style
  // damage-over-time entries (see combatSettleSpellDots()), rooted_until/flee_override_until
  // are Entangle's root/100%-flee-chance expiry timestamps (0 = inactive).
  "ALTER TABLE combat_sessions ADD COLUMN spell_cooldowns TEXT NOT NULL DEFAULT '{}'",
  "ALTER TABLE combat_sessions ADD COLUMN spell_dots TEXT NOT NULL DEFAULT '[]'",
  "ALTER TABLE combat_sessions ADD COLUMN rooted_until INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE combat_sessions ADD COLUMN flee_override_until INTEGER NOT NULL DEFAULT 0",
  // v0.23.2 (Night delve): same belt-and-suspenders as the columns above -- CREATE TABLE IF
  // NOT EXISTS above already includes is_night for a fresh database, this covers one that
  // already existed before this change shipped.
  "ALTER TABLE combat_sessions ADD COLUMN is_night INTEGER NOT NULL DEFAULT 0",
  // v0.24.1 (B5 "The Pacifist" / C2): tracks, for THIS fight only, whether the player has
  // landed any direct attack swing or spell-damage tick against the monster. Thorns reflect
  // damage (combatResolveMonsterTurn) never sets this. Monotonic within a fight (set to 1,
  // never reset back to 0) -- see combatFinalizeMonsterKill()'s Pacifist check below.
  "ALTER TABLE combat_sessions ADD COLUMN player_dealt_direct_damage INTEGER NOT NULL DEFAULT 0",
  // v0.24.1 (C3): elapsed-time clock for HP/Stamina/Mana regen ticking during combat --
  // see combatTickRegenElapsed()'s own comment for why this needs to be wall-clock-based
  // (like last_monster_hit_at) rather than a flat "once per request" tick.
  "ALTER TABLE combat_sessions ADD COLUMN last_regen_tick_at INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE combat_sessions ADD COLUMN player_poison_dots TEXT NOT NULL DEFAULT '[]'",
  "ALTER TABLE combat_sessions ADD COLUMN sting_lockout_until INTEGER NOT NULL DEFAULT 0",
]) {
  try { db.exec(stmt); } catch (e) { /* column already exists, fine */ }
}

// Full combat-relevant subset of index.html's CLASSES table (adds chain/damage/crit/armor/
// regen on top of what TRIAL_CLASSES above already carries -- kept as its own object rather
// than extending TRIAL_CLASSES so the already-shipped, already-tested Trial endpoint can
// never be affected by anything combat-specific).
// v0.20.4: each class used to carry its own innate "armor" field here too (a flat 0-0.25
// PERCENTAGE damage reduction) -- retired, see CLASSES' own matching v0.20.4 comment in
// index.html for the full rationale. combatGetArmor() below now derives ALL Armor from
// gear (flat) plus total Strength * CB.STR_TO_ARMOR_RATIO, for every class.
// v0.23.0 (Part B1): base_mana per class, keyed the same way every other per-class base
// stat here is (base_hp, hp_per_level, etc.) --5 tiers by chain, per Gwen's exact spec:
// Wizard chain 160 (its whole identity is spellcasting), Windrider chain 100 (a hybrid
// fighter/caster), Thornguard chain 40 (barely casts at all), with the two off-hand-Orb
// hybrid healers (Druid, windrider chain; Emberpriest, thornguard chain -- see the offhand
// Orb class restrictions above) bumped to 160 to match their actual spellcasting role
// despite their chain's base. Must stay in sync with CLASSES[*].base_mana in index.html.
const COMBAT_CLASSES = {
  wizard:      { tier: 1, chain: "wizard",      base_hp: 60,  hp_per_level: 8,  base_damage_min: 8,  base_damage_max: 14, damage_per_level: 1.4, base_crit: 0.05, regen: 0.0, base_mana: 160 },
  thornguard:  { tier: 1, chain: "thornguard",  base_hp: 100, hp_per_level: 14, base_damage_min: 6,  base_damage_max: 10, damage_per_level: 1.1, base_crit: 0.05, regen: 0.0, base_mana: 40 },
  windrider:   { tier: 1, chain: "windrider",   base_hp: 75,  hp_per_level: 10, base_damage_min: 7,  base_damage_max: 12, damage_per_level: 1.2, base_crit: 0.20, regen: 0.0, base_mana: 100 },
  sorcerer:    { tier: 2, chain: "wizard",      base_hp: 80,  hp_per_level: 10, base_damage_min: 12, base_damage_max: 20, damage_per_level: 1.9, base_crit: 0.08, regen: 0.0, base_mana: 160 },
  stonewarden: { tier: 2, chain: "thornguard",  base_hp: 140, hp_per_level: 18, base_damage_min: 10, base_damage_max: 16, damage_per_level: 1.6, base_crit: 0.10, regen: 0.0, base_mana: 40 },
  galestrider: { tier: 2, chain: "windrider",   base_hp: 100, hp_per_level: 13, base_damage_min: 11, base_damage_max: 18, damage_per_level: 1.7, base_crit: 0.25, regen: 0.0, base_mana: 100 },
  warlock:     { tier: 3, chain: "wizard",      base_hp: 105, hp_per_level: 13, base_damage_min: 16, base_damage_max: 26, damage_per_level: 2.4, base_crit: 0.10, regen: 1.0, base_mana: 160 },
  treesinger:  { tier: 3, chain: "thornguard",  base_hp: 160, hp_per_level: 20, base_damage_min: 14, base_damage_max: 22, damage_per_level: 2.0, base_crit: 0.15, regen: 2.5, base_mana: 40 },
  shadowbloom: { tier: 3, chain: "windrider",   base_hp: 125, hp_per_level: 16, base_damage_min: 18, base_damage_max: 28, damage_per_level: 2.4, base_crit: 0.35, regen: 0.0, base_mana: 100 },
  necromancer: { tier: 4, chain: "wizard",      base_hp: 135, hp_per_level: 17, base_damage_min: 22, base_damage_max: 34, damage_per_level: 3.0, base_crit: 0.12, regen: 1.5, base_mana: 160 },
  rootbinder:  { tier: 4, chain: "thornguard",  base_hp: 210, hp_per_level: 26, base_damage_min: 18, base_damage_max: 28, damage_per_level: 2.6, base_crit: 0.12, regen: 3.5, base_mana: 40 },
  druid:       { tier: 4, chain: "windrider",   base_hp: 160, hp_per_level: 20, base_damage_min: 24, base_damage_max: 36, damage_per_level: 3.1, base_crit: 0.20, regen: 2.0, base_mana: 160 },
  emberpriest: { tier: 5, chain: "thornguard",  base_hp: 190, hp_per_level: 24, base_damage_min: 16, base_damage_max: 24, damage_per_level: 2.3, base_crit: 0.10, regen: 6.0, base_mana: 160 },
  archmage:    { tier: 5, chain: "wizard",      base_hp: 165, hp_per_level: 21, base_damage_min: 28, base_damage_max: 42, damage_per_level: 3.6, base_crit: 0.15, regen: 2.0, base_mana: 160 },
  galeshaper:  { tier: 5, chain: "windrider",   base_hp: 195, hp_per_level: 25, base_damage_min: 30, base_damage_max: 44, damage_per_level: 3.8, base_crit: 0.28, regen: 2.4, base_mana: 100 },
};

// v0.22.1 (Part A2): mirrors index.html's MONSTER_BAND_INFO exactly -- band key -> display
// label (unused server-side, kept for parity/reference), spawn-gate area level, and band size.
// combatMonsterAllowedForAreaLevel() below reads minAreaLevel; combatRollRarityForBand()'s
// MONSTER_BAND_LOOT_RARITY_WEIGHTS keys off the same 5 band names.
const COMBAT_MONSTER_BAND_INFO = {
  Common: { minAreaLevel: 1 }, Uncommon: { minAreaLevel: 8 }, Rare: { minAreaLevel: 13 },
  Epic: { minAreaLevel: 21 }, Legendary: { minAreaLevel: 34 },
};
// Verbatim mirror of index.html's MONSTERS array (id/tier/base stats/loot_table only -- name
// is used to build the combat log/session `name` field).
const COMBAT_MONSTERS = [
  { id: "sprout", name: "Sprout", tier: "Common", base_hp: 20, base_damage: 3, base_xp: 8, gold_min: 1, gold_max: 4, loot_table: "common" },
  { id: "windwisp", name: "Windwisp", tier: "Common", base_hp: 16, base_damage: 3.5, base_xp: 7, gold_min: 1, gold_max: 3, loot_table: "common" },
  { id: "mosshide", name: "Mosshide", tier: "Common", base_hp: 22, base_damage: 4.5, base_xp: 8, gold_min: 2, gold_max: 5, loot_table: "common" },
  { id: "pebblekin", name: "Pebblekin", tier: "Common", base_hp: 26, base_damage: 4.5, base_xp: 9, gold_min: 2, gold_max: 5, loot_table: "common" },
  { id: "cinderling", name: "Cinderling", tier: "Uncommon", base_hp: 24, base_damage: 5.5, base_xp: 9, gold_min: 2, gold_max: 6, loot_table: "uncommon" },
  { id: "boglurker", name: "Boglurker", tier: "Uncommon", base_hp: 42, base_damage: 7, base_xp: 12, gold_min: 3, gold_max: 8, loot_table: "uncommon" },
  { id: "thornling", name: "Thornling", tier: "Uncommon", base_hp: 32, base_damage: 8, base_xp: 12, gold_min: 3, gold_max: 9, loot_table: "uncommon" },
  { id: "bramble_knight", name: "Bramble Knight", tier: "Uncommon", base_hp: 38, base_damage: 8, base_xp: 12, gold_min: 4, gold_max: 10, loot_table: "uncommon" },
  { id: "emberkin", name: "Emberkin", tier: "Rare", base_hp: 28, base_damage: 9.5, base_xp: 13, gold_min: 4, gold_max: 9, loot_table: "rare" },
  { id: "fernstalker", name: "Fernstalker", tier: "Rare", base_hp: 30, base_damage: 10.5, base_xp: 13, gold_min: 4, gold_max: 10, loot_table: "rare" },
  { id: "ashwalker", name: "Ashwalker", tier: "Rare", base_hp: 60, base_damage: 11, base_xp: 18, gold_min: 8, gold_max: 18, loot_table: "rare" },
  { id: "bark_golem", name: "Bark Golem", tier: "Rare", base_hp: 90, base_damage: 11.5, base_xp: 20, gold_min: 10, gold_max: 20, loot_table: "rare" },
  { id: "stumplurker", name: "Stumplurker", tier: "Epic", base_hp: 58, base_damage: 12, base_xp: 17, gold_min: 7, gold_max: 15, loot_table: "epic" },
  { id: "vinewraith", name: "Vinewraith", tier: "Epic", base_hp: 65, base_damage: 13, base_xp: 18, gold_min: 8, gold_max: 16, loot_table: "epic" },
  { id: "stormroot", name: "Stormroot", tier: "Epic", base_hp: 50, base_damage: 14.5, base_xp: 19, gold_min: 8, gold_max: 17, loot_table: "epic" },
  { id: "ancient_treant", name: "Ancient Treant", tier: "Epic", base_hp: 95, base_damage: 20, base_xp: 30, gold_min: 16, gold_max: 28, loot_table: "epic" },
  { id: "stonewaker", name: "Stonewaker", tier: "Legendary", base_hp: 110, base_damage: 21.5, base_xp: 32, gold_min: 17, gold_max: 30, loot_table: "legendary" },
  { id: "wildfire", name: "Wildfire", tier: "Legendary", base_hp: 100, base_damage: 23, base_xp: 34, gold_min: 18, gold_max: 32, loot_table: "legendary" },
  { id: "tempest_elm", name: "Tempest Elm", tier: "Legendary", base_hp: 105, base_damage: 24, base_xp: 36, gold_min: 19, gold_max: 34, loot_table: "legendary" },
];

// Verbatim mirror of index.html's LOOT_TABLES.
// v0.22.6 (#26): removed every "dry_branch" (Dry Log) entry -- v0.22.3 (#6b) deprecated Dry
// Logs client-side, but this server-side mirror was never updated to match, so kills were
// still handing them out from the server's own loot roll despite the client no longer
// generating them anywhere. Confirmed via audit that no other server loot source (chest,
// stronghold chest, merchant stock) references dry_branch either.
const COMBAT_LOOT_TABLES = {
  // v0.29 (credit: Gwen): mana potions are in the loot tables now. They were absent from every
  // band, so a caster could only ever buy mana while a melee character was handed health for
  // free -- and mana is the most expensive potion line in the shop (14g to 1,134g), which made
  // that gap sharper the further a caster progressed.
  //
  // The existing consumable weight is SPLIT between health and mana rather than mana being added
  // on top, so the overall rate at which a kill yields a potion is unchanged and only WHICH
  // potion it is has changed. That does halve health-potion drops; adding mana on top instead
  // would have made potions meaningfully more common overall, which is a larger economy change
  // than the request called for. Each band grants the same TIER it already did, so a Legendary
  // kill yields a Supreme of whichever kind it rolls.
  common: [{ type: "nothing", weight: 155 }, { type: "gear", weight: 50 }, { type: "consumable", weight: 33, item_id: "minor_health_potion" }, { type: "consumable", weight: 33, item_id: "minor_mana_potion" }, { type: "herb", weight: 1, herb_id: "sunpetal" }],
  uncommon: [{ type: "nothing", weight: 139 }, { type: "gear", weight: 66 }, { type: "consumable", weight: 25, item_id: "health_potion" }, { type: "consumable", weight: 25, item_id: "mana_potion" }, { type: "herb", weight: 1, herb_id: "emberroot" }],
  rare: [{ type: "nothing", weight: 112 }, { type: "gear", weight: 99 }, { type: "consumable", weight: 25, item_id: "medium_health_potion" }, { type: "consumable", weight: 25, item_id: "medium_mana_potion" }, { type: "herb", weight: 1, herb_id: "frostvine" }],
  epic: [{ type: "nothing", weight: 83 }, { type: "gear", weight: 149 }, { type: "consumable", weight: 25, item_id: "greater_health_potion" }, { type: "consumable", weight: 25, item_id: "greater_mana_potion" }, { type: "herb", weight: 1, herb_id: "frostvine" }],
  legendary: [{ type: "nothing", weight: 60 }, { type: "gear", weight: 180 }, { type: "consumable", weight: 25, item_id: "supreme_health_potion" }, { type: "consumable", weight: 25, item_id: "supreme_mana_potion" }, { type: "herb", weight: 1, herb_id: "starveil" }],
};
// v0.22.1 (Part A2d): "Each band drops mostly its own [gear] rarity, with a small weighted
// tail into adjacent rarities" -- ~70% own rarity / ~15% each adjacent, clamped at the ends
// (Newborn/Ancient only have one neighbor, so their own share absorbs the missing 15%).
// Keys are the SAME band names as monster.tier; values name-index into COMBAT_RARITY_TABLE.
const MONSTER_BAND_LOOT_RARITY_WEIGHTS = {
  Common: [{ name: "Common", weight: 85 }, { name: "Uncommon", weight: 15 }],
  Uncommon: [{ name: "Common", weight: 15 }, { name: "Uncommon", weight: 70 }, { name: "Rare", weight: 15 }],
  Rare: [{ name: "Uncommon", weight: 15 }, { name: "Rare", weight: 70 }, { name: "Epic", weight: 15 }],
  Epic: [{ name: "Rare", weight: 15 }, { name: "Epic", weight: 70 }, { name: "Legendary", weight: 15 }],
  // v0.23.6: small "Set" tail added to the top band only -- Set pieces are endgame-only, so
  // only the hardest monster band gets any chance at one, at the expense of a few points of
  // Legendary's own share.
  Legendary: [{ name: "Epic", weight: 15 }, { name: "Legendary", weight: 80 }, { name: "Set", weight: 5 }],
};

// Only the fields combat's use-item endpoint actually needs from index.html's ITEMS.
const COMBAT_CONSUMABLES = {
  minor_health_potion: { heal_amount: 20 }, health_potion: { heal_amount: 40 }, medium_health_potion: { heal_amount: 75 }, greater_health_potion: { heal_amount: 120 }, supreme_health_potion: { heal_amount: 180 },
  minor_stamina_potion: { stamina_amount: 35 }, stamina_potion: { stamina_amount: 60 }, medium_stamina_potion: { stamina_amount: 100 }, greater_stamina_potion: { stamina_amount: 150 }, supreme_stamina_potion: { stamina_amount: 220 },
  // v0.23.0 (Part B3): mana potion tier line -- same 5-tier naming convention as health/stamina
  // potions above, mana_amount mirrors heal_amount/stamina_amount exactly.
  minor_mana_potion: { mana_amount: 20 }, mana_potion: { mana_amount: 40 }, medium_mana_potion: { mana_amount: 60 }, greater_mana_potion: { mana_amount: 90 }, supreme_mana_potion: { mana_amount: 140 },
  antidote: { cures_poison: true },
};
// v0.25 (credit: Gwen): Scrolls of Wisdom, a re-introduction of the retired scroll consumable
// line. Five tiers, each granting a flat lump of experience. Two separate gates, deliberately
// different from each other:
//   - DROP gate: a scroll of tier T only ever drops from an area level at or above its own
//     tier's bracket, exactly like gear tiers (combatItemTierForAreaLevel), so you cannot find
//     a Scroll of Wisdom IV in the starting woods.
//   - USE gate: the character must have REACHED the level that tier's bracket starts at before
//     it can be consumed, so a low-level character who buys a Tier V scroll on the Auction
//     House holds an asset they have to grow into rather than an instant shortcut past the
//     early game. Both gates use combatTierLevelRequirement(), the same 1/5/10/15/20 ladder gear
//     and spell tiers already run on, so there is one ladder in the game rather than three.
// Tradeable on the Auction House by design; never sold by any NPC.
const SCROLL_OF_WISDOM_XP = { 1: 100, 2: 300, 3: 500, 4: 1000, 5: 2000 };
const SCROLL_OF_WISDOM_IDS = { 1: "scroll_of_wisdom_1", 2: "scroll_of_wisdom_2", 3: "scroll_of_wisdom_3", 4: "scroll_of_wisdom_4", 5: "scroll_of_wisdom_5" };
function scrollOfWisdomTierForItemId(itemId) {
  for (const t of [1, 2, 3, 4, 5]) if (SCROLL_OF_WISDOM_IDS[t] === itemId) return t;
  return 0;
}
// Chance for a kill to additionally drop a scroll, on top of its normal loot. Kept low: this is
// a bonus find and an Auction House trade good, not a competitive XP source.
const SCROLL_OF_WISDOM_DROP_CHANCE = 0.02;
const COMBAT_BAG_CAPACITY = { traveler_pouch: 8, woven_bag: 16, bramble_sack: 24, rootpack_ancient: 40 };
const COMBAT_BASE_NAMES = {
  weapon: ["Twig Wand", "Bramblestaff", "Rootcarver", "Thornbow", "Charwood Axe", "Emberbrand", "Thornfang", "Bramblespike", "Quickthorn", "Goedendag"],
  head: ["Bramble Circlet", "Mosscap", "Antlercrown", "Leafwood Hood", "Wizard Hat"],
  shoulders: ["Bark Mantle", "Rootguard Pauldrons", "Thistle Shoulderguard", "Bark Pauldrons"],
  armor: ["Big Leaf Wrap", "Bark Plate", "Mosscloak", "Reedmail", "Ashen Hide", "Chainmail"],
  pants: ["Root-woven Leggings", "Bark Greaves", "Vine Trousers"],
  gloves: ["Thornweave Gloves", "Barkgrip Gauntlets", "Mossback Handwraps", "Windrider Gloves"],
  boots: ["Rootstep Boots", "Mossy Treads", "Bramblehide Boots", "Windstep Boots"],
  ring: ["Acorn Ring", "Petal Ring", "Vinewrought Ring"],
  amulet: ["Emberstone Pendant", "Driftwood Amulet", "Heartwood Talisman"],
  belt: ["Vine Cinch", "Barkweave Belt", "Root Sash"],
  // v0.22.6 (#11-14): mirrors index.html's IF.BASE_NAMES.offhand -- both Shield and Orb base
  // names share the single "offhand" GENERATION_SLOTS key; COMBAT_OFFHAND_TYPE_BY_BASE_NAME
  // below resolves which TYPE a given name is.
  offhand: ["Bramble Buckler", "Rootwood Shield", "Bark Aegis", "Moonpetal Orb", "Emberheart Orb", "Wisp-Light Orb"],
};
// v0.23.6: "Wizard Wand" added -- the Wizard Set's weapon piece, same "wand" TYPE (not a new
// restriction) as Twig Wand.
const COMBAT_WEAPON_TYPE_BY_BASE_NAME = { "Twig Wand": "wand", "Bramblestaff": "staff", "Rootcarver": "sword", "Thornbow": "bow", "Charwood Axe": "axe", "Emberbrand": "sword", "Thornfang": "dagger", "Bramblespike": "dagger", "Quickthorn": "dagger", "Goedendag": "dagger", "Wizard Wand": "wand" };
// v0.23.6: rebuilt to a strict 2-weapon-types-per-class distribution (see the Aug 3 balance/
// itemization handover), replacing the old uneven 1-4 map. Dagger now has its own explicit
// entry too, so it is no longer implicitly "every class allowed" via the missing-entry
// convention -- it is restricted like every other weapon type. Must stay byte-identical to
// index.html's IF.WEAPON_TYPE_CLASS_RESTRICTIONS.
const COMBAT_WEAPON_TYPE_CLASS_RESTRICTIONS = {
  wand: ["wizard", "sorcerer", "archmage", "emberpriest", "shadowbloom"],
  staff: ["wizard", "sorcerer", "necromancer", "archmage", "rootbinder", "druid"],
  sword: ["thornguard", "stonewarden", "treesinger", "galestrider", "shadowbloom", "druid"],
  axe: ["warlock", "thornguard", "stonewarden", "windrider", "galeshaper"],
  bow: ["treesinger", "emberpriest", "windrider", "galestrider"],
  dagger: ["warlock", "necromancer", "rootbinder", "galeshaper"],
};
// v0.22.6 (#11-14): must stay byte-identical to index.html's IF.OFFHAND_TYPE_BY_BASE_NAME /
// OFFHAND_TYPE_CLASS_RESTRICTIONS. Shield has no class-restriction entry (usable by everyone,
// same "missing entry = every class allowed" convention dagger relies on above); Orb is gated
// to the wizard chain plus emberpriest/druid.
// v0.23.6: "Wizard Orb" added -- the Wizard Set's offhand piece, same "orb" TYPE as the other 3 orbs.
const COMBAT_OFFHAND_TYPE_BY_BASE_NAME = { "Bramble Buckler": "shield", "Rootwood Shield": "shield", "Bark Aegis": "shield", "Moonpetal Orb": "orb", "Emberheart Orb": "orb", "Wisp-Light Orb": "orb", "Wizard Orb": "orb" };
const COMBAT_OFFHAND_TYPE_CLASS_RESTRICTIONS = {
  orb: ["wizard", "sorcerer", "warlock", "necromancer", "archmage", "emberpriest", "druid"],
};
// v0.23.6: class-exclusive armor -- gated by CHAIN (any tier of that chain can equip it), not a
// single class, so legality is COMBAT_CLASSES[classId].chain==="wizard"/etc. rather than a
// hardcoded id list. Must stay byte-identical to index.html's IF.CHAIN_EXCLUSIVE_BASE_NAMES.
const COMBAT_CHAIN_EXCLUSIVE_BASE_NAMES = {
  "Wizard Hat": "wizard",
  "Chainmail": "thornguard",
  "Bark Pauldrons": "thornguard",
  "Windrider Gloves": "windrider",
  "Windstep Boots": "windrider",
  // v0.23.6: Wizard Set's 3 remaining armor-slot pieces (weapon/offhand pieces are gated via
  // the WEAPON/OFFHAND maps above instead); Wizard Hat is dual-purpose (chain-exclusive AND
  // this set's head piece).
  "Wizard Robes": "wizard",
  "Wizard Gloves": "wizard",
  "Wizard Sash": "wizard",
};
// v0.23.6: Set item framework -- mirrors index.html's IF.ITEM_SETS exactly. Only Wizard Set is
// populated this batch; Thornguard/Windrider are empty stubs per Gwen's "Ship Wizard Set only" call.
// NOTE: the actual const COMBAT_ITEM_SETS declaration is placed further down, AFTER `const CB`
// finishes initializing (see the comment there) -- its bonuses array reads CB.WIZARD_SET_*
// eagerly (template literals evaluated at object-construction time, not lazily like a function
// body), so declaring it up here at the original insertion point threw "Cannot access 'CB'
// before initialization" (CB.WIZARD_SET_2PC_MANA_REGEN_PCT etc. weren't defined yet -- CB itself
// is declared with `const` further down in the file, so referencing it here hit the temporal
// dead zone). combatSetPieceCandidatesForSlot()/combatEquippedSetCounts()/combatSetBonusesFor()
// below are ordinary function DECLARATIONS, though -- those don't execute until called (well
// after CB is fully initialized), so they're safe to leave right here.
function combatSetPieceCandidatesForSlot(slot) {
  const out = [];
  for (const setId in COMBAT_ITEM_SETS) { const bn = COMBAT_ITEM_SETS[setId].pieces[slot]; if (bn) out.push({ set_id: setId, base_name: bn }); }
  return out;
}
// v0.23.6: counts this character's currently-equipped (and equip-VALID, mirroring how
// combatGearBonus() already excludes under-level/wrong-class gear) pieces per set.
function combatEquippedSetCounts(data) {
  const counts = {};
  const equipped = data.equipped || {};
  for (const slot of COMBAT_EQUIPPED_SLOT_KEYS) {
    const inst = equipped[slot];
    if (!inst || !inst.set_id) continue;
    if (!combatCanEquipGear(data, inst)) continue;
    counts[inst.set_id] = (counts[inst.set_id] || 0) + 1;
  }
  return counts;
}
function combatSetBonusesFor(data, setId) {
  const def = COMBAT_ITEM_SETS[setId];
  if (!def) return { count: 0, active: [], def: null };
  const count = combatEquippedSetCounts(data)[setId] || 0;
  return { count, active: def.bonuses.filter((b) => b.count <= count), def };
}
// v0.23.6: inherent attribute-scaling bonus -- a property of the base item TYPE, identical for
// every class allowed to equip it (there is no class-conditional here, only the wearer's OWN
// attribute value differs). inherentBonus = (totalScalingAttr/50) x per50. Stacks additively
// with any rolled affix of the same stat (see combatGearBonus()). Must stay byte-identical to
// index.html's IF.INHERENT_SCALING_BY_BASE_NAME.
const COMBAT_INHERENT_SCALING_BY_BASE_NAME = {
  "Wizard Hat": { attr: "int", stat: "spell_power", per50: 3 },
  "Chainmail": { attr: "str", stat: "armor", per50: 4 },
  "Bark Pauldrons": { attr: "str", stat: "armor", per50: 3 },
  "Windrider Gloves": { attr: "dex", stat: "attack_speed", per50: 4 },
  "Windstep Boots": { attr: "dex", stat: "block_chance", per50: 2 },
};
function combatInherentScalingFor(inst) { return (inst && inst.base_name) ? (COMBAT_INHERENT_SCALING_BY_BASE_NAME[inst.base_name] || null) : null; }
const COMBAT_EQUIPPED_SLOT_KEYS = ["weapon", "head", "shoulders", "armor", "pants", "gloves", "boots", "ring1", "ring2", "amulet", "belt", "offhand"];
// v0.23.6: "Set" rarity added at the same flat weight as Legendary but fewer affix slots (3,
// same as Rare) -- see combatGenerateGearItem()'s Set-rarity branch for what actually
// constrains a roll to a real set piece. Must stay byte-identical to index.html's Balance.RARITY_TABLE.
const COMBAT_RARITY_TABLE = [{ name: "Common", slots: 1, weight: 60 }, { name: "Uncommon", slots: 2, weight: 25 }, { name: "Rare", slots: 3, weight: 10 }, { name: "Epic", slots: 5, weight: 4 }, { name: "Legendary", slots: 8, weight: 1 }, { name: "Set", slots: 3, weight: 1 }];

// Balance-equivalent constants combat needs, ported verbatim from index.html's Balance
// object (see that file's own comments for the full rationale behind each number).
const CB = {
  // v0.25 (credit: Gwen): every class gains this much max Mana per point of total Intelligence.
  // Mirrors Balance.MANA_PER_INTELLIGENCE in index.html.
  // v0.26.3 (credit: Gwen): Intelligence grants flat Mana to every class, but not equally. The
  // Wizard path lives on Intelligence and gets the most; the two hybrid tiers that drift INTO
  // Intelligence (Treesinger, Shadowbloom) get an intermediate rate, so the drift pays for itself
  // in a number rather than only in flavour text. Must stay identical to Balance's copy in
  // index.html -- combatGetMaxMana() and PS.getMaxMana() have to agree exactly or the client will
  // show a mana pool the server refuses to let you spend.
  MANA_PER_INTELLIGENCE: 2,
  MANA_PER_INTELLIGENCE_ARCANE: 5,
  MANA_PER_INTELLIGENCE_HYBRID: 3,
  // v0.28 (credit: Gwen): Spell Power percentage granted per point of total Intelligence, on the
  // same three class bands as the mana grant above. An Archmage at 60 Intelligence carries +90%
  // Spell Power from attributes alone, before any gear -- which is the point: a caster's power
  // should come from the character they built, not only from which spell tiers they could afford.
  SPELL_POWER_PCT_PER_INT: 0.5,
  SPELL_POWER_PCT_PER_INT_ARCANE: 1.5,
  SPELL_POWER_PCT_PER_INT_HYBRID: 1.0,
  // v0.28: Spell Cooldown Reduction was rolled on gear, shown on the Character Sheet, and read by
  // nothing -- the exact same dead-affix state Life on Hit was in. It is a caster's attack speed,
  // so it now actually shortens cooldowns. Hard-capped, because a cooldown approaching zero turns
  // every fight into a single spell spammed without limit and makes mana the only real cost.
  // v0.29.1 (credit: Gwen): the 50% cap is gone. The 100 that remains is an arithmetic floor, not
  // a balance cap -- past 100% the reduction yields a negative cooldown, which every timer and the
  // ready-at comparison below would read as nonsense. At 100% a spell has no cooldown and mana
  // becomes the only limiter, which is the intended consequence. Mirrors Balance.SPELL_CDR_MAX_PCT.
  SPELL_CDR_MAX_PCT: 100,
  ARCANE_CLASS_IDS: ["wizard", "sorcerer", "warlock", "summoner", "archmage"],
  INT_HYBRID_CLASS_IDS: ["treesinger", "shadowbloom"],
  // v0.22.7 (#18): player crit floor dropped from 1.75 to 1.5, mirroring index.html's
  // Balance.CRIT_MULTIPLIER. Monsters no longer use this constant as their own floor at all
  // (see combatGetMonsterCritFloor()/combatGetMonsterCritCeiling() below) -- CB.CRIT_MULTIPLIER
  // is the PLAYER'S floor only now.
  // v0.24.1 (C5): crit multiplier ladder shifted down 0.25 across the board (was 1.5/1.5-2.0/
  // 1.75-2.25.../2.0-2.5...) -- see combatGetMonsterCritFloor()/combatGetMonsterCritCeiling()
  // below for the matching monster-side shift; must stay in sync with Balance.CRIT_MULTIPLIER
  // in index.html.
  CRIT_MULTIPLIER: 1.25, FLEE_FAIL_CHANCE: 0.20, MONSTER_FIRST_STRIKE_CHANCE: 0.5,
  // v0.20 (#9.6): dialed down from 1% to 0.5% per point of Dexterity -- must stay in sync
  // with Balance.BLOCK_CHANCE_PER_DEX in index.html (see its comment for the full rationale).
  BLOCK_CHANCE_PER_DEX: 0.005, BLOCK_CHANCE_MAX: 0.60,
  // v0.20 (#9.7): Vitality rework -- must stay in sync with Balance.VIT_HP_PER_POINT_RATIO/
  // VIT_STAMINA_PER_POINT_RATIO in index.html (see that constant's comment for the full
  // rationale). Replaces the old per-level-up bonus_hp_from_attributes/bonus_stamina_from_
  // attributes accumulator (see combatAddXp()) with a flat, non-compounding, class-based
  // per-point amount computed live in combatGetMaxHp()/combatGetMaxStamina() instead.
  VIT_HP_PER_POINT_RATIO: 0.35, VIT_STAMINA_PER_POINT_RATIO: 0.15,
  MOB_BLOCK_CHANCE_BASE: 0.05, MOB_BLOCK_CHANCE_PER_LEVEL: 0.005,
  MONSTER_HP_MULT: 2.0, MONSTER_DAMAGE_MULT: 2.0,
  // v0.22.2 (#1): Part A (v0.22.1) flattened monster damage to a single number, which made every
  // non-crit hit land for the exact same amount -- reads as robotic. This reintroduces a small
  // symmetric per-hit swing band around that flattened value (dmg_min/dmg_max stop being equal),
  // so cbRandRange(dmg_min, dmg_max) produces gentle per-swing variance again while the mean stays
  // exactly the flattened number (no DPS/balance change, purely cosmetic). 0.05 = +/-5%.
  MONSTER_DAMAGE_JITTER: 0.05,
  // v0.22.6 (#24): mirror of MONSTER_DAMAGE_JITTER above, but for the player's own non-crit
  // hits -- a tight symmetric +/-% band around the flat computed damage value so a run of
  // swings reads as 64, 66, 63... instead of a flat 66 every time. Average damage across many
  // hits is unchanged; crits skip this (their own multiplier roll already varies the result).
  PLAYER_DAMAGE_JITTER: 0.05,
  // v0.22 (batch2 #3): each class chain's primary attribute grants +1 damage/point by default
  // (thornguard->str, windrider->dex, wizard->int, see combatGetDamageRange() above) -- this
  // doubles ONLY the Wizard chain's Intelligence term to +2/point, per Gwen's exact spec. Must
  // stay in sync with Balance.WIZARD_INT_DAMAGE_PER_POINT in index.html.
  WIZARD_INT_DAMAGE_PER_POINT: 2,
  // v0.23.0 (Part A4): every level-up now ALSO auto-allocates +2 into the character's chain
  // primary attribute directly (thornguard->str, windrider->dex, wizard->int, the same mapping
  // WIZARD_INT_DAMAGE_PER_POINT's comment above already documents, see combatAddXp() below),
  // and grants level-derived flat HP/Mana, computed LIVE from current level in combatGetMaxHp()/
  // combatGetMaxMana() (mirrors hp_per_level's own "(level-1) * per-level amount" convention),
  // never stored as an accumulating counter, so a level-1 reset (bridge trial, ladder reset)
  // removes it for free. Must stay in sync with Balance.LEVEL_UP_* in index.html.
  LEVEL_UP_PRIMARY_ATTR_BONUS: 2,
  LEVEL_UP_HP_BONUS_PER_LEVEL: 10,
  LEVEL_UP_MANA_BONUS_PER_LEVEL: 5,
  // v0.23.0 (Part B2): base Mana regen, ticked the same way/place as Health/Stamina Regen
  // (once per real second in town/maze, once per combat round in a fight) -- see
  // combatGetManaRegen() below. Stacks with the "mana_regen" gear affix. Must stay in sync
  // with Balance.MANA_REGEN_PER_SEC in index.html.
  MANA_REGEN_PER_SEC: 1.0,
  // v0.24.1 (C3): elapsed-time cap for combatTickRegenElapsed()'s wall-clock regen catch-up --
  // prevents a session that's sat idle for a long time (tab backgrounded, etc.) from awarding
  // one huge regen lump-sum the instant it's touched again.
  COMBAT_REGEN_MAX_CATCHUP_SEC: 30,
  // v0.24.1 (C4): Poison Sting -- a small set of "venomous" monsters can, on a landed hit,
  // sting instead of striking: that hit's fully-mitigated damage roll is queued as poison
  // (spread over POISON_STING_DURATION_MS) instead of applied instantly, and the monster
  // pauses for POISON_STING_LOCKOUT_MS before its normal attack cadence resumes (the poison
  // itself keeps ticking through that pause, and stacks if another sting lands later).
  POISON_STING_MONSTER_IDS: ["bramble_knight", "thornling", "boglurker", "vinewraith"],
  POISON_STING_CHANCE: 0.10,
  POISON_STING_DURATION_MS: 20000,
  POISON_STING_LOCKOUT_MS: 2000,
  // v0.20.4 HOTFIX: AREA_HP_GROWTH/AREA_DAMAGE_GROWTH dialed down from 0.15/0.12 (0.105/
  // 0.09), and LATE_GAME_MONSTER_GROWTH_PER_LEVEL below is now permanently unused (see
  // combatLateGameGrowthMult()) -- must stay in sync with Balance.AREA_HP_GROWTH/
  // AREA_DAMAGE_GROWTH's own v0.20.4 comment in index.html for the full rationale, the
  // tier-blended-monster-pool bug this accounts for, and why the late-game multiplier was
  // removed outright instead of re-tuned again. Full methodology in BALANCE_REPORT_v0.20.4.md.
  // v0.23.6: second pass -- the one-shot problem was fundamentally a damage problem, so damage
  // growth is cut further (0.09 -> 0.06) while HP growth is raised (0.105 -> 0.13) to shift
  // danger from burst kills toward a longer, more tactical attrition fight. Validated against
  // a Monte-Carlo combat sim (player vs Bramble Knight, per class) before/after; late-game
  // multiplier stays disabled, do not re-enable it.
  AREA_HP_GROWTH: 0.13, AREA_DAMAGE_GROWTH: 0.06, AREA_XP_GROWTH: 0.062,
  LATE_GAME_MONSTER_GROWTH_START_LEVEL: 10, LATE_GAME_MONSTER_GROWTH_PER_LEVEL: 0.10,
  // v0.20.4: every point of total Strength grants this much flat Armor, for every class --
  // must stay in sync with Balance.STR_TO_ARMOR_RATIO in index.html (see that constant's
  // comment for the full calibration).
  STR_TO_ARMOR_RATIO: 0.25,
  // v0.23.6: Wizard Set threshold-bonus magnitudes -- must stay in sync with index.html's
  // Balance.WIZARD_SET_* constants (see that file's comment for the rationale).
  WIZARD_SET_2PC_MANA_REGEN_PCT: 5,
  WIZARD_SET_3PC_SPELL_POWER_PCT: 15,
  WIZARD_SET_4PC_SPELL_POWER_PCT_PER_MANA: 0.05,
  WIZARD_SET_6PC_MANA_COST_REDUCTION_PCT: 20,
  WIZARD_SET_6PC_BONUS_SPELL_SLOTS: 1,
  // v0.22 (batch2 #4): guardians are engage-chosen (the player picks the fight) and are meant
  // to be the hardest encounter in a maze, so they now carry the tougher numbers that used to
  // belong to roamers (including a brand-new damage multiplier -- guardians previously hit at
  // plain monster damage). Roamers, which can ambush the player mid-step, get the milder set
  // that used to belong to guardians. Must stay in sync with Balance.STRONGHOLD_GUARDIAN_HP_MULT/
  // XP_MULT/DAMAGE_MULT and Balance.ROAMING_MOB_HP_MULT/DAMAGE_MULT/XP_MULT in index.html.
  STRONGHOLD_GUARDIAN_HP_MULT: 4.0, STRONGHOLD_GUARDIAN_XP_MULT: 2.0, STRONGHOLD_GUARDIAN_DAMAGE_MULT: 1.5,
  // v0.24.2: cut from 0.30 to 0.05 per Gwen -- a Stronghold Key is now a genuinely rare
  // per-guardian drop rather than something most guardians hand over. Mirrors
  // Balance.STRONGHOLD_KEY_DROP_CHANCE in index.html.
  STRONGHOLD_KEY_DROP_CHANCE: 0.05,
  ROAMING_MOB_HP_MULT: 3.0, ROAMING_MOB_DAMAGE_MULT: 1.0, ROAMING_MOB_XP_MULT: 1.5,
  // v0.20.1 (#10): every kill now gets a shot at a SECOND (and rarely third) loot drop, on
  // top of the always-resolved first drop above -- base chance is deliberately small so it
  // reads as a nice surprise rather than the new normal, but climbs with Magic Find so
  // stacking that stat (gear affix + kill streak + shrine buff, see combatGetMagicFind())
  // has a second lever to pull beyond just rarity. Roaming mobs/Stronghold guardians (the
  // "special" spawns) get a flat bonus on top of that -- both to the chance of a bonus drop
  // happening at all, AND to the effective Magic Find used when rolling what that bonus drop
  // actually IS, so their extra loot skews toward better quality too, exactly per Gwen's spec.
  EXTRA_LOOT_BASE_CHANCE_PCT: 8, EXTRA_LOOT_MAGIC_FIND_SCALING_PCT: 0.5, EXTRA_LOOT_MAX_CHANCE_PCT: 60,
  EXTRA_LOOT_SPECIAL_SPAWN_CHANCE_BONUS_PCT: 15, EXTRA_LOOT_SPECIAL_SPAWN_QUALITY_BONUS_PCT: 20,
  // Chance of a THIRD drop is this fraction of whatever the (already-boosted) 2nd-drop chance
  // rolled out to, so triple-drops stay meaningfully rarer than doubles even at very high MF.
  EXTRA_LOOT_THIRD_DROP_FACTOR: 0.35,
  // v0.19.1 (#14): % damage per level now, not a flat +1/level -- must stay in sync with
  // Balance.WEAPON_SKILL_DAMAGE_PCT_PER_LEVEL in index.html (see combatGetDamageRange()).
  WEAPON_SKILL_HIT_STEP: 100, WEAPON_SKILL_DAMAGE_PCT_PER_LEVEL: 0.01,
  KILL_STREAK_MAGIC_FIND_PCT_PER_KILL: 1,
  // v0.30 (Gwen): named rather than left as a bare 500 inside combatGetMagicFind(), because the
  // party magic-find ceiling has to be the same number and a second literal would drift.
  // Mirrors Balance.MAGIC_FIND_CAP_PCT in index.html.
  MAGIC_FIND_CAP_PCT: 500,
  // v0.20 (#7): must stay in sync with Balance.SHRINE_MAGIC_FIND_PCT in index.html -- this
  // was missing entirely server-side until the same bug-hunt that fixed the round-tick sync
  // issue below turned it up: combatGetMagicFind() was still reading the OLD wall-clock
  // temp_buffs bag (combatGetActiveTempBuff(data, "magic_find")) that Magic Find moved off
  // of in #7, which the client stopped ever populating -- so the shrine's bonus silently
  // never affected a single server-resolved loot roll after that change shipped.
  SHRINE_MAGIC_FIND_PCT: 25,
  // v0.23.2 (Night delve): applied on top of every other Magic Find/Gold Find source, exactly
  // once, only when the combat_sessions row this kill resolved against has is_night=1 (see
  // combatGetMagicFind()/combatGetGoldFindMult() below and POST /api/combat/start's is_night
  // handling). Must stay in sync with Balance.NIGHT_MAGIC_FIND_BONUS_PCT/NIGHT_GOLD_FIND_
  // BONUS_PCT in index.html (those are display-copy mirrors only -- this is the real bonus).
  NIGHT_MAGIC_FIND_BONUS_PCT: 15, NIGHT_GOLD_FIND_BONUS_PCT: 15,
  // v0.22.3 (#10): raised 60 -> 100 -- the season win condition moves from "reach tier 5"
  // to "reach tier 5 AND level 100", so the level cap has to actually allow level 100.
  STAT_POINTS_PER_LEVEL: 3, LEVEL_CAP: 100,
  // Keeper of the Emerald endgame choice: the Crown's "massive gold payout" from the design
  // doc. Must stay in sync with Balance.CROWN_GOLD_REWARD in index.html.
  CROWN_GOLD_REWARD: 50000,
  KILLS_PER_LEVEL_TARGET: 20, MONSTER_BASE_XP: 8.0, LEVEL_XP_GROWTH: 0.11,
  // v0.20 (#9.1): dialed down from +10% to +1% per point of positive Forest Reputation --
  // must stay in sync with Balance.FOREST_REPUTATION_XP_PCT_PER_POINT in index.html.
  FOREST_REPUTATION_XP_PCT_PER_POINT: 1,
  // v0.20 (#9.2): the "Currently playing" community XP buff used to be +100% per EXTRA
  // active player (i.e. the raw headcount doubled/tripled/etc. as a multiplier) -- way too
  // generous once more than a couple people were online at once. Now it's +10% per extra
  // player, and (v0.20 #9.3) stacks ADDITIVELY with every other XP bonus source instead of
  // multiplying them together -- see combatGetTotalXpBonusPct()'s comment for the full
  // stacking model. Must stay in sync with Balance.COMMUNITY_XP_PCT_PER_EXTRA_PLAYER.
  COMMUNITY_XP_PCT_PER_EXTRA_PLAYER: 10,
  // v0.20 (#9.5): farming an area BELOW your own character level now pays out steeply
  // reduced XP -- previously the only thing discouraging this was the two independent XP
  // curves drifting apart over time (see combatXpRequiredForLevel() vs
  // combatMonsterXpReward()), which did nothing for a character who deliberately stayed
  // camped in a trivial area right after leveling up. Multiplier decays geometrically per
  // level of gap (character level minus area level) once the gap is positive, floored so it
  // never hits a hard 0. Venturing into an area AT or ABOVE your own level (the "climbing"
  // direction) gets no penalty at all -- that risk is already priced in via tougher mobs.
  // Must stay in sync with Balance.UNDERLEVEL_XP_DECAY_PER_LEVEL/UNDERLEVEL_XP_MIN_MULT in
  // index.html.
  UNDERLEVEL_XP_DECAY_PER_LEVEL: 0.80, UNDERLEVEL_XP_MIN_MULT: 0.05,
  POTION_HEAL_DURATION_MS: 5000,
  ITEM_TIER_BRACKET_WIDTH: 5,
  TIER_DROP_WEIGHTS_BY_OFFSET: [40, 30, 20, 6, 4],
  WEAPON_DAMAGE_GUARANTEE_CHANCE: 0.70,
  ELEMENT_ITEM_CHANCE: 0.3, ELEMENT_IDS: ["fire", "wind", "earth", "water"],
  ELEMENT_AFFIX_BIAS: { fire: "damage", wind: "stamina_max", earth: "armor", water: "regen" },
  EYESIGHT_AFFIX_CHANCE: 0.03,
  STAMINA_MAX_BASE: 175.0,
  AREA_LEVEL_MAX: 100,
  // v0.21.1 (#9/#10/#11/#12): attack-speed system. Both player and monster default to
  // exactly 1 attack/sec; the player's own rate only rises via the new "attack_speed" gear
  // affix (see combatGetAttackSpeed()), while a monster's rate climbs in breakpoints every 5
  // area levels (see combatMonsterAttackSpeed()) -- same "every 5 levels" cadence Gwen's
  // existing crit-multiplier scaling already uses, kept consistent on purpose. Must stay in
  // sync with index.html's Balance.PLAYER_BASE_ATTACK_SPEED/MONSTER_BASE_ATTACK_SPEED/
  // MONSTER_ATTACK_SPEED_PCT_PER_5_LEVELS.
  PLAYER_BASE_ATTACK_SPEED: 1.0, MONSTER_BASE_ATTACK_SPEED: 0.5,
  MONSTER_ATTACK_SPEED_PCT_PER_5_LEVELS: 5,
  // Safety cap on how many "owed" monster hits a single request will ever resolve at once
  // (e.g. a browser tab left open/suspended for hours) -- protects against a pathologically
  // long catch-up loop; any hits beyond this cap are simply resolved on a LATER request
  // instead of all at once.
  MONSTER_CATCH_UP_MAX_HITS: 20,
};

// v0.23.6: Set item framework -- mirrors index.html's IF.ITEM_SETS exactly. Only Wizard Set is
// populated this batch; Thornguard/Windrider are empty stubs per Gwen's "Ship Wizard Set only"
// call. Declared HERE (after CB, not up near COMBAT_WEAPON_TYPE_CLASS_RESTRICTIONS/
// COMBAT_CHAIN_EXCLUSIVE_BASE_NAMES where the rest of this patch's data tables live) because its
// bonuses array's template literals read CB.WIZARD_SET_* eagerly at object-construction time --
// see the comment left at the original insertion point for the full explanation of the crash
// this avoids.
const COMBAT_ITEM_SETS = {
  wizard_set: {
    display_name: "Wizard's Regalia",
    pieces: { weapon: "Wizard Wand", offhand: "Wizard Orb", head: "Wizard Hat", armor: "Wizard Robes", gloves: "Wizard Gloves", belt: "Wizard Sash" },
    bonuses: [
      { count: 2, desc: `+${CB.WIZARD_SET_2PC_MANA_REGEN_PCT}% Mana Regeneration` },
      { count: 3, desc: `+${CB.WIZARD_SET_3PC_SPELL_POWER_PCT}% Spell Power` },
      { count: 4, desc: "Spell damage also scales off Max Mana" },
      { count: 6, desc: `Spells cost ${CB.WIZARD_SET_6PC_MANA_COST_REDUCTION_PCT}% less Mana, +${CB.WIZARD_SET_6PC_BONUS_SPELL_SLOTS} Spell Slot` },
    ],
  },
  thornguard_set: { display_name: "", pieces: {}, bonuses: [] },
  windrider_set: { display_name: "", pieces: {}, bonuses: [] },
};

// v0.24.2 (credit: Gwen): there is no such thing as half a point of Health, Stamina, Mana or
// damage in this game, so nothing fractional may ever reach the client. Damage, Thorns, DOT
// ticks, monster HP and every max-pool figure are already rounded at their own source (see
// combatMonsterHp()/combatGetMaxHp()). Regen is the one exception that must stay fractional
// INTERNALLY: it accrues at rates well below 1 per tick, and rounding the stored value each
// tick would floor those increments away and stop slow regen from ever accumulating at all.
// So regen keeps its precision in the saved row and gets rounded here, once, on its way out --
// every combat response's player payload goes through this.
function cbInt(v) { return Math.round(v || 0); }
function cbClampf(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function cbClampi(v, lo, hi) { return Math.max(lo, Math.min(hi, Math.round(v))); }
function cbRandRange(a, b) { return a + Math.random() * (b - a); }
function cbRandIntRange(a, b) { return a + Math.floor(Math.random() * (b - a + 1)); }
function cbPick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function cbShuffle(arr) { for (let i = arr.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1));[arr[i], arr[j]] = [arr[j], arr[i]]; } }

// v0.20 (#9.4): must stay in sync with Balance.xpRequiredForLevel() in index.html (see its
// comment for the full rationale) AND trialXpRequiredForLevel() below -- all three compute
// the exact same curve.
function combatXpRequiredForLevel(level) { return Math.round((CB.KILLS_PER_LEVEL_TARGET + (level - 1)) * CB.MONSTER_BASE_XP * Math.pow(1.0 + CB.LEVEL_XP_GROWTH, level)); }
function combatMonsterXpReward(baseXp, areaLevel) { return baseXp * Math.pow(1.0 + CB.AREA_XP_GROWTH, areaLevel); }
// v0.20 (#9.5): see CB.UNDERLEVEL_XP_DECAY_PER_LEVEL's comment for the full rationale. Only
// a POSITIVE gap (character level above area level) is ever penalized; an equal or negative
// gap (area at or above character level -- i.e. climbing into harder-than-you content) always
// returns exactly 1.0 (no penalty, no bonus).
function combatLevelDiffXpMult(charLevel, areaLevel) {
  const gap = (charLevel || 1) - (areaLevel || 1);
  if (gap <= 0) return 1.0;
  return Math.max(CB.UNDERLEVEL_XP_MIN_MULT, Math.pow(CB.UNDERLEVEL_XP_DECAY_PER_LEVEL, gap));
}
// v0.20.4: retired -- permanently returns 1.0. See Balance.lateGameMonsterGrowthMult()'s
// matching comment in index.html for why this is a no-op now instead of deleted outright.
function combatLateGameGrowthMult(areaLevel) {
  return 1.0;
}
// v0.24.2 BUG FIX (credit: Gwen): this used to return a raw float, and that single fact was
// the source of every long decimal players saw in the combat log. Damage is clamped to the
// monster's REMAINING hp on a killing blow (Math.min(hp, ...) in combatSettleSpellDots() and
// the Thorns reflect in combatResolveMonsterTurn()), so the last hit of a fight reported the
// fractional leftover verbatim -- "Fireflies burns the Roaming Sprout for 17.90123917247
// damage" was literally the monster's remaining HP. Rounding at the source means every clamp
// downstream can only ever produce a whole number. Floor of 1 so a very weak monster at a very
// low area level can never spawn already dead.
function combatMonsterHp(baseHp, areaLevel) { return Math.max(1, Math.round(baseHp * CB.MONSTER_HP_MULT * Math.pow(1.0 + CB.AREA_HP_GROWTH, areaLevel) * combatLateGameGrowthMult(areaLevel))); }
function combatMonsterDamage(baseDmg, areaLevel) { return baseDmg * CB.MONSTER_DAMAGE_MULT * Math.pow(1.0 + CB.AREA_DAMAGE_GROWTH, areaLevel) * combatLateGameGrowthMult(areaLevel); }
function combatMobBlockChance(areaLevel) { return CB.MOB_BLOCK_CHANCE_BASE + CB.MOB_BLOCK_CHANCE_PER_LEVEL * Math.max(0, (areaLevel || 1) - 1); }
function combatItemTierForAreaLevel(areaLevel) { return cbClampi(1 + Math.floor((areaLevel || 1) / CB.ITEM_TIER_BRACKET_WIDTH), 1, ITEM_TIER_MAX); }
function combatRollItemTier(areaLevel) {
  const nat = combatItemTierForAreaLevel(areaLevel);
  let total = 0; for (let o = 0; o < nat; o++) total += (CB.TIER_DROP_WEIGHTS_BY_OFFSET[o] || 1);
  let roll = Math.random() * total, cum = 0;
  for (let o = 0; o < nat; o++) { cum += (CB.TIER_DROP_WEIGHTS_BY_OFFSET[o] || 1); if (roll < cum) return nat - o; }
  return nat;
}
function combatRollRarity(mfPct) {
  mfPct = mfPct || 0;
  const rollOnce = () => {
    let total = 0; for (const r of COMBAT_RARITY_TABLE) total += r.weight;
    let roll = Math.floor(Math.random() * total), cum = 0;
    for (const r of COMBAT_RARITY_TABLE) { cum += r.weight; if (roll < cum) return r; }
    return COMBAT_RARITY_TABLE[0];
  };
  const first = rollOnce();
  if (!mfPct) return first;
  if (Math.random() < Math.min(0.9, mfPct / 100)) {
    const second = rollOnce();
    const fi = COMBAT_RARITY_TABLE.indexOf(first), si = COMBAT_RARITY_TABLE.indexOf(second);
    return si > fi ? second : first;
  }
  return first;
}
// v0.22.1 (Part A2d): same shape/Magic-Find-reroll behavior as combatRollRarity() above, but
// weighted by the killed monster's band (MONSTER_BAND_LOOT_RARITY_WEIGHTS) instead of the
// flat COMBAT_RARITY_TABLE weights -- this is what makes a Newborn mostly drop Crude gear and
// an Ancient mostly drop Legendary, while still allowing an occasional one-step-off surprise.
function combatRollRarityForBand(bandKey, mfPct) {
  const weights = MONSTER_BAND_LOOT_RARITY_WEIGHTS[bandKey];
  if (!weights) return combatRollRarity(mfPct);
  mfPct = mfPct || 0;
  const rollOnce = () => {
    let total = 0; for (const w of weights) total += w.weight;
    let roll = Math.random() * total, cum = 0;
    for (const w of weights) { cum += w.weight; if (roll < cum) return COMBAT_RARITY_TABLE.find((r) => r.name === w.name) || COMBAT_RARITY_TABLE[0]; }
    return COMBAT_RARITY_TABLE.find((r) => r.name === weights[0].name) || COMBAT_RARITY_TABLE[0];
  };
  const first = rollOnce();
  if (!mfPct) return first;
  if (Math.random() < Math.min(0.9, mfPct / 100)) {
    const second = rollOnce();
    const fi = COMBAT_RARITY_TABLE.indexOf(first), si = COMBAT_RARITY_TABLE.indexOf(second);
    return si > fi ? second : first;
  }
  return first;
}
// v0.20.1 (#10): decides how many BONUS loot entries (beyond the always-resolved first
// drop) a kill produces -- 0 by default, occasionally 1, rarely 2. isSpecialSpawn is true
// for roaming mobs and Stronghold guardians, which get their own flat chance bonus on top
// of whatever Magic Find is already contributing (see CB.EXTRA_LOOT_* above).
function combatRollExtraLootCount(mfPct, isSpecialSpawn) {
  mfPct = mfPct || 0;
  let chance = CB.EXTRA_LOOT_BASE_CHANCE_PCT + mfPct * CB.EXTRA_LOOT_MAGIC_FIND_SCALING_PCT;
  if (isSpecialSpawn) chance += CB.EXTRA_LOOT_SPECIAL_SPAWN_CHANCE_BONUS_PCT;
  chance = Math.min(chance, CB.EXTRA_LOOT_MAX_CHANCE_PCT);
  let count = 0;
  if (Math.random() * 100 < chance) {
    count++;
    if (Math.random() * 100 < chance * CB.EXTRA_LOOT_THIRD_DROP_FACTOR) count++;
  }
  return count;
}
function combatRollLoot(tableId, mfPct) {
  mfPct = mfPct || 0;
  const table = COMBAT_LOOT_TABLES[tableId] || [];
  const rollOnce = () => {
    if (table.length === 0) return { type: "nothing" };
    let total = 0; for (const e of table) total += e.weight;
    if (total <= 0) return { type: "nothing" };
    let roll = Math.floor(Math.random() * total), cum = 0;
    for (const e of table) { cum += e.weight; if (roll < cum) return e; }
    return { type: "nothing" };
  };
  const first = rollOnce();
  if (first.type !== "nothing" || !mfPct) return first;
  if (Math.random() < Math.min(0.9, mfPct / 100)) {
    const second = rollOnce();
    if (second.type !== "nothing") return second;
  }
  return first;
}
// Mirrors IF.generate() exactly, reusing the SAME live constants (ITEM_AFFIX_POOL,
// ITEM_AFFIX_TIER1_MAX via itemAffixMaxForTier, ITEM_WEAPON_DAMAGE_AFFIX_MULT,
// ITEM_GENERATION_SLOTS) already defined above for validateGearItem/the reroll endpoint --
// so a server-rolled drop and a server-rerolled item can never drift out of sync with each
// other, and both automatically stay valid against validateGearItem() by construction.
function combatGenerateGearItem(tier, magicFindPct, monsterBand) {
  const chosenSlot = cbPick(ITEM_GENERATION_SLOTS);
  // v0.22.1 (Part A2d): only monster-kill drops pass a monsterBand (Merchant stock, Gambling
  // Goblin, admin spawn, etc. all still call this with no 3rd arg and get the flat roll).
  let rarity = monsterBand ? combatRollRarityForBand(monsterBand, magicFindPct) : combatRollRarity(magicFindPct);
  // v0.23.6 (Item 4): rarity is now rolled BEFORE baseName -- mirrors index.html's IF.generate()
  // reorder, since a "Set" rarity roll needs to override baseName selection to pull from
  // COMBAT_ITEM_SETS. If the slot has no populated set piece (e.g. pants/ring/amulet/boots
  // aren't Wizard Set slots), fall back to a Legendary roll instead.
  let baseName = null, setTag = null;
  if (rarity.name === "Set") {
    const candidates = combatSetPieceCandidatesForSlot(chosenSlot);
    if (candidates.length > 0) {
      const chosen = cbPick(candidates);
      baseName = chosen.base_name;
      setTag = { set_id: chosen.set_id, set_slot: chosenSlot };
    } else {
      rarity = COMBAT_RARITY_TABLE.find((r) => r.name === "Legendary") || rarity;
    }
  }
  if (!baseName) baseName = cbPick(COMBAT_BASE_NAMES[chosenSlot] || ["Item"]);
  const slotCount = rarity.slots;
  // v0.22.3 (#16): filter to only the affixes legal for this slot BEFORE rolling -- mirrors
  // IF.generate()'s identical filter client-side, so a server-rolled drop can never carry a
  // slot-illegal affix in the first place.
  // v0.22.6 (#14): routed through legalAffixesForSlotAndBaseName() so an "offhand" roll pulls
  // from the Shield or Orb pool (by baseName) instead of an undifferentiated "offhand" list.
  const legalPool = legalAffixesForSlotAndBaseName(chosenSlot, baseName);
  let pool = [...(legalPool.length ? legalPool : ITEM_AFFIX_POOL)];
  cbShuffle(pool);
  if (chosenSlot === "weapon" && Math.random() < CB.WEAPON_DAMAGE_GUARANTEE_CHANCE && pool.includes("damage")) {
    pool = pool.filter((s) => s !== "damage");
    pool.unshift("damage");
  }
  let element = "";
  if (Math.random() < CB.ELEMENT_ITEM_CHANCE) {
    element = cbPick(CB.ELEMENT_IDS);
    const biasStat = CB.ELEMENT_AFFIX_BIAS[element] || "";
    if (biasStat && pool.includes(biasStat)) {
      pool = pool.filter((s) => s !== biasStat);
      pool.unshift(biasStat);
    }
  }
  const affixCount = Math.min(slotCount, pool.length);
  const affixes = [];
  for (let i = 0; i < affixCount; i++) {
    const stat = pool[i];
    let value = rollAffixValue(stat, tier);
    if (chosenSlot === "weapon" && stat === "damage") value = round2(value * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    affixes.push({ stat, value });
  }
  // v0.22.3 (#16): Eyesight only ever rolls on head/amulet -- mirrors IF.generate()'s identical gate.
  if ((chosenSlot === "head" || chosenSlot === "amulet") && Math.random() < CB.EYESIGHT_AFFIX_CHANCE) {
    affixes.push({ stat: "eyesight", value: 2 });
  }
  return { instance_id: `srv_${Date.now()}_${Math.floor(Math.random() * 999999)}`, slot: chosenSlot, base_name: baseName, tier, rarity: rarity.name, affixes, element, ...(setTag ? { set_id: setTag.set_id, set_slot: setTag.set_slot } : {}) };
}

function combatTierLevelRequirement(tier) { const t = cbClampi(tier, 1, ITEM_TIER_MAX); return Math.max(1, (t - 1) * CB.ITEM_TIER_BRACKET_WIDTH); }
function combatWeaponTypeForInstance(inst) { return (inst && inst.base_name) ? (COMBAT_WEAPON_TYPE_BY_BASE_NAME[inst.base_name] || null) : null; }
// v0.22.6 (#11-14): offhand analogue of combatWeaponTypeForInstance.
function combatOffhandTypeForInstance(inst) { return (inst && inst.base_name) ? (COMBAT_OFFHAND_TYPE_BY_BASE_NAME[inst.base_name] || null) : null; }
function combatClassCanEquipItem(classId, inst) {
  if (!inst) return true;
  if (inst.slot === "weapon") {
    const wt = combatWeaponTypeForInstance(inst);
    if (!wt) return true;
    const list = COMBAT_WEAPON_TYPE_CLASS_RESTRICTIONS[wt];
    return !list || list.includes(classId);
  }
  // v0.22.6 (#13): mirrors the weapon branch above -- gates Orb to the wizard chain plus
  // emberpriest/druid, while Shield (no COMBAT_OFFHAND_TYPE_CLASS_RESTRICTIONS entry) stays
  // usable by every class.
  if (inst.slot === "offhand") {
    const ot = combatOffhandTypeForInstance(inst);
    if (!ot) return true;
    const list = COMBAT_OFFHAND_TYPE_CLASS_RESTRICTIONS[ot];
    return !list || list.includes(classId);
  }
  // v0.23.6: class-exclusive armor -- gated by chain, e.g. Wizard Hat requires
  // COMBAT_CLASSES[classId].chain==="wizard" (wizard/sorcerer/warlock/necromancer/archmage all pass).
  const requiredChain = COMBAT_CHAIN_EXCLUSIVE_BASE_NAMES[inst.base_name];
  if (requiredChain) {
    const c = COMBAT_CLASSES[classId];
    return !!(c && c.chain === requiredChain);
  }
  return true;
}
function combatCanEquipGear(data, inst) { return (data.level || 1) >= combatTierLevelRequirement(inst.tier) && combatClassCanEquipItem(data.class_id, inst); }
function combatAffixTotal(inst, stat) {
  if (!inst || !inst.affixes) return 0;
  let total = 0; for (const a of inst.affixes) if (a.stat === stat) total += a.value;
  return total;
}
function combatGearBonus(data, stat) {
  let total = 0;
  const equipped = data.equipped || {};
  for (const slot of COMBAT_EQUIPPED_SLOT_KEYS) {
    const inst = equipped[slot];
    if (inst && combatCanEquipGear(data, inst)) {
      total += combatAffixTotal(inst, stat);
      // v0.23.6: class-exclusive armor's inherent attribute-scaling bonus -- same formula for
      // every wearer, reads THIS character's own attribute (not class-conditional), stacks
      // additively with any rolled affix of the same stat above. combatGetTotalAttr() is safe
      // to call here (not infinite recursion): it calls combatGearBonus(data, gearStat) with
      // gearStat in {"strength","dexterity","vitality","intelligence"} -- a namespace that
      // never matches an inherent.stat value ("armor"/"spell_power"/"attack_speed"/
      // "block_chance"), so the nested call's own inherent branch is always a no-op.
      const inherent = combatInherentScalingFor(inst);
      if (inherent && inherent.stat === stat) {
        total += (combatGetTotalAttr(data, inherent.attr) / 50) * inherent.per50;
      }
    }
  }
  return total;
}
function combatGetActiveTempBuff(data, type) {
  const b = data.temp_buffs && data.temp_buffs[type];
  if (b && Date.now() < b.expires_at) return b.amount;
  return 0;
}
function combatGetTotalAttr(data, key) {
  const gearStat = { str: "strength", dex: "dexterity", vit: "vitality", int: "intelligence" }[key];
  // v-quest: quest_attr_bonus is a SEPARATE additive layer on top of data.attributes (see the
  // Quest System section's DATA-MODEL NOTE, above applyLadderReset()) -- folded in HERE, the
  // one choke point str/dex/int already flow through for damage/armor/block chance, so a
  // quest-allocated point actually does something in combat without duplicating any of those
  // formulas.
  const questBonus = (data.quest_attr_bonus && data.quest_attr_bonus[key]) || 0;
  return ((data.attributes && data.attributes[key]) || 0) + questBonus + combatGearBonus(data, gearStat) + combatGetActiveTempBuff(data, gearStat);
}
// v0.20 (#11): now also sums the "block_chance" gear affix (see ITEM_AFFIX_TIER1_MAX's
// comment, +2/4/6/8/10% by tier) on top of the Dexterity-derived and temp-buff amounts --
// mirrors index.html's PS.getBlockChance() exactly.
function combatGetBlockChance(data) {
  return cbClampf(combatGetTotalAttr(data, "dex") * CB.BLOCK_CHANCE_PER_DEX + combatGetActiveTempBuff(data, "block_chance") / 100.0 + combatGearBonus(data, "block_chance") / 100.0, 0, CB.BLOCK_CHANCE_MAX);
}
// v0.21 (#6): sums the "flee_chance" gear affix (+2/4/6/8/10% by tier, see
// ITEM_AFFIX_TIER1_MAX). Deliberately NOT clamped -- per Gwen's exact spec, a total above 100%
// should always let the player flee successfully. Mirrors index.html's PS.getFleeChanceBonus().
function combatGetFleeChanceBonus(data) {
  return combatGearBonus(data, "flee_chance") / 100.0;
}
// v0.23.0 (Part B8): raw "spell_power" gear-affix sum (already a percent value per its
// ITEM_AFFIX_TIER1_MAX comment, 2/4/6/8/10 by tier) -- server.js has no generic
// affixIsPercent() registry like index.html's Balance.affixIsPercent() (that's a display-only
// lookup table); every affix's percent-ness is implicit in whichever combatGet*() consumer
// divides its combatGearBonus() sum by 100, exactly like combatGetBlockChance()/
// combatGetFleeChanceBonus() just above. Feeds combatGetSpellEffectivenessMult() below.
// v0.28 (credit: Gwen): Intelligence now feeds Spell Power directly, which is what gives a
// caster a build to grow rather than a shopping list of spell tiers to buy. It mirrors exactly
// how Strength feeds weapon damage: a flat percentage per point, from every source (assigned
// points, level-derived points, quest points, gear affixes, a Potion of Intellect), read through
// the same combatGetTotalAttr() choke point.
//
// The rate is deliberately class-dependent and uses the SAME three bands as the mana grant, so a
// path's relationship with Intelligence is one fact rather than two: the arcane path gets the
// most, the two hybrids that drift into Intelligence get an intermediate rate, and a Strength or
// Dexterity class still gets something for the points it spends there but never enough to make
// Intelligence its best investment.
function combatSpellPowerPerIntelligence(data) {
  if (CB.ARCANE_CLASS_IDS.includes(data.class_id)) return CB.SPELL_POWER_PCT_PER_INT_ARCANE;
  if (CB.INT_HYBRID_CLASS_IDS.includes(data.class_id)) return CB.SPELL_POWER_PCT_PER_INT_HYBRID;
  return CB.SPELL_POWER_PCT_PER_INT;
}
function combatGetSpellPowerPct(data) {
  let pct = combatGearBonus(data, "spell_power");
  pct += combatGetTotalAttr(data, "int") * combatSpellPowerPerIntelligence(data);
  // v0.23.6 (Item 4): Wizard Set 3pc/4pc bonuses -- mirrors index.html's PS.getSpellPowerPct().
  const wiz = combatSetBonusesFor(data, "wizard_set");
  if (wiz.count >= 3) pct += CB.WIZARD_SET_3PC_SPELL_POWER_PCT;
  if (wiz.count >= 4) pct += combatGetMaxMana(data) * CB.WIZARD_SET_4PC_SPELL_POWER_PCT_PER_MANA;
  return pct;
}
// v0.20 (#9.7): Vitality's HP/Stamina contribution is computed LIVE from attributes.vit
// (see CB.VIT_HP_PER_POINT_RATIO's comment) instead of reading the old bonus_hp_from_
// attributes/bonus_stamina_from_attributes accumulator fields, which are no longer written
// to anywhere (see combatAddXp()) but are left untouched in the data model for old saves.
function combatVitHpBonus(classData, vit) { return (vit || 0) * (classData.hp_per_level || 5) * CB.VIT_HP_PER_POINT_RATIO; }
function combatVitStaminaBonus(classData, vit) { return (vit || 0) * (classData.hp_per_level || 5) * CB.VIT_STAMINA_PER_POINT_RATIO; }
function combatGetMaxHp(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  // v0.24.2 BUG FIX (credit: Gwen): this used to read data.attributes.vit directly, on purpose,
  // which meant gear Vitality contributed NOTHING to max HP -- swapping in a +Vitality piece
  // visibly changed the Character Sheet's Vitality number while the health bar stayed exactly
  // where it was. Vitality grants HP for every class, so it has to count from wherever it
  // comes. Now reads combatGetTotalAttr(data, "vit"), the same choke point Strength/Dexterity/
  // Intelligence already flow through for damage/armor/block, which folds in base attributes,
  // the quest_attr_bonus layer, gear affixes AND active attribute potions in one place.
  const vit = combatGetTotalAttr(data, "vit");
  // v0.23.0 (Part A4): +CB.LEVEL_UP_HP_BONUS_PER_LEVEL per level above 1, computed live from
  // the current level (never a stored accumulator) so it naturally zeroes out on a level-1
  // reset -- mirrors hp_per_level's own (level-1) term right beside it.
  const levelUpBonus = CB.LEVEL_UP_HP_BONUS_PER_LEVEL * ((data.level || 1) - 1);
  // v-quest: quest_bonus_hp is the Quest System's town-scoped permanent max-HP reward (e.g.
  // the "explore"/"wanderers" quests) -- added as its own flat term, cleared on a town change
  // by questRebuildBoardForTown() alongside quest_attr_bonus/unspent_quest_stat_points.
  const flat = (c.base_hp || 50) + (c.hp_per_level || 5) * ((data.level || 1) - 1) + levelUpBonus + combatGearBonus(data, "hp") + combatVitHpBonus(c, vit) + (data.quest_bonus_hp || 0);
  // v0.24.2 BUG FIX (credit: Gwen): "Max Health %" (max_health_pct) rolled on gear and showed
  // in tooltips but had no consumer anywhere -- a +1.35% piece did literally nothing. It is a
  // MULTIPLIER on the finished flat total (so it scales with everything that fed into it),
  // applied last, exactly once. Rounded because HP is only ever a whole number -- see the
  // integer-only note on combatMonsterHp().
  return Math.round(flat * (1 + combatGearBonus(data, "max_health_pct") / 100.0));
}
function combatGetMaxStamina(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  // v0.24.2: same fix as combatGetMaxHp() just above -- gear/quest/potion Vitality now counts
  // toward the Stamina pool too, instead of base attributes only.
  const vit = combatGetTotalAttr(data, "vit");
  return Math.round(CB.STAMINA_MAX_BASE + combatGearBonus(data, "stamina_max") + combatVitStaminaBonus(c, vit));
}
// v0.23.0 (Part A4): the level-derived half of the future Mana resource (base per-class amount,
// current_mana tracking, and regen land with Part B) -- lands now so Part B's mana pool inherits
// +CB.LEVEL_UP_MANA_BONUS_PER_LEVEL per level for free, same live-from-level, no-stored-
// accumulator pattern as combatGetMaxHp() above. c.base_mana is undefined until Part B adds it
// per class, so this is purely gear + level-up bonus until then (mirrors index.html's dormant
// "mana"/"mana_regen" gear affixes, which already read real values with no effect yet).
function combatGetMaxMana(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  const levelUpBonus = CB.LEVEL_UP_MANA_BONUS_PER_LEVEL * ((data.level || 1) - 1);
  // v0.25 (credit: Gwen): Intelligence now grants mana for EVERY class, from every source --
  // level-derived points, manually assigned points, quest-granted points, gear affixes and
  // Potion of Intellect alike. combatGetTotalAttr() is exactly the choke point that sums all
  // of those, the same way combatGetMaxHp() reads Vitality through it.
  const manaPerInt = CB.ARCANE_CLASS_IDS.includes(data.class_id) ? CB.MANA_PER_INTELLIGENCE_ARCANE
    : CB.INT_HYBRID_CLASS_IDS.includes(data.class_id) ? CB.MANA_PER_INTELLIGENCE_HYBRID
    : CB.MANA_PER_INTELLIGENCE;
  const intMana = combatGetTotalAttr(data, "int") * manaPerInt;
  return Math.round((c.base_mana || 0) + levelUpBonus + intMana + combatGearBonus(data, "mana"));
}
// v0.22 (batch2 #5): Health Regen (the "regen" affix key, unchanged internally -- only its
// Tier-1 max and display label changed) and the new sibling "stamina_regen" affix. Mirrors
// index.html's PS.getRegen()/getStaminaRegen() exactly -- must stay in sync with those. Class
// base regen (COMBAT_CLASSES) feeds Health Regen only; base Stamina Regen is always 0 unless
// a future patch adds per-class values.
function combatGetRegen(data) { const c = COMBAT_CLASSES[data.class_id] || {}; return (c.regen || 0.0) + combatGearBonus(data, "regen"); }
function combatGetStaminaRegen(data) { return combatGearBonus(data, "stamina_regen"); }
// v0.23.0 (Part B2): CB.MANA_REGEN_PER_SEC (flat, same for every class) plus the "mana_regen"
// gear affix -- mirrors combatGetStaminaRegen()'s shape exactly. Ticked in the SAME places
// (combatTickCombatRoundBuffs() below, once per combat round; index.html's town/maze
// setInterval, once per real second) so mana regen follows identical town/maze/combat rules.
function combatGetManaRegen(data) {
  let regen = CB.MANA_REGEN_PER_SEC + combatGearBonus(data, "mana_regen");
  // v0.23.6 (Item 4): Wizard Set 2pc bonus -- mirrors index.html's PS.getManaRegen() (multiplicative).
  if (combatSetBonusesFor(data, "wizard_set").count >= 2) regen *= (1 + CB.WIZARD_SET_2PC_MANA_REGEN_PCT / 100);
  return regen;
}
// v0.23.0 (Part B1): current_mana is read with the same "|| 0" fallback current_hp/
// current_stamina already use everywhere, EXCEPT there's no historical save that ever had a
// real value here -- an old character predating this patch has no `current_mana` field at
// all. Falling back to 0 would hand every existing character an empty mana bar forever (never
// full until a mana potion/regen tick nudges it up from 0). This mirrors the lazy-hydration
// pattern other new per-character fields use (e.g. forest_reputation defaulting to {} on
// read): the FIRST time a request touches a character with no current_mana field, it's
// hydrated to full and written back onto `data` so the very next save persists it for good.
function combatGetCurrentMana(data) {
  const maxMana = combatGetMaxMana(data);
  if (data.current_mana == null) { data.current_mana = maxMana; return maxMana; }
  return Math.round(Math.min(data.current_mana, maxMana));
}
function combatGetEquippedWeaponType(data) { return combatWeaponTypeForInstance(data.equipped && data.equipped.weapon); }
function combatWeaponSkillCumulativeHitsForLevel(level) { return CB.WEAPON_SKILL_HIT_STEP * level * (level + 1) / 2; }
function combatWeaponSkillLevelForHits(hits) {
  let level = 0;
  while (combatWeaponSkillCumulativeHitsForLevel(level + 1) <= hits) level++;
  return level;
}
function combatGetWeaponSkillLevel(data, weaponType) {
  const hits = (data.weapon_skills && data.weapon_skills[weaponType] && data.weapon_skills[weaponType].hits) || 0;
  return combatWeaponSkillLevelForHits(hits);
}
// v0.23.0 (Part B9): Spellcasting proficiency -- a single, non-weapon-typed skill mirroring
// the weapon-skill hit-counter/level-curve exactly (reuses combatWeaponSkillLevelForHits(),
// i.e. the same CB.WEAPON_SKILL_HIT_STEP cumulative-hits-per-level formula, rather than a
// second copy of the curve), incremented once per successful spell cast (see
// combatRegisterSpellcastHit(), called from POST /api/combat/:sessionId/cast below) instead
// of once per weapon hit. Storage mirrors a single weapon_skills[weaponType] entry's {hits}
// shape, just not keyed by type since there's only the one skill:
// data.spellcasting_skill = {hits: N}. Persists through bridge/ladder resets exactly like
// weapon_skills already does -- applyTrialResolutionReset()/applyLadderReset() never touch
// weapon_skills, and this field isn't referenced by either of them either.
function combatGetSpellcastingSkillLevel(data) {
  return combatWeaponSkillLevelForHits((data.spellcasting_skill && data.spellcasting_skill.hits) || 0);
}
function combatRegisterSpellcastHit(data) {
  if (!data.spellcasting_skill) data.spellcasting_skill = { hits: 0 };
  const before = combatWeaponSkillLevelForHits(data.spellcasting_skill.hits);
  data.spellcasting_skill.hits++;
  const after = combatWeaponSkillLevelForHits(data.spellcasting_skill.hits);
  return { leveledUp: after > before, newLevel: after };
}
// Grants +1% spell effectiveness per level -- reuses CB.WEAPON_SKILL_DAMAGE_PCT_PER_LEVEL,
// the same 1%/level curve weapon skills already use for their own damage bonus, per Gwen's
// spec ("this project uses 1%/level, don't invent a different curve").
function combatGetSpellcastingEffectivenessPct(data) {
  return combatGetSpellcastingSkillLevel(data) * CB.WEAPON_SKILL_DAMAGE_PCT_PER_LEVEL * 100;
}
// v0.23.0 (Part B5): 1 + SpellPower%/100 + SpellcastingSkill%/100, additively stacked before
// applying to a spell's base magnitude. Durations/cooldowns and Entangle's root/flee-override
// are NEVER scaled by this (pure utility, no magnitude to scale) -- only Heal's restore
// amount and Fireflies' per-hit damage are, per Gwen's exact spec.
function combatGetSpellEffectivenessMult(data) {
  return 1 + combatGetSpellPowerPct(data) / 100 + combatGetSpellcastingEffectivenessPct(data) / 100;
}
// v0.28: the player's total Spell Cooldown Reduction, clamped to the cap. Kept as its own
// function so the client can mirror the identical clamp -- if the two disagreed, the client would
// offer a cast the server then refuses as still on cooldown.
function combatGetSpellCdrPct(data) {
  return cbClampf(combatGearBonus(data, "spell_cdr"), 0, CB.SPELL_CDR_MAX_PCT);
}
// A spell's effective cooldown for this character, in milliseconds.
function combatGetSpellCooldownMs(data, baseCooldownMs) {
  return Math.round((baseCooldownMs || 0) * (1 - combatGetSpellCdrPct(data) / 100));
}
function combatGetDamageRange(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  const perLevel = (c.damage_per_level || 1.0) * ((data.level || 1) - 1);
  const gearBonus = combatGearBonus(data, "damage");
  let statBonus = 0;
  if (c.chain === "thornguard") statBonus = combatGetTotalAttr(data, "str");
  else if (c.chain === "windrider") statBonus = combatGetTotalAttr(data, "dex");
  // v0.22 (batch2 #3): Wizard chain's Intelligence damage term doubled from x1 to x2 per point
  // (CB.WIZARD_INT_DAMAGE_PER_POINT) -- must stay in sync with index.html's
  // PS.getDamageBreakdown()/getAttackDamage(), which apply the identical x2 multiplier.
  // v0.29 (credit: Gwen): Intelligence no longer feeds WEAPON damage -- it feeds Spell Power (see
  // combatGetSpellPowerPct), and driving both meant a point of Intelligence made a Wizard better
  // at swinging a stick as well as at casting. The Wizard chain therefore has no attribute term
  // in its physical damage at all now; its weapon hits are base plus gear only. Must stay in sync
  // with PS.getDamageBreakdown() in index.html, which drops the same term.
  // v0.19.1 (#14): % damage bonus applied multiplicatively to the whole base+gear total,
  // mirroring PS.getDamageBreakdown() in index.html -- must stay in sync with that function.
  const weaponType = combatGetEquippedWeaponType(data);
  const skillLevel = weaponType ? combatGetWeaponSkillLevel(data, weaponType) : 0;
  const weaponSkillPct = skillLevel * CB.WEAPON_SKILL_DAMAGE_PCT_PER_LEVEL;
  const baseMin = (c.base_damage_min || 1) + perLevel + statBonus;
  const baseMax = (c.base_damage_max || 2) + perLevel + statBonus;
  const preSkillMin = baseMin + gearBonus, preSkillMax = baseMax + gearBonus;
  return [preSkillMin * (1 + weaponSkillPct), preSkillMax * (1 + weaponSkillPct)];
}
function combatGetCritChance(data) { const c = COMBAT_CLASSES[data.class_id] || {}; return cbClampf((c.base_crit || 0.05) + combatGearBonus(data, "crit") / 100.0, 0, 0.95); }
// v0.19.1 (#10): monsters now crit too -- 10% base at area level 1, +1% per additional level
// (a level 24 mob: 10 + 23 = 33%), per Gwen's exact spec. Used for both the monster's
// counter-attack (combatResolveMonsterTurn) and the pre-fight "strikes first" roll in
// POST /api/combat/start -- both use the fight's own area_level as the monster's level.
// v0.22.7 (#18): halved -- was 10% + 1%/level (L1=10%, L25=34%), now 5% + 0.5%/level
// (L1=5%, L25=17%). Mirrors index.html's Balance.monsterCritChance() exactly.
function combatGetMonsterCritChance(areaLevel) { return cbClampf(0.05 + (Math.max(1, areaLevel) - 1) * 0.005, 0, 0.95); }
// v0.21 (#8): the NEW stat -- how hard a crit hits, separate from combatGetCritChance()'s
// odds of landing one at all. Raw gear sum divided by 100 (see ITEM_AFFIX_TIER1_MAX's
// crit_multiplier comment), added on top of the shared CB.CRIT_MULTIPLIER floor. Mirrors
// index.html's PS.getCritMultiplierBonus()/getCritMultiplierMax() exactly.
function combatGetCritMultiplierBonus(data) { return combatGearBonus(data, "crit_multiplier") / 100.0; }
function combatGetCritMultiplierMax(data) { return CB.CRIT_MULTIPLIER + combatGetCritMultiplierBonus(data); }
// v0.21.1 (#11): the player's own attacks/sec -- base 1.0, increased by the "attack_speed"
// gear affix (a plain % sum, same shape every other percentage-based gear stat uses). Drives
// the client's mousedown hold-to-fire auto-repeat interval (see startCombatSession()'s
// response, which echoes this back) -- mirrors index.html's PS.getAttackSpeed() exactly.
function combatGetAttackSpeed(data) { return CB.PLAYER_BASE_ATTACK_SPEED * (1 + combatGearBonus(data, "attack_speed") / 100.0); }
// v0.21.1 (#12): monster attack speed now scales in breakpoints every 5 area levels (same
// cadence as the existing crit-multiplier scaling) -- a level 25 monster (5 full brackets)
// attacks 25% faster than the level-1 default of 1/sec. Mirrors index.html's
// Balance.monsterAttackSpeed() exactly.
function combatMonsterAttackSpeed(areaLevel) {
  const brackets = Math.floor(Math.max(1, areaLevel || 1) / 5);
  return CB.MONSTER_BASE_ATTACK_SPEED * (1 + brackets * (CB.MONSTER_ATTACK_SPEED_PCT_PER_5_LEVELS / 100));
}
// v0.22.7 (#18): replaces the old fixed-floor/rising-ceiling model (v0.21 #9) with a RISING
// floor AND a rising ceiling, both driven by `bracket = floor(areaLevel/5)`:
//   floor   = 1.5 + 0.25 * floor(bracket/2)   (rises every OTHER bracket, i.e. every 10 levels)
//   ceiling = 2.0 + 0.25 * bracket            (rises every bracket, i.e. every 5 levels)
// Verification against Gwen's spec: L1 (bracket 0) 1.5-2.0 -- L5 (bracket 1) 1.5-2.25 --
// L10 (bracket 2) 1.75-2.5 -- L15 (bracket 3) 1.75-2.75 -- L20 (bracket 4) 2.0-3.0 --
// L25 (bracket 5) 2.0-3.25 -- L30 (bracket 6) 2.25-3.5. Uncapped past L30 by design (level cap
// is 100); a ceiling cap can be added later if very-high-level crits feel too spiky. Mirrors
// index.html's Balance.monsterCritFloor()/monsterCritCeiling() exactly.
// v0.24.1 (C5): base shifted down 0.25 (was 1.5/2.0) -- the per-bracket growth increments are
// untouched, just the floor of the whole ladder. Must stay in sync with index.html's identical
// pair of functions.
function combatGetMonsterCritFloor(areaLevel) {
  const bracket = Math.floor(Math.max(1, areaLevel) / 5);
  return 1.25 + 0.25 * Math.floor(bracket / 2);
}
function combatGetMonsterCritCeiling(areaLevel) {
  const bracket = Math.floor(Math.max(1, areaLevel) / 5);
  return 1.75 + 0.25 * bracket;
}
// v0.21 (#10): a landed crit doesn't always hit for the full ceiling -- it's a fresh random
// roll each time, somewhere between CB.CRIT_MULTIPLIER (the player's own floor) and the
// player's combatGetCritMultiplierMax(data), per Gwen's exact spec: "a player with 5.50
// crit multiplier can crit randomly between 1.5 and 5.50, not 5.50 all the time." PLAYER-ONLY
// as of v0.22.7 (#18) -- see combatRollMonsterCritMultiplier() below for the monster path,
// which no longer reuses CB.CRIT_MULTIPLIER as its floor.
function combatRollCritMultiplier(maxMult) { return CB.CRIT_MULTIPLIER + Math.random() * Math.max(0, maxMult - CB.CRIT_MULTIPLIER); }
// v0.22.7 (#18): monster-specific crit roll -- unlike the player's combatRollCritMultiplier()
// above, a monster's floor is NOT the shared CB.CRIT_MULTIPLIER constant; it's its own
// area-level-dependent value from combatGetMonsterCritFloor(). Takes both bounds explicitly
// so it can never accidentally fall back to the player's floor.
function combatRollMonsterCritMultiplier(floor, ceiling) { return floor + Math.random() * Math.max(0, ceiling - floor); }
// v0.20.4: Armor rework -- used to be a 0-0.75 PERCENTAGE (class-innate base + gear%),
// multiplicatively reducing monster damage (`mdmg *= 1-armor`, see the two call sites
// below). Now a FLAT number: gear's "armor" affix (flat 2/4/6/8/10 per tier, see
// ITEM_AFFIX_TIER1_MAX's comment) plus total Strength * CB.STR_TO_ARMOR_RATIO, for every
// class chain -- subtracted directly from incoming damage instead, floored at 0, mirrors
// index.html's PS.getArmor() exactly. No upper clamp -- unlike the old percentage (which
// had to cap below 100% or damage could go negative), a flat subtraction can never turn a
// hit into healing on its own; combatResolveMonsterTurn()/the first-strike branch below
// both apply Math.max(0, mdmg-armor) as the actual floor.
function combatGetArmor(data) {
  return combatGearBonus(data, "armor") + combatGetTotalAttr(data, "str") * CB.STR_TO_ARMOR_RATIO;
}
function combatGetMagicFind(data, session) {
  // v0.20 BUG FIX: this used to read combatGetActiveTempBuff(data, "magic_find") -- the OLD
  // wall-clock temp-buff bag Magic Find moved off of in v0.20 (#7) -- see CB.SHRINE_MAGIC_FIND_PCT's
  // comment for why that silently zeroed the shrine's effect on server-resolved loot.
  const shrineBonus = (data.magic_find_rounds_left || 0) > 0 ? CB.SHRINE_MAGIC_FIND_PCT : 0;
  // Maze restyle (night delve): session is the server-tracked combat_sessions row, which now
  // carries the client-asserted is_night flag from /api/combat/start (see the SCOPE NOTE ON MAZE
  // LEGITIMACY comment above the combat_sessions table) -- the bonus itself is still applied here,
  // server-side, so it can't be forged past this point even though the flag's origin is trusted.
  const nightBonus = session && session.is_night ? CB.NIGHT_MAGIC_FIND_BONUS_PCT : 0;
  return cbClampf(combatGearBonus(data, "magic_find") + shrineBonus + nightBonus + (data.kill_streak || 0) * CB.KILL_STREAK_MAGIC_FIND_PCT_PER_KILL, 0, CB.MAGIC_FIND_CAP_PCT);
}
function combatGetGoldFindMult(data, session) {
  const nightBonus = session && session.is_night ? CB.NIGHT_GOLD_FIND_BONUS_PCT : 0;
  return 1.0 + (combatGearBonus(data, "gold_find") + nightBonus) / 100.0;
}
// v0.20 (#9.3): XP Find is expressed as a plain percentage (gear affixes already store it
// that way), so it plugs directly into combatGetTotalXpBonusPct()'s additive stack without
// any 1+x/100 conversion -- unlike Gold Find, which still stays its own independent
// multiplier (only XP bonuses were asked to become cumulative, not every "Find" stat).
function combatGetXpFindPct(data) { return combatGearBonus(data, "xp_find"); }
// v0.20 (#9.3): the Experience Shrine buff (SHRINE_XP_BUFF_MULT=1.5, i.e. "+50% XP") is
// still stored/persisted as a multiplier (data.xp_buff_multiplier) for backward
// compatibility with already-saved characters -- converting it to a percentage here, right
// at the point it joins the additive stack, avoids a persisted-field rename that could
// silently drop an in-flight buff for any player who happens to have one active the moment
// this update deploys (see this function's caller for the full stacking rationale).
function combatGetShrineXpPct(data) { return (data.xp_buff_encounters_left > 0) ? ((data.xp_buff_multiplier || 1) - 1) * 100 : 0; }
function combatGetForestReputationXpPct(data) {
  const tier = data.highest_tier_reached || 1;
  const rep = (data.forest_reputation && data.forest_reputation[tier]) || 0;
  return Math.max(0, rep) * CB.FOREST_REPUTATION_XP_PCT_PER_POINT;
}
// v0.18.2 (#7)'s "Currently playing" community XP bonus -- the server already knows the
// live active-player count (getActiveCharacters(), defined further down this file but
// hoisted, so callable here), so this reads that directly instead of trusting the client's
// own Net.activePlayersCache the way index.html's getGlobalXpMultiplier() has to.
// v0.20 (#9.2): used to be "+100% per EXTRA player" (the raw headcount WAS the multiplier --
// 3 players = 3x). Became "+10% per EXTRA player" (2 players = +10%, 4 players = +30%, ...).
// v0.20.1 (#25) CORRECTION: Gwen's actual spec is the raw headcount itself times 10%, not
// headcount-minus-one -- "2 players online" should read +20%, not +10% -- while a solo
// player (nobody else online) still gets exactly 0%, same as before (see the solo-player
// message added back in v0.19.2 #11). So: 1 player -> 0%, 2 players -> 20%, 3 -> 30%, etc.
function combatGetCommunityXpPct() {
  const n = Math.max(1, getActiveCharacters(100).length);
  if (n < 2) return 0;
  return n * CB.COMMUNITY_XP_PCT_PER_EXTRA_PLAYER;
}
// v0.20 (#9.3): EVERY XP bonus source now stacks ADDITIVELY (summed percentages, applied
// once) instead of multiplicatively (each factor compounding on the last). Before this, a
// player with +20% XP Find gear, an active Experience Shrine (+50%), 3 other people online
// (previously a flat 4x from combatGetCommunityXpMult()), and +10 Forest Reputation
// (previously +100%) would have multiplied ALL of those together into a wildly compounding
// number (1.2 * 1.5 * 4 * 2 = 14.4x) -- Gwen's spec calls that out explicitly as too
// swingy/exploitable. The additive model instead sums the four percentages (20+50+300+100=
// 470% under the OLD per-source magnitudes, or a much saner 20+50+30+10=110% under the NEW
// v0.20 #9.1/#9.2 magnitudes) and applies that ONE combined bonus once, so no single stacked
// combination of buffs can multiply the others' effect the way compounding factors could.
function combatGetTotalXpBonusPct(data) {
  return combatGetXpFindPct(data) + combatGetShrineXpPct(data) + combatGetCommunityXpPct() + combatGetForestReputationXpPct(data);
}
function combatIsInvulnerable(data) { return (data.invuln_rounds_left || 0) > 0; }
function combatHasQuadDamage(data) { return (data.quad_dmg_rounds_left || 0) > 0; }
// v0.20 BUG FIX (credit: dcfroggert): a player reported that Touch of Unicorn
// (Invulnerability) never expired as long as they avoided maze traps -- it turns out this
// DID tick down correctly right here,
// on the server, every real combat round (see the /attack and /flee handlers below), but the
// decremented value was never sent back to the client. The client's OWN copy of
// invuln_rounds_left (in PS, held in the browser) stayed frozen at whatever it was when the
// shrine was triggered, because nothing in screenCombat() ever wrote the server's answer back
// into it -- and every autosave() after a combat round (see index.html) pushes that stale,
// un-decremented client copy right back up to the server, silently undoing the tick that had
// just happened moments earlier in this very request. Net effect: as long as combat was the
// only thing ticking it (maze traps have their own separate, correctly-synced decrement path
// in onStepMaze()), the shrine was permanent. The exact same blind spot affected Quad Damage
// AND Magic Find too -- neither had ANY other decrement path at all, so once triggered they
// never expired, full stop, combat or no combat.
// The fix is two-sided: this function now also ticks magic_find_rounds_left (previously only
// invuln/quad damage were even considered "combat round" buffs here), and every combat
// endpoint below now echoes the current post-tick values back in its `player` payload (see
// combatRoundBuffsPayload()) so the client can adopt them as the one authoritative source
// instead of trusting its own untouched copy.
function combatTickCombatRoundBuffs(data) {
  if (data.invuln_rounds_left > 0) data.invuln_rounds_left--;
  if (data.quad_dmg_rounds_left > 0) data.quad_dmg_rounds_left--;
  if (data.magic_find_rounds_left > 0) data.magic_find_rounds_left--;
  // v0.22 (batch2 #5): apply Health/Stamina Regen gear-affix ticks once per combat round.
  // Combat HP/Stamina is server-authoritative, so a client-only regen tick during combat
  // would desync -- this mirrors the client's town/maze wall-clock regen tick (see the
  // setInterval() in index.html) but keyed to combat rounds instead of real seconds. Every
  // combat endpoint below already echoes data.current_hp/current_stamina back in its
  // `player` payload, so the client picks this up automatically as the authoritative value.
  const regen = combatGetRegen(data);
  if (regen > 0) data.current_hp = Math.min(combatGetMaxHp(data), (data.current_hp || 0) + regen);
  const staminaRegen = combatGetStaminaRegen(data);
  if (staminaRegen > 0) data.current_stamina = Math.min(combatGetMaxStamina(data), (data.current_stamina || 0) + staminaRegen);
  // v0.23.0 (Part B2): Mana Regen ticks once per combat round here, same as HP/Stamina Regen
  // just above -- combatGetCurrentMana(data) both lazily hydrates a legacy character's missing
  // current_mana to full AND clamps it, so this can never push current_mana above max.
  const manaRegen = combatGetManaRegen(data);
  if (manaRegen > 0) data.current_mana = Math.round(Math.min(combatGetMaxMana(data), combatGetCurrentMana(data) + manaRegen));
}
// v0.24.1 (C3): HP/Mana/Stamina regen must visibly tick DURING combat, not just once per
// "round" (attack/failed-flee, see combatTickCombatRoundBuffs() above) -- most of a real fight
// now passes through /use-item, /cast, the idle /tick poll, and the WS combat-tick pusher,
// none of which used to call any regen tick at all. A flat per-call increment (like
// combatTickCombatRoundBuffs' regen lines) would badly over- or under-tick depending on how
// often each of THOSE fires (the WS pusher alone is every 500ms vs. town's real per-second
// rate), so this is wall-clock elapsed-time-based instead -- same "advance a persisted
// timestamp, apply rate * elapsed-seconds" shape combatCatchUpMonsterHits() already uses for
// the monster's own attack clock. Deliberately kept SEPARATE from combatTickCombatRoundBuffs()
// (which still only fires on a real round) so calling this from every endpoint below can't
// accidentally speed up how fast Invulnerability/Quad Damage/Magic Find round-counters expire.
// Capped at CB.COMBAT_REGEN_MAX_CATCHUP_SEC of elapsed time so a long-abandoned-then-resumed
// session can't award a huge lump-sum regen burst. Mutates session.last_regen_tick_at in
// place; the caller is responsible for persisting it via its own updateCombatSession() call.
function combatTickRegenElapsed(data, session) {
  const now = Date.now();
  const lastAt = session.last_regen_tick_at || now;
  const elapsedSec = Math.min(Math.max(0, (now - lastAt) / 1000), CB.COMBAT_REGEN_MAX_CATCHUP_SEC);
  session.last_regen_tick_at = now;
  if (elapsedSec <= 0) return;
  const regen = combatGetRegen(data);
  if (regen > 0) data.current_hp = Math.min(combatGetMaxHp(data), (data.current_hp || 0) + regen * elapsedSec);
  const staminaRegen = combatGetStaminaRegen(data);
  if (staminaRegen > 0) data.current_stamina = Math.min(combatGetMaxStamina(data), (data.current_stamina || 0) + staminaRegen * elapsedSec);
  const manaRegen = combatGetManaRegen(data);
  if (manaRegen > 0) data.current_mana = Math.min(combatGetMaxMana(data), combatGetCurrentMana(data) + manaRegen * elapsedSec);
}
// Shared fragment merged into every combat endpoint's `player` response object so the client
// always has a fresh, authoritative snapshot of all 3 round-based shrine buffs to overwrite
// its own local copy with -- see the comment above combatTickCombatRoundBuffs() for why that
// matters even on endpoints (like use-item) that don't tick anything themselves.
// v0.23.0 (Part B1/B2): also carries current_mana/max_mana -- folding these into this SAME
// shared fragment (rather than editing every "player: {...}" object below individually) means
// every combat endpoint's response picks up the mana pool for free, exactly like it already
// does for the three shrine-buff counters. combatGetCurrentMana(data) lazily hydrates/clamps.
function combatRoundBuffsPayload(data) {
  return {
    invuln_rounds_left: data.invuln_rounds_left || 0,
    quad_dmg_rounds_left: data.quad_dmg_rounds_left || 0,
    magic_find_rounds_left: data.magic_find_rounds_left || 0,
    current_mana: cbInt(combatGetCurrentMana(data)),
    max_mana: combatGetMaxMana(data),
  };
}

// Lazy heal-over-time settlement -- the "hard part" server_authoritative_combat_design.md
// flags: rather than a 200ms tick loop (impractical over HTTP request/response), each heal
// is stored as {rate (HP per ms), remainingMs, lastSettledAt}, and settled (delivered amount
// applied, remainingMs/lastSettledAt advanced) lazily every time a combat request touches
// this character -- mathematically identical to the client's own perTick/ticksLeft tick
// array, just computed on demand instead of every 200ms.
function combatSettleHeal(heal) {
  if (!heal) return { delivered: 0, heal: null };
  const now = Date.now();
  let elapsed = Math.max(0, now - heal.lastSettledAt);
  elapsed = Math.min(elapsed, heal.remainingMs);
  const delivered = heal.rate * elapsed;
  const remainingMs = heal.remainingMs - elapsed;
  if (remainingMs <= 0) return { delivered, heal: null };
  return { delivered, heal: { rate: heal.rate, remainingMs, lastSettledAt: now } };
}
// Mirrors PS._queueGradualHeal()'s merge-not-stack fix: a heal already in flight has its
// pending amount extended (capped at headroom) at the SAME rate, rather than starting a
// second, independent stream alongside it.
function combatQueueHeal(existingHeal, amount, headroom, durationMs) {
  if (headroom <= 0) return existingHeal || null;
  const now = Date.now();
  if (existingHeal) {
    const currentPending = existingHeal.rate * existingHeal.remainingMs;
    const newPending = Math.min(headroom, currentPending + amount);
    return { rate: existingHeal.rate, remainingMs: existingHeal.rate > 0 ? newPending / existingHeal.rate : 0, lastSettledAt: now };
  }
  const rate = amount / durationMs;
  const pending = Math.min(headroom, amount);
  return { rate, remainingMs: rate > 0 ? pending / rate : 0, lastSettledAt: now };
}
// v0.24.2: every settlement below rounds, so a heal-over-time delivering a fractional slice of
// its total can never leave current_hp/stamina/mana sitting on a decimal.
function combatSettleAllHeals(data) {
  const maxHp = combatGetMaxHp(data);
  if (data.srv_heal) {
    const { delivered, heal } = combatSettleHeal(data.srv_heal);
    data.current_hp = Math.round(Math.min(maxHp, (data.current_hp || 0) + delivered));
    data.srv_heal = heal;
  }
  const maxStamina = combatGetMaxStamina(data);
  if (data.srv_stamina_heal) {
    const { delivered, heal } = combatSettleHeal(data.srv_stamina_heal);
    data.current_stamina = Math.round(Math.min(maxStamina, (data.current_stamina || 0) + delivered));
    data.srv_stamina_heal = heal;
  }
  // v0.23.0 (Part B3): mana potions use the exact same lazy heal-over-time settlement as
  // health/stamina potions -- see combatQueueHeal()'s call in the use-item endpoint below.
  const maxMana = combatGetMaxMana(data);
  if (data.srv_mana_heal) {
    const { delivered, heal } = combatSettleHeal(data.srv_mana_heal);
    data.current_mana = Math.round(Math.min(maxMana, combatGetCurrentMana(data) + delivered));
    data.srv_mana_heal = heal;
  } else {
    combatGetCurrentMana(data); // lazily hydrate/clamp even when no heal is in flight
  }
}

// v0.20 (#9.3): `amount` is now the RAW, un-buffed base XP (session.xp) -- every bonus
// source (gear XP Find, Experience Shrine, Community XP, Forest Reputation) is summed into
// ONE combined percentage (see combatGetTotalXpBonusPct()) and applied exactly once here,
// instead of the old chain of independent multiplications a caller had to pre-apply some of
// before even calling this function. Returns the actual, final, post-bonus XP amount that
// was awarded (not just whether it leveled up) so the caller can show the player the REAL
// number in the kill result text -- previously the displayed "+X XP" was only the
// community/forest-rep-multiplied amount and silently underreported the true total whenever
// XP Find gear or an active shrine buff was involved (those were folded in afterward,
// invisibly, right here).
function combatAddXp(data, amount) {
  const totalPct = combatGetTotalXpBonusPct(data);
  const buffed = amount * (1 + totalPct / 100);
  const xpGained = Math.round(buffed);
  data.xp = (data.xp || 0) + xpGained;
  data.lifetime_xp = (data.lifetime_xp || 0) + xpGained;
  let leveled = false;
  while (data.xp >= data.xp_to_next && (data.level || 1) < CB.LEVEL_CAP) {
    data.xp -= data.xp_to_next;
    data.level = (data.level || 1) + 1;
    data.xp_to_next = combatXpRequiredForLevel(data.level);
    data.unspent_stat_points = (data.unspent_stat_points || 0) + CB.STAT_POINTS_PER_LEVEL;
    // v0.23.0 (Part A4): on top of the manual stat points above, auto-allocate
    // +CB.LEVEL_UP_PRIMARY_ATTR_BONUS directly into the chain's primary attribute (same
    // thornguard->str / windrider->dex / wizard->int mapping combatGetDamageRange() and
    // WIZARD_INT_DAMAGE_PER_POINT already use). This is a real, persisted mutation of
    // data.attributes (like manual stat-point spends), but that's reset-safe for free: a
    // bridge trial promotion/failure or ladder reset already re-bases data.attributes to the
    // class's base stats via applyTrialResolutionReset(), so this rides along with that
    // existing reset behavior with no special-casing needed here.
    const levelUpClass = COMBAT_CLASSES[data.class_id] || {};
    if (!data.attributes) data.attributes = {};
    if (levelUpClass.chain === "thornguard") data.attributes.str = (data.attributes.str || 0) + CB.LEVEL_UP_PRIMARY_ATTR_BONUS;
    else if (levelUpClass.chain === "windrider") data.attributes.dex = (data.attributes.dex || 0) + CB.LEVEL_UP_PRIMARY_ATTR_BONUS;
    else if (levelUpClass.chain === "wizard") data.attributes.int = (data.attributes.int || 0) + CB.LEVEL_UP_PRIMARY_ATTR_BONUS;
    // v0.20 (#9.7): no longer touches bonus_hp_from_attributes/bonus_stamina_from_attributes
    // here -- see combatGetMaxHp()'s comment. The full-HP heal below is still needed on
    // every level-up regardless (now also reflects the +10/level HP bonus baked into
    // combatGetMaxHp() itself, see its own v0.23.0 comment).
    data.current_hp = combatGetMaxHp(data);
    leveled = true;
  }
  if (data.level >= CB.LEVEL_CAP) data.xp = 0;
  return { leveled, xpGained };
}
function combatRegisterWeaponHit(data, weaponType) {
  if (!data.weapon_skills) data.weapon_skills = {};
  if (!data.weapon_skills[weaponType]) data.weapon_skills[weaponType] = { hits: 0 };
  const before = combatWeaponSkillLevelForHits(data.weapon_skills[weaponType].hits);
  data.weapon_skills[weaponType].hits++;
  const after = combatWeaponSkillLevelForHits(data.weapon_skills[weaponType].hits);
  return { leveledUp: after > before, newLevel: after };
}
function combatIncrementKillStreak(data) {
  data.kill_streak = (data.kill_streak || 0) + 1;
  if (data.kill_streak > (data.max_kill_streak || 0)) data.max_kill_streak = data.kill_streak;
  // v0.21 (#17): total_kills is a separate, NEVER-reset lifetime counter (kill_streak
  // resets to 0 on every town return/reload) -- feeds the new "Monsters Killed" leaderboard
  // tab. Incremented right alongside kill_streak/max_kill_streak since every code path that
  // credits a kill already calls this one function.
  data.total_kills = (data.total_kills || 0) + 1;
}
function combatGetInventoryCapacity(data) {
  let cap = 16; // Balance.BASE_INVENTORY_SLOTS
  for (const bagId of (data.equipped_bags || [])) cap += (COMBAT_BAG_CAPACITY[bagId] || 0);
  return cap;
}
function combatAutoEquipTargetSlot(data, inst) {
  const genSlot = inst.slot;
  const equipped = data.equipped || {};
  if (genSlot === "ring") return !equipped["ring1"] ? "ring1" : (!equipped["ring2"] ? "ring2" : null);
  if (!COMBAT_EQUIPPED_SLOT_KEYS.includes(genSlot)) return null;
  return equipped[genSlot] ? null : genSlot;
}
function combatCanAutoEquip(data, inst) { return combatCanEquipGear(data, inst) && !!combatAutoEquipTargetSlot(data, inst); }
function combatAddGearAutoEquip(data, inst) {
  if (!data.equipped) data.equipped = {};
  if (!data.gear_instances) data.gear_instances = [];
  if (combatCanAutoEquip(data, inst)) {
    data.equipped[combatAutoEquipTargetSlot(data, inst)] = inst;
    return { fit: true, autoEquipped: true };
  }
  if (data.gear_instances.length >= combatGetInventoryCapacity(data)) return { fit: false, autoEquipped: false };
  data.gear_instances.push(inst);
  return { fit: true, autoEquipped: false };
}
function combatAddConsumable(data, itemId, count) { if (!data.consumables) data.consumables = {}; data.consumables[itemId] = (data.consumables[itemId] || 0) + count; }
function combatAddHerb(data, herbId, count) { if (!data.herbs) data.herbs = {}; data.herbs[herbId] = (data.herbs[herbId] || 0) + count; }
function combatStrongholdKeyTierForAreaLevel(level) { if (level <= 4) return 1; return cbClampi(2 + Math.floor((level - 5) / 5), 1, ITEM_TIER_MAX); }
function combatStrongholdKeyItemIdForTier(tier) { return tier <= 1 ? "stronghold_key" : `stronghold_key_t${tier}`; }
function combatStrongholdKeyEligible(mazeDepth, playerLevel) { return (mazeDepth || 1) >= (playerLevel || 1) - 1; }

// Mirrors DL.pickRandomMonsterForArea()'s own tier gate -- used at /api/combat/start to
// reject a claimed monster id that isn't actually available at the claimed area level.
function combatMonsterAllowedForAreaLevel(monster, areaLevel) {
  // v0.22.1 (Part A2a): mirrors index.html's pickRandomMonsterForArea() -- spawn gating now
  // follows the new band system (COMBAT_MONSTER_BAND_INFO) instead of the old flat tier.
  const info = COMBAT_MONSTER_BAND_INFO[monster.tier];
  return !!info && areaLevel >= info.minAreaLevel;
}

function loadCharacterRow(accountId, slot) {
  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
  if (!row) return null;
  try { return migrateSpellTiers(JSON.parse(row.data)); } catch (e) { return null; }
}
function saveCharacterRow(accountId, slot, data) {
  data._save_seq = (data._save_seq || 0) + 1;
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(JSON.stringify(data), nowIso(), accountId, slot);
  return data._save_seq;
}
function getCombatSession(accountId, id) {
  return db.prepare("SELECT * FROM combat_sessions WHERE id = ? AND account_id = ?").get(id, accountId);
}
// v0.21.2: looks up an account's own active fight WITHOUT knowing its session id -- used by the
// server-driven combat-tick pusher (see "chat over WebSocket" below), which only knows which
// ACCOUNTS have a live socket, not which fight (if any) each one is currently in. An account can
// only ever have one active session at a time (POST /api/combat/start deletes any pre-existing
// active row for that account+slot before inserting a new one), so this is unambiguous.
function getActiveCombatSessionForAccount(accountId) {
  return db.prepare("SELECT * FROM combat_sessions WHERE account_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1").get(accountId);
}
function updateCombatSession(id, fields) {
  const keys = Object.keys(fields);
  if (keys.length === 0) return;
  const setSql = keys.map((k) => `${k} = ?`).join(", ") + ", updated_at = ?";
  db.prepare(`UPDATE combat_sessions SET ${setSql} WHERE id = ?`).run(...keys.map((k) => fields[k]), nowIso(), id);
}

// Resolves the monster's counter-attack against the (already-settled-heals) character,
// mirroring screenCombat()'s monster-turn block exactly: Invulnerability shrine first (full
// immunity), then the player's own Dexterity-derived block chance, then damage reduced by
// Armor. Mutates `data.current_hp` in place and returns a small result the caller folds into
// the round response; never touches the session/monster HP (that's the caller's job).
function combatResolveMonsterTurn(data, session, invulnActiveThisRound) {
  if (invulnActiveThisRound) return { invulnerable: true, blocked: false, damage: null, crit: false, crit_mult: null, fatal: false };
  // v0.23.0 (Part B7): Entangle roots the monster -- it deals 0 damage for the duration
  // (checked ahead of the normal block/damage roll, mirroring the invulnActiveThisRound
  // branch immediately above). Reached by every call site that already resolves a monster
  // turn/catch-up hit through this function, so the root applies uniformly whether the
  // monster "misses" via a real-time catch-up tick, a failed-flee counter, or an /attack round.
  if (session.rooted_until && Date.now() < session.rooted_until) {
    return { invulnerable: false, blocked: false, damage: null, crit: false, crit_mult: null, fatal: false, rooted: true };
  }
  if (Math.random() < combatGetBlockChance(data)) return { invulnerable: false, blocked: true, damage: null, crit: false, crit_mult: null, fatal: false };
  let mdmg = cbRandRange(session.dmg_min, session.dmg_max);
  // v0.19.1 (#10): the monster's own crit roll, applied BEFORE armor mitigation (same order
  // as the player's own crit-then-quad-damage math above) -- 10% + 1%/area-level.
  const crit = Math.random() < combatGetMonsterCritChance(session.area_level);
  // v0.22.7 (#18): the ACTUAL multiplier for this hit is its own random roll between this
  // monster's own area-level-dependent floor AND ceiling (see combatGetMonsterCritFloor()/
  // combatGetMonsterCritCeiling()) -- monsters do NOT reuse the player's CB.CRIT_MULTIPLIER
  // floor. critMult stays null on a non-crit hit (nothing to report), returned either way so
  // the client can render it.
  const critMult = crit ? combatRollMonsterCritMultiplier(combatGetMonsterCritFloor(session.area_level), combatGetMonsterCritCeiling(session.area_level)) : null;
  if (crit) mdmg *= critMult;
  // v0.20.4: flat subtraction instead of a percentage multiplier -- see combatGetArmor()'s
  // own comment for the full mechanic change.
  mdmg = Math.max(0, mdmg - combatGetArmor(data));
  // v0.24.1 (C4): Poison Sting -- a small set of venomous monsters can, on a landed hit, sting
  // instead of striking: this hit's fully-mitigated damage roll (mdmg, same value that would
  // otherwise have been subtracted instantly below) is queued as a poison DOT spread over
  // POISON_STING_DURATION_MS instead, and the monster's own attack cadence pauses for
  // POISON_STING_LOCKOUT_MS (see combatCatchUpMonsterHits' lockout gate above). The hit still
  // "lands" for every other purpose (Thorns still reflects below), it just doesn't deal its
  // damage instantly. Non-lethal by construction (see combatSettlePlayerPoison's floor).
  if (CB.POISON_STING_MONSTER_IDS.includes(session.monster_id) && Math.random() < CB.POISON_STING_CHANCE) {
    let dots;
    try { dots = JSON.parse(session.player_poison_dots || "[]"); } catch (e) { dots = []; }
    if (!Array.isArray(dots)) dots = [];
    const tickIntervalMs = 1000;
    const totalTicks = Math.round(CB.POISON_STING_DURATION_MS / tickIntervalMs);
    dots.push({
      hits_remaining: totalTicks,
      next_hit_at: Date.now() + tickIntervalMs,
      interval_ms: tickIntervalMs,
      dmg_per_tick: mdmg / totalTicks,
    });
    session.player_poison_dots = JSON.stringify(dots);
    session.sting_lockout_until = Date.now() + CB.POISON_STING_LOCKOUT_MS;
    // v0.24.2: rounded -- Thorns sums gear affix rolls, which carry 2 decimals, and set/inherent
    // scaling can add more. HP has no fractional part, so neither can the damage that removes it.
    const thornsSting = Math.round(combatGearBonus(data, "thorns"));
    let thornsStingDamage = 0;
    if (thornsSting > 0 && session.hp > 0) {
      thornsStingDamage = Math.min(session.hp, thornsSting);
      session.hp = Math.max(0, session.hp - thornsSting);
    }
    return { invulnerable: false, blocked: false, damage: null, crit, crit_mult: critMult, fatal: false, poison_sting: true, poison_total: Math.round(mdmg), thorns_damage: thornsStingDamage };
  }
  // v0.24.2: mdmg is already rounded into the returned payload below; round what actually
  // leaves the health bar too, so the player's own HP can never drift fractional either.
  data.current_hp = (data.current_hp || 0) - Math.round(mdmg);
  let fatal = false;
  if (data.current_hp < 1) { data.current_hp = 0; fatal = true; }
  // v0.22.7 (#1): Thorns is now a flat value (no longer a %) -- exactly that much damage
  // reflects back onto the monster every time ITS attack actually lands on the player (i.e.
  // past both the invulnerable and blocked early-returns above, so a fully-avoided hit
  // reflects nothing). session.hp is mutated directly here since session is the same object
  // the caller reads back from afterward (both the /attack handler's `newMonsterHp =
  // session.hp` and the elapsed-time catch-up path) -- this lets a landed monster hit
  // potentially finish off the monster even on a round where the player's own swing was
  // blocked or hasn't happened yet.
  // v0.24.2: rounded, same reasoning as the Poison Sting branch above.
  const thorns = Math.round(combatGearBonus(data, "thorns"));
  let thornsDamage = 0;
  if (thorns > 0 && session.hp > 0) {
    thornsDamage = Math.min(session.hp, thorns);
    session.hp = Math.max(0, session.hp - thorns);
  }
  return { invulnerable: false, blocked: false, damage: Math.round(mdmg), crit, crit_mult: critMult, fatal, thorns_damage: thornsDamage };
}

// v0.21.1 (#9): continuous mob attacks. Resolves however many monster hits are "owed" since
// session.last_monster_hit_at, based on REAL elapsed wall-clock time and this session's own
// monster_attack_speed -- this is what makes the monster keep hitting the player once combat
// is initiated even if the player never clicks Attack again (an idle stretch, or simply the
// time between requests). v0.21.5: this is now the ONLY way a monster deals damage during
// /attack -- there is no separate guaranteed hit tied to the player's own action anymore (see
// that handler's own comment). Deliberately does NOT touch
// combatTickCombatRoundBuffs()'s round-based shrine-buff countdown (invuln/quad damage/magic
// find) -- those still tick exactly once per player-initiated action (attack/flee/use-item),
// not once per elapsed-time catch-up hit, so a shield doesn't drain faster just because the
// player paused. In practice this resolves 0 hits on every fast, actively-played round (and
// in every automated test, which calls endpoints back-to-back with near-zero real elapsed
// time) -- it only ever adds hits once genuine wall-clock seconds have passed since the last
// server round-trip, exactly the "continuous" behavior Gwen asked for without disturbing the
// existing once-per-attack guarantee everything else (and every existing test) already
// depends on.
function combatCatchUpMonsterHits(session, data, invulnActiveThisRound) {
  const now = Date.now();
  let lastAt = session.last_monster_hit_at || now;
  // v0.24.1 (C4): a landed Poison Sting pauses the monster's normal attack cadence for
  // POISON_STING_LOCKOUT_MS -- while locked out, no hits are owed at all (returned early
  // below), and once the lockout passes the clock resumes FROM the lockout's end rather than
  // owing a backlog for the paused stretch (that pause was intentional, not idle time).
  const lockoutUntil = session.sting_lockout_until || 0;
  if (lockoutUntil > now) return { ticks: [], newLastHitAt: lastAt, fatal: false };
  if (lockoutUntil > lastAt) lastAt = lockoutUntil;
  const speed = session.monster_attack_speed || CB.MONSTER_BASE_ATTACK_SPEED;
  const intervalMs = 1000 / Math.max(0.01, speed);
  const elapsedMs = Math.max(0, now - lastAt);
  const dueHits = Math.min(Math.floor(elapsedMs / intervalMs), CB.MONSTER_CATCH_UP_MAX_HITS);
  const ticks = [];
  let fatal = false;
  for (let i = 0; i < dueHits; i++) {
    if ((data.current_hp || 0) <= 0) { fatal = true; break; }
    const t = combatResolveMonsterTurn(data, session, invulnActiveThisRound);
    ticks.push(t);
    if (t.fatal) { fatal = true; break; }
  }
  const newLastHitAt = dueHits > 0 ? lastAt + dueHits * intervalMs : lastAt;
  return { ticks, newLastHitAt, fatal };
}

// ---------------- HOTFIX: server-authoritative hardcore permadeath ----------------
// CRITICAL bug fix: every fatal-hit call site above/below (combatCatchUpMonsterHits and
// combatResolveMonsterTurn's guaranteed failed-flee hit, reached from /attack, /flee,
// /use-item, /tick, /cast, and the WS combat-tick pusher near the bottom of this file) used
// to only ever RETURN fatal:true and leave the actual hardcore permadeath sequence
// (graveyard insert, leaderboard is_dead=1, characters row delete, death broadcast) entirely
// to the CLIENT's follow-up DELETE /api/characters/:slot call (see PS.handleDeath/
// Net.deleteCharacter in index.html) -- see the SCOPE NOTE comment near the top of this
// combat section for why that split originally existed. That split is exploitable/fragile
// two ways: (1) an in-flight or subsequent autosave PUT can re-INSERT the just-deleted slot
// via its ON CONFLICT upsert (see PUT /api/characters/:slot) before the DELETE lands or
// right after, resurrecting a character that was already announced dead in global chat;
// (2) if the client never completes the DELETE at all (tab closed, navigated away, or the
// fatal hit arrived via a catch-up batch the UI never fully reacted to), the kill never
// happens server-side and a "dead" hardcore character just keeps playing/leveling.
//
// combatHandleHardcoreDeath() is the single choke point every fatal-hit call site now routes
// through the INSTANT fatal is first produced, inside the SAME synchronous handler (no
// intervening await between the fatal roll and this call). Every route here is a plain
// (non-async) Express handler and node:sqlite's DatabaseSync calls are synchronous, so
// nothing else can interleave and observe the character "half dead" (still present but
// doomed, or deleted but not yet announced) -- Node's single-threaded event loop makes this
// sequence atomic with respect to any other concurrent request without needing a lock.
// No-ops instantly (returns {hardcoreKilled:false}) for a softcore character -- softcore
// death stays exactly the existing client-driven penalty-only flow (applyDeathPenalty in
// index.html), completely untouched by this function.
//
// Mirrors the exact graveyard/leaderboard/broadcast steps DELETE /api/characters/:slot's own
// hardcore_death branch already performs (see that route below) so none of the graveyard/
// leaderboard/chat side effects change for the player -- just WHEN and WHERE they happen.
// Wrapped in a raw BEGIN/COMMIT (this project's DatabaseSync has no higher-order transaction
// helper -- see migrateAccountGold() near the top of this file for the same pattern) purely
// for crash-safety: a mid-sequence process crash can never leave the graveyard/leaderboard/
// characters tables inconsistent with each other, even though ordinary (non-crash) execution
// is already atomic just from being synchronous.
// v0.24.2: claims the exclusive right to announce a given character's death. Returns true to
// exactly one caller: the UPDATE only touches a row whose death_announced is still 0, so the
// server-authoritative combat permadeath and the client's courtesy /api/announce/death call
// (which is the only path that can report a death the server never resolved itself, e.g. a
// lethal trap) can both fire freely and only the first one through actually broadcasts.
// node:sqlite runs synchronously on one thread, so there is no race window between the write
// and reading its own changes count.
function markDeathAnnounced(accountId, characterName) {
  const result = db
    .prepare("UPDATE leaderboard_bests SET death_announced = 1 WHERE account_id = ? AND character_name = ? AND death_announced = 0")
    .run(accountId, characterName);
  // No leaderboard row at all (a character that died before ever being saved to it) means
  // nothing can dedupe against it either -- announce rather than swallow the death silently.
  if (result.changes > 0) return true;
  const exists = db.prepare("SELECT 1 FROM leaderboard_bests WHERE account_id = ? AND character_name = ?").get(accountId, characterName);
  return !exists;
}

function combatHandleHardcoreDeath(accountId, username, slot, data, session) {
  if (!data || !data.hardcore) return { hardcoreKilled: false };
  const characterName = data.character_name || "Hero";
  const className = data.class_display_name || "";
  const level = data.level || 1;
  // The combat_sessions row already carries the exact monster name and area level this
  // fight was fought at (set once at POST /api/combat/start and never mutated) -- reusing
  // those instead of re-deriving anything keeps this server-authoritative cause string
  // consistent with what the player actually saw, and matches the WHO/WHERE wording of the
  // client's own pre-existing "Slain by a X (Area Level N)" death message (see
  // applyDeathAndRedirect() in index.html) even though this is now a different call site.
  const monsterName = (session && session.name) || "something in the wilds";
  const areaLevel = (session && session.area_level) || data.max_maze_depth_reached || 1;
  const cause = `Slain by a ${monsterName} (Area Level ${areaLevel})`;

  db.exec("BEGIN");
  try {
    db.prepare(
      "INSERT INTO graveyard (account_id, username, name, class_name, level, cause, died_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).run(accountId, username, characterName, className, level, cause, nowIso());
    // Keep the leaderboard entry (don't delete it) but mark it dead -- exactly mirrors
    // DELETE /api/characters/:slot's own hardcore_death branch.
    db.prepare("UPDATE leaderboard_bests SET is_dead = 1 WHERE account_id = ? AND character_name = ?").run(accountId, characterName);
    // The actual permadeath: the slot's characters row is gone the instant this transaction
    // commits, in the SAME request that produced fatal:true -- see PUT /api/characters/:slot's
    // is_dead/hardcore lookup for how a subsequent autosave is kept from re-creating it.
    db.prepare("DELETE FROM characters WHERE account_id = ? AND slot = ?").run(accountId, slot);
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
  // Deliberately broadcast AFTER the commit, not inside the try block -- announcing a death
  // that then failed to actually persist (had the transaction rolled back) would be worse
  // than a silent 500. broadcastSystemMessage() itself is a best-effort chat-history insert
  // + live WS push, not save-critical state, so it doesn't need to be part of the atomic unit
  // above. Reuses the exact same function/message shape as the pre-existing
  // POST /api/announce/death route (self-reported client announcements for other death
  // causes, e.g. trap deaths) so this reads identically in global chat either way.
  // v0.24.2 BUG FIX (credit: Gwen): this used to build its line as `has ${cause}` off the
  // graveyard cause string above, which starts with a capitalised "Slain by" -- producing the
  // ungrammatical "X has Slain by a Roaming Ancient Treant". The client's own courtesy
  // announce (see applyDeathAndRedirect() in index.html) sent the correct "has been slain by"
  // wording, so both fired and global chat showed the same death twice, once broken and once
  // right. Two changes: the wording here now matches the correct form, and
  // markDeathAnnounced() below makes whichever path gets there first the ONLY one that
  // broadcasts. The graveyard `cause` string itself is deliberately unchanged -- it reads
  // correctly as a standalone tombstone caption, which is where it's actually displayed.
  if (markDeathAnnounced(accountId, characterName)) {
    broadcastSystemMessage(`${characterName} (Lv ${level} ${className}) has been slain by a ${monsterName} (Area Level ${areaLevel}).`, "death");
  }
  return { hardcoreKilled: true };
}

/* ---------------- v0.23.0 (Part B4-B9); retiered v0.24 -----------------
   Server-authoritative spell definitions (SPELLS) used for validation/resolution --
   index.html's SPELLS is a display-only copy that must stay byte-identical. Every class can
   buy/use every spell (no class_id gate); every class has exactly SPELL_SLOT_COUNT=2 active
   spell slots, a flat constant.

   v0.24: reworked from one fixed price/level_req/magnitude per spell into 5 separately-bought
   TIERS per spell (I-V), gated by the same ladder gear itemization already uses (T1=lvl1,
   T2=lvl5, T3=lvl10, T4=lvl15, T5=lvl20) instead of each spell's own old fixed level_req.
   Tiers must be bought in order (owning tier N-1 is required to buy tier N; see
   POST /api/magician/buy) -- each purchase is an "upgrade" per Gwen's exact framing, not an
   independent unlock. Shared/effect-invariant fields (name, effect, and for the queued-DOT
   spells the fixed hit_count/duration_ms cadence) live on the outer spell object; every
   per-tier field (level_req, price, mana_cost, cooldown_ms, and that effect's magnitude
   field(s)) lives in .tiers[tier-1]. spellAtTier() below flattens a spell+tier into the same
   shape the old single-tier SPELLS[spellId] objects had, so every existing effect-resolution
   branch in POST /api/combat/:sessionId/cast keeps reading spell.heal_amount/hit_min/hit_max/
   mana_cost/cooldown_ms/level_req unchanged -- only WHICH tier's numbers get merged in changes.
   Entangle (v0.24, Option B per Gwen's lean): root duration_ms and cooldown_ms (locked at
   10000ms every tier, so the max 8s root at T5 always leaves a real gap) are still NEVER
   scaled by combatGetSpellEffectivenessMult() -- pure utility, no magnitude. It now ALSO
   carries a scaled crushing-vine damage-over-time (dot_min/dot_max per tier, roughly half of
   Fireflies' own curve since Entangle also locks the target), queued via the same
   combatSettleSpellDots() mechanism Fireflies/Verdant Siphon already use, on a fixed 6-hit/2s
   cadence (dot_hit_count/dot_duration_ms, shared across tiers like Fireflies' own hit_count/
   duration_ms) -- see the effect:"entangle" branch in the cast endpoint below. */
const SPELLS = {
  heal_1: {
    id: "heal_1", name: "Heal", effect: "heal",
    tiers: [
      { tier: 1, level_req: 1, price: 300, mana_cost: 15, cooldown_ms: 3000, heal_amount: 50 },
      { tier: 2, level_req: 5, price: 600, mana_cost: 30, cooldown_ms: 3000, heal_amount: 100 },
      { tier: 3, level_req: 10, price: 1200, mana_cost: 45, cooldown_ms: 3000, heal_amount: 150 },
      { tier: 4, level_req: 15, price: 2400, mana_cost: 60, cooldown_ms: 3000, heal_amount: 200 },
      { tier: 5, level_req: 20, price: 4800, mana_cost: 75, cooldown_ms: 3000, heal_amount: 250 },
    ],
  },
  fireflies: {
    id: "fireflies", name: "Fireflies", effect: "fireflies", hit_count: 6, duration_ms: 2000,
    tiers: [
      { tier: 1, level_req: 1, price: 600, mana_cost: 25, cooldown_ms: 5000, hit_min: 8, hit_max: 12 },
      { tier: 2, level_req: 5, price: 1200, mana_cost: 35, cooldown_ms: 5000, hit_min: 19, hit_max: 28 },
      { tier: 3, level_req: 10, price: 2400, mana_cost: 45, cooldown_ms: 5000, hit_min: 36, hit_max: 55 },
      { tier: 4, level_req: 15, price: 4800, mana_cost: 55, cooldown_ms: 5000, hit_min: 65, hit_max: 90 },
      { tier: 5, level_req: 20, price: 9600, mana_cost: 65, cooldown_ms: 5000, hit_min: 142, hit_max: 180 },
    ],
  },
  entangle: {
    id: "entangle", name: "Entangle", effect: "entangle", dot_hit_count: 6, dot_duration_ms: 2000,
    tiers: [
      { tier: 1, level_req: 1, price: 1200, mana_cost: 50, cooldown_ms: 10000, duration_ms: 4000, dot_min: 4, dot_max: 6 },
      { tier: 2, level_req: 5, price: 2400, mana_cost: 55, cooldown_ms: 10000, duration_ms: 5000, dot_min: 9, dot_max: 14 },
      { tier: 3, level_req: 10, price: 4800, mana_cost: 60, cooldown_ms: 10000, duration_ms: 6000, dot_min: 18, dot_max: 27 },
      { tier: 4, level_req: 15, price: 9600, mana_cost: 65, cooldown_ms: 10000, duration_ms: 7000, dot_min: 32, dot_max: 45 },
      { tier: 5, level_req: 20, price: 19200, mana_cost: 70, cooldown_ms: 10000, duration_ms: 8000, dot_min: 71, dot_max: 90 },
    ],
  },
  // v0.23.1 (#2): a 4th spell, usable by every class like the 3 above (no class_id gate).
  // effect:"drain" is handled by the SAME queued-DOT branch as fireflies below (reuses the
  // exact combatSettleSpellDots() tick-queueing/catch-up-safe mechanism), just with heal_pct
  // set so each tick also heals the caster for 50% of THAT tick's own realized damage. Duration
  // (2000ms/6 ticks) and cooldown are never scaled by combatGetSpellEffectivenessMult(), same
  // as Fireflies' own duration/cooldown.
  verdant_siphon: {
    id: "verdant_siphon", name: "Verdant Siphon", effect: "drain", hit_count: 6, duration_ms: 2000, heal_pct: 0.5,
    tiers: [
      { tier: 1, level_req: 1, price: 900, mana_cost: 30, cooldown_ms: 6000, hit_min: 5, hit_max: 9 },
      { tier: 2, level_req: 5, price: 1800, mana_cost: 40, cooldown_ms: 6000, hit_min: 13, hit_max: 20 },
      { tier: 3, level_req: 10, price: 3600, mana_cost: 50, cooldown_ms: 6000, hit_min: 26, hit_max: 40 },
      { tier: 4, level_req: 15, price: 7200, mana_cost: 60, cooldown_ms: 6000, hit_min: 46, hit_max: 66 },
      { tier: 5, level_req: 20, price: 14400, mana_cost: 70, cooldown_ms: 6000, hit_min: 95, hit_max: 133 },
    ],
  },
};
const SPELL_SLOT_COUNT = 2;
// v0.24: flattens a spell + owned tier (1-5) into the same flat shape the old single-tier
// SPELLS[spellId] objects had (id/name/effect/mana_cost/cooldown_ms/level_req/price plus that
// effect's magnitude field(s)), so every existing effect-resolution branch can keep reading
// spell.heal_amount/hit_min/hit_max/etc. unchanged. Returns null for an unknown spell id or an
// out-of-range tier. Mirrors index.html's identical spellAtTier() exactly.
// v0.25.1 (credit: Gwen): a character keeps every spell tier it has ever bought, but can only
// CAST the highest one whose level requirement it currently meets. This matters because a Broken
// Bridge Trial resets the character to level 1 while leaving purchased spell tiers intact -- so a
// spell bought up to Tier V became completely uncastable, and the only way back was to re-buy
// the lower tiers that were already owned. Now the tier simply steps back down on its own and
// climbs again as the character re-levels, with nothing to re-purchase and no UI to operate.
// owned_spell_tiers still records the highest tier PURCHASED, which is what the Magician prices
// the next upgrade against; this function answers the different question of what can be cast
// right now. Mirrors PS.getSpellTier() in index.html.
function combatEffectiveSpellTier(data, spellId) {
  const owned = (data.owned_spell_tiers && data.owned_spell_tiers[spellId]) || 0;
  if (!owned) return 0;
  const level = data.level || 1;
  for (let t = owned; t >= 1; t--) {
    const def = spellAtTier(spellId, t);
    if (def && level >= (def.level_req || 1)) return t;
  }
  return 0;
}

function spellAtTier(spellId, tier) {
  const base = SPELLS[spellId];
  if (!base || !Number.isInteger(tier) || tier < 1 || tier > 5) return null;
  const t = base.tiers[tier - 1];
  if (!t) return null;
  return Object.assign({}, base, t);
}
// v0.24: migrates the old flat owned_spells array (pre-tier system) into the new
// owned_spell_tiers map the tier system actually reads, grandfathering every spell a character
// already knew as Tier I for free -- per Gwen's exact answer ("grandfather existing spell
// owners as Tier I, they can then buy Tiers II-V as upgrades"). Idempotent (an id already
// present in owned_spell_tiers is left alone), so it's safe to call on every character load.
// owned_spells itself is left untouched on the data object; owned_spell_tiers is the new
// source of truth for ownership/casting everywhere below.
function migrateSpellTiers(data) {
  if (!data.owned_spell_tiers || typeof data.owned_spell_tiers !== "object") data.owned_spell_tiers = {};
  const legacy = Array.isArray(data.owned_spells) ? data.owned_spells : [];
  for (const id of legacy) if (!data.owned_spell_tiers[id]) data.owned_spell_tiers[id] = 1;
  return data;
}
// v0.23.6 (Item 4): Wizard Set 6pc grants +WIZARD_SET_6PC_BONUS_SPELL_SLOTS on top of the flat
// SPELL_SLOT_COUNT -- mirrors index.html's PS.getEffectiveSpellSlotCount().
function combatGetEffectiveSpellSlotCount(data) {
  let n = SPELL_SLOT_COUNT;
  if (combatSetBonusesFor(data, "wizard_set").count >= 6) n += CB.WIZARD_SET_6PC_BONUS_SPELL_SLOTS;
  return n;
}

// Lazy, on-demand settlement of a session's queued Fireflies-style damage-over-time entries --
// mirrors combatSettleHeal()/combatSettleAllHeals()'s "computed on demand instead of a real
// 200ms tick loop" philosophy, adapted to discrete RANDOM-per-hit damage (8-12 per hit, not a
// smooth rate) rather than a continuous rate, since each Fireflies hit must independently roll
// its own damage.
// v0.24.1 (C1): used to deliberately clamp at hp=1 and never land the kill blow itself, per its
// own now-obsolete comment -- per Gwen's exact ask, spell DOT damage must be able to finish the
// monster off directly instead of always deferring to the next landed swing or idle tick. Every
// call site now checks the resulting session.hp itself and calls the shared
// combatFinalizeMonsterKill() helper (see right before POST /api/combat/:sessionId/attack) the
// instant it reaches 0, so the loot/XP/gold/quest-tracking resolution genuinely fires from a
// killing DOT tick, not just a cosmetic "1 HP forever" state.
// v0.24.1 (B5/C2): also stamps session.player_dealt_direct_damage=1 the moment any tick lands
// (mutated in place; the caller's own updateCombatSession() call persists it) -- this is player-
// cast spell damage, so it always counts as "direct damage" for The Pacifist quest, same as a
// landed physical swing does in /attack. Thorns reflect damage is a completely separate code
// path (combatResolveMonsterTurn) and never touches this flag.
// v0.23.1 (#2): takes an optional `data` (the loaded character row) so a dot that carries a
// `heal_pct` (Verdant Siphon's channeled drain) can heal the caster for a % of THIS tick's own
// realized damage -- computed from `dmg` below, i.e. AFTER hp-clamping/rounding, never a
// separately-scaled or independently-rolled amount. `data` is optional/backward-compatible:
// every existing call site already has the character row loaded by the time it calls this, but
// passing it is harmless for Fireflies/Entangle-only sessions (dots without heal_pct just skip
// the new branch entirely, identical to pre-v0.23.1 behavior).
function combatSettleSpellDots(session, data) {
  let dots;
  try { dots = JSON.parse(session.spell_dots || "[]"); } catch (e) { dots = []; }
  if (!Array.isArray(dots) || dots.length === 0) return { ticks: [], newHp: session.hp, newDots: [], dotsChanged: false };
  const now = Date.now();
  let hp = session.hp;
  const ticks = [];
  const remaining = [];
  const maxHp = data ? combatGetMaxHp(data) : null;
  for (const dot of dots) {
    let d = dot;
    let changed = false;
    while (d.hits_remaining > 0 && d.next_hit_at <= now && hp > 0) {
      // v0.24.1 (C1): hp can now reach exactly 0 (a genuine killing blow), not just hp-1.
      const dmg = Math.min(hp, Math.round(cbRandRange(d.dmg_min, d.dmg_max)));
      hp -= dmg;
      session.player_dealt_direct_damage = 1;
      const tick = { spell_id: d.spell_id, damage: dmg };
      if (d.heal_pct && data) {
        const healAmt = Math.round(dmg * d.heal_pct);
        if (healAmt > 0) {
          data.current_hp = Math.min(maxHp, (data.current_hp || 0) + Math.round(healAmt));
          tick.heal = healAmt;
        }
      }
      ticks.push(tick);
      d = Object.assign({}, d, { hits_remaining: d.hits_remaining - 1, next_hit_at: d.next_hit_at + d.interval_ms });
      changed = true;
    }
    if (changed || ticks.length === 0) { /* no-op, just avoids an unused-var lint concern */ }
    // v0.24.1 (C1): a dot that just landed the killing blow (hp===0) has nothing left to do --
    // don't keep it queued for future ticks even if hits_remaining>0, since there's no monster
    // left to keep hitting (a fresh fight starts an entirely new session/spell_dots anyway).
    if (d.hits_remaining > 0 && hp > 0) remaining.push(d);
  }
  return { ticks, newHp: hp, newDots: remaining, dotsChanged: true };
}

// v0.24.1 (C4): Poison Sting settlement -- mirrors combatSettleSpellDots() above exactly (same
// lazy on-demand discrete-tick pattern reused from the player's own spell DOTs, just aimed at
// data.current_hp instead of session.hp), with one deliberate difference: this DOT is
// NON-LETHAL, per the locked design call -- a sting can never be what actually kills the
// player, it only ever drains down to 1 HP and sits there until something else finishes the
// job (a normal monster swing, a real-time catch-up hit, etc.). Multiple stings can be queued
// at once (each push in combatResolveMonsterTurn below just appends another entry -- this
// function settles all of them independently every call, so they genuinely stack).
function combatSettlePlayerPoison(session, data) {
  let dots;
  try { dots = JSON.parse(session.player_poison_dots || "[]"); } catch (e) { dots = []; }
  if (!Array.isArray(dots) || dots.length === 0) return { ticks: [], dotsChanged: false };
  const now = Date.now();
  let hp = data.current_hp || 0;
  const ticks = [];
  const remaining = [];
  for (const dot of dots) {
    let d = dot;
    while (d.hits_remaining > 0 && d.next_hit_at <= now) {
      // Floors at 1 HP -- never the killing blow (locked design call).
      const dmg = Math.min(Math.max(0, hp - 1), d.dmg_per_tick);
      hp -= dmg;
      const tick = { damage: dmg };
      ticks.push(tick);
      d = Object.assign({}, d, { hits_remaining: d.hits_remaining - 1, next_hit_at: d.next_hit_at + d.interval_ms });
    }
    if (d.hits_remaining > 0) remaining.push(d);
  }
  data.current_hp = hp;
  return { ticks, dotsChanged: true, newDots: remaining };
}

// v0.23.0 (Part B4/B6): Magician spell-purchase endpoint -- mirrors the Blacksmith reroll
// endpoint's exact validate -> deduct gold -> mutate -> save -> respond shape (gold is
// account-bound via getAccountGold/creditAccountGold, same pattern as every other
// gold-spending route).
// v0.24: buys ONE tier of a spell -- tiers are bought in strict order (tier N requires already
// owning tier N-1; tier 1 requires owning nothing), each an "upgrade" purchase at that tier's
// own price/level_req, per Gwen's exact spec. `req.body.tier` is now required (the old flat
// "buy the spell outright" shape had no tier concept at all).
app.post("/api/magician/buy", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const spellId = req.body?.spell_id;
  const tier = Number(req.body?.tier);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  const spellDef = SPELLS[spellId];
  if (!spellDef) return res.status(400).json({ error: "Unknown spell." });
  if (!Number.isInteger(tier) || tier < 1 || tier > 5) return res.status(400).json({ error: "Invalid spell tier." });
  const tierData = spellDef.tiers[tier - 1];

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try { data = JSON.parse(row.data); } catch (e) { return res.status(500).json({ error: "Corrupt character save." }); }
  migrateSpellTiers(data);

  const currentTier = data.owned_spell_tiers[spellId] || 0;
  if (currentTier >= tier) return res.status(400).json({ error: "You already know that tier." });
  if (tier !== currentTier + 1) return res.status(400).json({ error: "Buy spell tiers in order." });
  if ((data.level || 1) < tierData.level_req) return res.status(400).json({ error: `Requires level ${tierData.level_req} to learn this.` });
  if (getAccountGold(req.account.id) < tierData.price) return res.status(400).json({ error: `Not enough gold, this tier costs ${tierData.price}g.` });

  const accountGoldAfter = creditAccountGold(req.account.id, -tierData.price);
  data.owned_spell_tiers[spellId] = tier;
  // Keep the legacy owned_spells array in sync too (belt-and-suspenders for any stray reader
  // that hasn't been updated to owned_spell_tiers -- harmless either way, since
  // migrateSpellTiers() never downgrades an entry already present in owned_spell_tiers).
  if (!Array.isArray(data.owned_spells)) data.owned_spells = [];
  if (!data.owned_spells.includes(spellId)) data.owned_spells.push(spellId);
  data._save_seq = (data._save_seq || 0) + 1;
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(JSON.stringify(data), nowIso(), req.account.id, slot);
  res.json({ ok: true, owned_spell_tiers: data.owned_spell_tiers, account_gold: accountGoldAfter, _save_seq: data._save_seq });
});

// v0.23.0 (Part B6): sets/clears one of the SPELL_SLOT_COUNT flat spell slots. Slotting is a
// Character Sheet/Town-only CLIENT concern per spec ("slotting only happens outside combat")
// -- this endpoint itself doesn't reject based on combat state, since nothing about the slots
// array is combat-session-scoped (it lives on the character, not the session).
app.post("/api/magician/slot", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const slotIndex = Number(req.body?.slot_index);
  const spellId = req.body?.spell_id == null ? null : req.body.spell_id;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (spellId != null && !SPELLS[spellId]) return res.status(400).json({ error: "Unknown spell." });

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try { data = JSON.parse(row.data); } catch (e) { return res.status(500).json({ error: "Corrupt character save." }); }
  migrateSpellTiers(data);

  // v0.23.6 (Item 4): bound check moved below the data load so it can use the EFFECTIVE slot
  // count (flat SPELL_SLOT_COUNT + Wizard Set 6pc bonus slot) instead of the flat constant.
  const effectiveSlotCount = combatGetEffectiveSpellSlotCount(data);
  if (!Number.isInteger(slotIndex) || slotIndex < 0 || slotIndex >= effectiveSlotCount) return res.status(400).json({ error: "Invalid spell slot index." });

  // v0.24: ownership is now tier-based -- any owned tier (>=1) is enough to slot the spell,
  // since casting itself always resolves against whichever tier the character currently owns.
  if (spellId != null && !(data.owned_spell_tiers[spellId] > 0)) return res.status(400).json({ error: "You don't own that spell." });

  const spellSlots = (Array.isArray(data.spell_slots) ? data.spell_slots.slice(0, effectiveSlotCount) : []);
  while (spellSlots.length < effectiveSlotCount) spellSlots.push(null);
  spellSlots[slotIndex] = spellId;
  data.spell_slots = spellSlots;
  data._save_seq = (data._save_seq || 0) + 1;
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(JSON.stringify(data), nowIso(), req.account.id, slot);
  res.json({ ok: true, spell_slots: data.spell_slots, _save_seq: data._save_seq });
});

// v0.23.1 (#7): pre-combat encounter step -- resolves ONLY the flee-chance roll for the new
// client-side Fight/Flee choice gate that now sits before a fight actually starts (see
// index.html's screenPreCombatEncounter()). No combat_sessions row exists yet at this point
// (that's only created by /api/combat/start below), so this reuses the exact same BASE
// flee-chance formula/gear-bonus lookup as the in-combat /flee endpoint above
// (CB.FLEE_FAIL_CHANCE - combatGetFleeChanceBonus()) but deliberately does NOT check
// session.flee_override_until (Entangle's guaranteed-flee override) since that's a
// combat-session-scoped buff and no session exists to hold it here, and does NOT resolve any
// monster turn/damage -- there's no monster attacking yet, so a failed roll here just means
// "proceed into a normal fight," where /api/combat/start's own engage first-strike roll
// (unchanged) is the only place an opening hit can land, exactly like it already does today.
app.post("/api/combat/pre-flee", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  const failed = Math.random() < Math.max(0, CB.FLEE_FAIL_CHANCE - combatGetFleeChanceBonus(data));
  res.json({ ok: true, fled: !failed });
});

app.post("/api/combat/start", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const monsterId = req.body?.monster_id;
  const areaLevel = Number(req.body?.area_level);
  const isGuardian = !!req.body?.is_guardian;
  const isRoamer = !!req.body?.is_roamer;
  const isNight = !!req.body?.is_night;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!Number.isInteger(areaLevel) || areaLevel < 1 || areaLevel > CB.AREA_LEVEL_MAX) return res.status(400).json({ error: "Invalid area level." });
  const monster = COMBAT_MONSTERS.find((m) => m.id === monsterId);
  if (!monster) return res.status(400).json({ error: "Unknown monster." });
  if (!combatMonsterAllowedForAreaLevel(monster, areaLevel)) return res.status(400).json({ error: "That monster isn't found at this area level." });

  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  if (areaLevel > (data.max_maze_depth_reached || 1)) return res.status(400).json({ error: "You haven't reached that area level yet." });
  // v0.24.1 (B1+B2): run the locked->active unlock pass at combat START too, not only at
  // kill-resolution time. A dev-bumped character's level can jump with no natural level-up
  // event to trigger this, so without a call here, a fight fought entirely at that already-
  // qualifying level could still resolve its kill against a quest that's technically still
  // "locked" if this were ever the very first quest-touching call of the session. The
  // kill-resolution call further down (in /attack's monsterDefeated branch) already covers the
  // reported repros on its own; this is the explicit "and on combat start" half of the fix.
  questEnsureState(data);

  // A stale/abandoned session (e.g. the client navigated away mid-fight) can't be reused --
  // starting a new encounter always retires whatever was previously active for this slot.
  db.prepare("DELETE FROM combat_sessions WHERE account_id = ? AND slot = ? AND status = 'active'").run(req.account.id, slot);

  const hpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_HP_MULT : isRoamer ? CB.ROAMING_MOB_HP_MULT : 1;
  const dmgMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_DAMAGE_MULT : isRoamer ? CB.ROAMING_MOB_DAMAGE_MULT : 1;
  const xpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_XP_MULT : isRoamer ? CB.ROAMING_MOB_XP_MULT : 1;
  // v0.24.2: rounded again after the guardian/roamer multiplier, so an elite's inflated pool is
  // a whole number too (combatMonsterHp() alone can't know about hpMult).
  const maxHp = Math.max(1, Math.round(combatMonsterHp(monster.base_hp, areaLevel) * hpMult));
  // v0.22.1 (Part A1): monster damage is now a single flat base_damage (the average of the
  // old min/max pair) instead of an independent per-hit range.
  // v0.22.2 (#1): dmgMin/dmgMax are no longer forced equal -- they're a tight symmetric band
  // around the flattened value, so the existing per-hit cbRandRange(dmgMin, dmgMax) roll (used
  // by both the first-strike hit below and combatResolveMonsterTurn()'s counter-attack/catch-up)
  // naturally produces gentle per-swing variance again (e.g. 41, 45, 39, 43...) while the mean
  // stays exactly the flattened value -- no DPS/balance change, purely cosmetic. Crit multiplier
  // still multiplies on top of whichever jittered value gets rolled.
  const dmgFlat = combatMonsterDamage(monster.base_damage, areaLevel) * dmgMult;
  const dmgMin = dmgFlat * (1 - CB.MONSTER_DAMAGE_JITTER), dmgMax = dmgFlat * (1 + CB.MONSTER_DAMAGE_JITTER);
  // v0.20 (#9.5): session.xp is the RAW pre-bonus-stack base reward (see combatAddXp()) --
  // the level-difference penalty is baked in here, at the source, alongside the
  // guardian/roamer multiplier, so it applies uniformly regardless of which bonus sources
  // later stack on top of it.
  const xp = combatMonsterXpReward(monster.base_xp, areaLevel) * xpMult * combatLevelDiffXpMult(data.level, areaLevel);
  const name = isGuardian ? `Guardian ${monster.name}` : isRoamer ? `Roaming ${monster.name}` : monster.name;

  combatSettleAllHeals(data);
  const log = [];
  let firstStrike = null;
  // v0.22.7 (#1): the engage first-strike also counts as "a monster's attack lands on the
  // player" -- Thorns reflects onto the monster here too, subtracted from its starting HP
  // before the combat_sessions row is even inserted below.
  let engageThornsDamage = 0;
  // v0.22 (batch2 #18): Touch of Unicorn (Invulnerability) is supposed to block ALL incoming
  // damage while active, but this engage-time first-strike roll never checked it -- a player
  // who triggered the shrine and then walked straight into a fight would still eat the
  // "monster strikes first" hit before ever getting a turn. combatIsInvulnerable(data) is the
  // same check /attack, /flee, and /use-item already gate their monster-turn damage on.
  if (!combatIsInvulnerable(data) && Math.random() < CB.MONSTER_FIRST_STRIKE_CHANCE) {
    let mdmg = cbRandRange(dmgMin, dmgMax);
    // v0.19.1 (#10): the first-strike hit can crit too, same formula/order as the regular
    // counter-attack in combatResolveMonsterTurn. v0.22.7 (#18): critMult is this hit's own
    // random roll between this monster's own floor and ceiling, same as that function.
    const crit = Math.random() < combatGetMonsterCritChance(areaLevel);
    const critMult = crit ? combatRollMonsterCritMultiplier(combatGetMonsterCritFloor(areaLevel), combatGetMonsterCritCeiling(areaLevel)) : null;
    if (crit) mdmg *= critMult;
    mdmg = Math.max(0, mdmg - combatGetArmor(data));
    data.current_hp = Math.max(1, (data.current_hp || 0) - Math.round(mdmg));
    firstStrike = { damage: Math.round(mdmg), crit, crit_mult: critMult };
    log.push(`The ${name} strikes first, hitting you for ${Math.round(mdmg)} damage${crit ? ` (Critical! x${critMult.toFixed(2)})` : ""} before you can react!`);
    // v0.28.1 BUG FIX (credit: Gwen, FOURTH report of decimals in the combat log). This is the
    // site every previous fix missed: a fourth, separate Thorns application that only fires on the
    // monster's first-strike at combat START, and which read combatGearBonus() raw while the two
    // in combatResolveMonsterTurn() were rounded. That is exactly why the fraction always appeared
    // on the FIRST line of the log and never again -- every later reflect came from the rounded
    // paths. Rounded here too, and the log line is built from the rounded value.
    const thorns = Math.round(combatGearBonus(data, "thorns"));
    if (thorns > 0) {
      engageThornsDamage = Math.round(Math.min(maxHp, thorns));
      log.push(`Your Thorns reflect ${engageThornsDamage} damage back at the ${name}!`);
    }
  }

  const id = crypto.randomBytes(16).toString("hex");
  const now = nowIso();
  // v0.21.1 (#9/#10): this session's own attack-speed pair, rolled once at fight-start and
  // held fixed for the fight's duration (re-gearing mid-fight doesn't retroactively speed up
  // an in-progress encounter) -- combatGetAttackSpeed() folds in the player's "attack_speed"
  // gear affix, combatMonsterAttackSpeed() the area-level breakpoint scaling. last_monster_hit_at
  // seeds the elapsed-time catch-up clock (see combatCatchUpMonsterHits()) at "now" so the very
  // first /attack or /tick call owes nothing yet -- the monster's first real hit is still either
  // the firstStrike roll above or the guaranteed counter-attack on the player's first action.
  const playerAttackSpeed = combatGetAttackSpeed(data);
  const monsterAttackSpeed = combatMonsterAttackSpeed(areaLevel);
  const lastMonsterHitAt = Date.now();
  // v0.22.7 (#1): the monster's persisted starting HP already reflects any Thorns damage from
  // the engage first-strike above -- a subsequent /attack call will naturally resolve the kill
  // (with full XP/gold/loot) once the player's next action lands, same as any other kill.
  const startingHp = Math.max(0, maxHp - engageThornsDamage);
  // v0.24.1 (C3): seed the regen clock at the same "now" as last_monster_hit_at -- the very
  // first combat-touching call owes 0 elapsed seconds of regen, same logic as the monster's
  // own catch-up clock.
  db.prepare(
    `INSERT INTO combat_sessions (id, account_id, slot, monster_id, name, area_level, max_hp, hp, dmg_min, dmg_max, xp, gold_min, gold_max, loot_table, is_guardian, is_roamer, is_night, status, created_at, updated_at, player_attack_speed, monster_attack_speed, last_monster_hit_at, last_regen_tick_at)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'active', ?, ?, ?, ?, ?, ?)`
  ).run(id, req.account.id, slot, monster.id, name, areaLevel, maxHp, startingHp, dmgMin, dmgMax, xp, monster.gold_min, monster.gold_max, monster.loot_table, isGuardian ? 1 : 0, isRoamer ? 1 : 0, isNight ? 1 : 0, now, now, playerAttackSpeed, monsterAttackSpeed, lastMonsterHitAt, lastMonsterHitAt);

  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({
    ok: true, session_id: id,
    monster: { name, hp: startingHp, max_hp: maxHp, attack_speed: monsterAttackSpeed },
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), attack_speed: playerAttackSpeed, ...combatRoundBuffsPayload(data) },
    first_strike: firstStrike, log, _save_seq: saveSeq,
  });
});

// v0.24.1 (C1/C2/B5): shared monster-defeat resolution -- extracted verbatim from
// POST /api/combat/:sessionId/attack's own monsterDefeated branch (which now just calls this)
// so /flee, /use-item, /tick, /cast, and the WS combat-tick pusher can ALL resolve a genuine
// kill instead of leaving the fight hanging at 0 HP forever whenever the elapsed-time
// catch-up's Thorns reflect (C2) or a settled spell DOT (C1) finishes the monster off outside
// of a player-initiated attack. Every gold/XP/loot/quest-tracking side effect is identical
// regardless of which call site triggers it -- the monster doesn't know or care whether the
// killing blow came from a landed swing, a Fireflies tick, or a Thorns counter-hit.
// `extraSessionFields` lets each caller merge in whatever ELSE it needs to persist on this same
// updateCombatSession() write (e.g. last_monster_hit_at, spell_dots) without this helper needing
// to know about every caller's own bookkeeping.
function combatFinalizeMonsterKill(req, session, data, extraSessionFields) {
  const gold = cbRandIntRange(session.gold_min, session.gold_max);
  const goldCredited = Math.round(gold * combatGetGoldFindMult(data, session));
  // v0.20 (#9.3): session.xp (the monster's raw base reward) is now handed to
  // combatAddXp() UNMODIFIED -- every bonus (Community XP, Forest Reputation, XP Find
  // gear, Experience Shrine) is summed and applied exactly once inside that function
  // instead of being partly pre-multiplied here and partly multiplied again inside it.
  const xpResult = combatAddXp(data, session.xp);
  const leveled = xpResult.leveled;
  // v0.22.3 (#10): the season is now won by "tier 5 AND Level 100", not by reaching tier 5
  // alone -- only check on an actual level-up, and only a tier-5 character can possibly
  // qualify (maybeDeclareSeasonWinner() checks both and no-ops otherwise).
  if (leveled) maybeDeclareSeasonWinner(req.account.id, data);
  creditAccountGold(req.account.id, goldCredited);
  combatIncrementKillStreak(data);
  // v-quest: Quest System hooks -- run on every server-resolved kill, mirroring exactly the
  // same trusted event the gold/XP/kill-streak grants right above already ride on. See the
  // Quest System section (above applyLadderReset()) for questTrackKillStreak()/
  // questTrackRoamerKill()/questTrackGuardianKill()/questTrackEliteKill()'s own comments.
  questEnsureState(data);
  questTrackKillStreak(data);
  questTrackTotalKills(data); // v0.25: "Cleanse the Forest"

  if (session.is_roamer) questTrackRoamerKill(data);
  if (session.is_guardian) questTrackGuardianKill(data, session.area_level);
  // v0.19.1 (#19): this endpoint used to persist the raw characters-table row via
  // saveCharacterRow() below but never forward max_kill_streak (or level/xp) into the
  // separate leaderboard_bests table that GET /api/leaderboard/killstreak actually reads --
  // so kills scored through server-authoritative combat never showed up on that leaderboard
  // tab. The Broken Bridge Trial endpoint already does this correctly; mirror it here.
  upsertLeaderboardBests(req.account.id, data);

  const magicFind = combatGetMagicFind(data, session);
  // v0.22.1 (Part A2d): look up the slain monster's band once, so both the guaranteed drop
  // and any bonus drops below skew toward its matching item rarity (see
  // combatRollRarityForBand()/MONSTER_BAND_LOOT_RARITY_WEIGHTS).
  const monsterForLoot = COMBAT_MONSTERS.find((m) => m.id === session.monster_id);
  const monsterBand = monsterForLoot ? monsterForLoot.tier : null;
  // v-quest: elder-band kill tracking (Q7) needs the monster's band, computed just above.
  questTrackEliteKill(data, monsterBand);
  // v0.24.1 (B5 "The Pacifist"): if the player never landed a direct hit/spell-damage tick
  // this whole fight, the monster still died (only possible via a now-lethal Thorns reflect,
  // C2) -- that's exactly the challenge this quest asks for.
  if (!session.player_dealt_direct_damage) questTrackPacifist(data);
  const rolled = combatRollLoot(session.loot_table, magicFind);
  let loot = { type: rolled.type };
  if (rolled.type === "gear") {
    const tier = combatRollItemTier(session.area_level);
    const inst = combatGenerateGearItem(tier, magicFind, monsterBand);
    const placement = combatAddGearAutoEquip(data, inst);
    loot = { type: "gear", item: inst, fit: placement.fit, auto_equipped: placement.autoEquipped };
  } else if (rolled.type === "consumable") {
    combatAddConsumable(data, rolled.item_id, 1);
    loot = { type: "consumable", item_id: rolled.item_id };
  } else if (rolled.type === "herb") {
    combatAddHerb(data, rolled.herb_id, 1);
    loot = { type: "herb", herb_id: rolled.herb_id };
  }

  // v0.20.1 (#10): roll for bonus loot beyond the guaranteed drop above. Roaming mobs and
  // Stronghold guardians ("special" spawns) both boost the CHANCE of a bonus drop AND the
  // effective Magic Find used to decide what it actually is, so their extra items skew
  // toward better quality too -- everything else about the roll (same loot table, same
  // gear-tier/auto-equip/consumable/herb handling as the guaranteed drop) is identical.
  const isSpecialSpawn = !!(session.is_roamer || session.is_guardian);
  const bonusMagicFind = isSpecialSpawn ? magicFind + CB.EXTRA_LOOT_SPECIAL_SPAWN_QUALITY_BONUS_PCT : magicFind;
  const extraLootCount = combatRollExtraLootCount(magicFind, isSpecialSpawn);
  const bonusLoot = [];
  for (let i = 0; i < extraLootCount; i++) {
    const extraRolled = combatRollLoot(session.loot_table, bonusMagicFind);
    if (extraRolled.type === "gear") {
      const extraTier = combatRollItemTier(session.area_level);
      const extraInst = combatGenerateGearItem(extraTier, bonusMagicFind, monsterBand);
      const extraPlacement = combatAddGearAutoEquip(data, extraInst);
      bonusLoot.push({ type: "gear", item: extraInst, fit: extraPlacement.fit, auto_equipped: extraPlacement.autoEquipped });
    } else if (extraRolled.type === "consumable") {
      combatAddConsumable(data, extraRolled.item_id, 1);
      bonusLoot.push({ type: "consumable", item_id: extraRolled.item_id });
    } else if (extraRolled.type === "herb") {
      combatAddHerb(data, extraRolled.herb_id, 1);
      bonusLoot.push({ type: "herb", herb_id: extraRolled.herb_id });
    }
    // "nothing" results simply contribute no entry -- same as the guaranteed roll can do.
  }

  // v0.25: Scroll of Wisdom roll. Its tier is the highest one this area level can legitimately
  // produce (the same bracket gear tiers use), so deeper areas hand out the bigger scrolls.
  let scrollDrop = null;
  if (Math.random() < SCROLL_OF_WISDOM_DROP_CHANCE) {
    const scrollTier = combatItemTierForAreaLevel(session.area_level);
    const scrollItemId = SCROLL_OF_WISDOM_IDS[scrollTier];
    if (scrollItemId) {
      combatAddConsumable(data, scrollItemId, 1);
      scrollDrop = { tier: scrollTier, item_id: scrollItemId };
    }
  }

  let keyDrop = null;
  if (session.is_guardian) {
    const keyEligible = combatStrongholdKeyEligible(session.area_level, data.level);
    if (keyEligible && Math.random() < CB.STRONGHOLD_KEY_DROP_CHANCE) {
      const keyTier = combatStrongholdKeyTierForAreaLevel(session.area_level);
      const keyItemId = combatStrongholdKeyItemIdForTier(keyTier);
      combatAddConsumable(data, keyItemId, 1);
      keyDrop = { tier: keyTier, item_id: keyItemId };
    }
    keyDrop = keyDrop || { eligible: keyEligible, dropped: false };
  }

  const kill = {
    gold, gold_credited: goldCredited, xp_gained: xpResult.xpGained, leveled,
    loot, bonus_loot: bonusLoot, key_drop: keyDrop, scroll_drop: scrollDrop, kill_streak: data.kill_streak, max_kill_streak: data.max_kill_streak,
    total_kills: data.total_kills,
    is_guardian: !!session.is_guardian, is_roamer: !!session.is_roamer,
    // v0.24.2 BUG FIX (credit: Gwen): the quest trackers above just moved this character's
    // board forward, but nothing ever told the client about it -- so PS.quests stayed frozen at
    // whatever it held when the screen last loaded, and the next autosave pushed that stale copy
    // back (see PUT /api/characters/:slot, which no longer accepts it). Riding the authoritative
    // board back on the kill payload itself, rather than on each individual response body, means
    // every call site that can land a kill carries it for free: the attack/cast/use-item/flee
    // routes AND the server-clock WebSocket pusher, which is the only path a Thorns or spell-DOT
    // killing blow ever travels.
    quests: data.quests,
    quest_attr_bonus: data.quest_attr_bonus,
    quest_bonus_hp: data.quest_bonus_hp,
    unspent_quest_stat_points: data.unspent_quest_stat_points,
    // The Pacifist and several gear-driven fixes can change the HP ceiling mid-fight, so hand
    // back the recomputed pools alongside it.
    max_hp: combatGetMaxHp(data),
  };
  updateCombatSession(session.id, Object.assign({ hp: 0, status: "won" }, extraSessionFields || {}));
  return kill;
}

app.post("/api/combat/:sessionId/attack", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });

  combatSettleAllHeals(data);
  combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player
  // v0.23.0 (Part B7): settle any queued Fireflies DOT ticks against the monster before this
  // round's own action -- same lazy on-demand pattern as combatSettleAllHeals() just above,
  // applied to session.hp instead of the player's HP/Stamina/Mana.
  const spellDotResult = combatSettleSpellDots(session, data);
  if (spellDotResult.dotsChanged) {
    session.hp = spellDotResult.newHp;
    // v0.24.1 (C1/B5): combatSettleSpellDots() stamps session.player_dealt_direct_damage=1 in
    // place the moment a tick lands -- persist that alongside hp/spell_dots so it survives past
    // this request (it's a monotonic flag, so re-writing 1 here whenever it's already 1 is a
    // harmless no-op, and it's the only place a Fireflies/Siphon/Entangle tick's damage gets
    // written back to the row at all).
    updateCombatSession(session.id, { hp: spellDotResult.newHp, spell_dots: JSON.stringify(spellDotResult.newDots || []), player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });
  }
  const quadActiveThisRound = combatHasQuadDamage(data);
  const invulnActiveThisRound = combatIsInvulnerable(data);

  // v0.21.1 (#9): resolve any monster hits owed from real elapsed wall-clock time BEFORE this
  // round's own action -- see combatCatchUpMonsterHits(). On a fast, actively-played round (and
  // in every existing automated test, which calls endpoints back-to-back with near-zero real
  // elapsed time) this resolves 0 ticks and changes nothing below. If catch-up alone is fatal,
  // the fight ends here -- the player's own swing never happens, matching how a real
  // continuously-attacking monster would have already won. v0.21.5: this catch-up is now the
  // ONLY source of monster damage during this whole handler -- see the non-kill branch below.
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    // HOTFIX: perform the atomic hardcore permadeath (if applicable) in THIS same handler,
    // right where fatal is first produced -- see combatHandleHardcoreDeath()'s own comment.
    // A hardcore-killed character's row no longer exists, so saveCharacterRow() is skipped
    // for it (it would just be a harmless 0-row UPDATE, but skipping is clearer intent).
    const hcResult = combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session);
    const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, mob_blocked: false, player_hit: null, weapon_skill: null,
      monster: { hp: session.hp, max_hp: session.max_hp, defeated: false },
      kill: null, monster_turn: null, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
      fatal: true, hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
    });
  }

  combatTickCombatRoundBuffs(data);

  const mobBlocked = Math.random() < combatMobBlockChance(session.area_level);
  let playerHit = null, weaponSkill = null, newMonsterHp = session.hp;
  // v0.26.2: how much Life on Hit healed on this swing, reported back so the client can log it
  // and float it off the player rather than the heal happening invisibly.
  let playerLifeOnHit = 0;
  if (!mobBlocked) {
    // v0.19.2 (#19): player damage is now a flat number (the average of what used to be its
    // min-max range) instead of an independent per-hit RNG roll -- per Gwen's exact spec,
    // attacks already carry plenty of variance from crit chance and block chance, so a THIRD
    // independent roll on top of those (the old cbRandRange(lo, hi) here) was making outcomes
    // swingier than intended without adding any real player-facing choice. The class's base
    // min/max spread (COMBAT_CLASSES/index.html's CLASSES) is kept as-is and still feeds
    // combatGetDamageRange() unchanged -- only collapsed to its average at the very last step,
    // right before crit/quad multipliers apply, so every other formula (gear bonus, weapon
    // skill %, level scaling) stays exactly the same as before.
    const [lo, hi] = combatGetDamageRange(data);
    let dmg = (lo + hi) / 2;
    const crit = Math.random() < combatGetCritChance(data);
    // v0.21 (#8/#10): critMult is THIS hit's own random roll between CB.CRIT_MULTIPLIER and
    // the player's current ceiling (combatGetCritMultiplierMax(), driven by the new
    // "crit_multiplier" gear affix) -- not a fixed 1.75 every time. null on a non-crit hit.
    const critMult = crit ? combatRollCritMultiplier(combatGetCritMultiplierMax(data)) : null;
    if (crit) {
      dmg *= critMult;
    } else {
      // v0.22.6 (#24): tight +/-% jitter band on non-crit hits only, mirroring the monster's
      // own per-hit jitter (v0.22.2 #1) -- see CB.PLAYER_DAMAGE_JITTER above.
      dmg *= cbRandRange(1 - CB.PLAYER_DAMAGE_JITTER, 1 + CB.PLAYER_DAMAGE_JITTER);
    }
    if (quadActiveThisRound) dmg *= 4;
    // v0.26.2 BUG FIX (credit: Gwen, third report of decimals in the combat log -- and my two
    // previous fixes both missed this). `dmg` is a float: the crit multiplier and the per-hit
    // jitter band above both scale it. The number REPORTED to the player was rounded (see
    // playerHit below), but the number SUBTRACTED FROM THE MONSTER was not -- so from the very
    // first swing session.hp carried a fractional part, and everything downstream that clamps to
    // it inherited that fraction. A Thorns reflect or a spell DOT landing the killing blow does
    // Math.min(session.hp, ...), which is exactly how "reflect 3.1999999999999997 damage" reached
    // the log. Rounding the monster's spawn HP and rounding the log line both treated symptoms;
    // this is the source. Every write to session.hp is integral now, so no clamp anywhere can
    // produce a fraction again.
    dmg = Math.round(dmg);
    newMonsterHp = session.hp - dmg;
    // v0.24.1 (B5/C2): a landed physical swing always counts as "direct damage" for The
    // Pacifist quest -- mutated in place here so combatFinalizeMonsterKill() (if this swing is
    // also the killing blow) sees it immediately; persisted below regardless of which branch
    // (monsterDefeated or not) actually runs.
    session.player_dealt_direct_damage = 1;
    // v0.26.2 BUG FIX (credit: Gwen): "Life on Hit" rolled on gear, showed on the Character Sheet
    // and had no consumer anywhere -- it healed nothing and logged nothing, because it was never
    // wired up. It heals a flat amount on every LANDED physical swing (not a miss, not a spell,
    // not a Thorns reflect), which is what the affix name promises. Clamped to missing health so
    // it can never overheal, and rounded like every other pool figure.
    const lifeOnHit = Math.round(combatGearBonus(data, "life_on_hit"));
    if (lifeOnHit > 0) {
      const maxHpNow = combatGetMaxHp(data);
      const healed = Math.min(lifeOnHit, Math.max(0, maxHpNow - (data.current_hp || 0)));
      if (healed > 0) { data.current_hp = (data.current_hp || 0) + healed; playerLifeOnHit = healed; }
    }
    playerHit = { damage: dmg, crit, crit_mult: critMult, quad_damage: quadActiveThisRound, life_on_hit: playerLifeOnHit };
    // v0.22.3 (#20, credit: Gwen): no weapon-skill XP from a hit swung with an equipped
    // weapon that no longer meets its level/class requirement (e.g. after a demotion) --
    // mirrors the same combatCanEquipGear() gate used everywhere else gear validity matters.
    const weaponType = combatGetEquippedWeaponType(data);
    if (weaponType && data.equipped && data.equipped.weapon && combatCanEquipGear(data, data.equipped.weapon)){
      weaponSkill = { weapon_type: weaponType, ...combatRegisterWeaponHit(data, weaponType) };
    }
  }

  // v0.22.7 (#1): used to require `!mobBlocked` too, on the assumption that monster HP could
  // only ever drop from the player's OWN landed swing this round. Thorns (above) can now also
  // drain the monster's HP purely from the elapsed-time catch-up hits it took THIS round,
  // independent of whether the player's own swing connects -- so a monster already at 0 from
  // thorns must still register as defeated even on a round where the player's own hit was
  // blocked.
  const monsterDefeated = newMonsterHp <= 0;
  let kill = null, monsterTurn = null, fatal = false;

  if (monsterDefeated) {
    newMonsterHp = 0;
    // v0.24.1 (C1/C2/B5): delegates to the shared helper (see its own comment right before this
    // route) -- identical gold/XP/loot/quest-tracking/Pacifist-check behavior as before, just
    // reusable from every other combat call site that can also land a kill now.
    kill = combatFinalizeMonsterKill(req, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0, player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });
  } else {
    // v0.21.5 BUG FIX: this used to ALSO call combatResolveMonsterTurn() here -- a guaranteed
    // monster counter-hit every time the player attacked and didn't land a killing blow, on
    // top of the independent elapsed-time catch-up above. That was a leftover from the old
    // turn-based combat model (pre-v0.21.1) and, combined with the new continuous attack-speed
    // system, meant the player took damage twice: once from the monster's own tempo, and once
    // more just for having clicked. Per Gwen's exact spec, the two attack loops must be fully
    // decoupled -- the monster ONLY ever deals damage on its own independent attack-speed tempo
    // (combatCatchUpMonsterHits, above), never as a side effect of the player's own swing.
    // monsterTurn stays null and fatal stays false here now; a kill this round can only ever
    // come from the elapsed-time catch-up already resolved earlier in this handler.
    updateCombatSession(session.id, { hp: newMonsterHp, last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0, player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });
  }

  if (fatal) updateCombatSession(session.id, { status: "lost" });
  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, mob_blocked: mobBlocked, player_hit: playerHit, weapon_skill: weaponSkill,
    monster: { hp: Math.max(0, newMonsterHp), max_hp: session.max_hp, defeated: monsterDefeated },
    kill, monster_turn: monsterTurn, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks,
    // v0.22 (batch2 #5): current_stamina/max_stamina now echoed here too (previously only
    // current_hp was) -- combatTickCombatRoundBuffs() above may have applied a Stamina Regen
    // gear-affix tick this round, and the client must adopt the server's post-tick value the
    // same way it already does for current_hp, or its own copy silently desyncs.
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
    fatal, _save_seq: saveSeq,
  });
});

app.post("/api/combat/:sessionId/flee", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });

  combatSettleAllHeals(data);
  combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player
  // v0.21.1 (#9): elapsed-time catch-up applies to fleeing too -- deciding whether to run
  // still costs real wall-clock time the monster keeps swinging through. Captured before the
  // fail-roll so a fatal catch-up ends the fight before the flee attempt itself resolves.
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    const hcResult = combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session);
    const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, failed: null, monster_turn: null, monster_ticks: catchUp.ticks, fatal: true,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
      hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
    });
  }

  // v0.24.1 (C2): the elapsed-time catch-up above can now land a genuinely lethal Thorns
  // reflect hit even though the PLAYER survived (catchUp.fatal only covers the player's own
  // death) -- if the monster is already dead, resolve that kill right here instead of letting
  // the flee attempt proceed against a corpse (or worse, silently leaving the fight "active"
  // forever at 0 HP, the exact bug this ticket reports).
  if (session.hp <= 0) {
    const kill = combatFinalizeMonsterKill(req, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, failed: null, monster_turn: null, monster_ticks: catchUp.ticks, fatal: false,
      monster: { hp: 0, max_hp: session.max_hp, defeated: true }, kill,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
      hardcore_killed: false, _save_seq: saveSeq,
    });
  }

  // v0.23.0 (Part B7): Entangle overrides the flee roll entirely to a guaranteed success for
  // its duration (session.flee_override_until), checked ahead of the normal gear-modified
  // fail-chance formula -- reverts to that normal formula the instant the override expires.
  const fleeOverrideActive = !!(session.flee_override_until && Date.now() < session.flee_override_until);
  // v0.21 (#6): "flee_chance" gear affix reduces the fail chance (can drive it to 0 or below,
  // which Math.max clamps to 0 -- an always-succeed flee -- per Gwen's exact spec).
  const failed = fleeOverrideActive ? false : (Math.random() < Math.max(0, CB.FLEE_FAIL_CHANCE - combatGetFleeChanceBonus(data)));
  let monsterTurn = null, fatal = false;
  // HOTFIX: tracks whether combatHandleHardcoreDeath() actually performed the atomic
  // permadeath below -- distinct from `fatal` alone, since `fatal` is also true for a
  // softcore character (which stays on the existing client-driven penalty-only flow).
  let hcResult = { hardcoreKilled: false };
  if (failed) {
    // v0.20 BUG FIX: a failed flee attempt still gives the monster a turn (see
    // combatResolveMonsterTurn() below) -- that's exactly the kind of round Touch of
    // Unicorn/Quad Damage/Magic Find are meant to count against, so it needs to tick the
    // same as a real attack does. invulnActiveThisRound was captured BEFORE the tick (above,
    // ahead of the catch-up call now too) so a shield that's about to expire still blocks
    // THIS round's hit, matching /attack's exact ordering.
    combatTickCombatRoundBuffs(data);
    monsterTurn = combatResolveMonsterTurn(data, session, invulnActiveThisRound);
    fatal = monsterTurn.fatal;
    updateCombatSession(session.id, fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 } : { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    // HOTFIX: this is the ONE fatal site in this whole combat section that is NOT reached
    // via combatCatchUpMonsterHits -- the failed-flee guaranteed hit calls
    // combatResolveMonsterTurn() directly, so it needs its own combatHandleHardcoreDeath()
    // call rather than sharing the catchUp.fatal early-return above.
    if (fatal) hcResult = combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session);
  } else {
    updateCombatSession(session.id, { status: "fled", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
  }
  const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, failed, monster_turn: monsterTurn, monster_ticks: catchUp.ticks, fatal,
    // v0.22 (batch2 #5): see the identical comment on /attack's success response above --
    // a failed flee still ticks combat-round buffs (including regen), so current_stamina must
    // be echoed here too.
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
    hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
  });
});

app.post("/api/combat/:sessionId/use-item", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const itemId = req.body?.item_id;
  const item = COMBAT_CONSUMABLES[itemId];
  if (!item) return res.status(400).json({ error: "Unknown consumable." });

  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  if (!((data.consumables && data.consumables[itemId]) > 0)) return res.status(400).json({ error: "You don't have that item." });

  combatSettleAllHeals(data);
  combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player

  // v0.21.1 (#9): elapsed-time catch-up applies here too -- opening the item menu and picking
  // a potion still costs real wall-clock time the monster keeps swinging through.
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    const hcResult = combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session);
    const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, fatal: true,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
      hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
    });
  }

  // v0.24.1 (C2/B5): THIS is the call site that matters most for The Pacifist -- a player who
  // never attacks and only drinks potions while Thorns kills the monster off would otherwise
  // hit this exact "opened the item menu against a corpse" gap forever, since nothing here used
  // to check whether the catch-up above just finished the monster off. Resolve the kill (and,
  // per the Pacifist tracking above, credit it as a no-direct-damage kill) instead of proceeding
  // into the potion-use logic below.
  if (session.hp <= 0) {
    const kill = combatFinalizeMonsterKill(req, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, fatal: false,
      monster: { hp: 0, max_hp: session.max_hp, defeated: true }, kill,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
      _save_seq: saveSeq,
    });
  }
  updateCombatSession(session.id, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });

  let used = false;
  const totalTicks = CB.POTION_HEAL_DURATION_MS; // durationMs directly, rate = amount/durationMs
  if ((item.heal_amount || 0) > 0) {
    const maxHp = combatGetMaxHp(data);
    const pendingHeal = data.srv_heal ? data.srv_heal.rate * data.srv_heal.remainingMs : 0;
    if ((data.current_hp || 0) + pendingHeal < maxHp) {
      data.srv_heal = combatQueueHeal(data.srv_heal, item.heal_amount, maxHp - (data.current_hp || 0), totalTicks);
      used = true;
    }
  }
  if ((item.stamina_amount || 0) > 0) {
    const maxStamina = combatGetMaxStamina(data);
    const pendingStamina = data.srv_stamina_heal ? data.srv_stamina_heal.rate * data.srv_stamina_heal.remainingMs : 0;
    if ((data.current_stamina || 0) + pendingStamina < maxStamina) {
      data.srv_stamina_heal = combatQueueHeal(data.srv_stamina_heal, item.stamina_amount, maxStamina - (data.current_stamina || 0), totalTicks);
      used = true;
    }
  }
  // v0.23.0 (Part B3): mana potions, mirrors the stamina_amount branch immediately above.
  if ((item.mana_amount || 0) > 0) {
    const maxMana = combatGetMaxMana(data);
    const curMana = combatGetCurrentMana(data);
    const pendingMana = data.srv_mana_heal ? data.srv_mana_heal.rate * data.srv_mana_heal.remainingMs : 0;
    if (curMana + pendingMana < maxMana) {
      data.srv_mana_heal = combatQueueHeal(data.srv_mana_heal, item.mana_amount, maxMana - curMana, totalTicks);
      used = true;
    }
  }
  if (item.cures_poison && data.poison_expires_at && data.poison_expires_at > Date.now()) {
    data.poison_expires_at = 0;
    data.poison_pct_per_sec = 0;
    used = true;
  }
  if (!used) {
    // Catch-up may still have damaged the player even though the item itself was a no-op
    // (e.g. already at full HP) -- persist that regardless of the 400 below.
    saveCharacterRow(req.account.id, session.slot, data);
    return res.status(400).json({ error: "That wouldn't do anything right now." });
  }

  const rem = (data.consumables[itemId] || 0) - 1;
  if (rem > 0) data.consumables[itemId] = rem; else delete data.consumables[itemId];

  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, monster_ticks: catchUp.ticks,
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
    _save_seq: saveSeq,
  });
});

// v0.21.1 (#9): idle-polling endpoint -- the client calls this periodically while a fight is
// active but the player isn't taking an action (e.g. reading the combat log, deciding what to
// do), purely so the elapsed-time catch-up (see combatCatchUpMonsterHits()) can land damage in
// real time instead of only being discovered on the player's next click.
app.post("/api/combat/:sessionId/tick", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });

  combatSettleAllHeals(data);
  combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player
  // v0.23.0 (Part B7): idle-polling must settle spell DOTs too, same as /attack -- otherwise
  // a Fireflies cast would only visibly progress while the player kept attacking.
  const spellDotResult = combatSettleSpellDots(session, data);
  if (spellDotResult.dotsChanged) {
    session.hp = spellDotResult.newHp;
    updateCombatSession(session.id, { hp: spellDotResult.newHp, spell_dots: JSON.stringify(spellDotResult.newDots || []), player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });
  }
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);

  // v0.24.1 (C1/C2): the idle-poll tick is the single most likely place a Thorns-reflect kill
  // (C2) or a settled Fireflies/Siphon/Entangle DOT kill (C1) actually lands, since it's the
  // one call site that fires purely from real elapsed time with no player action at all.
  // Resolve it here exactly like every other endpoint does now, before falling through to the
  // old "still fighting" response shape.
  if (!catchUp.fatal && session.hp <= 0) {
    const kill = combatFinalizeMonsterKill(req, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks, fatal: false,
      monster: { hp: 0, max_hp: session.max_hp, defeated: true }, kill,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
      hardcore_killed: false, _save_seq: saveSeq,
    });
  }

  updateCombatSession(session.id, catchUp.fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 } : { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });

  const hcResult = catchUp.fatal ? combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session) : { hardcoreKilled: false };
  const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks, fatal: catchUp.fatal,
    monster: { hp: session.hp, max_hp: session.max_hp },
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
    hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
  });
});

// v0.23.0 (Part B7): spellcasting -- mirrors POST /api/combat/:sessionId/attack's exact
// route/auth/session-loading boilerplate. Validates ownership/slotting/level-req/cooldown/
// mana entirely server-side (never trusts the client), applies the spell's effect, and tells
// the client to advance its own attack-timer beat (see "consumed_attack_beat" below) so a
// cast can't ALSO land a physical hit in the same instant.
app.post("/api/combat/:sessionId/cast", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const spellId = req.body?.spell_id;
  const spellDef = SPELLS[spellId];
  if (!spellDef) return res.status(400).json({ error: "Unknown spell." });

  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });

  // v0.24: casting resolves against the character's currently-OWNED tier of this spell (loadCharacterRow
  // already ran migrateSpellTiers(), so a pre-v0.24 owner reads as Tier I here for free).
  // v0.25.1: the CASTABLE tier, which steps down below the purchased tier when the character's
  // level no longer supports it (see combatEffectiveSpellTier). Returning 0 here means no tier of
  // this spell is usable yet, which the ownership check below reports.
  const ownedTier = combatEffectiveSpellTier(data, spellId);
  if (ownedTier < 1) return res.status(400).json({ error: "You don't know that spell." });
  const spell = spellAtTier(spellId, ownedTier);
  const spellSlots = Array.isArray(data.spell_slots) ? data.spell_slots : [];
  if (!spellSlots.includes(spellId)) return res.status(400).json({ error: "That spell isn't slotted." });
  if ((data.level || 1) < spell.level_req) return res.status(400).json({ error: `Requires level ${spell.level_req}.` });

  let cooldowns;
  try { cooldowns = JSON.parse(session.spell_cooldowns || "{}"); } catch (e) { cooldowns = {}; }
  const now = Date.now();
  if (cooldowns[spellId] && now < cooldowns[spellId]) {
    return res.status(400).json({ error: "That spell is still on cooldown.", ready_at: cooldowns[spellId] });
  }

  combatSettleAllHeals(data);
  combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player
  const currentMana = combatGetCurrentMana(data);
  // v0.23.6 (Item 4): Wizard Set 6pc bonus -- spells cost WIZARD_SET_6PC_MANA_COST_REDUCTION_PCT%
  // less mana. Computed once here and reused for both the affordability check and the actual
  // deduction below, so they can never disagree.
  const effectiveManaCost = (combatSetBonusesFor(data, "wizard_set").count >= 6)
    ? Math.round(spell.mana_cost * (1 - CB.WIZARD_SET_6PC_MANA_COST_REDUCTION_PCT / 100))
    : spell.mana_cost;
  if (currentMana < effectiveManaCost) return res.status(400).json({ error: "Not enough mana." });

  // Settle any already-queued spell DOT ticks (e.g. an earlier Fireflies cast) before this
  // cast layers a new effect on top -- same lazy on-demand pattern combatSettleAllHeals()
  // already uses for HP/Stamina/Mana potions, applied here to the monster's HP instead.
  const dotResult = combatSettleSpellDots(session, data);
  // v0.24.1 (C1/C2 latent-bug fix): session.hp MUST be synced from dotResult.newHp immediately,
  // BEFORE combatCatchUpMonsterHits runs below -- that call (and the Thorns reflect damage it
  // can apply as a side effect) mutates session.hp directly, not the local `monsterHp` variable.
  // Previously monsterHp was only a local copy, so any Thorns damage landed during THIS cast's
  // catch-up window was silently discarded when the endpoint's own updateCombatSession() call
  // overwrote hp with the stale pre-catch-up value. Keeping session.hp authoritative here fixes
  // that regardless of how monsterHp is used for the response payload below.
  session.hp = dotResult.newHp;
  let monsterHp = dotResult.newHp;
  let spellDots = dotResult.newDots || [];

  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0, hp: session.hp, spell_dots: JSON.stringify(spellDots), player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });
    const hcResult = combatHandleHardcoreDeath(req.account.id, req.account.username, session.slot, data, session);
    const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, spell_dot_ticks: dotResult.ticks, fatal: true,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
      hardcore_killed: hcResult.hardcoreKilled, _save_seq: saveSeq,
    });
  }

  // v0.24.1 (C2): a Thorns reflect hit landed during this cast's catch-up window (above) may
  // have finished the monster off before the cast's own effect even resolves -- e.g. queuing a
  // new Fireflies volley onto a monster that's about to die to reflect damage from an EARLIER
  // volley's catch-up tick. Resolve that kill now, exactly like every other endpoint does, and
  // bail out before layering a fresh spell effect onto an already-dead session.
  if (session.hp <= 0) {
    const kill = combatFinalizeMonsterKill(req, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0, spell_dots: "[]" });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, spell_dot_ticks: dotResult.ticks, fatal: false,
      monster: { hp: 0, max_hp: session.max_hp, defeated: true }, kill,
      player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_mana: cbInt(data.current_mana), max_mana: combatGetMaxMana(data), ...combatRoundBuffsPayload(data) },
      _save_seq: saveSeq,
    });
  }

  // Everything below only happens once we know the cast will actually resolve: deduct mana,
  // set this spell's cooldown, and increment the B9 spellcasting hit counter -- committed
  // regardless of which effect branch fires next.
  data.current_mana = currentMana - effectiveManaCost;
  // v0.28 (credit: Gwen): Spell Cooldown Reduction finally does something. This is the one place
  // a cooldown is started, so applying it here covers every spell without touching any of them.
  cooldowns[spellId] = now + combatGetSpellCooldownMs(data, spell.cooldown_ms);
  const skillResult = combatRegisterSpellcastHit(data);

  // v0.29.1 (credit: Gwen): Quad Damage now multiplies SPELL damage too. It only ever scaled the
  // weapon swing (see quadActiveThisRound in the attack route), so a caster who found a Furious
  // Shrine got a buff that did nothing for the way they actually fight -- and the shrine's own
  // description promises to quadruple "your damage output" without qualification.
  //
  // Applied to DAMAGE magnitudes only, deliberately not to Heal. Quadrupling a heal would turn a
  // damage shrine into the strongest sustain in the game for the rounds it lasts, which is a
  // different buff wearing the same name. Entangle's root duration and every cooldown are
  // likewise untouched: they are utility, not magnitude, which is the same line
  // combatGetSpellEffectivenessMult() already draws.
  //
  // spellMult is what every damage number below scales by; `mult` stays as the un-quadrupled
  // value so Heal keeps reading it.
  const mult = combatGetSpellEffectivenessMult(data);
  const quadActiveForSpell = combatHasQuadDamage(data);
  const spellMult = quadActiveForSpell ? mult * 4 : mult;
  const castResult = { spell_id: spellId, quad_damage: quadActiveForSpell };
  if (spell.effect === "heal") {
    const maxHp = combatGetMaxHp(data);
    const amount = Math.round(spell.heal_amount * mult);
    data.current_hp = Math.min(maxHp, (data.current_hp || 0) + Math.round(amount));
    castResult.heal_amount = amount;
  } else if (spell.effect === "fireflies" || spell.effect === "drain") {
    const scaledMin = Math.round(spell.hit_min * spellMult), scaledMax = Math.round(spell.hit_max * spellMult);
    const intervalMs = Math.round(spell.duration_ms / spell.hit_count);
    const dot = { spell_id: spellId, hits_remaining: spell.hit_count, next_hit_at: now + intervalMs, interval_ms: intervalMs, dmg_min: scaledMin, dmg_max: scaledMax };
    // v0.23.1 (#2): Verdant Siphon's effect:"drain" is otherwise IDENTICAL to Fireflies' queued-
    // DOT shape above -- it just also carries heal_pct, which combatSettleSpellDots() reads to
    // heal the caster for 50% of each tick's own realized (post-scaling) damage as that tick
    // resolves. Fireflies itself never sets heal_pct, so its dots are unaffected.
    if (spell.heal_pct) dot.heal_pct = spell.heal_pct;
    spellDots.push(dot);
    castResult.queued_hits = spell.hit_count;
  } else if (spell.effect === "entangle") {
    // Root/cooldown are pure utility -- NEVER scaled by `mult`, per Gwen's exact spec.
    updateCombatSession(session.id, { rooted_until: now + spell.duration_ms, flee_override_until: now + spell.duration_ms });
    castResult.rooted_until = now + spell.duration_ms;
    // v0.24 (Option B, Gwen's lean): Entangle also queues a scaled crushing-vine DOT on the
    // same queued-tick mechanism Fireflies/Verdant Siphon use above, on its own fixed 6-hit/2s
    // cadence (spellDef.dot_hit_count/dot_duration_ms, shared across every tier) -- only the
    // per-tick dot_min/dot_max magnitude climbs by tier, and IS scaled by `mult` like any other
    // damage magnitude (unlike the root duration/cooldown just above).
    const dotHitCount = spellDef.dot_hit_count || 6;
    const dotIntervalMs = Math.round((spellDef.dot_duration_ms || 2000) / dotHitCount);
    const scaledDotMin = Math.round(spell.dot_min * spellMult), scaledDotMax = Math.round(spell.dot_max * spellMult);
    spellDots.push({ spell_id: spellId, hits_remaining: dotHitCount, next_hit_at: now + dotIntervalMs, interval_ms: dotIntervalMs, dmg_min: scaledDotMin, dmg_max: scaledDotMax });
    castResult.queued_hits = dotHitCount;
  }

  updateCombatSession(session.id, {
    hp: monsterHp, spell_dots: JSON.stringify(spellDots),
    spell_cooldowns: JSON.stringify(cooldowns), last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0,
    player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0,
  });

  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, cast: castResult, spell_skill: { leveled_up: skillResult.leveledUp, new_level: skillResult.newLevel },
    monster_ticks: catchUp.ticks, spell_dot_ticks: dotResult.ticks,
    monster: { hp: Math.max(0, monsterHp), max_hp: session.max_hp },
    // v0.23.0 (Part B7 #3): this project's player attack-speed gate is entirely CLIENT-side
    // (see index.html's tryFireAttack()/cb._lastAttackFiredAt -- there is no server-held
    // "next attack allowed" field for the player at all, only the monster's own
    // last_monster_hit_at catch-up clock above). "Consuming this beat's attack action" is
    // therefore a client-side responsibility: the client stamps cb._lastAttackFiredAt the
    // instant this call succeeds, exactly as tryFireAttack() already does after a normal
    // landed swing, which delays the next auto-attack tick by the same attack-speed interval.
    // This flag just confirms to the client that it should do so.
    consumed_attack_beat: true,
    player: { current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data), current_mana: cbInt(data.current_mana), max_mana: combatGetMaxMana(data), ...combatRoundBuffsPayload(data) },
    _save_seq: saveSeq,
  });
});

/* Storage Vault (v0.11): one shared 200-slot chest per ACCOUNT (not per character), so
   every character slot on the same account can deposit into and withdraw from the same
   stash. Scoped strictly by req.account.id (set by requireAuth from the caller's own
   session token) -- there is no accountId taken from the request body or URL anywhere
   here, so there is no way to address another account's vault, logged-in or not.

   v0.15 BUG FIX (item duplication): this used to be a plain client-trusted full-replace
   blob with no concurrency control at all -- if two of your own characters had the vault
   open in two browser tabs, both would read the SAME snapshot, and whichever one's PUT
   landed LAST would win, silently discarding whatever the other tab did in between. Gwen's
   reported dupe (deposit from A, both withdraw, both re-deposit) is exactly this: two
   stale reads racing to write back their own full copy of "the vault as I last saw it."

   The fix is optimistic concurrency (compare-and-swap) on a `version` counter: GET now
   returns the version alongside the items, and PUT must include the version it read.
   The write only succeeds if that still matches the CURRENT version in the database --
   otherwise someone else's vault mutation landed first, and this PUT is rejected with 409
   so the client is forced to refetch the real current state and retry, instead of blindly
   clobbering it. This can't fully replace real server-side item ownership tracking (see
   the anti-cheat roadmap discussion), but it closes this specific race/dupe path. */
app.get("/api/vault", requireAuth, (req, res) => {
  const row = db.prepare("SELECT data, version FROM vaults WHERE account_id = ?").get(req.account.id);
  let items = [];
  let version = 0;
  if (row) {
    try {
      items = JSON.parse(row.data);
    } catch (e) {
      items = [];
    }
    version = row.version || 0;
  }
  res.json({ items, capacity: VAULT_CAPACITY, version });
});

app.put("/api/vault", requireAuth, (req, res) => {
  const items = req.body && req.body.items;
  const clientVersion = req.body && Number.isInteger(req.body.version) ? req.body.version : -1;
  if (!Array.isArray(items)) return res.status(400).json({ error: "items must be an array." });
  if (items.length > VAULT_CAPACITY) {
    return res.status(400).json({ error: `The vault only holds ${VAULT_CAPACITY} items.` });
  }

  const existing = db.prepare("SELECT version, data FROM vaults WHERE account_id = ?").get(req.account.id);
  const currentVersion = existing ? existing.version || 0 : 0;

  // v0.18.4: every "gear"-kind entry's item blob (see vaultDepositGear()'s {slot, kind:
  // "gear", inst} shape in index.html) gets the same bounds check as an Auction House
  // listing or a Blacksmith reroll -- otherwise a modified client could deposit a
  // fabricated, impossible item directly into long-term storage with no combat/crafting
  // ever involved. Consumable/herb entries are plain stackable counts with no RNG affix
  // roll to fabricate, so they're out of scope here (still just structurally sane-checked
  // below); a full per-item ownership ledger for those remains backlog work, same as the
  // existing version-based concurrency guard already notes above.
  //
  // IMPORTANT: only NEWLY-deposited gear (an instance_id not already present in this
  // account's currently-stored vault) is bounds-checked. A gear item legitimately rolled
  // under an OLDER balance formula (e.g. xp_find's T1 max was 10 before the v0.17.2 (#4)
  // hotfix tightened it to 1 -- see validateGearItem's ceiling-map comment) must never
  // start failing validation retroactively just because it's already resident in the
  // vault; that would lock a player out of touching their OWN vault at all (every
  // deposit/withdraw re-PUTs the full items array), which is exactly the kind of data-
  // loss-adjacent regression this project treats as unacceptable.
  let existingItems = [];
  if (existing) {
    try { existingItems = JSON.parse(existing.data); } catch (e) { existingItems = []; }
  }
  const alreadyStoredGearIds = new Set(
    existingItems.filter((e) => e && e.kind === "gear" && e.inst).map((e) => e.inst.instance_id)
  );
  for (const entry of items) {
    if (!entry || typeof entry !== "object" || !Number.isInteger(entry.slot) || entry.slot < 0 || entry.slot >= VAULT_CAPACITY) {
      return res.status(400).json({ error: "Invalid vault entry." });
    }
    if (entry.kind === "gear") {
      if (!entry.inst || !alreadyStoredGearIds.has(entry.inst.instance_id)) {
        const itemError = validateGearItem(entry.inst);
        if (itemError) return res.status(400).json({ error: `Invalid item in vault: ${itemError}` });
      }
    } else if (entry.kind === "consumable" || entry.kind === "herb") {
      if (!Number.isFinite(entry.qty) || entry.qty <= 0) return res.status(400).json({ error: "Invalid vault stack quantity." });
    } else {
      return res.status(400).json({ error: "Invalid vault entry kind." });
    }
  }

  if (clientVersion !== currentVersion) {
    // Someone else's change (another of your characters, in another tab/device) already
    // landed since this client last fetched the vault -- reject instead of overwriting it,
    // and hand back the real current state so the client can refresh and retry.
    let currentItems = [];
    if (existing) {
      const fresh = db.prepare("SELECT data FROM vaults WHERE account_id = ?").get(req.account.id);
      try { currentItems = JSON.parse(fresh.data); } catch (e) { currentItems = []; }
    }
    return res.status(409).json({
      error: "The vault changed elsewhere since you last looked -- refresh and try again.",
      items: currentItems,
      capacity: VAULT_CAPACITY,
      version: currentVersion,
    });
  }

  const newVersion = currentVersion + 1;
  const json = JSON.stringify(items);
  db.prepare(
    `INSERT INTO vaults (account_id, data, updated_at, version) VALUES (?, ?, ?, ?)
     ON CONFLICT(account_id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at, version=excluded.version
     WHERE vaults.version = ?`
  ).run(req.account.id, json, nowIso(), newVersion, currentVersion);
  res.json({ ok: true, version: newVersion });
});

// v0.13: the Graveyard is a public memorial, not a per-account death log -- every
// fallen hardcore character should be visible to every player, not just their own.
// v0.16 (#334): now also includes each grave's `id` (needed to target a tribute) and a
// `tributes` array of usernames who've left a flower on that grave -- fetched as a single
// second query keyed by grave id, then grouped in JS, rather than an N+1 query per grave.
app.get("/api/graveyard", requireAuth, (req, res) => {
  const rows = db
    .prepare("SELECT id, username, name, class_name, level, cause, died_at FROM graveyard ORDER BY died_at DESC LIMIT 200")
    .all();
  const graveIds = rows.map((r) => r.id);
  const tributesByGrave = {};
  if (graveIds.length > 0) {
    const placeholders = graveIds.map(() => "?").join(",");
    const tributeRows = db
      .prepare(`SELECT grave_id, username FROM graveyard_tributes WHERE grave_id IN (${placeholders}) ORDER BY created_at ASC`)
      .all(...graveIds);
    for (const t of tributeRows) {
      (tributesByGrave[t.grave_id] = tributesByGrave[t.grave_id] || []).push(t.username);
    }
  }
  const entries = rows.map((r) => Object.assign({}, r, { tributes: tributesByGrave[r.id] || [] }));
  res.json({ entries });
});

// v0.16 (#334): leave a flower/herb tribute on a fallen hero's grave -- purely a cosmetic,
// lighthearted "mockery" gesture (per Gwen's spec), no gold/item cost, but still fully
// server-validated: the grave must actually exist, and the tribute is attributed to
// req.account.username from the caller's OWN session token, never a client-supplied name
// (so nobody can post a tribute "as" another player). The UNIQUE(grave_id, account_id)
// constraint on graveyard_tributes caps each account to exactly one tribute per grave --
// enforced by the database itself, not just a client-side disabled button, so a modified
// client can't spam the same grave's tribute list with duplicate entries of its own name.
app.post("/api/graveyard/:id/tribute", requireAuth, (req, res) => {
  const graveId = Number(req.params.id);
  if (!Number.isInteger(graveId)) return res.status(400).json({ error: "Invalid grave id." });
  const grave = db.prepare("SELECT id FROM graveyard WHERE id = ?").get(graveId);
  if (!grave) return res.status(404).json({ error: "That grave doesn't exist." });
  try {
    db.prepare(
      "INSERT INTO graveyard_tributes (grave_id, account_id, username, created_at) VALUES (?, ?, ?, ?)"
    ).run(graveId, req.account.id, req.account.username, nowIso());
  } catch (e) {
    // UNIQUE constraint violation -- this account already left a tribute here.
    return res.status(400).json({ error: "You've already left a tribute on this grave." });
  }
  const tributeRows = db
    .prepare("SELECT username FROM graveyard_tributes WHERE grave_id = ? ORDER BY created_at ASC")
    .all(graveId);
  res.json({ ok: true, tributes: tributeRows.map((t) => t.username) });
});

app.get("/api/leaderboard", (req, res) => {
  // v0.17 (#1): sort order is now Class rank (highest_tier_reached) > bridge steps
  // (last_bridge_steps) > level > current-level XP (xp), per Gwen's spec -- gold dropped
  // out of the tiebreaker chain entirely (it's account-bound as of v0.17 anyway, see the
  // account-gold migration, so it's no longer a meaningful per-character skill signal).
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, level, highest_tier_reached, gold, lifetime_xp, hardcore, is_dead, last_bridge_steps, xp, updated_at
       FROM leaderboard_bests ORDER BY highest_tier_reached DESC, last_bridge_steps DESC, level DESC, xp DESC LIMIT 1000`
    )
    .all();
  // join usernames without leaking passcode data
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return {
      // v0.17.3 (#15): account_id is now included so the client's "Inspect Player"
      // magnifying-glass button can address GET /api/leaderboard/inspect/:accountId/:name --
      // it's already public knowledge via the join above (this same row's `player` username
      // is looked up FROM this exact id), so exposing the numeric id itself leaks nothing new.
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
      last_bridge_steps: r.last_bridge_steps || 0,
      xp: r.xp || 0,
      // v0.18 (#17): leaderboard_bests.updated_at is already stamped with nowIso() on
      // every single character save (same upsert that maintains level/xp/tier above) --
      // exactly the same "last active" signal /api/active-players computes off the
      // characters table, just already sitting right here with zero extra plumbing.
      last_active_at: r.updated_at,
    };
  });
  res.json({ entries: withNames });
});

// v0.17.3 (#13): "Skill Progression" leaderboard tab -- unlike the main /api/leaderboard
// above (capped at the top 50 by tier/steps/level/xp), this returns EVERY character's raw
// weapon-skill hit counters + herbalism points, uncapped, because a character with a
// modest tier but heavily-farmed weapon proficiency could easily fall outside that top-50
// cut yet still belong on a top-5-per-skill list. The client computes each skill's actual
// level/progress from these raw counters using the exact same Balance.weaponSkillLevelForHits/
// herbalismLevelForPoints math the character sheet itself uses (see skillsBonusPanel()) --
// deliberately NOT duplicated here, so there is exactly one place that formula can drift.
app.get("/api/leaderboard/skills", (req, res) => {
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, level, weapon_skills, herbalism_points, spellcasting_hits, is_dead
       FROM leaderboard_bests`
    )
    .all();
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    let weaponSkills = {};
    try {
      weaponSkills = JSON.parse(r.weapon_skills || "{}");
    } catch (e) {
      /* corrupt/legacy row -- treat as no weapon-skill progress rather than failing the whole list */
    }
    return {
      account_id: r.account_id,
      player: acc ? acc.username : "?",
      character_name: r.character_name,
      class_name: r.class_name,
      level: r.level,
      weapon_skills: weaponSkills,
      herbalism_points: r.herbalism_points || 0,
      // v0.23.0 (Part B9): raw hit counter -- client computes level/progress from it via the
      // exact same Balance.weaponSkillLevelForHits() math weapon skills already use.
      spellcasting_hits: r.spellcasting_hits || 0,
      is_dead: !!r.is_dead,
    };
  });
  res.json({ entries: withNames });
});

// v0.17.3 (#14): "Gold" leaderboard tab -- gold has been account-bound (not per-character)
// since v0.17, so this queries `accounts` directly instead of `leaderboard_bests` (whose own
// `gold` column is now just a stale per-save echo, see the PUT /api/characters/:slot route's
// comment on why it's no longer a meaningful tiebreaker). Deliberately public/unauthenticated,
// matching GET /api/leaderboard above -- only username + gold are exposed, never passcode
// hashes/salts, email, or session tokens.
app.get("/api/leaderboard/gold", (req, res) => {
  const rows = db.prepare("SELECT username, gold FROM accounts ORDER BY gold DESC LIMIT 200").all();
  res.json({ entries: rows.map((r) => ({ player: r.username, gold: r.gold || 0 })) });
});

// v0.18.2 (#8): "Max Killstreak" leaderboard tab -- explicitly per-CHARACTER, not per-account
// (Gwen's spec: "different characters on the same account can have different max killstreak
// records"), same as the Skill Progression tab above and unlike the account-level Gold tab.
// max_kill_streak is a MAX()'d lifetime record (see the character-save upsert's ON CONFLICT
// clause), so a plain server-side ORDER BY + LIMIT is enough -- no client-side ranking math
// needed, unlike Skill Progression's per-weapon-type breakdown.
app.get("/api/leaderboard/killstreak", (req, res) => {
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, max_kill_streak, is_dead
       FROM leaderboard_bests ORDER BY max_kill_streak DESC LIMIT 1000`
    )
    .all();
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return {
      account_id: r.account_id,
      player: acc ? acc.username : "?",
      character_name: r.character_name,
      class_name: r.class_name,
      max_kill_streak: r.max_kill_streak || 0,
      is_dead: !!r.is_dead,
    };
  });
  res.json({ entries: withNames });
});

// v0.21 (#17): "Monsters Killed" leaderboard tab -- same shape/reasoning as "Max Killstreak"
// directly above (per-CHARACTER, not per-account; total_kills is a MAX()'d lifetime record
// so a plain ORDER BY + LIMIT is enough), just ranking by the never-resetting lifetime kill
// counter instead of the best streak achieved.
app.get("/api/leaderboard/monsterskilled", (req, res) => {
  const rows = db
    .prepare(
      `SELECT account_id, character_name, class_name, total_kills, is_dead
       FROM leaderboard_bests ORDER BY total_kills DESC LIMIT 1000`
    )
    .all();
  const withNames = rows.map((r) => {
    const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(r.account_id);
    return {
      account_id: r.account_id,
      player: acc ? acc.username : "?",
      character_name: r.character_name,
      class_name: r.class_name,
      total_kills: r.total_kills || 0,
      is_dead: !!r.is_dead,
    };
  });
  res.json({ entries: withNames });
});

// v0.18 (#14/#17): "activity" tracking piggybacks entirely on the `characters.updated_at`
// column that autosave (PUT /api/characters/:slot) already stamps on essentially every
// meaningful in-game action -- no new column, no client heartbeat, nothing extra to keep in
// sync. A character counts as "active" if ANY of that account's character slots saved within
// the last ACTIVE_WINDOW_MS; only the single most-recently-updated slot per account is
// surfaced (an account can only actually be playing one character at a time), so a player
// with 6 characters never shows up as 6 separate "currently playing" entries.
const ACTIVE_WINDOW_MS = 5 * 60 * 1000;
function getActiveCharacters(limit) {
  const cutoff = new Date(Date.now() - ACTIVE_WINDOW_MS).toISOString();
  const rows = db
    .prepare(
      `SELECT c.data, c.updated_at
       FROM characters c
       INNER JOIN (
         SELECT account_id, MAX(updated_at) AS max_updated FROM characters GROUP BY account_id
       ) latest ON c.account_id = latest.account_id AND c.updated_at = latest.max_updated
       WHERE c.updated_at >= ?
       ORDER BY c.updated_at DESC
       LIMIT ?`
    )
    .all(cutoff, limit);
  const out = [];
  for (const row of rows) {
    try {
      const data = JSON.parse(row.data);
      if (data && data.character_name) out.push({ character_name: data.character_name, last_active_at: row.updated_at });
    } catch (e) {
      /* a corrupt row is just skipped -- never worth failing the whole list over */
    }
  }
  return out;
}
// v0.18 (#14): public (no auth) -- this is no more sensitive than the character NAMES
// already shown, unauthenticated, on /api/leaderboard. Capped generously above the client's
// own "5 most recent, plus an 'and N others' count" truncation so the client always has
// enough rows to compute an accurate "N others" figure itself.
app.get("/api/active-players", (req, res) => {
  res.json({ entries: getActiveCharacters(100) });
});

// v0.17.3 (#15): "Inspect Player" read-only character viewer. SECURITY-CRITICAL per Gwen's
// explicit spec: this must be impossible to use to intervene with, move, sell, or otherwise
// manipulate another account's characters/gear/gold in any way. Concretely, that means:
//   - GET only. There is no corresponding PUT/POST/DELETE anywhere that accepts an
//     accountId/characterName pair from an arbitrary caller -- every mutating character
//     route (PUT/DELETE /api/characters/:slot, the vault routes, auction routes) is scoped
//     strictly to req.account.id from the caller's OWN session token, never from a URL
//     param or request body. This route cannot be chained into a mutation anywhere else.
//   - Requires a logged-in session (requireAuth) purely as an extra scraping speed-bump --
//     the data itself is no more sensitive than what's already public on /api/leaderboard,
//     but a full gear/stat snapshot per character is a bigger scrape target than a one-line
//     ranking row, so this asks for a session token same as every other authenticated route.
//   - Hand-picked field whitelist below, NOT a raw dump of the character's `data` JSON blob.
//     That blob also contains things like backpack contents (gear_instances), in-progress
//     trial/bonfire/poison/heal timers, and internal bookkeeping (_save_seq) that have no
//     business being visible to another player and aren't part of Gwen's spec ("current
//     gear, equipment, stats, skills progression") -- only equipped gear, attributes,
//     weapon_skills, and herbalism_points are ever returned.
app.get("/api/leaderboard/inspect/:accountId/:characterName", requireAuth, (req, res) => {
  const accountId = Number(req.params.accountId);
  const characterName = decodeURIComponent(req.params.characterName);
  if (!Number.isInteger(accountId)) return res.status(400).json({ error: "Invalid account id." });

  const acc = db.prepare("SELECT username FROM accounts WHERE id = ?").get(accountId);
  if (!acc) return res.status(404).json({ error: "No such player." });

  // Same json_extract-on-the-stored-blob lookup already used by the character-delete route
  // to find a character by name without needing to know its slot number ahead of time.
  const row = db
    .prepare(
      `SELECT data FROM characters WHERE account_id = ? AND json_extract(data, '$.character_name') = ?`
    )
    .get(accountId, characterName);
  if (!row) return res.status(404).json({ error: "That character no longer exists." });

  let data;
  try {
    data = JSON.parse(row.data);
  } catch (e) {
    return res.status(500).json({ error: "That character's data is corrupt." });
  }

  const lbRow = db
    .prepare("SELECT highest_tier_reached, hardcore, is_dead, last_bridge_steps FROM leaderboard_bests WHERE account_id = ? AND character_name = ?")
    .get(accountId, characterName);

  res.json({
    player: acc.username,
    character_name: data.character_name || characterName,
    class_id: data.class_id || "",
    class_display_name: data.class_display_name || "",
    level: data.level || 1,
    hardcore: !!(lbRow ? lbRow.hardcore : data.hardcore),
    is_dead: !!(lbRow && lbRow.is_dead),
    highest_tier_reached: lbRow ? lbRow.highest_tier_reached : data.highest_tier_reached || 1,
    // v0.18.1 (#1): Broken Bridge Trial progression -- how far this character got before
    // their most recent trial failure (0 if they haven't failed one yet at their current
    // tier, or just crossed successfully). This is NOT new exposure: it's the exact same
    // last_bridge_steps value already shown to everyone on the public Leaderboard's
    // "(Tier N, Step X/10)" row -- Inspect Player just surfaces it in its own dedicated
    // read-only section instead of making players cross-reference the ladder separately.
    last_bridge_steps: lbRow ? (lbRow.last_bridge_steps || 0) : (data.last_bridge_steps || 0),
    // v0.19.2 (#23): the running count of failed Trial attempts at the character's CURRENT
    // tier (resets to 0 the moment they finally cross) -- same small int already shown to
    // the character's own owner on the Broken Bridge Trial screen itself ("currently +N"),
    // just surfaced here too so Inspect Player's step visualization can show it alongside
    // last_bridge_steps. Not sensitive: no plank-by-plank replay, just a fail count.
    bridge_fail_streak: data.bridge_fail_streak || 0,
    // v0.22.3 (#12): the SAME red/green plank-progress data the character's own owner sees
    // on the real Broken Bridge Trial screen -- discoveredSides (which plank was safe at
    // each cleared step) and stepFailedPlanks (which planks are already known-broken at the
    // in-progress step), for the character's CURRENT class only. Read-only: this route never
    // accepts a plank guess or resolves anything, it only lets bridgeTrialView() render the
    // same grid the inspecting player already sees for their own character.
    trial_progress_current: (data.trial_progress && data.trial_progress[data.class_id]) || { discoveredSides: [], stepFailedPlanks: {} },
    attributes: data.attributes || { str: 0, dex: 0, vit: 0, int: 0 },
    equipped: data.equipped || {},
    weapon_skills: data.weapon_skills || {},
    // v0.23.0 (Part B9): Spellcasting proficiency, surfaced to Inspect Player alongside the
    // weapon skills it's a direct parallel of.
    spellcasting_skill: data.spellcasting_skill || { hits: 0 },
    herbalism_points: data.herbalism_points || 0,
    // v0.18.2 (#6): Forest Reputation, per Gwen's spec ("also shown in the player inspect
    // view") -- read straight off the character's own data blob (there's no separate
    // leaderboard_bests column for this, no cross-account ranking is needed, so it never
    // needed a schema migration at all), keyed by class tier same as the client keeps it.
    forest_reputation: data.forest_reputation || {},
  });
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
      `SELECT account_id, character_name, class_name, level, highest_tier_reached, gold, lifetime_xp, hardcore, is_dead, updated_at, last_bridge_steps, xp
       FROM leaderboard_bests ORDER BY highest_tier_reached DESC, last_bridge_steps DESC, level DESC, xp DESC`
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
      last_bridge_steps: r.last_bridge_steps || 0,
      xp: r.xp || 0,
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

// v0.18.1 (#2): read-only account list for admins -- createdOn/email/lastActiveAt per
// account. lastActiveAt is the account-wide MAX(characters.updated_at) across every
// character slot (same join shape as getActiveCharacters()'s "currently playing" query,
// just without its 5-minute cutoff/LIMIT), NOT the leaderboard's last_active_at (which is
// scoped to a single "best" character and only exists for accounts with a ladder entry).
// Accounts with zero characters still show up, with last_active_at: null. No account
// secrets (passcode_hash/passcode_salt) are ever included.
app.get("/api/admin/accounts", requireAuth, requireAdmin, (req, res) => {
  const rows = db
    .prepare(
      `SELECT a.id AS account_id, a.username, a.email, a.created_at, latest.max_updated AS last_active_at
       FROM accounts a
       LEFT JOIN (
         SELECT account_id, MAX(updated_at) AS max_updated FROM characters GROUP BY account_id
       ) latest ON latest.account_id = a.id
       ORDER BY a.created_at DESC`
    )
    .all();
  res.json({
    entries: rows.map((r) => ({
      account_id: r.account_id,
      username: r.username,
      email: r.email || "",
      created_at: r.created_at,
      last_active_at: r.last_active_at || null,
    })),
  });
});

// v0.24.1 (A3): admin mutations targeting a character OTHER than the admin's own must never
// be silently overwritten by that player's own client autosaving its pre-mutation, now-stale
// in-memory state back in a moment later. This is the exact same autosave-vs-server-write race
// shape as the hardcore-death choke point already solves for combat -- here the admin's write
// stands in for the "other" writer. Two safeguards, reused everywhere an admin route touches a
// live character row:
//   1. Fence the save sequence far ahead of anything a live client could reach on its own, so
//      the strictly-greater-seq guard on PUT /api/characters/:slot (see the WHERE clause on the
//      upsert earlier in this file) rejects any in-flight or queued autosave still carrying the
//      player's pre-mutation state.
//   2. Push a "your character was updated by an admin, please reload" signal down the player's
//      own already-open chat WebSocket (the same channel every other live broadcast feature --
//      chat, combat ticks -- already reuses), so a currently-online player sees the change
//      immediately instead of only on next login.
// ADMIN_SEQ_FENCE_JUMP is comically large relative to how _save_seq actually grows in practice
// (PS._nextSaveSeq() floors once at Date.now(), then increments by 1 per save from there -- a
// client autosaving nonstop, once a second, forever, would take over 31,000 years to add 1e12 to
// its own seq), which is the point: it needs to permanently outrun any seq a live client could
// organically reach, not merely win "for now".
const ADMIN_SEQ_FENCE_JUMP = 1e12;
function applyAdminSaveSeqFence(data) {
  const current = Number(data._save_seq) || 0;
  data._save_seq = Math.max(current, Date.now()) + ADMIN_SEQ_FENCE_JUMP;
  return data._save_seq;
}
function notifyCharacterForceReload(accountId, slot) {
  const payload = JSON.stringify({ type: "force_reload", slot });
  for (const client of chatClients) {
    if (client.readyState === client.OPEN && client.accountId === accountId) client.send(payload);
  }
}

/* ---------------- v0.22.3 (#10 section C): admin "Reset Ladder" tab ----------------
   Human-in-the-loop, per-character ladder reset. Deliberately NO bulk "reset everyone"
   endpoint -- the admin resets one character at a time, from a listing of every character
   across every account, exactly the same "review then act on one row" shape as the existing
   Legacy Gear tool. Also exposes the current season-winner record (if any) so the admin tab
   can show whether/who has already won this season. */
app.get("/api/admin/reset-ladder", requireAuth, requireAdmin, (req, res) => {
  const rows = db
    .prepare(
      `SELECT c.account_id, c.slot, a.username,
              json_extract(c.data, '$.character_name') AS character_name,
              json_extract(c.data, '$.class_display_name') AS class_display_name,
              json_extract(c.data, '$.highest_tier_reached') AS highest_tier_reached,
              json_extract(c.data, '$.level') AS level
       FROM characters c
       JOIN accounts a ON a.id = c.account_id
       WHERE json_extract(c.data, '$.character_name') IS NOT NULL
       ORDER BY a.username, c.slot`
    )
    .all();
  const season = db.prepare("SELECT * FROM season_state WHERE id = 1").get() || {};
  res.json({
    entries: rows.map((r) => ({
      account_id: r.account_id,
      slot: r.slot,
      username: r.username,
      character_name: r.character_name || "",
      class_display_name: r.class_display_name || "",
      highest_tier_reached: r.highest_tier_reached || 1,
      level: r.level || 1,
    })),
    season: {
      winner_declared: !!season.winner_declared,
      winner_character_name: season.winner_character_name || "",
      winner_class_name: season.winner_class_name || "",
      won_at: season.won_at || null,
    },
  });
});

app.post("/api/admin/reset-ladder/:accountId/:slot", requireAuth, requireAdmin, (req, res) => {
  const accountId = Number(req.params.accountId);
  const slot = Number(req.params.slot);
  if (!Number.isInteger(accountId)) return res.status(400).json({ error: "Invalid account id." });
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try { data = JSON.parse(row.data); } catch (e) { return res.status(500).json({ error: "Corrupt character save." }); }

  applyLadderReset(data);
  // v0.24.1 (A3): was a fragile +1 bump (identical shape to the /api/keeper/choice race A1
  // uncovered) -- a stale online client's own next autosave, carrying a newer seq of its own,
  // would silently clobber the reset a moment later. Fence it, then push a forced reload so an
  // online target sees the reset immediately instead of the change only "sticking" once they
  // happen to log out and back in.
  applyAdminSaveSeqFence(data);
  const nowStr = nowIso();
  db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
    JSON.stringify(data),
    nowStr,
    accountId,
    slot
  );
  upsertLeaderboardBests(accountId, data);
  notifyCharacterForceReload(accountId, slot);
  console.log(`[admin] ${req.account.username} reset the ladder for "${data.character_name}" (account_id=${accountId}, slot=${slot})`);
  res.json({
    ok: true,
    character_name: data.character_name,
    class_display_name: data.class_display_name,
    level: data.level,
    highest_tier_reached: data.highest_tier_reached,
  });
});

// v0.24.1 (A4): compensation tool -- a player lost heavily-invested gear rolls when the
// itemization bulk update ran, so this credits a chosen character's account gold and/or the
// character's own materials (Item Scraps) in one shot. Gold is account-bound (see
// creditAccountGold() above, a separate `accounts` table column the character-save PUT route
// never touches) so it's immune to the autosave-clobber race by construction -- only materials
// lives inside the per-character JSON, so that side gets the same save-seq fence treatment as
// every other admin character-write above. Always pushes the forced-reload notification so a
// live online target's gold display (refreshed by the client's Net.refreshCharacters() call in
// that same handler) and any granted materials both show up immediately without a relog, even
// when only gold was granted.
app.post("/api/admin/grant/:accountId/:slot", requireAuth, requireAdmin, (req, res) => {
  const accountId = Number(req.params.accountId);
  const slot = Number(req.params.slot);
  if (!Number.isInteger(accountId)) return res.status(400).json({ error: "Invalid account id." });
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  const goldAmount = Math.round(Number(req.body?.gold) || 0);
  const materialsAmount = Math.round(Number(req.body?.materials) || 0);
  if (!goldAmount && !materialsAmount) return res.status(400).json({ error: "Enter a gold or materials amount to grant." });

  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
  if (!row) return res.status(404).json({ error: "No character in that slot." });
  let data;
  try { data = JSON.parse(row.data); } catch (e) { return res.status(500).json({ error: "Corrupt character save." }); }

  let accountGoldAfter = getAccountGold(accountId);
  if (goldAmount) accountGoldAfter = creditAccountGold(accountId, goldAmount);

  if (materialsAmount) {
    data.materials = Math.max(0, (data.materials || 0) + materialsAmount);
    applyAdminSaveSeqFence(data);
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data), nowIso(), accountId, slot
    );
  }
  notifyCharacterForceReload(accountId, slot);
  console.log(`[admin] ${req.account.username} granted account_id=${accountId} slot=${slot}: gold=${goldAmount}, materials=${materialsAmount}`);
  res.json({
    ok: true,
    character_name: data.character_name,
    gold_granted: goldAmount,
    materials_granted: materialsAmount,
    account_gold: accountGoldAfter,
    materials: data.materials,
  });
});

// v0.22.3 (#17): admin tool to push a one-off system message to every connected player's
// global chat -- handy for announcing maintenance/server resets to a live playerbase.
// Reuses the exact same broadcastSystemMessage() every other system announcement (login,
// trial result, graveyard death, season win) already goes through, so it renders identically.
const ADMIN_BROADCAST_MAX_LEN = 500;
app.post("/api/admin/broadcast", requireAuth, requireAdmin, (req, res) => {
  const message = String(req.body?.message || "").trim();
  if (!message) return res.status(400).json({ error: "Message can't be empty." });
  const trimmed = message.length > ADMIN_BROADCAST_MAX_LEN ? message.slice(0, ADMIN_BROADCAST_MAX_LEN) : message;
  // v0.24.2: tagged "admin" so the client can give it its own blue treatment, distinct from
  // ordinary system lines (logins, trial results, new characters).
  broadcastSystemMessage(trimmed, "admin");
  console.log(`[admin] ${req.account.username} broadcast a system message: "${trimmed}"`);
  res.json({ ok: true });
});

// v0.24.2: the known-issues banner. GET is deliberately public and unauthenticated -- it is
// read on the login screen too, before any token exists, and carries nothing private. Only
// the setter is admin-gated.
// v0.25.1 (credit: Gwen): compensate a player directly -- the case that prompted it being a
// tester who spent heavily on reroll costs just before a legacy-gear rebalance. Gold is
// account-bound so it is credited to the account; materials live per-character, so a materials
// grant needs a specific character and is matched by name. Both are optional and independent:
// granting only gold needs no character name at all.
app.post("/api/admin/grant", requireAuth, requireAdmin, (req, res) => {
  const username = String(req.body?.username || "").trim();
  const characterName = String(req.body?.character_name || "").trim();
  const gold = Math.floor(Number(req.body?.gold) || 0);
  const materials = Math.floor(Number(req.body?.materials) || 0);
  if (!username) return res.status(400).json({ error: "An account username is required." });
  if (!gold && !materials) return res.status(400).json({ error: "Specify an amount of gold, materials, or both." });
  // Negative amounts are allowed on purpose: the same tool has to be able to undo a mistake,
  // and an admin who has just been trusted with the token can be trusted to subtract too.
  const account = db.prepare("SELECT id, username FROM accounts WHERE LOWER(username) = LOWER(?)").get(username);
  if (!account) return res.status(404).json({ error: "No account with that username." });

  const result = { ok: true, username: account.username };
  if (gold) result.account_gold = creditAccountGold(account.id, gold);

  if (materials) {
    if (!characterName) return res.status(400).json({ error: "Materials are stored per character, so a character name is required." });
    const rows = db.prepare("SELECT slot, data FROM characters WHERE account_id = ?").all(account.id);
    let matched = null;
    for (const row of rows) {
      try {
        const d = JSON.parse(row.data);
        if ((d.character_name || "").toLowerCase() === characterName.toLowerCase()) { matched = { slot: row.slot, data: d }; break; }
      } catch (e) { /* skip a corrupt row rather than failing the whole grant */ }
    }
    if (!matched) return res.status(404).json({ error: "That account has no character with that name." });
    matched.data.materials = Math.max(0, (matched.data.materials || 0) + materials);
    saveCharacterRow(account.id, matched.slot, matched.data);
    result.character_name = matched.data.character_name;
    result.materials = matched.data.materials;
  }

  // Delivered as a private mailbox message so the player is told what happened and why, rather
  // than silently discovering a changed balance. Live over the socket if they're connected.
  const parts = [];
  if (gold) parts.push(`${gold > 0 ? "+" : ""}${gold.toLocaleString()} gold`);
  if (materials) parts.push(`${materials > 0 ? "+" : ""}${materials.toLocaleString()} materials`);
  sendPrivateMessage(account.id, `An administrator has adjusted your account: ${parts.join(" and ")}.`, result.account_gold != null ? { gold: result.account_gold } : {});
  console.log(`[admin] ${req.account.username} granted ${parts.join(" and ")} to ${account.username}${result.character_name ? " / " + result.character_name : ""}`);
  res.json(result);
});

app.get("/api/announcement", (req, res) => {
  res.json(getAdminAnnouncement());
});

app.get("/api/admin/announcement", requireAuth, requireAdmin, (req, res) => {
  res.json(getAdminAnnouncement());
});

// Sending an empty message is the documented way to CLEAR the banner, not an error -- the
// Dev Tools panel's Clear button posts exactly that.
app.post("/api/admin/announcement", requireAuth, requireAdmin, (req, res) => {
  const message = String(req.body?.message || "").trim();
  const result = setAdminAnnouncement(message);
  console.log(`[admin] ${req.account.username} set the known-issues banner to: "${result.message}"`);
  res.json(Object.assign({ ok: true }, result));
});

/* ---------------- v0.19.1 (#26): "Legacy Gear" admin review + manual rescale tool ----------------
   Over successive balance patches, some stat maxes for a given tier have been tightened (e.g.
   xp_find's Tier-1 max went 10 -> 1 -> 5, see ITEM_AFFIX_TIER1_MAX_CEILING's own comment
   above). validateGearItem() deliberately still ACCEPTS such items (checked against the
   loosest-ever ceiling) so nobody's existing gear silently breaks on the next deploy -- but
   that leaves genuinely over-tuned items sitting in players' backpacks/equipped slots/vaults/
   auction listings indefinitely. This tool surfaces every one of them for an admin to review,
   showing a before/after comparison, and lets the admin rescale them down to what today's
   LIVE balance would actually allow -- one item at a time, only on explicit confirmation,
   never automatically and never in bulk. Per the standing data-safety directive, this must
   never silently mutate anyone's gear. */

// Returns the {stat, value, max} affix entries on `item` that exceed today's LIVE max for its
// tier (itemAffixMaxForTier, the current-patch number) -- as opposed to validateGearItem's
// looser ceiling check (itemAffixCeilingForTier). An empty array means the item is a
// legitimate item by validateGearItem's rules AND already matches current balance.
function legacyGearOutOfSpecAffixes(item) {
  if (!item || !Array.isArray(item.affixes) || !Number.isInteger(item.tier)) return [];
  const out = [];
  // v0.22.6 (#14): routed through legalAffixesForItem() so an offhand item is checked against
  // its own Shield/Orb pool instead of an undifferentiated "offhand" list.
  const legalForSlot = legalAffixesForItem(item);
  for (const a of item.affixes) {
    if (!a || typeof a.stat !== "string" || !Number.isFinite(a.value)) continue;
    // v0.22.3 (#16): flag eyesight sitting on any slot besides head/amulet (pre-dates the
    // new roll-site gate) alongside the pre-existing over-max-value flagging below.
    if (a.stat === "eyesight") {
      if (item.slot !== "head" && item.slot !== "amulet") out.push({ stat: a.stat, value: a.value, slotIllegal: true });
      continue;
    }
    // v0.22.3 (#16): also flag any affix that's simply illegal for this item's slot (e.g.
    // armor rolled onto a weapon) -- pre-dates the new SLOT_AFFIXES filter at generation/reforge.
    if (!legalForSlot.includes(a.stat)) { out.push({ stat: a.stat, value: a.value, slotIllegal: true }); continue; }
    let max = itemAffixMaxForTier(a.stat, item.tier);
    if (item.slot === "weapon" && a.stat === "damage") max = round2(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    // v0.22.7 (#11): old percent-era Thorns rolls are already covered by this same over-max
    // check below -- the percent-to-flat conversion (item 1) kept the exact same tier-max
    // numbers (4/8/12/16/20), so a legacy Thorns value can only be out of spec here if it
    // exceeds that ceiling, same as any other stat. No separate thorns-specific branch needed.
    if (a.value > max) out.push({ stat: a.stat, value: a.value, max });
  }
  return out;
}

// Returns a brand NEW item object (never mutates `item`) with every out-of-spec affix value
// clamped down to today's live max. Same stats, same tier/rarity/slot/element/instance_id --
// this is intentionally NOT a full re-roll (the player keeps the exact item they've always
// had, just corrected to current balance) and never touches affixes that are already in spec.
function legacyGearRescale(item) {
  // v0.22.3 (#16): a slot-illegal eyesight (anywhere but head/amulet) is simply dropped --
  // there's no "legal eyesight slot" to reforge it into that preserves its rare-bonus intent,
  // and eyesight was never counted against the item's normal-affix slotCount to begin with.
  if (item.slot !== "head" && item.slot !== "amulet") {
    const filtered = item.affixes.filter((a) => !a || a.stat !== "eyesight");
    if (filtered.length !== item.affixes.length) item = Object.assign({}, item, { affixes: filtered });
  }
  // v0.22.6 (#14): routed through legalAffixesForItem() so a slot-illegal offhand affix gets
  // reforged into its own Shield/Orb pool instead of an undifferentiated "offhand" list.
  const legalForSlot = legalAffixesForItem(item);
  const existingStats = new Set(item.affixes.filter((a) => a && a.stat !== "eyesight").map((a) => a.stat));
  const affixes = item.affixes.map((a) => {
    if (!a || a.stat === "eyesight") return a;
    // v0.22.3 (#16): a slot-illegal affix (e.g. armor on a weapon) gets reforged into a
    // random LEGAL stat for this slot (that the item doesn't already carry), rolled fresh on
    // the current tier -- same "keep the item, fix the stat" philosophy as the existing
    // over-max-value clamp below, just for the newer illegal-stat case.
    if (!legalForSlot.includes(a.stat)) {
      const candidatePool = legalForSlot.filter((s) => !existingStats.has(s));
      const newStat = candidatePool.length > 0
        ? candidatePool[Math.floor(Math.random() * candidatePool.length)]
        : a.stat;
      existingStats.delete(a.stat);
      existingStats.add(newStat);
      let newValue = rollAffixValue(newStat, item.tier);
      if (item.slot === "weapon" && newStat === "damage") newValue = round2(newValue * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
      return { stat: newStat, value: newValue };
    }
    let max = itemAffixMaxForTier(a.stat, item.tier);
    if (item.slot === "weapon" && a.stat === "damage") max = round2(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    return a.value > max ? Object.assign({}, a, { value: max }) : a;
  });
  return Object.assign({}, item, { affixes });
}

// Scans every gear item currently in the game -- equipped + backpack across every character
// slot, every account's Storage Vault, and every live Auction House listing -- and returns
// only the ones with at least one out-of-spec affix, each tagged with exactly where it lives
// and who owns it so an admin can find and act on it. Read-only; touches nothing.
function legacyGearScan() {
  const out = [];
  const accountUsername = new Map();
  for (const r of db.prepare("SELECT id, username FROM accounts").all()) accountUsername.set(r.id, r.username);

  const charRows = db.prepare("SELECT account_id, slot, data FROM characters").all();
  for (const row of charRows) {
    let data;
    try { data = JSON.parse(row.data); } catch (e) { continue; }
    const characterName = data.character_name || "";
    const equipped = data.equipped || {};
    for (const equipSlot of Object.keys(equipped)) {
      const inst = equipped[equipSlot];
      const bad = legacyGearOutOfSpecAffixes(inst);
      if (bad.length) {
        out.push({
          location: { kind: "equipped", account_id: row.account_id, slot: row.slot, equip_slot: equipSlot, instance_id: inst.instance_id },
          username: accountUsername.get(row.account_id) || "?",
          character_name: characterName,
          where: `Equipped (${equipSlot})`,
          item: inst,
          out_of_spec: bad,
          rescaled: legacyGearRescale(inst),
        });
      }
    }
    for (const inst of (Array.isArray(data.gear_instances) ? data.gear_instances : [])) {
      const bad = legacyGearOutOfSpecAffixes(inst);
      if (bad.length) {
        out.push({
          location: { kind: "backpack", account_id: row.account_id, slot: row.slot, instance_id: inst.instance_id },
          username: accountUsername.get(row.account_id) || "?",
          character_name: characterName,
          where: "Backpack",
          item: inst,
          out_of_spec: bad,
          rescaled: legacyGearRescale(inst),
        });
      }
    }
  }

  const vaultRows = db.prepare("SELECT account_id, data FROM vaults").all();
  for (const row of vaultRows) {
    let items;
    try { items = JSON.parse(row.data); } catch (e) { continue; }
    if (!Array.isArray(items)) continue;
    for (const entry of items) {
      if (!entry || entry.kind !== "gear" || !entry.inst) continue;
      const bad = legacyGearOutOfSpecAffixes(entry.inst);
      if (bad.length) {
        out.push({
          location: { kind: "vault", account_id: row.account_id, instance_id: entry.inst.instance_id },
          username: accountUsername.get(row.account_id) || "?",
          character_name: "",
          where: "Storage Vault",
          item: entry.inst,
          out_of_spec: bad,
          rescaled: legacyGearRescale(entry.inst),
        });
      }
    }
  }

  const listingRows = db
    .prepare("SELECT id, seller_account_id, seller_username, seller_character_name, item_json FROM auction_listings WHERE type = 'gear'")
    .all();
  for (const row of listingRows) {
    if (!row.item_json) continue;
    let inst;
    try { inst = JSON.parse(row.item_json); } catch (e) { continue; }
    const bad = legacyGearOutOfSpecAffixes(inst);
    if (bad.length) {
      out.push({
        location: { kind: "auction", account_id: row.seller_account_id, listing_id: row.id, instance_id: inst.instance_id },
        username: row.seller_username,
        character_name: row.seller_character_name,
        where: "Auction House listing",
        item: inst,
        out_of_spec: bad,
        rescaled: legacyGearRescale(inst),
      });
    }
  }

  return out;
}

app.get("/api/admin/legacy-gear", requireAuth, requireAdmin, (req, res) => {
  res.json({ entries: legacyGearScan() });
});

// Rescales exactly ONE item, at the exact location the admin selected, and ONLY on this
// explicit call -- there is no bulk/"fix everything" endpoint, and nothing here runs
// automatically or on a schedule. Every numeric affix value on the rescaled result is
// recomputed fresh server-side from the live balance constants (never trusting the client's
// own "after" preview), so a tampered admin request can't be used to smuggle in an arbitrary
// stat boost. Re-fetches the item fresh from its current storage right before mutating it
// (not the copy the admin's list view was built from), so a player who moved/sold/re-rolled
// the item in the meantime can't have it clobbered out from under them.
// v0.21.1 (#1): shared by both the single-item rescale endpoint and the new bulk "Update All"
// endpoint below -- does the actual find/validate/rescale/persist work for exactly one
// location, but deliberately does NOT log or broadcast a chat message itself, so the bulk
// path can rescale many items and still only ever post ONE chat announcement at the end.
// Throws an Error with a `.httpStatus` property (and the same message the old inline
// per-kind checks used) on any failure, so callers can respond/skip consistently.
function rescaleLegacyGearAtLocation(location) {
  if (!location || typeof location !== "object" || typeof location.instance_id !== "string" || !location.instance_id) {
    throw Object.assign(new Error("Invalid location."), { httpStatus: 400 });
  }
  const { kind, instance_id } = location;
  const accountId = Number(location.account_id);
  if (!Number.isInteger(accountId)) throw Object.assign(new Error("Invalid account."), { httpStatus: 400 });

  if (kind === "equipped" || kind === "backpack") {
    const slot = Number(location.slot);
    if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) throw Object.assign(new Error("Invalid slot."), { httpStatus: 400 });
    const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
    if (!row) throw Object.assign(new Error("Character not found."), { httpStatus: 404 });
    let data;
    try { data = JSON.parse(row.data); } catch (e) { throw Object.assign(new Error("Corrupt character data."), { httpStatus: 500 }); }

    let target, applyBack;
    if (kind === "equipped") {
      const equipSlot = location.equip_slot;
      const inst = data.equipped && data.equipped[equipSlot];
      if (!inst || inst.instance_id !== instance_id) throw Object.assign(new Error("That item is no longer there (equipped slot changed)."), { httpStatus: 404 });
      target = inst;
      applyBack = (rescaled) => { data.equipped[equipSlot] = rescaled; };
    } else {
      const list = Array.isArray(data.gear_instances) ? data.gear_instances : [];
      const idx = list.findIndex((i) => i && i.instance_id === instance_id);
      if (idx === -1) throw Object.assign(new Error("That item is no longer in the backpack."), { httpStatus: 404 });
      target = list[idx];
      applyBack = (rescaled) => { list[idx] = rescaled; data.gear_instances = list; };
    }

    const bad = legacyGearOutOfSpecAffixes(target);
    if (!bad.length) throw Object.assign(new Error("This item is already within current spec -- nothing to rescale."), { httpStatus: 409 });
    const rescaled = legacyGearRescale(target);
    applyBack(rescaled);
    // v0.24.1 (A3 hardening): this write previously carried no _save_seq bump at all -- the
    // same race shape the ladder reset had, just with no fence whatsoever, so an online
    // player's own next autosave (which always carries a strictly newer seq than whatever's
    // stored) would silently revert the rescale entirely. Same fence + forced-reload treatment.
    applyAdminSaveSeqFence(data);
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data), nowIso(), accountId, slot
    );
    notifyCharacterForceReload(accountId, slot);
    console.log(`[admin] rescaled a legacy gear item (${kind}) for account_id=${accountId} slot=${slot}`);
    return { kind, before: target, after: rescaled };
  }

  if (kind === "vault") {
    const row = db.prepare("SELECT data FROM vaults WHERE account_id = ?").get(accountId);
    if (!row) throw Object.assign(new Error("Vault not found."), { httpStatus: 404 });
    let items;
    try { items = JSON.parse(row.data); } catch (e) { throw Object.assign(new Error("Corrupt vault data."), { httpStatus: 500 }); }
    if (!Array.isArray(items)) throw Object.assign(new Error("Corrupt vault data."), { httpStatus: 500 });
    const idx = items.findIndex((e) => e && e.kind === "gear" && e.inst && e.inst.instance_id === instance_id);
    if (idx === -1) throw Object.assign(new Error("That item is no longer in the vault."), { httpStatus: 404 });
    const target = items[idx].inst;
    const bad = legacyGearOutOfSpecAffixes(target);
    if (!bad.length) throw Object.assign(new Error("This item is already within current spec -- nothing to rescale."), { httpStatus: 409 });
    const rescaled = legacyGearRescale(target);
    items[idx] = Object.assign({}, items[idx], { inst: rescaled });
    db.prepare("UPDATE vaults SET data = ?, updated_at = ?, version = version + 1 WHERE account_id = ?").run(
      JSON.stringify(items), nowIso(), accountId
    );
    console.log(`[admin] rescaled a legacy gear item (vault) for account_id=${accountId}`);
    return { kind, before: target, after: rescaled };
  }

  if (kind === "auction") {
    const listingId = Number(location.listing_id);
    if (!Number.isInteger(listingId)) throw Object.assign(new Error("Invalid listing."), { httpStatus: 400 });
    const row = db.prepare("SELECT item_json FROM auction_listings WHERE id = ? AND type = 'gear'").get(listingId);
    if (!row || !row.item_json) throw Object.assign(new Error("Listing not found."), { httpStatus: 404 });
    let target;
    try { target = JSON.parse(row.item_json); } catch (e) { throw Object.assign(new Error("Corrupt listing data."), { httpStatus: 500 }); }
    if (target.instance_id !== instance_id) throw Object.assign(new Error("That listing no longer holds the expected item."), { httpStatus: 404 });
    const bad = legacyGearOutOfSpecAffixes(target);
    if (!bad.length) throw Object.assign(new Error("This item is already within current spec -- nothing to rescale."), { httpStatus: 409 });
    const rescaled = legacyGearRescale(target);
    db.prepare("UPDATE auction_listings SET item_json = ? WHERE id = ?").run(JSON.stringify(rescaled), listingId);
    console.log(`[admin] rescaled a legacy gear item (auction listing #${listingId})`);
    return { kind, before: target, after: rescaled };
  }

  throw Object.assign(new Error("Invalid location kind."), { httpStatus: 400 });
}

// v0.22.7 (#10): a hard, permanent per-item delete for exactly one gear instance at the exact
// location the admin selected -- for legacy items that are too broken/unwanted to bother
// rescaling (e.g. an old percent-era Thorns roll, or a slot-illegal affix nobody wants
// reforged). Mirrors rescaleLegacyGearAtLocation()'s find/validate/persist shape but removes
// the instance instead of replacing its values, and re-reads the character/vault/listing row
// fresh right before mutating so a player who moved the item in the meantime can't have an
// unrelated item deleted out from under them. Per-item only -- there is intentionally no bulk
// "delete all" counterpart, per the standing data-safety directive. Leaderboards in this codebase
// are computed fresh from the `characters` table on every request (see /api/leaderboard*), so
// nothing extra needs to run here to keep them in sync -- persisting the character row is enough.
function deleteLegacyGearAtLocation(location) {
  if (!location || typeof location !== "object" || typeof location.instance_id !== "string" || !location.instance_id) {
    throw Object.assign(new Error("Invalid location."), { httpStatus: 400 });
  }
  const { kind, instance_id } = location;
  const accountId = Number(location.account_id);
  if (!Number.isInteger(accountId)) throw Object.assign(new Error("Invalid account."), { httpStatus: 400 });

  if (kind === "equipped" || kind === "backpack") {
    const slot = Number(location.slot);
    if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) throw Object.assign(new Error("Invalid slot."), { httpStatus: 400 });
    const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
    if (!row) throw Object.assign(new Error("Character not found."), { httpStatus: 404 });
    let data;
    try { data = JSON.parse(row.data); } catch (e) { throw Object.assign(new Error("Corrupt character data."), { httpStatus: 500 }); }

    let removed;
    if (kind === "equipped") {
      const equipSlot = location.equip_slot;
      const inst = data.equipped && data.equipped[equipSlot];
      if (!inst || inst.instance_id !== instance_id) throw Object.assign(new Error("That item is no longer there (equipped slot changed)."), { httpStatus: 404 });
      removed = inst;
      delete data.equipped[equipSlot];
    } else {
      const list = Array.isArray(data.gear_instances) ? data.gear_instances : [];
      const idx = list.findIndex((i) => i && i.instance_id === instance_id);
      if (idx === -1) throw Object.assign(new Error("That item is no longer in the backpack."), { httpStatus: 404 });
      removed = list[idx];
      list.splice(idx, 1);
      data.gear_instances = list;
    }
    // v0.24.1 (A3 hardening): same missing-fence gap as the rescale path above -- fence +
    // forced-reload so a live online player's own autosave can't silently un-delete the item.
    applyAdminSaveSeqFence(data);
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data), nowIso(), accountId, slot
    );
    notifyCharacterForceReload(accountId, slot);
    console.log(`[admin] deleted a legacy gear item (${kind}) for account_id=${accountId} slot=${slot}`);
    return { kind, removed };
  }

  if (kind === "vault") {
    const row = db.prepare("SELECT data FROM vaults WHERE account_id = ?").get(accountId);
    if (!row) throw Object.assign(new Error("Vault not found."), { httpStatus: 404 });
    let items;
    try { items = JSON.parse(row.data); } catch (e) { throw Object.assign(new Error("Corrupt vault data."), { httpStatus: 500 }); }
    if (!Array.isArray(items)) throw Object.assign(new Error("Corrupt vault data."), { httpStatus: 500 });
    const idx = items.findIndex((e) => e && e.kind === "gear" && e.inst && e.inst.instance_id === instance_id);
    if (idx === -1) throw Object.assign(new Error("That item is no longer in the vault."), { httpStatus: 404 });
    const removed = items[idx].inst;
    items.splice(idx, 1);
    db.prepare("UPDATE vaults SET data = ?, updated_at = ?, version = version + 1 WHERE account_id = ?").run(
      JSON.stringify(items), nowIso(), accountId
    );
    console.log(`[admin] deleted a legacy gear item (vault) for account_id=${accountId}`);
    return { kind, removed };
  }

  if (kind === "auction") {
    const listingId = Number(location.listing_id);
    if (!Number.isInteger(listingId)) throw Object.assign(new Error("Invalid listing."), { httpStatus: 400 });
    const row = db.prepare("SELECT item_json FROM auction_listings WHERE id = ? AND type = 'gear'").get(listingId);
    if (!row || !row.item_json) throw Object.assign(new Error("Listing not found."), { httpStatus: 404 });
    let target;
    try { target = JSON.parse(row.item_json); } catch (e) { throw Object.assign(new Error("Corrupt listing data."), { httpStatus: 500 }); }
    if (target.instance_id !== instance_id) throw Object.assign(new Error("That listing no longer holds the expected item."), { httpStatus: 404 });
    db.prepare("DELETE FROM auction_listings WHERE id = ?").run(listingId);
    console.log(`[admin] deleted a legacy gear item (auction listing #${listingId})`);
    return { kind, removed: target };
  }

  throw Object.assign(new Error("Invalid location kind."), { httpStatus: 400 });
}

app.post("/api/admin/legacy-gear/delete", requireAuth, requireAdmin, (req, res) => {
  const location = req.body && req.body.location;
  try {
    const result = deleteLegacyGearAtLocation(location);
    console.log(`[admin] ${req.account.username} permanently deleted a legacy gear item (${result.kind})`);
    broadcastSystemMessage("An admin permanently removed an out-of-date item from a player's gear.");
    return res.json({ ok: true, removed: result.removed });
  } catch (e) {
    return res.status(e.httpStatus || 500).json({ error: e.message || "Could not delete that item." });
  }
});

app.post("/api/admin/legacy-gear/rescale", requireAuth, requireAdmin, (req, res) => {
  const location = req.body && req.body.location;
  try {
    const result = rescaleLegacyGearAtLocation(location);
    console.log(`[admin] ${req.account.username} rescaled a legacy gear item (${result.kind})`);
    broadcastSystemMessage("An admin recalibrated an out-of-date item to match current game balance.");
    return res.json({ ok: true, before: result.before, after: result.after });
  } catch (e) {
    return res.status(e.httpStatus || 500).json({ error: e.message || "Could not rescale that item." });
  }
});

// v0.21.1 (#1): "Update All" -- rescales every currently out-of-spec legacy gear item found
// by a fresh legacyGearScan() in one shot, and posts exactly ONE chat announcement summarizing
// the total instead of one message per item (which would spam global chat if there were dozens
// of stale items queued up). Re-scans fresh (never trusts a client-supplied list) so this is
// safe to call even if the admin's on-screen list is stale. Any individual item that fails to
// rescale (e.g. moved/sold since the scan) is silently skipped rather than aborting the whole
// batch -- the response still reports exactly how many succeeded.
app.post("/api/admin/legacy-gear/rescale-all", requireAuth, requireAdmin, (req, res) => {
  const entries = legacyGearScan();
  let succeeded = 0;
  const failures = [];
  for (const entry of entries) {
    try {
      rescaleLegacyGearAtLocation(entry.location);
      succeeded++;
    } catch (e) {
      failures.push({ location: entry.location, error: e.message || "Unknown error" });
    }
  }
  console.log(`[admin] ${req.account.username} bulk-rescaled ${succeeded} legacy gear item(s)`);
  if (succeeded > 0) {
    broadcastSystemMessage(`An admin recalibrated ${succeeded} out-of-date item${succeeded === 1 ? "" : "s"} to match current game balance.`);
  }
  res.json({ ok: true, rescaled_count: succeeded, failed_count: failures.length });
});

app.get("/api/health", (req, res) => res.json({ ok: true, time: nowIso() }));

/* ---------------- system chat announcements (trial results, hardcore deaths) ---------------- */
// These are self-reported by the client (consistent with this phase's "not cheat-proof yet"
// trust model, see the top-of-file note) but are flavor-only chat text, no economy impact.

// v0.24.2: `kind` rides along on both the stored row and the live push so the client can
// style a death notice (grey + tombstone) or an admin broadcast (blue) without pattern
// matching the message text the way the old promotion-line regex had to. Defaults to
// "system", so every existing call site keeps its exact previous behaviour untouched.
function broadcastSystemMessage(message, kind) {
  const created_at = nowIso();
  const k = kind || "system";
  db.prepare("INSERT INTO chat_messages (username, message, created_at, kind) VALUES (?, ?, ?, ?)").run("System", message, created_at, k);
  const payload = JSON.stringify({ type: "chat", username: "System", message, created_at, kind: k });
  for (const client of chatClients) {
    if (client.readyState === client.OPEN) client.send(payload);
  }
}

// v0.24.2: the admin-editable known-issues banner. Stored as a single row (same shape as
// season_state) so it survives a process restart, and pushed live over the chat socket the
// moment it changes -- a player already mid-session sees a newly posted known issue without
// reloading. An empty string means "nothing to announce"; the client hides the banner entirely.
function getAdminAnnouncement() {
  const row = db.prepare("SELECT message, updated_at FROM admin_announcement WHERE id = 1").get();
  return { message: (row && row.message) || "", updated_at: (row && row.updated_at) || null };
}
function setAdminAnnouncement(message) {
  const msg = (message || "").toString().slice(0, ADMIN_ANNOUNCEMENT_MAX_LEN);
  const updated_at = msg ? nowIso() : null;
  db.prepare("UPDATE admin_announcement SET message = ?, updated_at = ? WHERE id = 1").run(msg, updated_at);
  const payload = JSON.stringify({ type: "announcement", message: msg, updated_at });
  for (const client of chatClients) {
    if (client.readyState === client.OPEN) client.send(payload);
  }
  return { message: msg, updated_at };
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
  // v0.24.2: gated behind the same markDeathAnnounced() claim combatHandleHardcoreDeath()
  // uses, so a death the server already resolved and announced itself isn't repeated here.
  // This route still matters for the deaths the server genuinely cannot see (a lethal maze
  // trap is entirely client-resolved), which is why it isn't simply removed.
  if (markDeathAnnounced(req.account.id, character_name)) {
    broadcastSystemMessage(`${character_name} (Lv ${level || 1} ${class_name}) has ${cause}.`, "death");
  }
  res.json({ ok: true });
});

app.post("/api/announce/created", requireAuth, (req, res) => {
  const { character_name, class_name } = req.body || {};
  if (!character_name || !class_name) {
    return res.status(400).json({ error: "Invalid announcement." });
  }
  broadcastSystemMessage(`Nature has given birth to a new ${class_name} named ${character_name}.`);
  res.json({ ok: true });
});

/* ---------------- private mailbox (auction sale notifications) ---------------- */

// v0.17 (#29): `extra` optionally rides along on the LIVE WebSocket push only (never
// persisted to mailbox_messages, which is just plain notification text) -- used by the
// Auction House sale credit below to also carry a live gold_sync figure to a seller who
// happens to be connected at the moment of the sale. See index.html's ensureChat() handler
// for the client side of this.
function sendPrivateMessage(accountId, message, extra) {
  const created_at = nowIso();
  let delivered = 0;
  for (const client of chatClients) {
    if (client.accountId === accountId && client.readyState === client.OPEN) {
      client.send(JSON.stringify(Object.assign({ type: "private", message, created_at }, extra || {})));
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

// v0.22.3 (#4): charge-bearing consumables (currently only Elixir of Clairvoyance) must never
// be listed below full charges -- a buyer paying for "an elixir" should always get a full one.
// Unopened elixirs sitting in `consumables` are always full stacks (the currently-open one's
// partial charge count lives in a separate per-character field, `clarity_charges_left`, which
// never becomes part of a listing today) -- this guard is both the label source and a defense-
// in-depth check against any future/modified-client path that could attach a partial charge
// count to a listing via an explicit `charges_left` field.
const CHARGE_BEARING_CONSUMABLES = { elixir_of_clarity: 10 };

app.post("/api/auction", requireAuth, (req, res) => {
  const { type, price, character_name, item, item_key, quantity, display_name, seller_slot, charges_left } = req.body || {};
  if (!["gear", "consumable", "herb", "material"].includes(type)) return res.status(400).json({ error: "Invalid item type." });
  if (type === "consumable" && CHARGE_BEARING_CONSUMABLES[item_key] != null) {
    const maxCharges = CHARGE_BEARING_CONSUMABLES[item_key];
    if (charges_left != null && Number(charges_left) < maxCharges) {
      return res.status(400).json({ error: "Only a full, unused elixir can be listed." });
    }
  }
  const numPrice = Number(price);
  if (!Number.isFinite(numPrice) || numPrice < 1) return res.status(400).json({ error: "Invalid price." });
  if (!character_name || !display_name) return res.status(400).json({ error: "Missing listing details." });
  // v0.18.4: a gear listing's `item` blob used to be trusted completely -- a modified
  // client could list (and another account could then buy) a completely fabricated item
  // with every affix maxed out, an impossible tier/rarity pairing, or stats that could
  // never come from IF.generate()/IF.reroll(). Same bounds check as the Blacksmith
  // reroll endpoint and the Storage Vault deposit route above; see validateGearItem's own
  // comment for what this does and doesn't prove (numbers-plausible, not provenance-proven).
  if (type === "gear") {
    const itemError = validateGearItem(item);
    if (itemError) return res.status(400).json({ error: `This item can't be listed: ${itemError}` });
  }
  const numQty = type === "gear" ? 1 : Math.max(1, Number(quantity) || 1);
  const itemJson = type === "gear" ? JSON.stringify(item || {}) : null;
  const roundedPrice = Math.round(numPrice);

  // v0.16: charge the listing fee against the seller's OWN stored gold, server-side, rather
  // than trusting whatever gold figure the client's request happens to carry -- the
  // client-side check in index.html is just a convenience/early-exit for the player.
  // v0.17 (#29): gold is account-bound now, so this charges accounts.gold directly instead
  // of a specific character row's own (now-vestigial for gold purposes) data blob. Still
  // requires a real, existing character row at `seller_slot` -- that's just proving the
  // requester actually owns a character to sell as, not where the gold itself lives.
  const slot = Number.isInteger(seller_slot) ? seller_slot : -1;
  if (slot < 0) return res.status(400).json({ error: "Missing seller character slot." });
  const charRow = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!charRow) return res.status(400).json({ error: "Character not found." });

  // v0.22.3 (#14): materials are per-character (not account-bound like gold), so listing a
  // material stack deducts it from THIS character's own data.materials, server-side and
  // atomically -- same discipline as the v0.22.1 gold-dupe fix, never trusting a client-
  // supplied "I have enough" figure. The deduction happens here (before the listing is even
  // inserted) so a listing can never exist without the materials having actually left the
  // seller's character.
  if (type === "material") {
    let sellerData;
    try { sellerData = JSON.parse(charRow.data); } catch (e) { return res.status(500).json({ error: "Corrupt character save." }); }
    if ((sellerData.materials || 0) < numQty) {
      return res.status(400).json({ error: "Not enough Salvage Materials." });
    }
    sellerData.materials -= numQty;
    sellerData._save_seq = (sellerData._save_seq || 0) + 1;
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(sellerData),
      nowIso(),
      req.account.id,
      slot
    );
  }

  const fee = auctionListingFee(roundedPrice);
  if (getAccountGold(req.account.id) < fee) {
    return res.status(400).json({ error: `Not enough gold for the ${fee}g listing fee.` });
  }
  const goldAfterFee = creditAccountGold(req.account.id, -fee);

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
      slot,
      type,
      type === "gear" ? null : String(item_key || ""),
      itemJson,
      display_name,
      numQty,
      roundedPrice,
      nowIso()
    );
  res.json({ ok: true, id: Number(info.lastInsertRowid), fee, account_gold: goldAfterFee });
});

app.delete("/api/auction/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const listing = db.prepare("SELECT * FROM auction_listings WHERE id = ?").get(id);
  if (!listing) return res.status(404).json({ error: "Listing not found." });
  if (listing.seller_account_id !== req.account.id) return res.status(403).json({ error: "Not your listing." });
  db.prepare("DELETE FROM auction_listings WHERE id = ?").run(id);
  // v0.22.3 (#14): a delisted material stack is credited straight back to the seller's OWN
  // character row (the same one it was deducted from at listing time, listing.seller_slot),
  // server-side -- mirrors the listing-time deduction above so materials are never created
  // or destroyed by a list/cancel round trip.
  if (listing.type === "material") {
    const sellerRow = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(listing.seller_account_id, listing.seller_slot);
    if (sellerRow) {
      try {
        const sellerData = JSON.parse(sellerRow.data);
        sellerData.materials = (sellerData.materials || 0) + listing.quantity;
        sellerData._save_seq = (sellerData._save_seq || 0) + 1;
        db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
          JSON.stringify(sellerData),
          nowIso(),
          listing.seller_account_id,
          listing.seller_slot
        );
      } catch (e) { /* corrupt row -- nothing more we can safely do here */ }
    }
  }
  res.json({ ok: true, refund: { type: listing.type, item_key: listing.item_key, item: listing.item_json ? JSON.parse(listing.item_json) : null, quantity: listing.quantity } });
});

app.post("/api/auction/:id/buy", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const { character_name, buyer_slot } = req.body || {};
  const listing = db.prepare("SELECT * FROM auction_listings WHERE id = ?").get(id);
  if (!listing) return res.status(404).json({ error: "That listing is no longer available." });
  if (listing.seller_account_id === req.account.id) return res.status(400).json({ error: "You can't buy your own listing." });

  // v0.17 (#29): gold is account-bound now -- a single, authoritative check against the
  // buyer's own account gold, replacing the old best-effort scan across their character
  // rows (which was really just working around gold being scattered per-character in the
  // first place; that whole problem no longer exists).
  if (getAccountGold(req.account.id) < listing.price) return res.status(400).json({ error: "Not enough gold." });

  // v0.22.3 (#14): a material listing needs the BUYER's own character slot to credit
  // data.materials to (materials are per-character, unlike account-bound gold) -- validated
  // up front, before the listing is removed, so a bad/missing slot fails clean rather than
  // consuming the listing without paying anything out.
  let buyerSlotNum = null;
  if (listing.type === "material") {
    buyerSlotNum = Number(buyer_slot);
    if (!Number.isInteger(buyerSlotNum) || buyerSlotNum < 0 || buyerSlotNum >= MAX_CHARACTER_SLOTS) {
      return res.status(400).json({ error: "Missing buyer character slot." });
    }
    const buyerCharRow = db.prepare("SELECT 1 FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, buyerSlotNum);
    if (!buyerCharRow) return res.status(400).json({ error: "Character not found." });
  }

  // v0.25.3 CRITICAL BUG FIX (reported by Stration, via Gwen): buying a GEAR listing with a full
  // backpack destroyed both sides of the trade for the buyer. The server took the gold and
  // removed the listing, then handed the item back in the response for the client to store --
  // and PS.addGearAutoEquip() silently dropped it when there was nowhere to put it. Gold gone,
  // item gone, no error shown.
  //
  // To be clear about the blast radius, since it was asked: the SELLER was always paid. Gold has
  // been account-bound since v0.17 and creditAccountGold() below runs unconditionally, so there
  // was exactly one victim, not two.
  //
  // The check has to live here rather than only in the client, because this route is what
  // actually spends the gold, and it must happen BEFORE the listing is deleted so a refused
  // purchase leaves the listing intact and buyable by someone else. Capacity is computed from
  // the buyer's own stored character row with the same helper the combat loot path uses, so the
  // two can't disagree about what "full" means. An empty matching equipment slot counts as room,
  // because the client auto-equips into one -- that is a real destination for the item, not a
  // backpack slot, and refusing there would block a legitimate purchase.
  if (listing.type === "gear") {
    const buyerGearSlot = Number(buyer_slot);
    if (!Number.isInteger(buyerGearSlot) || buyerGearSlot < 0 || buyerGearSlot >= MAX_CHARACTER_SLOTS) {
      return res.status(400).json({ error: "Missing buyer character slot." });
    }
    const buyerData = loadCharacterRow(req.account.id, buyerGearSlot);
    if (!buyerData) return res.status(400).json({ error: "Character not found." });
    let inst = null;
    try { inst = listing.item_json ? JSON.parse(listing.item_json) : null; } catch (e) { inst = null; }
    const gearList = buyerData.gear_instances || [];
    const hasBackpackRoom = gearList.length < combatGetInventoryCapacity(buyerData);
    const autoEquipSlot = inst ? combatAutoEquipTargetSlot(buyerData, inst) : null;
    if (!hasBackpackRoom && !autoEquipSlot) {
      return res.status(400).json({ error: "Your backpack is full. Make room before buying this." });
    }
  }

  // Remove the listing first (best-effort race protection against double-buy).
  const del = db.prepare("DELETE FROM auction_listings WHERE id = ?").run(id);
  if (del.changes === 0) return res.status(409).json({ error: "Someone already bought that." });

  if (listing.type === "material") {
    const buyerRow = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, buyerSlotNum);
    if (buyerRow) {
      try {
        const buyerData = JSON.parse(buyerRow.data);
        buyerData.materials = (buyerData.materials || 0) + listing.quantity;
        buyerData._save_seq = (buyerData._save_seq || 0) + 1;
        db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
          JSON.stringify(buyerData),
          nowIso(),
          req.account.id,
          buyerSlotNum
        );
      } catch (e) { /* corrupt row -- gold still refunds correctly below, materials just can't be credited */ }
    }
  }

  const buyerGoldAfter = creditAccountGold(req.account.id, -listing.price);
  // v0.17 (#29) BUG FIX: this used to credit the seller's OWN CHARACTER ROW directly, which
  // silently failed to grant any gold at all if that specific character row no longer
  // existed -- e.g. a Hardcore character that had since died (its row is deleted on death,
  // see DELETE /api/characters/:slot) still had listings that could sell, but the payout
  // vanished into thin air the moment the guarded `if (sellerRow)` check failed. Gold is now
  // account-bound, and accounts are never deleted, so crediting the account directly (no
  // guard needed) fixes this completely -- the seller gets paid no matter what happened to
  // the specific character that originally listed the item.
  const sellerGoldAfter = creditAccountGold(listing.seller_account_id, listing.price);

  // v0.25 ("The Long Road to Market"): count this sale against the SELLING CHARACTER, not the
  // account -- the questline is per-character even though the gold is account-shared. Matched
  // on the stored slot AND the stored character name together: character names are globally
  // unique among LIVE characters (see PUT /api/characters/:slot's conflict check) but a name is
  // released when its character is deleted or dies in hardcore, so slot alone could credit a
  // completely different character that has since taken that slot, and name alone could credit
  // a later namesake. Requiring both means a replacement character starts its own count at zero.
  // Failing silently is correct here: a hardcore seller whose character died before the sale
  // completed has no row left to credit, and the gold above has already been paid to the
  // account regardless.
  try {
    const sellerCharRow = db
      .prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?")
      .get(listing.seller_account_id, listing.seller_slot);
    if (sellerCharRow) {
      const sellerData = JSON.parse(sellerCharRow.data);
      if ((sellerData.character_name || "") === listing.seller_character_name) {
        sellerData.auction_sales = (sellerData.auction_sales || 0) + 1;
        questEnsureState(sellerData);
        sellerData._save_seq = (sellerData._save_seq || 0) + 1;
        db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?")
          .run(JSON.stringify(sellerData), nowIso(), listing.seller_account_id, listing.seller_slot);
      }
    }
  } catch (e) { /* corrupt row: the sale itself and its gold are unaffected */ }

  const label = listing.quantity > 1 ? `${listing.display_name} x${listing.quantity}` : listing.display_name;
  sendPrivateMessage(req.account.id, `You bought ${label} for ${listing.price} gold from ${listing.seller_character_name}.`, { gold: buyerGoldAfter });
  // The chat notification itself was already sent unconditionally regardless of the
  // seller's active character (sendPrivateMessage is keyed by account, not character slot)
  // -- what was actually broken was the gold credit above, now fixed. The `gold` field here
  // additionally gives the seller a live gold_sync if they happen to be connected right now
  // (see index.html's ensureChat() WS handler); if not, they'll pick up the fresh total the
  // next time GET /api/characters runs, at login (see that route's own comment).
  sendPrivateMessage(listing.seller_account_id, `Your ${label} has been sold to ${character_name || req.account.username} for ${listing.price} gold.`, { gold: sellerGoldAfter });

  res.json({
    ok: true,
    type: listing.type,
    item_key: listing.item_key,
    item: listing.item_json ? JSON.parse(listing.item_json) : null,
    quantity: listing.quantity,
    account_gold: buyerGoldAfter,
  });
});

const server = http.createServer(app);


/* ==================================================================================
   CO-OP MULTIPLAYER -- PARTY CORE                                    (v0.30, Gwen)
   ==================================================================================
   Parties of up to 4 share one maze, one set of encounters and one loot pool.

   ARCHITECTURE NOTE, and the single most important thing to understand here:
   solo play is NOT touched by any of this. Solo combat keeps its own combat_sessions
   table, its own routes and its own server pusher entry, exactly as before. Party
   combat is a SECOND, parallel system (party_encounters below). That is a deliberate
   choice -- converging them would mean rewriting the most battle-tested code in the
   game on the same night a large feature ships. The cost is that combat rules now
   exist in two places and can drift; that is a real debt and it is written down here
   rather than discovered later.

   MAZE AUTHORITY: the leader's client generates the maze exactly as it does in solo
   play and relays the layout to the server, which stores it and forwards it to
   followers. The server does NOT generate or validate the maze yet -- it is the same
   client-trust model solo play already has (see the SCOPE NOTE ON MAZE LEGITIMACY
   above), with the same known gap. The stakes are higher here, because a dishonest
   leader would now be farming on behalf of three other accounts rather than only
   themselves, so server-side maze generation is the natural next step. Deliberate,
   discussed, and deferred.
   ================================================================================== */

db.exec(`
  CREATE TABLE IF NOT EXISTS parties (
    id TEXT PRIMARY KEY,
    leader_account_id INTEGER NOT NULL,
    min_level INTEGER NOT NULL DEFAULT 1,
    max_level INTEGER NOT NULL DEFAULT 99,
    status TEXT NOT NULL DEFAULT 'open',
    maze_json TEXT,
    area_level INTEGER NOT NULL DEFAULT 1,
    is_night INTEGER NOT NULL DEFAULT 0,
    kill_streak INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS party_members (
    party_id TEXT NOT NULL,
    account_id INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    username TEXT NOT NULL,
    character_name TEXT NOT NULL,
    class_id TEXT NOT NULL DEFAULT '',
    level INTEGER NOT NULL DEFAULT 1,
    hardcore INTEGER NOT NULL DEFAULT 0,
    join_order INTEGER NOT NULL,
    row_pos TEXT NOT NULL DEFAULT 'back',
    in_town INTEGER NOT NULL DEFAULT 0,
    last_attack_at INTEGER NOT NULL DEFAULT 0,
    last_regen_at INTEGER NOT NULL DEFAULT 0,
    spell_cooldowns TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (party_id, account_id)
  );
`);

/* v0.30.3 BUG FIX (credit: Gwen, "click or hold does not work, I cannot damage the mob at all").
   CREATE TABLE IF NOT EXISTS does exactly nothing to a table that already exists -- it does NOT
   add columns. The attack-cadence gate added in v0.30.1 put last_attack_at into the CREATE TABLE
   above, which only takes effect on a database created AFTER that change. On any database made at
   v0.30 the column was simply absent, so `UPDATE party_members SET last_attack_at = ?` -- which
   runs on EVERY swing -- threw, and every attack failed before it could deal damage.

   This is the same belt-and-suspenders pattern combat_sessions already uses a few hundred lines
   above, and the pattern I should have used the first time. Every ALTER is wrapped individually
   so an already-migrated database simply skips it. Any future party column goes here as well as
   in the CREATE TABLE, never only in the CREATE TABLE. */
for (const stmt of [
  "ALTER TABLE party_members ADD COLUMN last_attack_at INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE party_members ADD COLUMN last_regen_at INTEGER NOT NULL DEFAULT 0",
  "ALTER TABLE party_members ADD COLUMN spell_cooldowns TEXT NOT NULL DEFAULT '{}'",
]) {
  try { db.exec(stmt); } catch (e) { /* column already exists, fine */ }
}

const PARTY_MAX = 4;
// The party's own kill counter is worth a TENTH of what a solo kill streak is worth, and it is
// counted ONCE for the party rather than once per member (Gwen's spec). A party of four does not
// brew four streaks at the same time; it brews one, slowly. 5,000 kills reaches the cap on streak
// alone, which is a long delve's worth of value rather than a free ride.
const PARTY_MF_PER_KILL = 0.1;

function partyNewId() { return "p" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }

function partyRow(partyId) {
  return db.prepare("SELECT * FROM parties WHERE id = ?").get(partyId) || null;
}
function partyMembers(partyId) {
  return db.prepare("SELECT * FROM party_members WHERE party_id = ? ORDER BY join_order ASC").all(partyId);
}
function partyForAccount(accountId) {
  const m = db.prepare("SELECT party_id FROM party_members WHERE account_id = ?").get(accountId);
  return m ? partyRow(m.party_id) : null;
}
function partyMemberRow(partyId, accountId) {
  return db.prepare("SELECT * FROM party_members WHERE party_id = ? AND account_id = ?").get(partyId, accountId) || null;
}
function partyTouch(partyId, fields) {
  const f = Object.assign({}, fields || {});
  const keys = Object.keys(f);
  const setSql = (keys.length ? keys.map((k) => `${k} = ?`).join(", ") + ", " : "") + "updated_at = ?";
  db.prepare(`UPDATE parties SET ${setSql} WHERE id = ?`).run(...keys.map((k) => f[k]), nowIso(), partyId);
}

/* ---------------- magic find ----------------
   Gwen's rule: the party finds loot at the COMBINED rate of everyone in it, not at each member's
   own rate. A player who brings nothing still benefits from what everyone else brought, and a
   player with good gear makes the whole party richer -- which is the incentive to group up.
   Sum every member's own total Magic Find (gear, shrines, night delve -- whatever they each
   already have), then add the party streak once, then clamp to the same 500% ceiling solo play
   uses. Deliberately generous; the ceiling is what stops it running away. */
function partyMagicFindBreakdown(partyId) {
  const members = partyMembers(partyId);
  const party = partyRow(partyId);
  const rows = [];
  let membersTotal = 0;
  for (const m of members) {
    const data = loadCharacterRow(m.account_id, m.slot);
    if (!data) continue;
    // Reuse the SOLO magic-find function verbatim so a member's contribution is computed exactly
    // the way their own character sheet computes it -- one source of truth for what a player's
    // Magic Find is, whatever context asks.
    const own = combatGetMagicFind(data, { is_night: party ? party.is_night : 0 });
    membersTotal += own;
    rows.push({ account_id: m.account_id, character_name: m.character_name, magic_find: Math.round(own) });
  }
  const streakKills = party ? party.kill_streak || 0 : 0;
  const streakPct = streakKills * PARTY_MF_PER_KILL;
  const raw = membersTotal + streakPct;
  const total = cbClampf(raw, 0, CB.MAGIC_FIND_CAP_PCT);
  return {
    rows,
    members_total: Math.round(membersTotal),
    streak_kills: streakKills,
    streak_pct: Math.round(streakPct),
    raw: Math.round(raw),
    total: Math.round(total),
    cap: CB.MAGIC_FIND_CAP_PCT,
    at_cap: raw >= CB.MAGIC_FIND_CAP_PCT,
  };
}

/* ---------------- public shape ---------------- */
function partyPublicState(partyId) {
  const party = partyRow(partyId);
  if (!party) return null;
  const members = partyMembers(partyId).map((m) => {
    const data = loadCharacterRow(m.account_id, m.slot);
    return {
      account_id: m.account_id,
      username: m.username,
      character_name: m.character_name,
      class_id: m.class_id,
      level: m.level,
      hardcore: !!m.hardcore,
      is_leader: m.account_id === party.leader_account_id,
      row_pos: m.row_pos,
      in_town: !!m.in_town,
      join_order: m.join_order,
      // Live vitals for the in-maze roster. Read fresh rather than cached, because a member's HP
      // changes from their own combat actions on their own request thread.
      hp: data ? Math.round(data.current_hp || 0) : 0,
      max_hp: data ? Math.round(combatGetMaxHp(data)) : 0,
      mana: data ? Math.round(data.current_mana || 0) : 0,
      max_mana: data ? Math.round(combatGetMaxMana(data)) : 0,
      stamina: data ? Math.round(data.current_stamina || 0) : 0,
      max_stamina: data ? Math.round(combatGetMaxStamina(data)) : 0,
      dead: data ? (data.current_hp || 0) <= 0 : false,
    };
  });
  return {
    id: party.id,
    leader_account_id: party.leader_account_id,
    min_level: party.min_level,
    max_level: party.max_level,
    status: party.status,
    area_level: party.area_level,
    is_night: !!party.is_night,
    note: party.note,
    size: members.length,
    max_size: PARTY_MAX,
    members,
    magic_find: partyMagicFindBreakdown(partyId),
    // The party can only move as far as its weakest walker. A member who has stepped out to town
    // is excluded entirely -- that is the whole point of the town exception.
    stamina_gate: partyStaminaGate(members),
  };
}

function partyStaminaGate(members) {
  const walking = members.filter((m) => !m.in_town && !m.dead);
  if (walking.length === 0) return { blocked: true, who: null, stamina: 0, reason: "nobody_in_maze" };
  let low = walking[0];
  for (const m of walking) if (m.stamina < low.stamina) low = m;
  return {
    blocked: low.stamina <= 0,
    who: low.character_name,
    account_id: low.account_id,
    stamina: low.stamina,
    max_stamina: low.max_stamina,
  };
}

/* ---------------- broadcast ----------------
   Rides the SAME per-account WebSocket the chat and the solo combat pusher already use, so there
   is one socket per player and no second connection to manage. Every party broadcast carries a
   monotonically increasing sequence number: with four clients receiving independently, a client
   that processes an older message after a newer one would render a stale roster or replay a dead
   monster's damage. Clients drop anything they have already seen. */
let partySeq = 0;
function partyBroadcast(partyId, payload) {
  const members = partyMembers(partyId);
  const ids = new Set(members.map((m) => m.account_id));
  const body = JSON.stringify(Object.assign({ party_id: partyId, seq: ++partySeq }, payload));
  for (const client of chatClients) {
    if (client.readyState === client.OPEN && ids.has(client.accountId)) client.send(body);
  }
}
function partyBroadcastState(partyId) {
  const state = partyPublicState(partyId);
  if (state) partyBroadcast(partyId, { type: "party_state", state });
}
// A system line in party chat. Not persisted: a party is temporary, and a player who rejoins one
// later should not be able to read a scrollback from a party they were not in at the time.
function partySystem(partyId, message) {
  partyBroadcast(partyId, { type: "party_chat", system: true, message, created_at: nowIso() });
}

/* ---------------- lifecycle ---------------- */
function partyDisband(partyId, reason) {
  partySystem(partyId, reason || "The party has disbanded.");
  partyBroadcast(partyId, { type: "party_disbanded" });
  db.prepare("DELETE FROM party_members WHERE party_id = ?").run(partyId);
  db.prepare("DELETE FROM party_encounters WHERE party_id = ?").run(partyId);
  db.prepare("DELETE FROM parties WHERE id = ?").run(partyId);
}

/* Removing a member is the one place several rules meet, so they all live here rather than being
   repeated at each call site (leave, disconnect, hardcore death):
     - the party reopens for matchmaking whenever it drops below the cap,
     - leadership passes to the next member by JOIN ORDER (Gwen's rule) rather than being elected,
     - a party with nobody left is deleted rather than lingering as an empty row. */
function partyRemoveMember(partyId, accountId, reason) {
  const party = partyRow(partyId);
  if (!party) return;
  const member = partyMemberRow(partyId, accountId);
  if (!member) return;
  db.prepare("DELETE FROM party_members WHERE party_id = ? AND account_id = ?").run(partyId, accountId);
  const remaining = partyMembers(partyId);
  if (remaining.length === 0) {
    db.prepare("DELETE FROM party_encounters WHERE party_id = ?").run(partyId);
    db.prepare("DELETE FROM parties WHERE id = ?").run(partyId);
    return;
  }
  let leaderChanged = false;
  if (party.leader_account_id === accountId) {
    const next = remaining[0]; // join_order ASC -- the longest-standing member takes over
    db.prepare("UPDATE parties SET leader_account_id = ? WHERE id = ?").run(next.account_id, partyId);
    leaderChanged = true;
  }
  partyTouch(partyId, { status: remaining.length >= PARTY_MAX ? "full" : "open" });
  partySystem(partyId, `${member.character_name} ${reason || "left the party"} (${remaining.length}/${PARTY_MAX}).`);
  if (leaderChanged) {
    const next = partyMembers(partyId)[0];
    partySystem(partyId, `${next.character_name} is now leading.`);
  }
  partyBroadcastState(partyId);
}

/* ---------------- routes ---------------- */

// The lobby list. Only parties that would actually accept THIS character are shown, because a
// list full of parties you cannot join is a list you stop reading.
app.get("/api/party/list", requireAuth, (req, res) => {
  const slot = Number(req.query.slot);
  const data = loadCharacterRow(req.account.id, slot);
  const level = data ? data.level : 1;
  const rows = db.prepare("SELECT * FROM parties ORDER BY created_at DESC LIMIT 40").all();
  const out = [];
  for (const p of rows) {
    const members = partyMembers(p.id);
    if (members.length >= PARTY_MAX) continue;
    if (level < p.min_level || level > p.max_level) continue;
    const leader = members.find((m) => m.account_id === p.leader_account_id) || members[0];
    if (!leader) continue;
    out.push({
      id: p.id,
      leader_name: leader.character_name,
      leader_level: leader.level,
      min_level: p.min_level,
      max_level: p.max_level,
      size: members.length,
      max_size: PARTY_MAX,
      area_level: p.area_level,
      note: p.note,
      magic_find: partyMagicFindBreakdown(p.id).total,
    });
  }
  res.json({ ok: true, parties: out, your_level: level });
});

app.post("/api/party/create", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(400).json({ error: "No character in that slot." });
  if (partyForAccount(req.account.id)) return res.status(400).json({ error: "You are already in a party." });
  const spread = Math.max(1, Math.min(20, Number(req.body?.spread) || 3));
  const note = String(req.body?.note || "").slice(0, 90);
  const id = partyNewId();
  const now = nowIso();
  db.prepare(
    `INSERT INTO parties (id, leader_account_id, min_level, max_level, status, area_level, is_night, kill_streak, note, created_at, updated_at)
     VALUES (?, ?, ?, ?, 'open', 1, 0, 0, ?, ?, ?)`
  ).run(id, req.account.id, Math.max(1, data.level - spread), data.level + spread, note, now, now);
  partyAddMemberRow(id, req.account.id, slot, req.account.username, data, 0);
  res.json({ ok: true, party: partyPublicState(id) });
});

function partyAddMemberRow(partyId, accountId, slot, username, data, joinOrder) {
  db.prepare(
    `INSERT INTO party_members (party_id, account_id, slot, username, character_name, class_id, level, hardcore, join_order, row_pos, in_town)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`
  ).run(
    partyId, accountId, slot, username, data.character_name || username, data.class_id || "",
    data.level || 1, data.hardcore ? 1 : 0, joinOrder,
    // A new member starts in the BACK row. Walking into a fight and discovering you are tanking
    // is a worse surprise than walking in and discovering you are not.
    "back"
  );
}

app.post("/api/party/join", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const partyId = String(req.body?.party_id || "");
  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(400).json({ error: "No character in that slot." });
  if (partyForAccount(req.account.id)) return res.status(400).json({ error: "You are already in a party." });
  const party = partyRow(partyId);
  if (!party) return res.status(404).json({ error: "That party no longer exists." });
  const members = partyMembers(partyId);
  // The cap is absolute. Checked here, on the server, at the moment of joining -- two players
  // clicking Join on the last slot at the same instant are serialised by the single-threaded
  // runtime, so the second one reads a full party and is refused.
  if (members.length >= PARTY_MAX) return res.status(400).json({ error: "That party is full." });
  if (data.level < party.min_level || data.level > party.max_level) {
    return res.status(400).json({ error: `That party is taking levels ${party.min_level} to ${party.max_level}.` });
  }
  const joinOrder = members.reduce((a, m) => Math.max(a, m.join_order), 0) + 1;
  partyAddMemberRow(partyId, req.account.id, slot, req.account.username, data, joinOrder);
  const size = members.length + 1;
  partyTouch(partyId, { status: size >= PARTY_MAX ? "full" : "open" });
  const mf = partyMagicFindBreakdown(partyId);
  partySystem(partyId, `${data.character_name} joined the party (${size}/${PARTY_MAX}). Party Magic Find is now ${mf.total}%.`);
  partyBroadcastState(partyId);
  // v0.30.1 BUG FIX (credit: Gwen, "player 2 sees no maze"): the maze was only ever relayed at
  // the moment the LEADER generated one. A player joining a party whose leader was already in a
  // maze therefore never received it and stared at an empty frame. The current layout now rides
  // back on the join response, so a joiner is standing in the same maze as everyone else before
  // their first render.
  let joinMaze = null;
  if (party.maze_json) { try { joinMaze = JSON.parse(party.maze_json); } catch (e) { joinMaze = null; } }
  res.json({
    ok: true, party: partyPublicState(partyId),
    maze: joinMaze, area_level: party.area_level, is_night: !!party.is_night,
  });
});

app.post("/api/party/leave", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.json({ ok: true, left: false });
  partyRemoveMember(party.id, req.account.id, "left the party");
  res.json({ ok: true, left: true });
});

app.get("/api/party/state", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.json({ ok: true, party: null });
  res.json({ ok: true, party: partyPublicState(party.id) });
});

// Leader-only settings. Narrowing the range never removes anyone already in the party -- it only
// changes who can find it from here on (Gwen's spec).
app.post("/api/party/settings", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  if (party.leader_account_id !== req.account.id) return res.status(403).json({ error: "Only the leader can change this." });
  const fields = {};
  if (req.body?.spread != null) {
    const leader = partyMemberRow(party.id, req.account.id);
    const spread = Math.max(1, Math.min(20, Number(req.body.spread) || 3));
    fields.min_level = Math.max(1, (leader ? leader.level : 1) - spread);
    fields.max_level = (leader ? leader.level : 1) + spread;
  }
  if (req.body?.note != null) fields.note = String(req.body.note).slice(0, 90);
  partyTouch(party.id, fields);
  partyBroadcastState(party.id);
  res.json({ ok: true, party: partyPublicState(party.id) });
});

// Front row or back row. Any member sets their own -- this is not a leader decision, because the
// player taking the damage should be the one choosing to take it.
app.post("/api/party/row", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const row = req.body?.row === "front" ? "front" : "back";
  db.prepare("UPDATE party_members SET row_pos = ? WHERE party_id = ? AND account_id = ?").run(row, party.id, req.account.id);
  partyBroadcastState(party.id);
  res.json({ ok: true, row });
});

// Stepping out to town to rest, and coming back. While in town a member is not counted against
// the stamina gate, so the rest of the party keeps exploring without them.
app.post("/api/party/town", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const inTown = !!req.body?.in_town;
  db.prepare("UPDATE party_members SET in_town = ? WHERE party_id = ? AND account_id = ?").run(inTown ? 1 : 0, party.id, req.account.id);
  const me = partyMemberRow(party.id, req.account.id);
  partySystem(party.id, inTown
    ? `${me.character_name} went back to town to rest. The party can keep moving.`
    : `${me.character_name} rejoined the party in the maze.`);
  partyBroadcastState(party.id);
  // Rejoining from town drops you back onto the leader's position, so the maze has to come with
  // it -- parsed, not the raw JSON string, which is what this used to hand back.
  let backMaze = null;
  if (!inTown && party.maze_json) { try { backMaze = JSON.parse(party.maze_json); } catch (e) { backMaze = null; } }
  res.json({ ok: true, in_town: inTown, maze: backMaze, area_level: party.area_level, is_night: !!party.is_night });
});


/* ==================================================================================
   CO-OP MULTIPLAYER -- SHARED ENCOUNTERS                             (v0.30, Gwen)
   ==================================================================================
   A party encounter is ONE row holding N monsters and pointing at a party of M players.
   This is the second combat system referred to at the top of the party core; solo
   combat_sessions rows are untouched and continue to work exactly as before.

   WHY THIS IS NOT A TICK ENGINE. The existing solo pusher does not simulate frames --
   it computes damage OWED from elapsed wall-clock time and settles it (see
   combatCatchUpMonsterHits). That model extends to four players and four monsters
   without any new machinery: there is simply more owed damage to settle per pass. The
   party pusher below runs more often than the solo one (200ms vs 500ms) purely so the
   combat log arrives in readable dribs rather than half-second clumps.

   WHY THERE IS NO LOCKING. node:sqlite's DatabaseSync is synchronous and there is not a
   single `await` anywhere in the combat path, so Node's single thread runs each request
   to completion before starting the next. Four players attacking the same encounter are
   already serialised by the runtime. This is worth stating explicitly because it is the
   usual hard part of multiplayer combat and here it is free -- but it also means nothing
   in this file may become async without revisiting that guarantee.
   ================================================================================== */

db.exec(`
  CREATE TABLE IF NOT EXISTS party_encounters (
    id TEXT PRIMARY KEY,
    party_id TEXT NOT NULL,
    area_level INTEGER NOT NULL,
    is_night INTEGER NOT NULL DEFAULT 0,
    monsters TEXT NOT NULL,
    loot TEXT NOT NULL DEFAULT '[]',
    log TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
`);

const PARTY_PUSH_INTERVAL_MS = 200;
const PARTY_LOG_LIMIT = 200;
// Contested loot: a claim opens a window rather than resolving instantly, so a second player has
// a real chance to contest something they wanted. Three seconds is long enough to react to and
// short enough not to stall the party.
const PARTY_LOOT_CLAIM_WINDOW_MS = 3000;
// Experience bonus for grouping, on top of every player receiving the FULL kill reward.
const PARTY_XP_BONUS_PCT = { 1: 0, 2: 5, 3: 10, 4: 15 };

function partyEncounterFor(partyId) {
  return db.prepare("SELECT * FROM party_encounters WHERE party_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1").get(partyId) || null;
}
function partyEncounterSave(id, fields) {
  const keys = Object.keys(fields);
  if (!keys.length) return;
  db.prepare(`UPDATE party_encounters SET ${keys.map((k) => `${k} = ?`).join(", ")}, updated_at = ? WHERE id = ?`)
    .run(...keys.map((k) => fields[k]), nowIso(), id);
}
function partyEncMonsters(enc) { try { return JSON.parse(enc.monsters); } catch (e) { return []; } }
function partyEncLoot(enc) { try { return JSON.parse(enc.loot); } catch (e) { return []; } }
function partyEncLog(enc) { try { return JSON.parse(enc.log); } catch (e) { return []; } }

function partyEncPush(enc, lines) {
  const log = partyEncLog(enc).concat(lines);
  while (log.length > PARTY_LOG_LIMIT) log.shift();
  return log;
}

/* ---------------- building the fight ----------------
   Monster count is rolled between 1 and the party's size, so a party of four can still walk into
   a single monster and a party of two never faces more than two. Deliberately unweighted: the
   swinginess is the point, and the maths works out in solo play's favour anyway -- solo is one
   monster for one player, while a party of four averages 2.5 monsters shared between four, which
   is FEWER loot rolls per player per encounter, not more. */
function partyRollMonsterCount(partySize) {
  return cbRandIntRange(1, Math.max(1, Math.min(PARTY_MAX, partySize)));
}

function partyBuildMonster(idx, monsterId, areaLevel, isGuardian, isRoamer, leaderData) {
  const monster = COMBAT_MONSTERS.find((m) => m.id === monsterId);
  if (!monster) return null;
  const hpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_HP_MULT : isRoamer ? CB.ROAMING_MOB_HP_MULT : 1;
  const dmgMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_DAMAGE_MULT : isRoamer ? CB.ROAMING_MOB_DAMAGE_MULT : 1;
  const xpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_XP_MULT : isRoamer ? CB.ROAMING_MOB_XP_MULT : 1;
  const maxHp = Math.max(1, Math.round(combatMonsterHp(monster.base_hp, areaLevel) * hpMult));
  const dmgFlat = combatMonsterDamage(monster.base_damage, areaLevel) * dmgMult;
  // Gold is taken straight off the monster definition, unscaled by area level -- exactly what
  // solo /api/combat/start does. Any scaling belongs in one place for both systems, not invented
  // here for the party path only.
  return {
    idx,
    monster_id: monster.id,
    // Every monster in a shared fight is named individually, because "the Ashwalker hits you" is
    // ambiguous when there are three Ashwalkers and only one of them is on you.
    name: (isGuardian ? `Guardian ${monster.name}` : isRoamer ? `Roaming ${monster.name}` : monster.name),
    label: null,
    hp: maxHp,
    max_hp: maxHp,
    dmg_min: dmgFlat * (1 - CB.MONSTER_DAMAGE_JITTER),
    dmg_max: dmgFlat * (1 + CB.MONSTER_DAMAGE_JITTER),
    xp: combatMonsterXpReward(monster.base_xp, areaLevel) * xpMult,
    gold_min: monster.gold_min,
    gold_max: monster.gold_max,
    loot_table: monster.loot_table,
    is_guardian: !!isGuardian,
    is_roamer: !!isRoamer,
    attack_speed: CB.MONSTER_BASE_ATTACK_SPEED,
    last_hit_at: Date.now(),
    target_account_id: null,
    dead: false,
  };
}

/* Aggro. The whole of it, deliberately: monsters strike the front row, and if the front row is
   empty they pick at random among whoever is left. No threat table, no healer penalty, no even
   split -- a party that leaves the front empty is not punished with extra damage, it just loses
   the ability to choose who takes it. Recomputed on every settle rather than stored, because
   members change rows mid-fight and a stored target would go stale. */
function partyAssignTargets(partyId, monsters) {
  const state = partyPublicState(partyId);
  if (!state) return monsters;
  const alive = state.members.filter((m) => !m.in_town && !m.dead);
  if (alive.length === 0) return monsters;
  const front = alive.filter((m) => m.row_pos === "front");
  const pool = front.length ? front : alive;
  for (const mon of monsters) {
    if (mon.dead) { mon.target_account_id = null; continue; }
    // Keep an existing target if it is still a legal one, so a monster does not re-roll its
    // victim every 200ms and produce a log that reads like static.
    const stillValid = mon.target_account_id != null && pool.some((m) => m.account_id === mon.target_account_id);
    if (!stillValid) mon.target_account_id = pool[Math.floor(Math.random() * pool.length)].account_id;
  }
  return monsters;
}

/* ---------------- start ----------------
   Leader-only. The leader is the one walking the maze, so the leader is the one who reports
   having walked into something. Same client-asserted monster/area/flags as solo play, validated
   the same way -- see the scope note at the top of the party core. */
app.post("/api/party/encounter/start", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  if (party.leader_account_id !== req.account.id) return res.status(403).json({ error: "Only the leader starts fights." });
  if (partyEncounterFor(party.id)) return res.status(400).json({ error: "The party is already fighting." });

  const me = partyMemberRow(party.id, req.account.id);
  const leaderData = loadCharacterRow(req.account.id, me.slot);
  if (!leaderData) return res.status(400).json({ error: "No character." });

  const areaLevel = Math.max(1, Math.min(Number(req.body?.area_level) || 1, leaderData.max_maze_depth_reached || 1));
  const isGuardian = !!req.body?.is_guardian;
  const isRoamer = !!req.body?.is_roamer;
  const isNight = !!req.body?.is_night;

  const members = partyMembers(party.id);
  // Members resting in town do not swell the encounter. Rolling monster count against the party's
  // paper size while only two people are actually present would be a quiet difficulty spike.
  const present = members.filter((m) => !m.in_town);
  const count = isGuardian ? 1 : partyRollMonsterCount(present.length || 1);

  const monsters = [];
  for (let i = 0; i < count; i++) {
    const pick = req.body?.monster_id && i === 0 ? req.body.monster_id : partyPickMonsterId(areaLevel);
    const mon = partyBuildMonster(i, pick, areaLevel, isGuardian, isRoamer, leaderData);
    if (mon) monsters.push(mon);
  }
  if (!monsters.length) return res.status(400).json({ error: "No monster." });
  // Disambiguate duplicates: two Ashwalkers become "Ashwalker I" and "Ashwalker II" so the log and
  // the targeting arrows can name one without naming both.
  const byName = {};
  for (const m of monsters) byName[m.name] = (byName[m.name] || 0) + 1;
  const seen = {};
  for (const m of monsters) {
    if (byName[m.name] > 1) { seen[m.name] = (seen[m.name] || 0) + 1; m.label = `${m.name} ${romanNumeralSmall(seen[m.name])}`; }
    else m.label = m.name;
  }
  partyAssignTargets(party.id, monsters);

  const id = "e" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const now = nowIso();
  db.prepare(
    `INSERT INTO party_encounters (id, party_id, area_level, is_night, monsters, loot, log, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, '[]', ?, 'active', ?, ?)`
  ).run(id, party.id, areaLevel, isNight ? 1 : 0, JSON.stringify(monsters),
        JSON.stringify([`The party is set upon by ${monsters.length === 1 ? monsters[0].label : monsters.length + " monsters"}!`]), now, now);
  partyTouch(party.id, { area_level: areaLevel, is_night: isNight ? 1 : 0 });
  partyBroadcastEncounter(party.id);
  res.json({ ok: true, encounter: partyEncounterPublic(party.id) });
});

function romanNumeralSmall(n) { return ["", "I", "II", "III", "IV", "V", "VI"][n] || String(n); }

// Mirrors the area gating solo play already applies at /api/combat/start: only monsters whose
// tier is genuinely available at this area level can appear.
function partyPickMonsterId(areaLevel) {
  const eligible = COMBAT_MONSTERS.filter((m) => combatMonsterAllowedForAreaLevel(m, areaLevel));
  const pool = eligible.length ? eligible : COMBAT_MONSTERS;
  return pool[Math.floor(Math.random() * pool.length)].id;
}

/* v0.30.1 BUG FIX (credit: Gwen, "no loot, just 'fight ended'"): this read partyEncounterFor(),
   which only returns encounters with status 'active'. The instant the last monster died the
   status became 'cleared', so the very broadcast that should have shown the party its loot
   returned null instead and every client rendered "the fight is over" over an empty screen. The
   loot had been rolled and saved correctly the whole time -- it was simply unreachable.
   Reads the latest encounter regardless of status now; `status` is on the payload for the client
   to decide what to show. */
function partyEncounterPublic(partyId) {
  const enc = partyLastEncounter(partyId);
  if (!enc || enc.status === "closed") return null;
  return {
    id: enc.id,
    area_level: enc.area_level,
    is_night: !!enc.is_night,
    monsters: partyEncMonsters(enc).map((m) => ({
      idx: m.idx, label: m.label, hp: Math.round(m.hp), max_hp: m.max_hp,
      dead: m.dead, target_account_id: m.target_account_id,
      is_guardian: m.is_guardian, is_roamer: m.is_roamer, monster_id: m.monster_id,
    })),
    loot: partyEncLoot(enc),
    log: partyEncLog(enc),
    status: enc.status,
  };
}
function partyBroadcastEncounter(partyId, opts) {
  const enc = partyEncounterPublic(partyId);
  partyBroadcast(partyId, {
    type: "party_encounter", encounter: enc, party: partyPublicState(partyId),
    // v0.30.1 (credit: Gwen, "not even gold was seen"): a kill credits experience, gold, quest
    // progress and kill streak to every member's row in the DATABASE. Each client is still
    // holding its own older copy, and its next autosave would write that stale copy straight
    // over the rewards. This flag tells every member to reload their character before that can
    // happen -- it is a correctness signal, not a cosmetic refresh.
    rewards_changed: !!(opts && opts.rewards),
  });
}


/* ---------------- resolving a party fight ----------------
   Three entry points, all landing in the same helpers so the rules exist once:
     - a player attacks              -> POST /api/party/encounter/attack
     - the server's own clock ticks  -> the party pusher at the bottom of this file
     - a player casts a spell        -> POST /api/party/encounter/cast (thin wrapper)
   Monster damage is settled from ELAPSED TIME rather than per-request, exactly as solo play
   does, so a player who stops clicking still takes hits and a player who clicks fast gains
   nothing. */

function partySettleMonsterHits(party, enc, monsters) {
  const now = Date.now();
  const lines = [];
  const state = partyPublicState(party.id);
  if (!state) return { lines, monsters, casualties: [] };
  const casualties = [];

  for (const mon of monsters) {
    if (mon.dead) continue;
    const intervalMs = 1000 / (mon.attack_speed || CB.MONSTER_BASE_ATTACK_SPEED);
    let owed = Math.floor((now - (mon.last_hit_at || now)) / intervalMs);
    if (owed <= 0) continue;
    // Cap the catch-up so a party whose socket dropped for a minute does not eat sixty swings in
    // one pass. Solo play makes the same allowance for the same reason.
    owed = Math.min(owed, 12);
    mon.last_hit_at = (mon.last_hit_at || now) + owed * intervalMs;

    for (let i = 0; i < owed; i++) {
      const targetId = mon.target_account_id;
      if (targetId == null) break;
      const member = partyMemberRow(party.id, targetId);
      if (!member) { mon.target_account_id = null; break; }
      const data = loadCharacterRow(member.account_id, member.slot);
      if (!data || (data.current_hp || 0) <= 0) { mon.target_account_id = null; break; }

      // Invulnerability, block and thorns all reuse the SOLO helpers rather than being
      // reimplemented, so a Touch of Unicorn shrine works identically in a party fight.
      if (combatIsInvulnerable(data)) {
        lines.push(`${member.character_name} shrugs off the ${mon.label} -- Touch of Unicorn holds.`);
        continue;
      }
      let dmg = cbRandRange(mon.dmg_min, mon.dmg_max);
      const blocked = Math.random() < combatGetBlockChance(data);
      if (blocked) {
        lines.push(`${member.character_name} blocks the ${mon.label}.`);
        continue;
      }
      // v0.20.4 mechanic: flat armour SUBTRACTION, not a percentage multiplier. Copied from the
      // solo monster-hit path rather than approximated, because armour behaving differently in a
      // party fight than in a solo one is exactly the drift this second system risks.
      dmg = Math.round(Math.max(0, dmg - combatGetArmor(data)));
      data.current_hp = Math.max(0, (data.current_hp || 0) - dmg);
      lines.push(`The ${mon.label} hits ${member.character_name} for ${dmg} damage.`);

      // Thorns. Rounded at the point of application, the same discipline solo play now enforces
      // in all three of its own thorns sites -- an unrounded reflect is what produced
      // "11.399999999999999 damage" in the log for four separate reports.
      const thorns = Math.round(combatGearBonus(data, "thorns"));
      if (thorns > 0 && !mon.dead) {
        const reflect = Math.round(Math.min(mon.hp, thorns));
        mon.hp = Math.max(0, mon.hp - reflect);
        lines.push(`${member.character_name}'s Thorns reflect ${reflect} damage back at the ${mon.label}.`);
        if (mon.hp <= 0) { mon.dead = true; lines.push(`The ${mon.label} lies defeated.`); }
      }

      if ((data.current_hp || 0) <= 0) {
        lines.push(`${member.character_name} has fallen.`);
        casualties.push({ account_id: member.account_id, slot: member.slot, name: member.character_name, hardcore: !!member.hardcore });
        mon.target_account_id = null;
        saveCharacterRow(member.account_id, member.slot, data);
        break;
      }
      saveCharacterRow(member.account_id, member.slot, data);
      if (mon.dead) break;
    }
  }
  return { lines, monsters, casualties };
}

/* A monster dying is where all the reward rules meet, so they live in one function.
   Gwen's spec, deliberately generous to encourage grouping:
     - EVERY member receives the FULL experience for the kill, not a share of it,
     - plus a party bonus of 5/10/15% for a party of 2/3/4,
     - every member gets personal quest credit for the kill,
     - gold splits evenly, remainder to the leader,
     - loot rolls at the COMBINED party magic find and lands in the shared pool. */
function partyResolveKill(party, enc, mon) {
  const lines = [];
  const members = partyMembers(party.id);
  const present = members.filter((m) => !m.in_town);
  const bonusPct = PARTY_XP_BONUS_PCT[Math.min(PARTY_MAX, present.length)] || 0;

  // Party kill streak: ONE counter for the party, worth a tenth of a solo streak per kill.
  db.prepare("UPDATE parties SET kill_streak = kill_streak + 1 WHERE id = ?").run(party.id);
  const mf = partyMagicFindBreakdown(party.id);

  const gold = cbRandIntRange(mon.gold_min, mon.gold_max);
  const share = Math.floor(gold / Math.max(1, present.length));
  const remainder = gold - share * Math.max(1, present.length);

  for (const m of members) {
    const data = loadCharacterRow(m.account_id, m.slot);
    if (!data) continue;
    // A member resting in town does not share this kill -- they were not there for it.
    if (m.in_town) continue;
    const xpBase = mon.xp * combatLevelDiffXpMult(data.level, enc.area_level);
    const xpTotal = xpBase * (1 + bonusPct / 100);
    combatAddXp(data, xpTotal);
    let goldFor = Math.round(share * combatGetGoldFindMult(data, { is_night: enc.is_night }));
    if (m.account_id === party.leader_account_id) goldFor += remainder;
    // Gold is account-bound, not character-bound -- credited through the same helper every other
    // gold award uses so the account row stays the single source of truth.
    creditAccountGold(m.account_id, goldFor);
    // Personal quest and kill-streak credit for a shared kill, per Gwen: one kill shared by four
    // players advances four personal Cleanse counters.
    data.total_kills = (data.total_kills || 0) + 1;
    data.kill_streak = (data.kill_streak || 0) + 1;
    if (data.kill_streak > (data.max_kill_streak || 0)) data.max_kill_streak = data.kill_streak;
    questEnsureState(data);
    questTrackTotalKills(data);
    saveCharacterRow(m.account_id, m.slot, data);
  }

  lines.push(`The ${mon.label} lies defeated. ${gold} gold, ${share} each${remainder ? ` (the odd ${remainder} to the leader)` : ""}.`);
  // v0.30.2 (credit: Gwen, "I don't get experience gained"): the experience WAS being awarded --
  // it was simply never said out loud, so a player watching the log had no evidence of it. The
  // figure every member actually receives now appears in the shared log beside the gold.
  const xpShown = Math.round(mon.xp * (1 + bonusPct / 100));
  lines.push(bonusPct > 0
    ? `Everyone gains ${xpShown} experience (${Math.round(mon.xp)} plus ${bonusPct}% for a party of ${present.length}).`
    : `Everyone gains ${xpShown} experience.`);

  // One loot roll per monster, into the shared pool, at the party's combined magic find.
  const drop = combatRollLoot(mon.loot_table, mf.total);
  if (drop && drop.type !== "nothing") {
    const entry = partyLootEntryFrom(drop, enc.area_level, mon.monster_id, mf.total);
    if (entry) {
      const loot = partyEncLoot(enc);
      entry.id = "l" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
      entry.claims = [];
      entry.resolved_to = null;
      entry.rolls = null;
      entry.claim_opened_at = null;
      loot.push(entry);
      partyEncounterSave(enc.id, { loot: JSON.stringify(loot) });
      lines.push(`${entry.display} drops into the party's pool.`);
    }
  }
  return lines;
}

function partyLootEntryFrom(drop, areaLevel, monsterBand, mfPct) {
  if (drop.type === "gear") {
    // Same generator solo loot uses, so a party drop is indistinguishable from a solo drop of the
    // same tier -- including its affix rolls and roll medals.
    const inst = combatGenerateGearItem(combatRollItemTier(areaLevel), mfPct, monsterBand);
    if (!inst) return null;
    return { kind: "gear", instance: inst, display: `${inst.rarity} item (Tier ${inst.tier})` };
  }
  if (drop.type === "consumable") return { kind: "consumable", item_id: drop.item_id, qty: 1, display: drop.item_id };
  if (drop.type === "herb") return { kind: "herb", herb_id: drop.herb_id, qty: 1, display: drop.herb_id };
  return null;
}

function partyAfterResolve(party, enc, monsters, lines, rewards) {
  partyAssignTargets(party.id, monsters);
  const allDead = monsters.every((m) => m.dead);
  const fields = { monsters: JSON.stringify(monsters), log: JSON.stringify(partyEncPush(enc, lines)) };
  if (allDead) fields.status = "cleared";
  partyEncounterSave(enc.id, fields);
  partyBroadcastEncounter(party.id, { rewards: !!rewards });
  return allDead;
}

/* ---------------- a player swings ---------------- */
app.post("/api/party/encounter/attack", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const enc = partyEncounterFor(party.id);
  if (!enc) return res.status(400).json({ error: "The party is not fighting." });
  const me = partyMemberRow(party.id, req.account.id);
  if (!me) return res.status(400).json({ error: "You are not in this party." });
  if (me.in_town) return res.status(400).json({ error: "You are in town." });
  const data = loadCharacterRow(req.account.id, me.slot);
  if (!data) return res.status(400).json({ error: "No character." });
  if ((data.current_hp || 0) <= 0) {
    // Belt and braces: the client is told to leave on death, but if it somehow asks anyway, say
    // so plainly rather than letting it sit there swinging at nothing.
    partyNotifyCasualty(party.id, req.account.id, !!me.hardcore);
    return res.status(400).json({ error: "You have fallen." });
  }

  // v0.30.1 BUG FIX (credit: Gwen, "I can spam the button faster than my 1sec attackspeed").
  // Solo combat gates the cadence CLIENT-side, which is fine when the client is only cheating
  // itself. In a party a fast clicker would out-damage everyone else, so the gate lives here on
  // the server where it cannot be clicked past. A too-soon swing is not an error -- the client
  // polls at 50ms while a button is held, exactly as solo does -- it simply does nothing.
  const nowMs = Date.now();
  const cadenceMs = 1000 / Math.max(0.01, combatGetAttackSpeed(data));
  if (me.last_attack_at && nowMs - me.last_attack_at < cadenceMs) {
    return res.json({ ok: true, too_soon: true, encounter: partyEncounterPublic(party.id) });
  }
  db.prepare("UPDATE party_members SET last_attack_at = ? WHERE party_id = ? AND account_id = ?")
    .run(nowMs, party.id, req.account.id);

  let monsters = partyEncMonsters(enc);
  const lines = [];
  let anyKill = false;

  // Target: the one the player clicked if it is alive, otherwise the first living monster. A
  // party that focuses fire kills faster, and that is a real tactical choice worth allowing.
  let idx = Number(req.body?.target_idx);
  if (!monsters[idx] || monsters[idx].dead) idx = monsters.findIndex((m) => !m.dead);
  if (idx < 0) return res.json({ ok: true, encounter: partyEncounterPublic(party.id), cleared: true });
  const mon = monsters[idx];

  // Solo deliberately does NOT roll a third random inside the damage range -- it collapses the
  // range to its average and lets crit and quad supply the variance. Matched here exactly; a
  // party swing must not be a different distribution from a solo swing.
  const [dLo, dHi] = combatGetDamageRange(data);
  let dmg = (dLo + dHi) / 2;
  const crit = Math.random() < combatGetCritChance(data);
  let critMult = 1;
  if (crit) { critMult = combatRollCritMultiplier(combatGetCritMultiplierMax(data)); dmg *= critMult; }
  if (combatHasQuadDamage(data)) dmg *= 4;
  dmg = Math.round(dmg);
  mon.hp = Math.max(0, mon.hp - dmg);
  lines.push(`${me.character_name} hits the ${mon.label} for ${dmg} damage.${crit ? ` (Critical! x${critMult.toFixed(2)})` : ""}`);

  const lifeOnHit = Math.round(combatGearBonus(data, "life_on_hit"));
  if (lifeOnHit > 0) {
    const before = data.current_hp || 0;
    data.current_hp = Math.min(combatGetMaxHp(data), before + lifeOnHit);
    if (data.current_hp > before) lines.push(`${me.character_name} draws ${Math.round(data.current_hp - before)} life from the blow.`);
  }
  // Weapon proficiency only advances when the equipped weapon is one this character may actually
  // use -- the same gate the solo attack route applies, so an invalid weapon cannot farm skill.
  {
    const wType = combatGetEquippedWeaponType(data);
    if (wType && data.equipped && data.equipped.weapon && combatCanEquipGear(data, data.equipped.weapon)) {
      combatRegisterWeaponHit(data, wType);
    }
  }
  saveCharacterRow(req.account.id, me.slot, data);

  if (mon.hp <= 0 && !mon.dead) {
    mon.dead = true; anyKill = true;
    lines.push(...partyResolveKill(party, partyLastEncounter(party.id), mon));
  }

  const settled = partySettleMonsterHits(party, partyLastEncounter(party.id), monsters);
  lines.push(...settled.lines);
  for (const m of monsters) {
    if (m.hp <= 0 && !m.dead) { m.dead = true; anyKill = true; lines.push(...partyResolveKill(party, partyLastEncounter(party.id), m)); }
  }
  partyHandleCasualties(party, settled.casualties);

  const cleared = partyAfterResolve(party, partyLastEncounter(party.id), monsters, lines, anyKill);
  // The attacker's own character is handed straight back when a kill landed, so the client that
  // swung adopts its rewards immediately rather than waiting for the broadcast round trip.
  res.json({
    ok: true, encounter: partyEncounterPublic(party.id), cleared, rewards_changed: anyKill,
    character: anyKill ? loadCharacterRow(req.account.id, me.slot) : null,
  });
});

/* Death. Softcore members respawn in town and can rejoin; hardcore members are gone for good and
   leave the party, which reopens it for matchmaking. Both reuse the solo death paths so the
   graveyard, the leaderboard and the character row are handled identically. */
/* v0.30.3 (credit: Gwen, "when I eventually died I should be returned to town, I am now stuck in
   the combat view with no way out"). A fallen member was marked in_town server-side and their
   roster entry dimmed, but nothing ever told THEIR client to leave the fight -- so they sat on a
   combat screen they could no longer act in. Each casualty is now sent a message addressed to
   them personally, which their client answers by doing exactly what a solo death does. */
function partyNotifyCasualty(partyId, accountId, hardcore) {
  const body = JSON.stringify({ party_id: partyId, seq: ++partySeq, type: "party_you_died", hardcore: !!hardcore });
  for (const client of chatClients) {
    if (client.readyState === client.OPEN && client.accountId === accountId) client.send(body);
  }
}
function partyHandleCasualties(party, casualties) {
  for (const c of casualties || []) {
    partyNotifyCasualty(party.id, c.account_id, c.hardcore);
    if (c.hardcore) {
      partySystem(party.id, `${c.name} has fallen for the last time.`);
      const hcData = loadCharacterRow(c.account_id, c.slot);
      const hcAcct = db.prepare("SELECT username FROM accounts WHERE id = ?").get(c.account_id);
      combatHandleHardcoreDeath(c.account_id, hcAcct ? hcAcct.username : "", c.slot, hcData, null);
      partyRemoveMember(party.id, c.account_id, "was lost to the forest");
    } else {
      partySystem(party.id, `${c.name} has fallen and will wake in town.`);
      db.prepare("UPDATE party_members SET in_town = 1 WHERE party_id = ? AND account_id = ?").run(party.id, c.account_id);
    }
  }
  if ((casualties || []).length) partyBroadcastState(party.id);
}

/* v0.30.1 (credit: Gwen): potions and spells in party fights. Both delegate to the SOLO helpers
   rather than reimplementing their effects, so a Greater Health Potion heals the same amount and
   a spell costs the same mana in both systems. */
app.post("/api/party/encounter/use-item", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const me = partyMemberRow(party.id, req.account.id);
  if (!me) return res.status(400).json({ error: "You are not in this party." });
  const data = loadCharacterRow(req.account.id, me.slot);
  if (!data) return res.status(400).json({ error: "No character." });
  const itemId = String(req.body?.item_id || "");
  const have = (data.consumables && data.consumables[itemId]) || 0;
  if (have <= 0) return res.status(400).json({ error: "You have none of those." });
  const item = COMBAT_CONSUMABLES[itemId];
  if (!item) return res.status(400).json({ error: "Unknown item." });
  data.consumables[itemId] = have - 1;
  if (data.consumables[itemId] <= 0) delete data.consumables[itemId];
  // The same queued-heal shape solo uses (combatQueueHeal), so a potion in a party fight restores
  // over time exactly as it does in a solo one rather than snapping to full instantly.
  combatSettleAllHeals(data);
  const ticks = CB.POTION_HEAL_TICKS || 1;
  if ((item.heal_amount || 0) > 0) {
    data.srv_heal = combatQueueHeal(data.srv_heal, item.heal_amount, combatGetMaxHp(data) - (data.current_hp || 0), ticks);
  }
  if ((item.stamina_amount || 0) > 0) {
    data.srv_stamina_heal = combatQueueHeal(data.srv_stamina_heal, item.stamina_amount, combatGetMaxStamina(data) - (data.current_stamina || 0), ticks);
  }
  if ((item.mana_amount || 0) > 0) {
    data.srv_mana_heal = combatQueueHeal(data.srv_mana_heal, item.mana_amount, combatGetMaxMana(data) - (data.current_mana || 0), ticks);
  }
  if (itemId === "antidote") { data.player_poison_dots = []; }
  saveCharacterRow(req.account.id, me.slot, data);
  const enc = partyLastEncounter(party.id);
  if (enc) {
    partyEncounterSave(enc.id, { log: JSON.stringify(partyEncPush(enc, [`${me.character_name} uses a ${itemId.replace(/_/g, " ")}.`])) });
  }
  partyBroadcastEncounter(party.id, { rewards: true });
  res.json({ ok: true, character: data });
});

app.post("/api/party/encounter/cast", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const enc = partyEncounterFor(party.id);
  if (!enc) return res.status(400).json({ error: "The party is not fighting." });
  const me = partyMemberRow(party.id, req.account.id);
  if (!me || me.in_town) return res.status(400).json({ error: "You are not in this fight." });
  const data = loadCharacterRow(req.account.id, me.slot);
  if (!data || (data.current_hp || 0) <= 0) return res.status(400).json({ error: "You have fallen." });

  const spellId = String(req.body?.spell_id || "");
  const tier = (data.owned_spell_tiers || {})[spellId] || 0;
  if (!tier) return res.status(400).json({ error: "You do not know that spell." });
  const spell = spellAtTier(spellId, tier);
  if (!spell) return res.status(400).json({ error: "Unknown spell." });

  // Cooldown and mana, checked exactly as solo checks them, with the reduced cooldown.
  let cds = {};
  try { cds = JSON.parse(me.spell_cooldowns || "{}"); } catch (e) { cds = {}; }
  const now = Date.now();
  if (cds[spellId] && now < cds[spellId]) {
    return res.status(400).json({ error: "That spell is still on cooldown.", ready_at: cds[spellId] });
  }
  const currentMana = combatGetCurrentMana(data);
  if (currentMana < spell.mana_cost) return res.status(400).json({ error: "Not enough mana." });
  data.current_mana = currentMana - spell.mana_cost;
  cds[spellId] = now + combatGetSpellCooldownMs(data, spell.cooldown_ms);
  db.prepare("UPDATE party_members SET spell_cooldowns = ? WHERE party_id = ? AND account_id = ?")
    .run(JSON.stringify(cds), party.id, req.account.id);

  const mult = combatGetSpellEffectivenessMult(data) * (combatHasQuadDamage(data) ? 4 : 1);
  const monsters = partyEncMonsters(enc);
  const lines = [];
  let anyKill = false;

  if (spell.effect === "heal") {
    const amount = Math.round(spell.heal_amount * combatGetSpellEffectivenessMult(data));
    data.current_hp = Math.min(combatGetMaxHp(data), (data.current_hp || 0) + amount);
    lines.push(`${me.character_name} casts ${(SPELLS[spellId] && SPELLS[spellId].name) || spellId}, recovering ${amount} health.`);
  } else {
    let idx = Number(req.body?.target_idx);
    if (!monsters[idx] || monsters[idx].dead) idx = monsters.findIndex((m) => !m.dead);
    if (idx >= 0) {
      const mon = monsters[idx];
      const min = spell.hit_min != null ? spell.hit_min : spell.dmg_min;
      const max = spell.hit_max != null ? spell.hit_max : spell.dmg_max;
      const hits = (SPELLS[spellId] && SPELLS[spellId].hit_count) || 1;
      const dmg = Math.round(((min + max) / 2) * hits * mult);
      mon.hp = Math.max(0, mon.hp - dmg);
      lines.push(`${me.character_name} casts ${(SPELLS[spellId] && SPELLS[spellId].name) || spellId} at the ${mon.label} for ${dmg} damage.`);
      if (mon.hp <= 0 && !mon.dead) { mon.dead = true; anyKill = true; lines.push(...partyResolveKill(party, partyLastEncounter(party.id), mon)); }
    }
  }
  combatRegisterSpellcastHit(data);
  saveCharacterRow(req.account.id, me.slot, data);
  const cleared = partyAfterResolve(party, partyLastEncounter(party.id), monsters, lines, anyKill);
  res.json({ ok: true, encounter: partyEncounterPublic(party.id), cleared, character: data });
});

/* ---------------- loot: claim, contest, roll ----------------
   A claim opens a three-second window rather than resolving instantly, so somebody who wanted the
   same item has a real chance to say so. One claim in the window and it is theirs quietly; two or
   more and everyone contesting rolls 1-100, highest takes it, and the result goes to party chat
   so nobody has to take a stranger's word for how it went. */
app.post("/api/party/loot/claim", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  const enc = partyEncounterFor(party.id) || partyLastEncounter(party.id);
  if (!enc) return res.status(400).json({ error: "There is nothing to claim." });
  const loot = partyEncLoot(enc);
  const entry = loot.find((l) => l.id === req.body?.loot_id);
  if (!entry) return res.status(404).json({ error: "That item is gone." });
  if (entry.resolved_to) return res.status(400).json({ error: "Already claimed." });
  const me = partyMemberRow(party.id, req.account.id);
  if (!me) return res.status(400).json({ error: "You are not in this party." });
  if (entry.claims.some((c) => c.account_id === req.account.id)) return res.json({ ok: true, already: true });

  entry.claims.push({ account_id: req.account.id, name: me.character_name });
  if (!entry.claim_opened_at) entry.claim_opened_at = Date.now();
  partyEncounterSave(enc.id, { loot: JSON.stringify(loot) });
  partySystem(party.id, `${me.character_name} claimed ${entry.display}.`);
  partyBroadcastEncounter(party.id);
  res.json({ ok: true });
});

function partyLastEncounter(partyId) {
  return db.prepare("SELECT * FROM party_encounters WHERE party_id = ? ORDER BY created_at DESC LIMIT 1").get(partyId) || null;
}

/* Called from the party pusher, not from a request: the window has to close on the server's own
   clock or a party where nobody sends another request would leave the item pending forever. */
function partySettleLootWindows(party) {
  const enc = partyLastEncounter(party.id);
  if (!enc) return;
  const loot = partyEncLoot(enc);
  let changed = false;
  const now = Date.now();
  for (const entry of loot) {
    if (entry.resolved_to || !entry.claim_opened_at) continue;
    if (now - entry.claim_opened_at < PARTY_LOOT_CLAIM_WINDOW_MS) continue;
    if (entry.claims.length === 1) {
      entry.resolved_to = entry.claims[0].account_id;
      partySystem(party.id, `${entry.claims[0].name} takes ${entry.display}.`);
    } else {
      const rolls = entry.claims.map((c) => ({ account_id: c.account_id, name: c.name, roll: 1 + Math.floor(Math.random() * 100) }));
      rolls.sort((a, b) => b.roll - a.roll);
      entry.rolls = rolls;
      entry.resolved_to = rolls[0].account_id;
      partySystem(party.id, rolls.map((r) => `${r.name} rolled ${r.roll}`).join(". ") + `. ${rolls[0].name} takes ${entry.display}.`);
    }
    partyGrantLoot(party, entry);
    changed = true;
  }
  if (changed) {
    partyEncounterSave(enc.id, { loot: JSON.stringify(loot) });
    partyBroadcastEncounter(party.id);
  }
}

function partyGrantLoot(party, entry) {
  const m = partyMemberRow(party.id, entry.resolved_to);
  if (!m) return;
  const data = loadCharacterRow(m.account_id, m.slot);
  if (!data) return;
  if (entry.kind === "gear") {
    data.gear_instances = data.gear_instances || [];
    data.gear_instances.push(entry.instance);
  } else if (entry.kind === "consumable") {
    data.consumables = data.consumables || {};
    data.consumables[entry.item_id] = (data.consumables[entry.item_id] || 0) + (entry.qty || 1);
  } else if (entry.kind === "herb") {
    data.herbs = data.herbs || {};
    data.herbs[entry.herb_id] = (data.herbs[entry.herb_id] || 0) + (entry.qty || 1);
  }
  saveCharacterRow(m.account_id, m.slot, data);
}

/* Leaving the maze with items still in the pool. Gwen's rule: unclaimed loot is littered, and
   littering the forest costs every member a point of Forest Reputation. It gives ignoring the
   pool a real cost, which is a better answer than a timer nobody reads. */
/* v0.30.2 (credit: Gwen): only the LEADER may move the party on from a finished fight, exactly
   as only the leader walks. Everyone else stays on the corpses until they do, which is what gives
   the party time to argue over the loot. */
app.post("/api/party/encounter/leave", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  if (party.leader_account_id !== req.account.id) {
    return res.status(403).json({ error: "Only the leader decides when to move on." });
  }
  const enc = partyLastEncounter(party.id);
  if (!enc) return res.json({ ok: true });
  const loot = partyEncLoot(enc);
  const abandoned = loot.filter((l) => !l.resolved_to);
  if (abandoned.length) {
    for (const m of partyMembers(party.id)) {
      const data = loadCharacterRow(m.account_id, m.slot);
      if (!data) continue;
      // forest_reputation is a per-area-tier map on the character; littering costs a point in the
      // tier the party is actually delving, not a global figure.
      // Reputation is tracked per area tier, keyed the same way combatGetForestReputationXpPct()
      // reads it: by the character's own highest tier reached.
      const repTier = data.highest_tier_reached || 1;
      data.forest_reputation = data.forest_reputation || {};
      data.forest_reputation[repTier] = (data.forest_reputation[repTier] || 0) - 1;
      saveCharacterRow(m.account_id, m.slot, data);
    }
    partySystem(party.id, `${abandoned.length} item${abandoned.length > 1 ? "s were" : " was"} left to rot. Forest Reputation -1 for everyone.`);
  }
  partyEncounterSave(enc.id, { status: "closed" });
  partyBroadcastEncounter(party.id);
  partyBroadcastState(party.id);
  res.json({ ok: true, littered: abandoned.length });
});


/* ---------------- maze relay ----------------
   The leader's client generates the maze exactly as it does in solo play and posts the layout
   here; the server stores it verbatim and hands it to followers. The server does not read,
   validate or generate it. See the authority note at the top of the party core for why this is
   acceptable today and what makes it the next thing to fix. */
app.post("/api/party/maze", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  if (party.leader_account_id !== req.account.id) return res.status(403).json({ error: "Only the leader shapes the maze." });
  const maze = req.body?.maze;
  if (maze == null) return res.status(400).json({ error: "No maze." });
  const json = JSON.stringify(maze);
  // A 20x10 maze relays at roughly 5 KB; anything an order of magnitude past that is not a maze.
  if (json.length > 200000) return res.status(400).json({ error: "Maze too large." });
  partyTouch(party.id, {
    maze_json: json,
    area_level: Math.max(1, Number(req.body?.area_level) || party.area_level),
    is_night: req.body?.is_night ? 1 : 0,
  });
  partyBroadcast(party.id, { type: "party_maze", maze, area_level: Number(req.body?.area_level) || party.area_level, is_night: !!req.body?.is_night });
  res.json({ ok: true });
});

/* Each step the leader takes. Costs stamina from EVERY member still in the maze, which is the
   real price of grouping -- a party covers less ground than a solo player before turning back.
   The gate is checked before the step is accepted, so the party genuinely cannot advance past
   its most tired member. */
app.post("/api/party/step", requireAuth, (req, res) => {
  const party = partyForAccount(req.account.id);
  if (!party) return res.status(400).json({ error: "You are not in a party." });
  if (party.leader_account_id !== req.account.id) return res.status(403).json({ error: "Only the leader moves the party." });

  const before = partyPublicState(party.id);
  if (before.stamina_gate.blocked) {
    return res.status(400).json({
      error: `${before.stamina_gate.who || "Someone"} is out of stamina.`,
      gate: before.stamina_gate,
    });
  }
  // The client sends the step cost it already computed for solo movement (gear, class and
  // trample state all feed it), which keeps one formula rather than a second server copy that
  // would drift. Clamped so a bad value cannot be free or absurd.
  const cost = cbClampf(Number(req.body?.cost) || 1, 0, 25);
  for (const m of partyMembers(party.id)) {
    if (m.in_town) continue;
    const data = loadCharacterRow(m.account_id, m.slot);
    if (!data) continue;
    data.current_stamina = Math.max(0, (data.current_stamina || 0) - cost);
    saveCharacterRow(m.account_id, m.slot, data);
  }
  partyBroadcast(party.id, { type: "party_step", pos: req.body?.pos, reveal: req.body?.reveal || [], roamers: req.body?.roamers || [] });
  const after = partyPublicState(party.id);
  if (after.stamina_gate.blocked) {
    partySystem(party.id, `${after.stamina_gate.who} is out of stamina. The party cannot move further.`);
  }
  partyBroadcastState(party.id);
  res.json({ ok: true, gate: after.stamina_gate });
});

/* ---------------- party pusher ----------------
   Runs faster than the solo pusher (200ms vs 500ms) for one reason only: with four players and
   up to four monsters, half a second of accumulated events arrives as an unreadable clump. It is
   still a CATCH-UP pass, not a simulation -- it settles damage owed by elapsed time and does
   nothing at all when a party is idle or not fighting. */
/* v0.30.2 BUG FIX (credit: Gwen, "he healed up in town and still shows 0hp"). Party state was
   only ever broadcast when something HAPPENED -- a join, a step, a kill. A member sitting in town
   regenerating changes their HP without any of those firing, so every other client kept showing
   the corpse-shaped roster entry they last saw. The pusher now watches the party's vitals and
   broadcasts when they actually change, throttled to once a second: enough to feel live, and
   quiet enough that an idle party costs nothing beyond the comparison itself. */
const partyVitalsSignature = new Map();
const partyVitalsLastSent = new Map();
const PARTY_VITALS_MIN_INTERVAL_MS = 1000;
/* NOTE, deliberately NOT done here (v0.30.2): server-side regeneration for members resting in
   town. The obvious-looking fix, and the wrong one -- a player in town is running their own
   client, which already regenerates locally and saves the result. Ticking regen here as well
   would heal them twice as fast as a solo player, silently. What was actually missing was nobody
   telling the OTHER clients that their vitals had changed, which is what the broadcast below
   does: it reads each member's row fresh, so a bonfire, a potion or ordinary regeneration all
   show up the moment they are saved, without this file duplicating any of them. */
function partyMaybeBroadcastVitals(party) {
  const state = partyPublicState(party.id);
  if (!state) return;
  const sig = state.members.map((m) =>
    `${m.account_id}:${m.hp}:${m.mana}:${m.stamina}:${m.in_town ? 1 : 0}:${m.row_pos}:${m.dead ? 1 : 0}`).join("|");
  if (partyVitalsSignature.get(party.id) === sig) return;
  const last = partyVitalsLastSent.get(party.id) || 0;
  if (Date.now() - last < PARTY_VITALS_MIN_INTERVAL_MS) return;
  partyVitalsSignature.set(party.id, sig);
  partyVitalsLastSent.set(party.id, Date.now());
  partyBroadcast(party.id, { type: "party_state", state });
}

setInterval(() => {
  const parties = db.prepare("SELECT * FROM parties").all();
  for (const party of parties) {
    try {
      partySettleLootWindows(party);
      partyMaybeBroadcastVitals(party);
      const enc = partyEncounterFor(party.id);
      if (!enc) continue;
      let monsters = partyEncMonsters(enc);
      if (!monsters.length || monsters.every((m) => m.dead)) continue;
      const settled = partySettleMonsterHits(party, enc, monsters);
      const lines = settled.lines;
      let anyKill = false;
      for (const m of monsters) {
        if (m.hp <= 0 && !m.dead) { m.dead = true; anyKill = true; lines.push(...partyResolveKill(party, partyLastEncounter(party.id), m)); }
      }
      partyHandleCasualties(party, settled.casualties);
      if (lines.length) partyAfterResolve(party, partyLastEncounter(party.id), monsters, lines, anyKill);
    } catch (e) {
      // One broken party must never take the pusher down for every other party.
      console.error("party pusher:", party.id, e && e.message);
    }
  }
}, PARTY_PUSH_INTERVAL_MS);

/* ---------------- chat over WebSocket ---------------- */

const wss = new WebSocketServer({ server, path: "/ws/chat" });
const chatClients = new Set();

function loadRecentChat() {
  return db
    .prepare("SELECT username, message, created_at, kind FROM chat_messages ORDER BY id DESC LIMIT ?")
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
  // v0.24.2: hand the freshly connected client the current known-issues banner immediately,
  // so it renders on the very first paint instead of only after the next admin edit.
  ws.send(JSON.stringify(Object.assign({ type: "announcement" }, getAdminAnnouncement())));

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
    // v0.30 (Gwen): three chat scopes on one socket. Global is unchanged and still persisted;
    // party and lobby are live-only and never written to chat_messages, because a party is
    // temporary and a player who rejoins one later should not be able to read a scrollback from
    // a party they were not in at the time.
    if (parsed.type === "party_chat" && typeof parsed.message === "string") {
      const party = partyForAccount(ws.accountId);
      if (!party) return;
      const msg = parsed.message.slice(0, 300).trim();
      if (!msg) return;
      const me = partyMemberRow(party.id, ws.accountId);
      partyBroadcast(party.id, {
        type: "party_chat", system: false,
        username: me ? me.character_name : ws.username, message: msg, created_at: nowIso(),
      });
      return;
    }
    if (parsed.type === "lobby_chat" && typeof parsed.message === "string") {
      const msg = parsed.message.slice(0, 300).trim();
      if (!msg) return;
      const body = JSON.stringify({ type: "lobby_chat", username: ws.username, message: msg, created_at: nowIso() });
      // Only players who have opted in receive it -- the whole point of a separate channel is
      // that someone mid-delve is not recruited at.
      for (const client of chatClients) {
        if (client.readyState === client.OPEN && client.inLobby) client.send(body);
      }
      return;
    }
    if (parsed.type === "lobby_presence") { ws.inLobby = !!parsed.active; return; }
    if (parsed.type !== "chat" || typeof parsed.message !== "string") return;
    const message = parsed.message.slice(0, 300).trim();
    if (!message) return;
    const created_at = nowIso();
    db.prepare("INSERT INTO chat_messages (username, message, created_at, kind) VALUES (?, ?, ?, 'chat')").run(
      ws.username,
      message,
      created_at
    );
    const payload = JSON.stringify({ type: "chat", username: ws.username, message, created_at, kind: "chat" });
    for (const client of chatClients) {
      if (client.readyState === client.OPEN) client.send(payload);
    }
  });

  ws.on("close", () => {
    chatClients.delete(ws);
    // v0.30 (Gwen): losing your connection takes you out of the party. The roster icon
    // disappears, the party drops below the cap and reopens for matchmaking, and leadership
    // passes by join order if it was yours -- all handled inside partyRemoveMember().
    try {
      const party = partyForAccount(ws.accountId);
      if (party) partyRemoveMember(party.id, ws.accountId, "disconnected");
    } catch (e) { /* a failed cleanup must not take the socket handler down */ }
  });
});

// v0.21.2: server-driven continuous combat. Previously the ONLY thing that ever resolved "owed"
// elapsed-time monster hits (see combatCatchUpMonsterHits() above) was the CLIENT asking -- either
// a real attack/flee/use-item, or its own 2-second idle poll to /api/combat/:id/tick. That's fine
// while a tab is open and focused, but a plain JS setInterval in a backgrounded/minimized browser
// tab can be throttled (or on some platforms fully suspended), so a player who steps away mid-
// fight might not actually take any damage until they come back and the client happens to poll
// again -- not truly "enforced". Gwen asked for the mob's own attack clock to be genuinely
// unavoidable ("10 seconds of loss of attention could actually get you slain"), so this pushes the
// SAME catch-up resolution over each player's own already-open chat WebSocket, on the SERVER's own
// clock, independent of whether the client is polling at all. The existing HTTP poll/on-action
// catch-up path is left completely in place as a fallback for any account without a live socket
// (e.g. mid-reconnect) -- this is purely additive, and never resolves fewer hits than before.
const COMBAT_PUSH_INTERVAL_MS = 500;
setInterval(() => {
  for (const client of chatClients) {
    if (client.readyState !== client.OPEN || client.accountId == null) continue;
    const session = getActiveCombatSessionForAccount(client.accountId);
    if (!session) continue;
    const data = loadCharacterRow(client.accountId, session.slot);
    if (!data) continue;
    combatSettleAllHeals(data);
    combatTickRegenElapsed(data, session); // v0.24.1 (C3): HP/Stamina/Mana regen ticks during combat too
  { const _poison = combatSettlePlayerPoison(session, data); if (_poison.dotsChanged) session.player_poison_dots = JSON.stringify(_poison.newDots || []); } // v0.24.1 (C4): settle any queued Poison Sting DOT ticks against the player
    // v0.23.0 (Part B7): settle queued Fireflies DOT ticks on the server's own clock too, so
    // they land even if the player never sends another attack/tick request while idle.
    const spellDotResult = combatSettleSpellDots(session, data);
    if (spellDotResult.dotsChanged) session.hp = spellDotResult.newHp;
    const invulnActiveThisRound = combatIsInvulnerable(data);
    const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
    if (catchUp.ticks.length === 0 && !catchUp.fatal && spellDotResult.ticks.length === 0) continue;
    if (spellDotResult.dotsChanged) updateCombatSession(session.id, { hp: spellDotResult.newHp, spell_dots: JSON.stringify(spellDotResult.newDots || []), player_dealt_direct_damage: session.player_dealt_direct_damage ? 1 : 0 });

    // v0.24.1 (C1/C2/B5): this pusher is a genuine kill call site now too -- a settled spell DOT
    // (C1) or a Thorns reflect landed during the catch-up above (C2) can finish the monster off
    // purely from server-clock elapsed time, with no HTTP request involved at all. Resolve the
    // kill through the exact same combatFinalizeMonsterKill() helper every other endpoint uses,
    // BEFORE the old fatal-only branch below (a kill and a player death can never both be true of
    // the same catch-up, since combatCatchUpMonsterHits() stops resolving further hits once the
    // player is dead -- but the monster can still die to Thorns/DOT in this same pass).
    if (!catchUp.fatal && session.hp <= 0) {
      const fakeReq = { account: { id: client.accountId, username: client.username } };
      const kill = combatFinalizeMonsterKill(fakeReq, session, data, { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
      const saveSeq = saveCharacterRow(client.accountId, session.slot, data);
      client.send(JSON.stringify({
        type: "combat_tick", session_id: session.id, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks, fatal: false,
        monster: { hp: 0, max_hp: session.max_hp, defeated: true }, kill,
        player: {
          current_hp: cbInt(data.current_hp), max_hp: combatGetMaxHp(data),
          current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data),
          current_mana: cbInt(combatGetCurrentMana(data)), max_mana: combatGetMaxMana(data),
        },
        hardcore_killed: false,
        _save_seq: saveSeq,
      }));
      continue;
    }

    updateCombatSession(session.id, catchUp.fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 } : { last_monster_hit_at: catchUp.newLastHitAt, last_regen_tick_at: session.last_regen_tick_at, player_poison_dots: session.player_poison_dots || "[]", sting_lockout_until: session.sting_lockout_until || 0 });
    // HOTFIX: this server-driven push is a real fatal-hit call site too (Gwen's exact repro:
    // a hardcore character was slain by an elapsed-time catch-up hit landed by THIS pusher
    // while the tab was backgrounded, then kept playing once they returned) -- route it
    // through the same atomic permadeath choke point as every HTTP combat route.
    const hcResult = catchUp.fatal ? combatHandleHardcoreDeath(client.accountId, client.username, session.slot, data, session) : { hardcoreKilled: false };
    const saveSeq = hcResult.hardcoreKilled ? null : saveCharacterRow(client.accountId, session.slot, data);
    client.send(JSON.stringify({
      type: "combat_tick", session_id: session.id, monster_ticks: catchUp.ticks, spell_dot_ticks: spellDotResult.ticks, fatal: catchUp.fatal,
      monster: spellDotResult.dotsChanged ? { hp: spellDotResult.newHp } : undefined,
      player: {
        current_hp: catchUp.fatal ? 0 : cbInt(data.current_hp), max_hp: combatGetMaxHp(data),
        current_stamina: cbInt(data.current_stamina), max_stamina: combatGetMaxStamina(data),
        // v0.23.0 (Part B2): Mana rides along on the WS-pushed combat tick too, so idle-in-combat
        // Mana regen/heal-over-time settlement (already computed above by combatSettleAllHeals())
        // is visible without waiting on a player-initiated attack/flee/use-item call.
        current_mana: cbInt(combatGetCurrentMana(data)), max_mana: combatGetMaxMana(data),
      },
      hardcore_killed: hcResult.hardcoreKilled,
      _save_seq: saveSeq,
    }));
  }
}, COMBAT_PUSH_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`Wandrian server listening on port ${PORT} (db: ${DB_PATH})`);
});
