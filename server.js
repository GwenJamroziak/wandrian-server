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
`);

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
  // v0.18 (#18): guard column for the one-time stale-bridge-steps cleanup below -- see
  // migrateStaleBridgeSteps() for what it's cleaning up and why a blanket reset (rather than
  // trying to guess which rows are actually corrupt) is the safe choice here.
  "ALTER TABLE leaderboard_bests ADD COLUMN bridge_steps_migrated_v18 INTEGER NOT NULL DEFAULT 0",
]) {
  try { db.exec(stmt); } catch (e) { /* column already exists, fine */ }
}

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
// 100-199, 3g for 200-299, and continue like that", i.e. +1g of fee per 100g bracket the
// asking price falls into. Charged server-side (not just checked/deducted on the client)
// so a modified client can't post free listings -- see its use in POST /api/auction below.
function auctionListingFee(price) {
  return Math.floor(price / 100) + 1;
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
    bySlot[row.slot] = { data, updated_at: row.updated_at };
  }
  const slots = [];
  for (let i = 0; i < MAX_CHARACTER_SLOTS; i++) slots.push(bySlot[i] ? { slot: i, ...bySlot[i] } : { slot: i, empty: true });
  res.json({ slots, account_gold: accountGold });
});

app.put("/api/characters/:slot", requireAuth, (req, res) => {
  const slot = Number(req.params.slot);
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) {
    return res.status(400).json({ error: "Invalid slot." });
  }
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid character data." });
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

  // v0.17 (#29): gold is account-bound now -- mirror whatever this (accepted, non-stale)
  // save reports as the account's shared total. Same last-write-wins trust model this
  // route already uses for level/xp/tier/etc. (see the top-of-file note on server-side
  // validation being future work, tracked separately), just repointed at a shared column
  // instead of a per-character one. Known limitation: two DIFFERENT character slots on the
  // same account, actively played in two different tabs/sessions at once, could still race
  // each other here (there's no per-account sequence number the way _save_seq guards a
  // single slot) -- an accepted, low-likelihood gap for now, not a silent-loss guarantee
  // like the bug this feature fixes (a sale crediting a deleted hardcore character's row).
  if (typeof data.gold === "number" && Number.isFinite(data.gold)) {
    setAccountGold(req.account.id, data.gold);
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
  if (data.character_name && data.class_display_name) {
    // v0.17.3 (#13): weapon_skills/herbalism_points ride along on the same upsert as
    // everything else here, stored as excluded.value (current snapshot, not MAX()'d) --
    // see the ALTER TABLE comment above for why that's correct.
    const weaponSkillsJson = JSON.stringify(data.weapon_skills || {});
    db.prepare(
      `INSERT INTO leaderboard_bests (account_id, character_name, class_name, level, highest_tier_reached, gold, updated_at, hardcore, lifetime_xp, is_dead, last_bridge_steps, xp, weapon_skills, herbalism_points)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
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
         herbalism_points=excluded.herbalism_points`
    ).run(
      req.account.id,
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
      data.herbalism_points || 0
    );
  }
  res.json({ ok: true, account_gold: getAccountGold(req.account.id) });
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

  const existing = db.prepare("SELECT version FROM vaults WHERE account_id = ?").get(req.account.id);
  const currentVersion = existing ? existing.version || 0 : 0;
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
       FROM leaderboard_bests ORDER BY highest_tier_reached DESC, last_bridge_steps DESC, level DESC, xp DESC LIMIT 50`
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
      `SELECT account_id, character_name, class_name, level, weapon_skills, herbalism_points, is_dead
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
    .prepare("SELECT highest_tier_reached, hardcore, is_dead FROM leaderboard_bests WHERE account_id = ? AND character_name = ?")
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
    attributes: data.attributes || { str: 0, dex: 0, vit: 0, int: 0 },
    equipped: data.equipped || {},
    weapon_skills: data.weapon_skills || {},
    herbalism_points: data.herbalism_points || 0,
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

app.post("/api/auction", requireAuth, (req, res) => {
  const { type, price, character_name, item, item_key, quantity, display_name, seller_slot } = req.body || {};
  if (!["gear", "consumable", "herb"].includes(type)) return res.status(400).json({ error: "Invalid item type." });
  const numPrice = Number(price);
  if (!Number.isFinite(numPrice) || numPrice < 1) return res.status(400).json({ error: "Invalid price." });
  if (!character_name || !display_name) return res.status(400).json({ error: "Missing listing details." });
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
  const charRow = db.prepare("SELECT 1 FROM characters WHERE account_id = ? AND slot = ?").get(req.account.id, slot);
  if (!charRow) return res.status(400).json({ error: "Character not found." });
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
  res.json({ ok: true, refund: { type: listing.type, item_key: listing.item_key, item: listing.item_json ? JSON.parse(listing.item_json) : null, quantity: listing.quantity } });
});

app.post("/api/auction/:id/buy", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const { character_name } = req.body || {};
  const listing = db.prepare("SELECT * FROM auction_listings WHERE id = ?").get(id);
  if (!listing) return res.status(404).json({ error: "That listing is no longer available." });
  if (listing.seller_account_id === req.account.id) return res.status(400).json({ error: "You can't buy your own listing." });

  // v0.17 (#29): gold is account-bound now -- a single, authoritative check against the
  // buyer's own account gold, replacing the old best-effort scan across their character
  // rows (which was really just working around gold being scattered per-character in the
  // first place; that whole problem no longer exists).
  if (getAccountGold(req.account.id) < listing.price) return res.status(400).json({ error: "Not enough gold." });

  // Remove the listing first (best-effort race protection against double-buy).
  const del = db.prepare("DELETE FROM auction_listings WHERE id = ?").run(id);
  if (del.changes === 0) return res.status(409).json({ error: "Someone already bought that." });

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
