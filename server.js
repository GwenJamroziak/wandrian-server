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
  // v0.18.2 (#8): max_kill_streak is a genuine lifetime record (only ever grows for a
  // given character, exactly like highest_tier_reached/lifetime_xp above) -- MAX()'d
  // against the existing row rather than overwritten with excluded.value, so an
  // out-of-order/retried save can never stomp a higher previously-recorded streak back
  // down to a smaller one.
  db.prepare(
    `INSERT INTO leaderboard_bests (account_id, character_name, class_name, level, highest_tier_reached, gold, updated_at, hardcore, lifetime_xp, is_dead, last_bridge_steps, xp, weapon_skills, herbalism_points, max_kill_streak, total_kills)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
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
       total_kills=MAX(leaderboard_bests.total_kills, excluded.total_kills)`
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
    data.total_kills || 0
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
const TRIAL_CLASSES = {
  wizard:      { tier: 1, next_class: "sorcerer",    display_name: "Wizard",      base_hp: 60,  hp_per_level: 8,  base_str: 2,  base_dex: 6,  base_vit: 16, base_int: 16 },
  thornguard:  { tier: 1, next_class: "stonewarden", display_name: "Thornguard",  base_hp: 100, hp_per_level: 14, base_str: 16, base_dex: 6,  base_vit: 16, base_int: 2 },
  windrider:   { tier: 1, next_class: "galestrider", display_name: "Windrider",   base_hp: 75,  hp_per_level: 10, base_str: 4,  base_dex: 16, base_vit: 16, base_int: 4 },
  sorcerer:    { tier: 2, next_class: "warlock",     display_name: "Sorcerer",    base_hp: 80,  hp_per_level: 10, base_str: 3,  base_dex: 8,  base_vit: 20, base_int: 21 },
  stonewarden: { tier: 2, next_class: "treesinger",  display_name: "Stonewarden", base_hp: 140, hp_per_level: 18, base_str: 20, base_dex: 8,  base_vit: 21, base_int: 3 },
  galestrider: { tier: 2, next_class: "shadowbloom", display_name: "Galestrider", base_hp: 100, hp_per_level: 13, base_str: 5,  base_dex: 21, base_vit: 21, base_int: 5 },
  warlock:     { tier: 3, next_class: "necromancer", display_name: "Warlock",     base_hp: 105, hp_per_level: 13, base_str: 3,  base_dex: 10, base_vit: 27, base_int: 26 },
  treesinger:  { tier: 3, next_class: "rootbinder",  display_name: "Treesinger",  base_hp: 160, hp_per_level: 20, base_str: 27, base_dex: 10, base_vit: 26, base_int: 3 },
  shadowbloom: { tier: 3, next_class: "druid",       display_name: "Shadowbloom", base_hp: 125, hp_per_level: 16, base_str: 7,  base_dex: 26, base_vit: 26, base_int: 7 },
  necromancer: { tier: 4, next_class: "archmage",    display_name: "Summoner",    base_hp: 135, hp_per_level: 17, base_str: 4,  base_dex: 12, base_vit: 33, base_int: 33 },
  rootbinder:  { tier: 4, next_class: "emberpriest", display_name: "Rootbinder",  base_hp: 210, hp_per_level: 26, base_str: 33, base_dex: 12, base_vit: 33, base_int: 4 },
  druid:       { tier: 4, next_class: "galeshaper",  display_name: "Druid",       base_hp: 160, hp_per_level: 20, base_str: 8,  base_dex: 33, base_vit: 33, base_int: 8 },
  emberpriest: { tier: 5, next_class: null,          display_name: "Emberpriest", base_hp: 190, hp_per_level: 24, base_str: 40, base_dex: 15, base_vit: 40, base_int: 5 },
  archmage:    { tier: 5, next_class: null,          display_name: "Archmage",    base_hp: 165, hp_per_level: 21, base_str: 5,  base_dex: 15, base_vit: 40, base_int: 40 },
  galeshaper:  { tier: 5, next_class: null,          display_name: "Galeshaper",  base_hp: 195, hp_per_level: 25, base_str: 10, base_dex: 40, base_vit: 40, base_int: 10 },
};

// Balance-equivalent constants/formulas, ported verbatim from index.html's Balance
// object -- see that file's own comments (search "BRIDGE_ROWS", "STARTING_STAT_POINTS")
// for the full rationale behind these exact numbers.
const TRIAL_BRIDGE_ROWS = 10, TRIAL_BRIDGE_MIN_PLANKS = 2, TRIAL_BRIDGE_MAX_PLANKS = 5;
const TRIAL_LEVEL_REQUIREMENT_PER_TIER = 10;
const TRIAL_STARTING_STAT_POINTS = 3;
const TRIAL_STAMINA_MAX_BASE = 100.0;
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
function applyTrialResolutionReset(data, classId, startingPoints) {
  const c = TRIAL_CLASSES[classId];
  data.level = 1;
  data.xp = 0;
  data.xp_to_next = trialXpRequiredForLevel(1);
  data.attributes = { str: c.base_str || 5, dex: c.base_dex || 5, vit: c.base_vit || 5, int: c.base_int || 5 };
  data.unspent_stat_points = startingPoints;
  data.bonus_hp_from_attributes = 0;
  data.bonus_stamina_from_attributes = 0;
  // getMaxHp()'s (level-1)*hp_per_level term is 0 at level 1, and gear/attribute bonuses
  // are both freshly zeroed above -- so base_hp alone is the exact, correct level-1 max
  // (see this section's top-of-file SCOPE NOTE for why gear is deliberately excluded).
  data.current_hp = c.base_hp || 50;
  data.current_stamina = TRIAL_STAMINA_MAX_BASE;
  data.max_maze_depth_reached = 1;
}

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
      current_hp: data.current_hp,
      current_stamina: data.current_stamina,
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
  applyTrialResolutionReset(data, nextClassId, TRIAL_STARTING_STAT_POINTS);
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
    current_hp: data.current_hp,
    current_stamina: data.current_stamina,
    max_maze_depth_reached: data.max_maze_depth_reached,
    last_bridge_steps: data.last_bridge_steps,
    bridge_fail_streak: data.bridge_fail_streak,
    _save_seq: data._save_seq,
  });
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
  "damage", "hp", "crit", "armor", "regen", "gold_find", "xp_find", "stamina_max",
  "strength", "dexterity", "vitality", "intelligence", "poison_resist", "magic_find",
  "block_chance", "flee_chance", "crit_multiplier", "attack_speed",
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
const ITEM_AFFIX_TIER1_MAX = {
  damage: 5, hp: 15, crit: 5, armor: 2, regen: 5, gold_find: 10, xp_find: 5,
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
};
// index.html's RARITY_TABLE's `slots` field, keyed by the RARITY_TABLE `name` (internal
// name, not the player-facing RARITY_DISPLAY_NAMES wording -- items store the internal
// name, e.g. "Epic" is displayed to players as "Rare").
const ITEM_RARITY_SLOTS = { Common: 1, Uncommon: 2, Rare: 3, Epic: 5, Legendary: 8 };
const ITEM_GENERATION_SLOTS = ["weapon", "head", "shoulders", "armor", "pants", "gloves", "boots", "ring", "amulet", "belt"];
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
const ITEM_AFFIX_TIER1_MAX_CEILING = Object.assign({}, ITEM_AFFIX_TIER1_MAX, { xp_find: 10, armor: 20 });

function itemAffixMaxForTier(stat, tier) {
  const base = ITEM_AFFIX_TIER1_MAX[stat];
  return base != null ? base * tier : 0;
}
function itemAffixCeilingForTier(stat, tier) {
  const base = ITEM_AFFIX_TIER1_MAX_CEILING[stat];
  return base != null ? base * tier : 0;
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
    let max = itemAffixCeilingForTier(a.stat, tier);
    if (item.slot === "weapon" && a.stat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    if (a.value < 1 || a.value > max) return `Affix "${a.stat}" value is beyond what a Tier ${tier} item could roll.`;
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
    let max = itemAffixMaxForTier(a.stat, inst.tier);
    if (inst.slot === "weapon" && a.stat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    const value = 1 + Math.floor(Math.random() * max);
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
  const existingStats = new Set(inst.affixes.map((a) => a.stat));
  const candidatePool = ITEM_AFFIX_POOL.filter((s) => !existingStats.has(s));
  // Every item has at most 8 normal affixes (Legendary) out of a 15-stat pool, so a fresh
  // stat is always available; this fallback only exists as a defensive no-op guard.
  const newStat = candidatePool.length > 0
    ? candidatePool[Math.floor(Math.random() * candidatePool.length)]
    : inst.affixes[targetPos].stat;

  let max = itemAffixMaxForTier(newStat, inst.tier);
  if (inst.slot === "weapon" && newStat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
  const newValue = 1 + Math.floor(Math.random() * Math.max(1, max));

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
  let max = itemAffixMaxForTier(targetStat, inst.tier);
  if (inst.slot === "weapon" && targetStat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
  const newValue = 1 + Math.floor(Math.random() * Math.max(1, max));

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
const COMBAT_CLASSES = {
  wizard:      { tier: 1, chain: "wizard",      base_hp: 60,  hp_per_level: 8,  base_damage_min: 8,  base_damage_max: 14, damage_per_level: 1.4, base_crit: 0.05, regen: 0.0 },
  thornguard:  { tier: 1, chain: "thornguard",  base_hp: 100, hp_per_level: 14, base_damage_min: 6,  base_damage_max: 10, damage_per_level: 1.1, base_crit: 0.05, regen: 0.0 },
  windrider:   { tier: 1, chain: "windrider",   base_hp: 75,  hp_per_level: 10, base_damage_min: 7,  base_damage_max: 12, damage_per_level: 1.2, base_crit: 0.20, regen: 0.0 },
  sorcerer:    { tier: 2, chain: "wizard",      base_hp: 80,  hp_per_level: 10, base_damage_min: 12, base_damage_max: 20, damage_per_level: 1.9, base_crit: 0.08, regen: 0.0 },
  stonewarden: { tier: 2, chain: "thornguard",  base_hp: 140, hp_per_level: 18, base_damage_min: 10, base_damage_max: 16, damage_per_level: 1.6, base_crit: 0.10, regen: 0.0 },
  galestrider: { tier: 2, chain: "windrider",   base_hp: 100, hp_per_level: 13, base_damage_min: 11, base_damage_max: 18, damage_per_level: 1.7, base_crit: 0.25, regen: 0.0 },
  warlock:     { tier: 3, chain: "wizard",      base_hp: 105, hp_per_level: 13, base_damage_min: 16, base_damage_max: 26, damage_per_level: 2.4, base_crit: 0.10, regen: 1.0 },
  treesinger:  { tier: 3, chain: "thornguard",  base_hp: 160, hp_per_level: 20, base_damage_min: 14, base_damage_max: 22, damage_per_level: 2.0, base_crit: 0.15, regen: 2.5 },
  shadowbloom: { tier: 3, chain: "windrider",   base_hp: 125, hp_per_level: 16, base_damage_min: 18, base_damage_max: 28, damage_per_level: 2.4, base_crit: 0.35, regen: 0.0 },
  necromancer: { tier: 4, chain: "wizard",      base_hp: 135, hp_per_level: 17, base_damage_min: 22, base_damage_max: 34, damage_per_level: 3.0, base_crit: 0.12, regen: 1.5 },
  rootbinder:  { tier: 4, chain: "thornguard",  base_hp: 210, hp_per_level: 26, base_damage_min: 18, base_damage_max: 28, damage_per_level: 2.6, base_crit: 0.12, regen: 3.5 },
  druid:       { tier: 4, chain: "windrider",   base_hp: 160, hp_per_level: 20, base_damage_min: 24, base_damage_max: 36, damage_per_level: 3.1, base_crit: 0.20, regen: 2.0 },
  emberpriest: { tier: 5, chain: "thornguard",  base_hp: 190, hp_per_level: 24, base_damage_min: 16, base_damage_max: 24, damage_per_level: 2.3, base_crit: 0.10, regen: 6.0 },
  archmage:    { tier: 5, chain: "wizard",      base_hp: 165, hp_per_level: 21, base_damage_min: 28, base_damage_max: 42, damage_per_level: 3.6, base_crit: 0.15, regen: 2.0 },
  galeshaper:  { tier: 5, chain: "windrider",   base_hp: 195, hp_per_level: 25, base_damage_min: 30, base_damage_max: 44, damage_per_level: 3.8, base_crit: 0.28, regen: 2.4 },
};

// Verbatim mirror of index.html's MONSTERS array (id/tier/base stats/loot_table only -- name
// is used to build the combat log/session `name` field).
const COMBAT_MONSTERS = [
  { id: "sprout", name: "Sprout", tier: "common", base_hp: 20, base_damage_min: 2, base_damage_max: 4, base_xp: 8, gold_min: 1, gold_max: 4, loot_table: "common" },
  { id: "mosshide", name: "Mosshide", tier: "common", base_hp: 22, base_damage_min: 3, base_damage_max: 6, base_xp: 8, gold_min: 2, gold_max: 5, loot_table: "common" },
  { id: "windwisp", name: "Windwisp", tier: "common", base_hp: 16, base_damage_min: 2, base_damage_max: 5, base_xp: 7, gold_min: 1, gold_max: 3, loot_table: "common" },
  { id: "pebblekin", name: "Pebblekin", tier: "common", base_hp: 26, base_damage_min: 3, base_damage_max: 6, base_xp: 9, gold_min: 2, gold_max: 5, loot_table: "common" },
  { id: "cinderling", name: "Cinderling", tier: "common", base_hp: 24, base_damage_min: 4, base_damage_max: 7, base_xp: 9, gold_min: 2, gold_max: 6, loot_table: "common" },
  { id: "bramble_knight", name: "Bramble Knight", tier: "uncommon", base_hp: 38, base_damage_min: 6, base_damage_max: 10, base_xp: 12, gold_min: 4, gold_max: 10, loot_table: "uncommon" },
  { id: "thornling", name: "Thornling", tier: "uncommon", base_hp: 32, base_damage_min: 6, base_damage_max: 10, base_xp: 12, gold_min: 3, gold_max: 9, loot_table: "uncommon" },
  { id: "boglurker", name: "Boglurker", tier: "uncommon", base_hp: 42, base_damage_min: 5, base_damage_max: 9, base_xp: 12, gold_min: 3, gold_max: 8, loot_table: "uncommon" },
  { id: "emberkin", name: "Emberkin", tier: "uncommon", base_hp: 28, base_damage_min: 7, base_damage_max: 12, base_xp: 13, gold_min: 4, gold_max: 9, loot_table: "uncommon" },
  { id: "fernstalker", name: "Fernstalker", tier: "uncommon", base_hp: 30, base_damage_min: 8, base_damage_max: 13, base_xp: 13, gold_min: 4, gold_max: 10, loot_table: "uncommon" },
  { id: "vinewraith", name: "Vinewraith", tier: "rare", base_hp: 65, base_damage_min: 10, base_damage_max: 16, base_xp: 18, gold_min: 8, gold_max: 16, loot_table: "rare" },
  { id: "stumplurker", name: "Stumplurker", tier: "rare", base_hp: 58, base_damage_min: 9, base_damage_max: 15, base_xp: 17, gold_min: 7, gold_max: 15, loot_table: "rare" },
  { id: "ashwalker", name: "Ashwalker", tier: "rare", base_hp: 60, base_damage_min: 8, base_damage_max: 14, base_xp: 18, gold_min: 8, gold_max: 18, loot_table: "rare" },
  { id: "stormroot", name: "Stormroot", tier: "rare", base_hp: 50, base_damage_min: 11, base_damage_max: 18, base_xp: 19, gold_min: 8, gold_max: 17, loot_table: "rare" },
  { id: "bark_golem", name: "Bark Golem", tier: "rare", base_hp: 90, base_damage_min: 9, base_damage_max: 14, base_xp: 20, gold_min: 10, gold_max: 20, loot_table: "rare" },
  { id: "ancient_treant", name: "Ancient Treant", tier: "epic", base_hp: 95, base_damage_min: 16, base_damage_max: 24, base_xp: 30, gold_min: 16, gold_max: 28, loot_table: "epic" },
  { id: "wildfire", name: "Wildfire", tier: "epic", base_hp: 100, base_damage_min: 18, base_damage_max: 28, base_xp: 34, gold_min: 18, gold_max: 32, loot_table: "epic" },
  { id: "stonewaker", name: "Stonewaker", tier: "epic", base_hp: 110, base_damage_min: 17, base_damage_max: 26, base_xp: 32, gold_min: 17, gold_max: 30, loot_table: "epic" },
  { id: "tempest_elm", name: "Tempest Elm", tier: "epic", base_hp: 105, base_damage_min: 19, base_damage_max: 29, base_xp: 36, gold_min: 19, gold_max: 34, loot_table: "epic" },
];

// Verbatim mirror of index.html's LOOT_TABLES.
const COMBAT_LOOT_TABLES = {
  common: [{ type: "nothing", weight: 155 }, { type: "gear", weight: 50 }, { type: "consumable", weight: 66, item_id: "minor_health_potion" }, { type: "herb", weight: 1, herb_id: "sunpetal" }, { type: "consumable", weight: 26, item_id: "dry_branch" }],
  uncommon: [{ type: "nothing", weight: 139 }, { type: "gear", weight: 66 }, { type: "consumable", weight: 50, item_id: "health_potion" }, { type: "herb", weight: 1, herb_id: "emberroot" }, { type: "consumable", weight: 26, item_id: "dry_branch" }],
  rare: [{ type: "nothing", weight: 112 }, { type: "gear", weight: 99 }, { type: "consumable", weight: 50, item_id: "medium_health_potion" }, { type: "herb", weight: 1, herb_id: "frostvine" }, { type: "consumable", weight: 20, item_id: "dry_branch" }],
  epic: [{ type: "nothing", weight: 83 }, { type: "gear", weight: 149 }, { type: "consumable", weight: 50, item_id: "greater_health_potion" }, { type: "herb", weight: 1, herb_id: "frostvine" }, { type: "consumable", weight: 17, item_id: "dry_branch" }],
};

// Only the fields combat's use-item endpoint actually needs from index.html's ITEMS.
const COMBAT_CONSUMABLES = {
  minor_health_potion: { heal_amount: 20 }, health_potion: { heal_amount: 40 }, medium_health_potion: { heal_amount: 75 }, greater_health_potion: { heal_amount: 120 }, supreme_health_potion: { heal_amount: 180 },
  minor_stamina_potion: { stamina_amount: 35 }, stamina_potion: { stamina_amount: 60 }, medium_stamina_potion: { stamina_amount: 100 }, greater_stamina_potion: { stamina_amount: 150 }, supreme_stamina_potion: { stamina_amount: 220 },
  antidote: { cures_poison: true },
};
const COMBAT_BAG_CAPACITY = { traveler_pouch: 8, woven_bag: 16, bramble_sack: 24, rootpack_ancient: 40 };
const COMBAT_BASE_NAMES = {
  weapon: ["Twig Wand", "Bramblestaff", "Rootcarver", "Thornbow", "Charwood Axe", "Emberbrand", "Thornfang", "Bramblespike", "Quickthorn", "Goedendag"],
  head: ["Bramble Circlet", "Mosscap", "Antlercrown", "Leafwood Hood"],
  shoulders: ["Bark Mantle", "Rootguard Pauldrons", "Thistle Shoulderguard"],
  armor: ["Big Leaf Wrap", "Bark Plate", "Mosscloak", "Reedmail", "Ashen Hide"],
  pants: ["Root-woven Leggings", "Bark Greaves", "Vine Trousers"],
  gloves: ["Thornweave Gloves", "Barkgrip Gauntlets", "Mossback Handwraps"],
  boots: ["Rootstep Boots", "Mossy Treads", "Bramblehide Boots"],
  ring: ["Acorn Ring", "Petal Ring", "Vinewrought Ring"],
  amulet: ["Emberstone Pendant", "Driftwood Amulet", "Heartwood Talisman"],
  belt: ["Vine Cinch", "Barkweave Belt", "Root Sash"],
};
const COMBAT_WEAPON_TYPE_BY_BASE_NAME = { "Twig Wand": "wand", "Bramblestaff": "staff", "Rootcarver": "sword", "Thornbow": "bow", "Charwood Axe": "axe", "Emberbrand": "sword", "Thornfang": "dagger", "Bramblespike": "dagger", "Quickthorn": "dagger", "Goedendag": "dagger" };
const COMBAT_WEAPON_TYPE_CLASS_RESTRICTIONS = {
  wand: ["wizard", "sorcerer"], staff: ["warlock", "rootbinder", "druid"],
  sword: ["thornguard", "stonewarden", "treesinger", "galestrider"],
  axe: ["thornguard", "stonewarden", "rootbinder", "windrider", "galestrider", "galeshaper"],
  bow: ["windrider", "galestrider", "treesinger"],
};
const COMBAT_EQUIPPED_SLOT_KEYS = ["weapon", "head", "shoulders", "armor", "pants", "gloves", "boots", "ring1", "ring2", "amulet", "belt"];
const COMBAT_RARITY_TABLE = [{ name: "Common", slots: 1, weight: 60 }, { name: "Uncommon", slots: 2, weight: 25 }, { name: "Rare", slots: 3, weight: 10 }, { name: "Epic", slots: 5, weight: 4 }, { name: "Legendary", slots: 8, weight: 1 }];

// Balance-equivalent constants combat needs, ported verbatim from index.html's Balance
// object (see that file's own comments for the full rationale behind each number).
const CB = {
  CRIT_MULTIPLIER: 1.75, FLEE_FAIL_CHANCE: 0.20, MONSTER_FIRST_STRIKE_CHANCE: 0.5,
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
  // v0.20.4 HOTFIX: AREA_HP_GROWTH/AREA_DAMAGE_GROWTH dialed down from 0.15/0.12 (0.105/
  // 0.09), and LATE_GAME_MONSTER_GROWTH_PER_LEVEL below is now permanently unused (see
  // combatLateGameGrowthMult()) -- must stay in sync with Balance.AREA_HP_GROWTH/
  // AREA_DAMAGE_GROWTH's own v0.20.4 comment in index.html for the full rationale, the
  // tier-blended-monster-pool bug this accounts for, and why the late-game multiplier was
  // removed outright instead of re-tuned again. Full methodology in BALANCE_REPORT_v0.20.4.md.
  AREA_HP_GROWTH: 0.105, AREA_DAMAGE_GROWTH: 0.09, AREA_XP_GROWTH: 0.062,
  LATE_GAME_MONSTER_GROWTH_START_LEVEL: 10, LATE_GAME_MONSTER_GROWTH_PER_LEVEL: 0.10,
  // v0.20.4: every point of total Strength grants this much flat Armor, for every class --
  // must stay in sync with Balance.STR_TO_ARMOR_RATIO in index.html (see that constant's
  // comment for the full calibration).
  STR_TO_ARMOR_RATIO: 0.25,
  STRONGHOLD_GUARDIAN_HP_MULT: 3.0, STRONGHOLD_GUARDIAN_XP_MULT: 1.5,
  // v0.21 (#5): doubled per Gwen's exact request (was 0.15) -- mirrors Balance.STRONGHOLD_KEY_DROP_CHANCE in index.html.
  STRONGHOLD_KEY_DROP_CHANCE: 0.30,
  ROAMING_MOB_HP_MULT: 4.0, ROAMING_MOB_DAMAGE_MULT: 1.5, ROAMING_MOB_XP_MULT: 2.0,
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
  // v0.20 (#7): must stay in sync with Balance.SHRINE_MAGIC_FIND_PCT in index.html -- this
  // was missing entirely server-side until the same bug-hunt that fixed the round-tick sync
  // issue below turned it up: combatGetMagicFind() was still reading the OLD wall-clock
  // temp_buffs bag (combatGetActiveTempBuff(data, "magic_find")) that Magic Find moved off
  // of in #7, which the client stopped ever populating -- so the shrine's bonus silently
  // never affected a single server-resolved loot roll after that change shipped.
  SHRINE_MAGIC_FIND_PCT: 25,
  STAT_POINTS_PER_LEVEL: 3, LEVEL_CAP: 60,
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
  STAMINA_MAX_BASE: 100.0,
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
function combatMonsterHp(baseHp, areaLevel) { return baseHp * CB.MONSTER_HP_MULT * Math.pow(1.0 + CB.AREA_HP_GROWTH, areaLevel) * combatLateGameGrowthMult(areaLevel); }
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
function combatGenerateGearItem(tier, magicFindPct) {
  const chosenSlot = cbPick(ITEM_GENERATION_SLOTS);
  const baseName = cbPick(COMBAT_BASE_NAMES[chosenSlot] || ["Item"]);
  const rarity = combatRollRarity(magicFindPct);
  const slotCount = rarity.slots;
  let pool = [...ITEM_AFFIX_POOL];
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
    const max = itemAffixMaxForTier(stat, tier);
    let value = 1 + Math.floor(Math.random() * max);
    if (chosenSlot === "weapon" && stat === "damage") value = Math.round(value * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    affixes.push({ stat, value });
  }
  if (Math.random() < CB.EYESIGHT_AFFIX_CHANCE) affixes.push({ stat: "eyesight", value: 2 });
  return { instance_id: `srv_${Date.now()}_${Math.floor(Math.random() * 999999)}`, slot: chosenSlot, base_name: baseName, tier, rarity: rarity.name, affixes, element };
}

function combatTierLevelRequirement(tier) { const t = cbClampi(tier, 1, ITEM_TIER_MAX); return Math.max(1, (t - 1) * CB.ITEM_TIER_BRACKET_WIDTH); }
function combatWeaponTypeForInstance(inst) { return (inst && inst.base_name) ? (COMBAT_WEAPON_TYPE_BY_BASE_NAME[inst.base_name] || null) : null; }
function combatClassCanEquipItem(classId, inst) {
  if (!inst || inst.slot !== "weapon") return true;
  const wt = combatWeaponTypeForInstance(inst);
  if (!wt) return true;
  const list = COMBAT_WEAPON_TYPE_CLASS_RESTRICTIONS[wt];
  return !list || list.includes(classId);
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
    if (inst && combatCanEquipGear(data, inst)) total += combatAffixTotal(inst, stat);
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
  return ((data.attributes && data.attributes[key]) || 0) + combatGearBonus(data, gearStat) + combatGetActiveTempBuff(data, gearStat);
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
// v0.20 (#9.7): Vitality's HP/Stamina contribution is computed LIVE from attributes.vit
// (see CB.VIT_HP_PER_POINT_RATIO's comment) instead of reading the old bonus_hp_from_
// attributes/bonus_stamina_from_attributes accumulator fields, which are no longer written
// to anywhere (see combatAddXp()) but are left untouched in the data model for old saves.
function combatVitHpBonus(classData, vit) { return (vit || 0) * (classData.hp_per_level || 5) * CB.VIT_HP_PER_POINT_RATIO; }
function combatVitStaminaBonus(classData, vit) { return (vit || 0) * (classData.hp_per_level || 5) * CB.VIT_STAMINA_PER_POINT_RATIO; }
function combatGetMaxHp(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  const vit = (data.attributes && data.attributes.vit) || 0;
  return (c.base_hp || 50) + (c.hp_per_level || 5) * ((data.level || 1) - 1) + combatGearBonus(data, "hp") + combatVitHpBonus(c, vit);
}
function combatGetMaxStamina(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  const vit = (data.attributes && data.attributes.vit) || 0;
  return CB.STAMINA_MAX_BASE + combatGearBonus(data, "stamina_max") + combatVitStaminaBonus(c, vit);
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
function combatGetDamageRange(data) {
  const c = COMBAT_CLASSES[data.class_id] || {};
  const perLevel = (c.damage_per_level || 1.0) * ((data.level || 1) - 1);
  const gearBonus = combatGearBonus(data, "damage");
  let statBonus = 0;
  if (c.chain === "thornguard") statBonus = combatGetTotalAttr(data, "str");
  else if (c.chain === "windrider") statBonus = combatGetTotalAttr(data, "dex");
  else if (c.chain === "wizard") statBonus = combatGetTotalAttr(data, "int");
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
function combatGetMonsterCritChance(areaLevel) { return cbClampf(0.10 + (Math.max(1, areaLevel) - 1) * 0.01, 0, 0.95); }
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
// v0.21 (#9): monsters also scale their crit MULTIPLIER ceiling in breakpoints every 5
// levels: +0.00/0.5/1.0/1.5/2.0/2.5, starting from Lv1 -- mirrors index.html's
// Balance.monsterCritMultiplierBonus()/monsterCritMultiplierMax() exactly (see that
// function's comment for the worked Lv25 example: floor(25/5)=5 -> +2.5, so a Lv25 mob's
// crit ceiling is CB.CRIT_MULTIPLIER(1.75)+2.5=4.25).
const MONSTER_CRIT_MULT_BONUS_TABLE = [0, 0.5, 1.0, 1.5, 2.0, 2.5];
function combatGetMonsterCritMultiplierBonus(areaLevel) {
  const idx = cbClampi(Math.floor(Math.max(1, areaLevel) / 5), 0, MONSTER_CRIT_MULT_BONUS_TABLE.length - 1);
  return MONSTER_CRIT_MULT_BONUS_TABLE[idx];
}
function combatGetMonsterCritMultiplierMax(areaLevel) { return CB.CRIT_MULTIPLIER + combatGetMonsterCritMultiplierBonus(areaLevel); }
// v0.21 (#10): a landed crit doesn't always hit for the full ceiling -- it's a fresh random
// roll each time, somewhere between the shared CB.CRIT_MULTIPLIER floor and whichever max
// applies (player's combatGetCritMultiplierMax(data) or a monster's
// combatGetMonsterCritMultiplierMax(areaLevel)), per Gwen's exact spec: "a player with 5.50
// crit multiplier can crit randomly between 1.75 and 5.50, not 5.50 all the time."
function combatRollCritMultiplier(maxMult) { return CB.CRIT_MULTIPLIER + Math.random() * Math.max(0, maxMult - CB.CRIT_MULTIPLIER); }
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
function combatGetMagicFind(data) {
  // v0.20 BUG FIX: this used to read combatGetActiveTempBuff(data, "magic_find") -- the OLD
  // wall-clock temp-buff bag Magic Find moved off of in v0.20 (#7) -- see CB.SHRINE_MAGIC_FIND_PCT's
  // comment for why that silently zeroed the shrine's effect on server-resolved loot.
  const shrineBonus = (data.magic_find_rounds_left || 0) > 0 ? CB.SHRINE_MAGIC_FIND_PCT : 0;
  return cbClampf(combatGearBonus(data, "magic_find") + shrineBonus + (data.kill_streak || 0) * CB.KILL_STREAK_MAGIC_FIND_PCT_PER_KILL, 0, 500);
}
function combatGetGoldFindMult(data) { return 1.0 + combatGearBonus(data, "gold_find") / 100.0; }
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
}
// Shared fragment merged into every combat endpoint's `player` response object so the client
// always has a fresh, authoritative snapshot of all 3 round-based shrine buffs to overwrite
// its own local copy with -- see the comment above combatTickCombatRoundBuffs() for why that
// matters even on endpoints (like use-item) that don't tick anything themselves.
function combatRoundBuffsPayload(data) {
  return {
    invuln_rounds_left: data.invuln_rounds_left || 0,
    quad_dmg_rounds_left: data.quad_dmg_rounds_left || 0,
    magic_find_rounds_left: data.magic_find_rounds_left || 0,
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
function combatSettleAllHeals(data) {
  const maxHp = combatGetMaxHp(data);
  if (data.srv_heal) {
    const { delivered, heal } = combatSettleHeal(data.srv_heal);
    data.current_hp = Math.min(maxHp, (data.current_hp || 0) + delivered);
    data.srv_heal = heal;
  }
  const maxStamina = combatGetMaxStamina(data);
  if (data.srv_stamina_heal) {
    const { delivered, heal } = combatSettleHeal(data.srv_stamina_heal);
    data.current_stamina = Math.min(maxStamina, (data.current_stamina || 0) + delivered);
    data.srv_stamina_heal = heal;
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
    // v0.20 (#9.7): no longer touches bonus_hp_from_attributes/bonus_stamina_from_attributes
    // here -- see combatGetMaxHp()'s comment. The full-HP heal below is still needed on
    // every level-up regardless.
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
  if (monster.tier === "common") return true;
  if (monster.tier === "uncommon") return areaLevel >= 8;
  if (monster.tier === "rare") return areaLevel >= 20;
  if (monster.tier === "epic") return areaLevel >= 35;
  return false;
}

function loadCharacterRow(accountId, slot) {
  const row = db.prepare("SELECT data FROM characters WHERE account_id = ? AND slot = ?").get(accountId, slot);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch (e) { return null; }
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
  if (Math.random() < combatGetBlockChance(data)) return { invulnerable: false, blocked: true, damage: null, crit: false, crit_mult: null, fatal: false };
  let mdmg = cbRandRange(session.dmg_min, session.dmg_max);
  // v0.19.1 (#10): the monster's own crit roll, applied BEFORE armor mitigation (same order
  // as the player's own crit-then-quad-damage math above) -- 10% + 1%/area-level.
  const crit = Math.random() < combatGetMonsterCritChance(session.area_level);
  // v0.21 (#9/#10): the ACTUAL multiplier for this hit is its own random roll between
  // CB.CRIT_MULTIPLIER and this monster's own crit ceiling (see
  // combatGetMonsterCritMultiplierMax()) -- not always the same fixed 1.75. critMult stays
  // null on a non-crit hit (nothing to report), returned either way so the client can render
  // it.
  const critMult = crit ? combatRollCritMultiplier(combatGetMonsterCritMultiplierMax(session.area_level)) : null;
  if (crit) mdmg *= critMult;
  // v0.20.4: flat subtraction instead of a percentage multiplier -- see combatGetArmor()'s
  // own comment for the full mechanic change.
  mdmg = Math.max(0, mdmg - combatGetArmor(data));
  data.current_hp = (data.current_hp || 0) - mdmg;
  let fatal = false;
  if (data.current_hp < 1) { data.current_hp = 0; fatal = true; }
  return { invulnerable: false, blocked: false, damage: Math.round(mdmg), crit, crit_mult: critMult, fatal };
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
  const lastAt = session.last_monster_hit_at || now;
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

app.post("/api/combat/start", requireAuth, (req, res) => {
  const slot = Number(req.body?.slot);
  const monsterId = req.body?.monster_id;
  const areaLevel = Number(req.body?.area_level);
  const isGuardian = !!req.body?.is_guardian;
  const isRoamer = !!req.body?.is_roamer;
  if (!Number.isInteger(slot) || slot < 0 || slot >= MAX_CHARACTER_SLOTS) return res.status(400).json({ error: "Invalid slot." });
  if (!Number.isInteger(areaLevel) || areaLevel < 1 || areaLevel > CB.AREA_LEVEL_MAX) return res.status(400).json({ error: "Invalid area level." });
  const monster = COMBAT_MONSTERS.find((m) => m.id === monsterId);
  if (!monster) return res.status(400).json({ error: "Unknown monster." });
  if (!combatMonsterAllowedForAreaLevel(monster, areaLevel)) return res.status(400).json({ error: "That monster isn't found at this area level." });

  const data = loadCharacterRow(req.account.id, slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });
  if (areaLevel > (data.max_maze_depth_reached || 1)) return res.status(400).json({ error: "You haven't reached that area level yet." });

  // A stale/abandoned session (e.g. the client navigated away mid-fight) can't be reused --
  // starting a new encounter always retires whatever was previously active for this slot.
  db.prepare("DELETE FROM combat_sessions WHERE account_id = ? AND slot = ? AND status = 'active'").run(req.account.id, slot);

  const hpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_HP_MULT : isRoamer ? CB.ROAMING_MOB_HP_MULT : 1;
  const dmgMult = isRoamer ? CB.ROAMING_MOB_DAMAGE_MULT : 1;
  const xpMult = isGuardian ? CB.STRONGHOLD_GUARDIAN_XP_MULT : isRoamer ? CB.ROAMING_MOB_XP_MULT : 1;
  const maxHp = combatMonsterHp(monster.base_hp, areaLevel) * hpMult;
  const dmgMin = combatMonsterDamage(monster.base_damage_min, areaLevel) * dmgMult;
  const dmgMax = combatMonsterDamage(monster.base_damage_max, areaLevel) * dmgMult;
  // v0.20 (#9.5): session.xp is the RAW pre-bonus-stack base reward (see combatAddXp()) --
  // the level-difference penalty is baked in here, at the source, alongside the
  // guardian/roamer multiplier, so it applies uniformly regardless of which bonus sources
  // later stack on top of it.
  const xp = combatMonsterXpReward(monster.base_xp, areaLevel) * xpMult * combatLevelDiffXpMult(data.level, areaLevel);
  const name = isGuardian ? `Guardian ${monster.name}` : isRoamer ? `Roaming ${monster.name}` : monster.name;

  combatSettleAllHeals(data);
  const log = [];
  let firstStrike = null;
  if (Math.random() < CB.MONSTER_FIRST_STRIKE_CHANCE) {
    let mdmg = cbRandRange(dmgMin, dmgMax);
    // v0.19.1 (#10): the first-strike hit can crit too, same formula/order as the regular
    // counter-attack in combatResolveMonsterTurn. v0.21 (#9/#10): critMult is this hit's own
    // random roll between CB.CRIT_MULTIPLIER and this monster's ceiling, same as that function.
    const crit = Math.random() < combatGetMonsterCritChance(areaLevel);
    const critMult = crit ? combatRollCritMultiplier(combatGetMonsterCritMultiplierMax(areaLevel)) : null;
    if (crit) mdmg *= critMult;
    mdmg = Math.max(0, mdmg - combatGetArmor(data));
    data.current_hp = Math.max(1, (data.current_hp || 0) - mdmg);
    firstStrike = { damage: Math.round(mdmg), crit, crit_mult: critMult };
    log.push(`The ${name} strikes first, hitting you for ${Math.round(mdmg)} damage${crit ? ` (Critical! x${critMult.toFixed(2)})` : ""} before you can react!`);
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
  db.prepare(
    `INSERT INTO combat_sessions (id, account_id, slot, monster_id, name, area_level, max_hp, hp, dmg_min, dmg_max, xp, gold_min, gold_max, loot_table, is_guardian, is_roamer, status, created_at, updated_at, player_attack_speed, monster_attack_speed, last_monster_hit_at)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'active', ?, ?, ?, ?, ?)`
  ).run(id, req.account.id, slot, monster.id, name, areaLevel, maxHp, maxHp, dmgMin, dmgMax, xp, monster.gold_min, monster.gold_max, monster.loot_table, isGuardian ? 1 : 0, isRoamer ? 1 : 0, now, now, playerAttackSpeed, monsterAttackSpeed, lastMonsterHitAt);

  const saveSeq = saveCharacterRow(req.account.id, slot, data);
  res.json({
    ok: true, session_id: id,
    monster: { name, hp: maxHp, max_hp: maxHp, attack_speed: monsterAttackSpeed },
    player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), current_stamina: data.current_stamina, max_stamina: combatGetMaxStamina(data), attack_speed: playerAttackSpeed, ...combatRoundBuffsPayload(data) },
    first_strike: firstStrike, log, _save_seq: saveSeq,
  });
});

app.post("/api/combat/:sessionId/attack", requireAuth, (req, res) => {
  const session = getCombatSession(req.account.id, req.params.sessionId);
  if (!session) return res.status(404).json({ error: "That fight no longer exists." });
  if (session.status !== "active") return res.status(409).json({ error: "That fight has already ended." });
  const data = loadCharacterRow(req.account.id, session.slot);
  if (!data) return res.status(404).json({ error: "No character in that slot." });

  combatSettleAllHeals(data);
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
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, mob_blocked: false, player_hit: null, weapon_skill: null,
      monster: { hp: session.hp, max_hp: session.max_hp, defeated: false },
      kill: null, monster_turn: null, monster_ticks: catchUp.ticks,
      player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
      fatal: true, _save_seq: saveSeq,
    });
  }

  combatTickCombatRoundBuffs(data);

  const mobBlocked = Math.random() < combatMobBlockChance(session.area_level);
  let playerHit = null, weaponSkill = null, newMonsterHp = session.hp;
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
    if (crit) dmg *= critMult;
    if (quadActiveThisRound) dmg *= 4;
    newMonsterHp = session.hp - dmg;
    playerHit = { damage: Math.round(dmg), crit, crit_mult: critMult, quad_damage: quadActiveThisRound };
    const weaponType = combatGetEquippedWeaponType(data);
    if (weaponType) weaponSkill = { weapon_type: weaponType, ...combatRegisterWeaponHit(data, weaponType) };
  }

  const monsterDefeated = !mobBlocked && newMonsterHp <= 0;
  let kill = null, monsterTurn = null, fatal = false;

  if (monsterDefeated) {
    newMonsterHp = 0;
    const gold = cbRandIntRange(session.gold_min, session.gold_max);
    const goldCredited = Math.round(gold * combatGetGoldFindMult(data));
    // v0.20 (#9.3): session.xp (the monster's raw base reward) is now handed to
    // combatAddXp() UNMODIFIED -- every bonus (Community XP, Forest Reputation, XP Find
    // gear, Experience Shrine) is summed and applied exactly once inside that function
    // instead of being partly pre-multiplied here and partly multiplied again inside it.
    const xpResult = combatAddXp(data, session.xp);
    const leveled = xpResult.leveled;
    creditAccountGold(req.account.id, goldCredited);
    combatIncrementKillStreak(data);
    // v0.19.1 (#19): this endpoint used to persist the raw characters-table row via
    // saveCharacterRow() below but never forward max_kill_streak (or level/xp) into the
    // separate leaderboard_bests table that GET /api/leaderboard/killstreak actually reads --
    // so kills scored through server-authoritative combat never showed up on that leaderboard
    // tab. The Broken Bridge Trial endpoint already does this correctly; mirror it here.
    upsertLeaderboardBests(req.account.id, data);

    const magicFind = combatGetMagicFind(data);
    const rolled = combatRollLoot(session.loot_table, magicFind);
    let loot = { type: rolled.type };
    if (rolled.type === "gear") {
      const tier = combatRollItemTier(session.area_level);
      const inst = combatGenerateGearItem(tier, magicFind);
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
        const extraInst = combatGenerateGearItem(extraTier, bonusMagicFind);
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

    kill = {
      gold, gold_credited: goldCredited, xp_gained: xpResult.xpGained, leveled,
      loot, bonus_loot: bonusLoot, key_drop: keyDrop, kill_streak: data.kill_streak, max_kill_streak: data.max_kill_streak,
      total_kills: data.total_kills,
      is_guardian: !!session.is_guardian, is_roamer: !!session.is_roamer,
    };
    updateCombatSession(session.id, { hp: 0, status: "won", last_monster_hit_at: catchUp.newLastHitAt });
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
    updateCombatSession(session.id, { hp: newMonsterHp, last_monster_hit_at: catchUp.newLastHitAt });
  }

  if (fatal) updateCombatSession(session.id, { status: "lost" });
  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, mob_blocked: mobBlocked, player_hit: playerHit, weapon_skill: weaponSkill,
    monster: { hp: Math.max(0, newMonsterHp), max_hp: session.max_hp, defeated: monsterDefeated },
    kill, monster_turn: monsterTurn, monster_ticks: catchUp.ticks,
    player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
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
  // v0.21.1 (#9): elapsed-time catch-up applies to fleeing too -- deciding whether to run
  // still costs real wall-clock time the monster keeps swinging through. Captured before the
  // fail-roll so a fatal catch-up ends the fight before the flee attempt itself resolves.
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, failed: null, monster_turn: null, monster_ticks: catchUp.ticks, fatal: true,
      player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
      _save_seq: saveSeq,
    });
  }

  // v0.21 (#6): "flee_chance" gear affix reduces the fail chance (can drive it to 0 or below,
  // which Math.max clamps to 0 -- an always-succeed flee -- per Gwen's exact spec).
  const failed = Math.random() < Math.max(0, CB.FLEE_FAIL_CHANCE - combatGetFleeChanceBonus(data));
  let monsterTurn = null, fatal = false;
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
    updateCombatSession(session.id, fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt } : { last_monster_hit_at: catchUp.newLastHitAt });
  } else {
    updateCombatSession(session.id, { status: "fled", last_monster_hit_at: catchUp.newLastHitAt });
  }
  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, failed, monster_turn: monsterTurn, monster_ticks: catchUp.ticks, fatal,
    player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), ...combatRoundBuffsPayload(data) },
    _save_seq: saveSeq,
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

  // v0.21.1 (#9): elapsed-time catch-up applies here too -- opening the item menu and picking
  // a potion still costs real wall-clock time the monster keeps swinging through.
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  if (catchUp.fatal) {
    updateCombatSession(session.id, { status: "lost", last_monster_hit_at: catchUp.newLastHitAt });
    const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
    return res.json({
      ok: true, monster_ticks: catchUp.ticks, fatal: true,
      player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), current_stamina: data.current_stamina, max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
      _save_seq: saveSeq,
    });
  }
  updateCombatSession(session.id, { last_monster_hit_at: catchUp.newLastHitAt });

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
    player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), current_stamina: data.current_stamina, max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
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
  const invulnActiveThisRound = combatIsInvulnerable(data);
  const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
  updateCombatSession(session.id, catchUp.fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt } : { last_monster_hit_at: catchUp.newLastHitAt });

  const saveSeq = saveCharacterRow(req.account.id, session.slot, data);
  res.json({
    ok: true, monster_ticks: catchUp.ticks, fatal: catchUp.fatal,
    monster: { hp: session.hp, max_hp: session.max_hp },
    player: { current_hp: data.current_hp, max_hp: combatGetMaxHp(data), current_stamina: data.current_stamina, max_stamina: combatGetMaxStamina(data), ...combatRoundBuffsPayload(data) },
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
       FROM leaderboard_bests ORDER BY max_kill_streak DESC LIMIT 50`
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
       FROM leaderboard_bests ORDER BY total_kills DESC LIMIT 50`
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
    attributes: data.attributes || { str: 0, dex: 0, vit: 0, int: 0 },
    equipped: data.equipped || {},
    weapon_skills: data.weapon_skills || {},
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
  for (const a of item.affixes) {
    if (!a || typeof a.stat !== "string" || a.stat === "eyesight" || !Number.isFinite(a.value)) continue;
    let max = itemAffixMaxForTier(a.stat, item.tier);
    if (item.slot === "weapon" && a.stat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
    if (a.value > max) out.push({ stat: a.stat, value: a.value, max });
  }
  return out;
}

// Returns a brand NEW item object (never mutates `item`) with every out-of-spec affix value
// clamped down to today's live max. Same stats, same tier/rarity/slot/element/instance_id --
// this is intentionally NOT a full re-roll (the player keeps the exact item they've always
// had, just corrected to current balance) and never touches affixes that are already in spec.
function legacyGearRescale(item) {
  const affixes = item.affixes.map((a) => {
    if (!a || a.stat === "eyesight") return a;
    let max = itemAffixMaxForTier(a.stat, item.tier);
    if (item.slot === "weapon" && a.stat === "damage") max = Math.round(max * ITEM_WEAPON_DAMAGE_AFFIX_MULT);
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
    db.prepare("UPDATE characters SET data = ?, updated_at = ? WHERE account_id = ? AND slot = ?").run(
      JSON.stringify(data), nowIso(), accountId, slot
    );
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
    const invulnActiveThisRound = combatIsInvulnerable(data);
    const catchUp = combatCatchUpMonsterHits(session, data, invulnActiveThisRound);
    if (catchUp.ticks.length === 0 && !catchUp.fatal) continue;
    updateCombatSession(session.id, catchUp.fatal ? { status: "lost", last_monster_hit_at: catchUp.newLastHitAt } : { last_monster_hit_at: catchUp.newLastHitAt });
    const saveSeq = saveCharacterRow(client.accountId, session.slot, data);
    client.send(JSON.stringify({
      type: "combat_tick", session_id: session.id, monster_ticks: catchUp.ticks, fatal: catchUp.fatal,
      player: {
        current_hp: catchUp.fatal ? 0 : data.current_hp, max_hp: combatGetMaxHp(data),
        current_stamina: data.current_stamina, max_stamina: combatGetMaxStamina(data),
      },
      _save_seq: saveSeq,
    }));
  }
}, COMBAT_PUSH_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`Wandrian server listening on port ${PORT} (db: ${DB_PATH})`);
});
