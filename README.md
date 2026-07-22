# Wandrian server, Phase 1

This is the backend for Wandrian, which is online-only: accounts, persistent character
saves, a shared graveyard, a single combined leaderboard (ranked by highest tier reached
first, then level and experience, with hardcore deaths shown but struck through), global
chat, private mailbox notifications, and a real auction house. It also serves the game
client itself (`public/index.html`), so the whole thing, game plus server, is one thing
you run and one thing you point a domain at.

## What's real vs. what's still client-trusted

Accounts, saves, graveyard, leaderboard, chat, and the auction house all live on the
server now, so friends can log in from anywhere and see each other. Auction sales credit
the seller's gold directly server-side (since the seller isn't present for the buyer's
request), and both sides get a private mailbox notification, delivered live over the
chat WebSocket if they're online, or queued and delivered next time they connect.

As of v0.11, there's also a Storage Vault: a 200-slot (20x10) stash shared across every
character on one account, so gear or consumables you stash on one character can be pulled
out on another. It's account-scoped, not character-scoped, and stored server-side as one
row per account (`GET`/`PUT /api/vault`), both behind `requireAuth` only (no admin needed).
Same security model as everything else in this phase: the account is always taken from
`req.account.id`, derived from the caller's Bearer token, never from anything the client
sends in the request body or URL, so there is no way for one account to read or overwrite
another account's vault. The one caveat that still applies here too: like character saves,
the vault's contents are a client-trusted full-replace payload (see the section above), so
a player editing their own vault client-side to fabricate items is the same pre-existing
class of issue as editing their own character save, not a new cross-account hole.

What this phase does **not** do yet: stop a player from opening devtools on their own
browser and editing the character JSON before it's sent to `PUT /api/characters/:slot`
(gold, level, items, all still client-computed and just handed to the server as-is).
That's the next phase: moving combat and loot rolls onto the server, so the client only
ever sends "I attack" and the server decides what happened. Until that lands, treat the
leaderboard as fun, not tamper-proof.

## Requirements

- Node.js 22.5 or newer (uses the built-in `node:sqlite` module, no native compiling,
  no separate database server to install). If you're stuck on an older Node, swap the
  three `DatabaseSync` calls in `server.js` for `better-sqlite3`; the API is the same shape.

## Running it locally

```
cd wandrian-server
npm install
npm start
```

Then open http://localhost:8787, that's the actual game, served by this same process.

(If you see a `node_modules` folder already sitting here, it's leftover from testing
this build, delete it and run `npm install` fresh, or just leave it, `npm install`
will reconcile either way.)

## Configuration (environment variables)

- `PORT`, what port to listen on (default 8787)
- `DB_PATH`, where to store the SQLite file (default `./data/evergrind.db`, kept as-is so
  an already-deployed server doesn't silently start a fresh empty database on restart;
  set this env var yourself if you want the file renamed)
- `CORS_ORIGIN`, lock this to your real domain once deployed (default `*`, fine for
  local testing, not for production if you ever split client and server across origins)
- `ADMIN_TOKEN`, a real secret that gates the entire Dev Tools panel plus the leaderboard
  moderation endpoints (view every entry, delete buggy or cheated ones). Unset by default,
  the admin endpoints return 503 and Dev Tools can't be unlocked until you set it. There is
  no separate client-side password anymore, `ADMIN_TOKEN` never appears in the client code;
  you type it into the Dev Tools unlock field yourself each time, it's verified against the
  server over HTTPS, and it's never saved to disk or localStorage.

## Deploying on your own domain

The simplest path: a small VPS (DigitalOcean, Hetzner, etc.), or any host that lets you
run a long-lived Node process.

1. Copy this server folder to the server, `npm install`, `npm start` (or use
   `pm2` / a systemd service so it restarts if it crashes or the box reboots).
2. Put a reverse proxy in front of it for TLS (HTTPS) and the real port 443, Caddy is
   the easiest option, since it gets you a free Let's Encrypt certificate with almost no
   config:
   ```
   yourdomain.com {
       reverse_proxy localhost:8787
   }
   ```
   nginx + certbot works the same way if you already run nginx.
3. Point your domain's DNS A record at the server's IP. Once the proxy is up, friends
   just visit `https://yourdomain.com`, no query params, no separate client hosting.
4. If you ever host the client (public/index.html) somewhere else instead of from this
   server's `public/` folder, add `?api=https://yourdomain.com` to the client URL so it
   knows where to send API/WebSocket calls, and set `CORS_ORIGIN` on the server to that
   client's origin.
5. To enable leaderboard moderation, set `ADMIN_TOKEN` on the droplet before starting the
   process with pm2, picking your own long random string, for example:
   ```
   ADMIN_TOKEN="pick-a-long-random-string-here" pm2 start server.js --name wandrian
   ```
   If wandrian is already running under pm2, set it and restart with the env var attached:
   ```
   ADMIN_TOKEN="pick-a-long-random-string-here" pm2 restart wandrian --update-env
   ```
   pm2 does not persist one-off shell env vars across reboots on its own, so if you use
   `pm2 save` / `pm2 resurrect`, either export `ADMIN_TOKEN` in your shell profile first,
   or add it to an `env` block in a pm2 ecosystem file. Once set, open Dev Tools in the
   game and paste this same `ADMIN_TOKEN` value into the unlock field, that both opens
   Dev Tools and lets you load/delete leaderboard entries in the "Leaderboard Moderation"
   section below it.

## What's next (not built yet)

- Server-side combat/loot resolution (the actual anti-cheat layer). The auction house's
  gold check is a partial exception, it verifies the buyer's last-saved gold server-side
  before letting a purchase through, but combat, loot rolls, and levels are still
  client-computed and just handed to the server as-is.
- Wrapping the client in Electron + Steamworks for a Steam build, once the above is
  in place
