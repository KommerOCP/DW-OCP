const WebSocket = require('ws');
const bcrypt    = require('bcryptjs');
const crypto    = require('crypto');
const fs        = require('fs');
const path      = require('path');

const PORT         = process.env.PORT || 8080;
const ACCOUNTS_FILE = path.join(__dirname, 'accounts.json');
const SALT_ROUNDS  = 10;

// ── Account storage ────────────────────────────────────────────
// NOTE: accounts.json persists across restarts but resets on Railway redeploy.
// For permanent storage, add a PostgreSQL/MongoDB addon to your Railway project.
let accounts = {};
try {
  if (fs.existsSync(ACCOUNTS_FILE))
    accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
} catch { accounts = {}; }

function saveAccounts() {
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2)); } catch {}
}

// ── Sessions ───────────────────────────────────────────────────
const sessions = new Map(); // token -> username
function generateToken() { return crypto.randomBytes(32).toString('hex'); }

// ── Runtime state ──────────────────────────────────────────────
const clients       = new Map(); // ws -> { username, room, color, authenticated }
const guildRooms    = new Map(); // roomId -> { name, password, lastActivity }
const guildZoneTrackers    = new Map(); // roomId -> { zoneName: { hp, rage, ts, by } }
const guildZoneEventHistory = new Map(); // roomId -> [{ text, ts }, ...]
const ZONE_EVENT_HISTORY_MAX = 50;
const MAX_GUILD_ROOMS = 10;
const GUILD_TTL       = 7 * 24 * 60 * 60 * 1000;
const GUILD_LOBBY     = '__guild_lobby__';

// ── Helpers ────────────────────────────────────────────────────
function broadcastToRoom(room, data) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients)
    if (info.room === room && ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function broadcast(room, data, excludeWs) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients)
    if (info.room === room && ws !== excludeWs && ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function getUserList(room) {
  const users = [];
  for (const [, info] of clients)
    if (info.room === room) users.push({ username: info.username, color: info.color || '#00e5a0' });
  return users;
}

function broadcastUserList(room) {
  broadcastToRoom(room, { type: 'userlist', users: getUserList(room) });
}

function getPublicGuildRooms() {
  return Array.from(guildRooms.entries()).map(([id, r]) => ({
    id, name: r.name, lastActivity: r.lastActivity,
    count: [...clients.values()].filter(c => c.room === id).length,
  }));
}

function broadcastGuildRoomList() {
  const rooms = getPublicGuildRooms();
  const msg = JSON.stringify({ type: 'guild_rooms', rooms });
  for (const [ws, info] of clients)
    if (info.room === GUILD_LOBBY && ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function makeRoomId() { return 'guild_' + Math.random().toString(36).slice(2, 9); }

function formatHP(hp) {
  if (hp >= 1e9) return (hp / 1e9).toFixed(2) + 'B';
  return Math.round(hp / 1e6) + 'M';
}

// ── DW room count helpers ──────────────────────────────────────
const DW_ROOMS = [
  'dw-global-1','dw-global-2','dw-global-3','dw-global-4',
  'dw-global-5','dw-global-6','dw-global-7','dw-global-8',
];
function getDWCounts() {
  const counts = {};
  for (const room of DW_ROOMS)
    counts[room] = [...clients.values()].filter(c => c.room === room).length;
  return counts;
}
// Send current DW counts to one socket
function sendDWCounts(ws) {
  if (ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify({ type: 'room_counts', counts: getDWCounts() }));
}
// Broadcast updated DW counts to every authenticated socket
function broadcastDWCounts() {
  const msg = JSON.stringify({ type: 'room_counts', counts: getDWCounts() });
  for (const [ws, info] of clients)
    if (info.authenticated && ws.readyState === WebSocket.OPEN) ws.send(msg);
}

// ── Guild boss alert cooldowns (shared across all guild members) ───
// Key: `${roomId}::${bossKey}` → last broadcast timestamp
const guildBossCooldowns = new Map();
const BOSS_ALERT_COOLDOWN = 2 * 60 * 1000; // 2 minutes

// Periodically purge expired entries
setInterval(() => {
  const cutoff = Date.now() - BOSS_ALERT_COOLDOWN;
  for (const [key, ts] of guildBossCooldowns)
    if (ts < cutoff) guildBossCooldowns.delete(key);
}, BOSS_ALERT_COOLDOWN);

function findAccount(username) {
  const lower = username?.trim().toLowerCase();
  return Object.keys(accounts).find(k => k.toLowerCase() === lower) || null;
}

// ── Cleanup expired guild rooms ────────────────────────────────
setInterval(() => {
  const cutoff = Date.now() - GUILD_TTL;
  for (const [id, room] of guildRooms) {
    if (room.lastActivity < cutoff) {
      guildRooms.delete(id);
      console.log(`[cleanup] Guild room "${room.name}" expired`);
      broadcastGuildRoomList();
    }
  }
}, 60 * 60 * 1000);

// ── Server ─────────────────────────────────────────────────────
const wss = new WebSocket.Server({ port: PORT });

wss.on('connection', (ws) => {
  console.log(`[+] New connection (total: ${wss.clients.size})`);
  clients.set(ws, { username: null, room: null, color: '#00e5a0', authenticated: false });

  ws.on('message', (raw) => {
    let data; try { data = JSON.parse(raw); } catch { return; }
    const { type, username, room, text, password, color, name, roomId, token, oldPassword, newPassword } = data;
    const info = clients.get(ws);

    switch (type) {

      // ── Register new account ───────────────────────────────
      case 'register': {
        const uname = username?.trim();
        if (!uname || !/^[a-zA-Z0-9_\-]{2,20}$/.test(uname)) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Username must be 2–20 characters (letters, numbers, _ or -).' }));
          break;
        }
        if (!password || password.length < 6) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Password must be at least 6 characters.' }));
          break;
        }
        if (findAccount(uname)) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'That username is already taken.' }));
          break;
        }
        const hash = bcrypt.hashSync(password, SALT_ROUNDS);
        accounts[uname] = { hash, createdAt: Date.now() };
        saveAccounts();
        const tok = generateToken();
        sessions.set(tok, uname);
        clients.set(ws, { ...info, username: uname, authenticated: true });
        ws.send(JSON.stringify({ type: 'auth_ok', token: tok, username: uname }));
        sendDWCounts(ws);
        console.log(`[register] ${uname}`);
        break;
      }

      // ── Log in with username + password ───────────────────
      case 'login': {
        const key = findAccount(username);
        if (!key || !bcrypt.compareSync(password || '', accounts[key].hash)) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Incorrect username or password.' }));
          break;
        }
        const tok = generateToken();
        sessions.set(tok, key);
        clients.set(ws, { ...info, username: key, authenticated: true });
        ws.send(JSON.stringify({ type: 'auth_ok', token: tok, username: key }));
        sendDWCounts(ws);
        console.log(`[login] ${key}`);
        break;
      }

      // ── Authenticate with existing session token ───────────
      case 'auth': {
        const uname = sessions.get(token);
        if (!uname) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Session expired. Please sign in again.' }));
          break;
        }
        clients.set(ws, { ...info, username: uname, authenticated: true });
        ws.send(JSON.stringify({ type: 'auth_ok', username: uname }));
        sendDWCounts(ws);
        break;
      }

      // ── Join a chat room ───────────────────────────────────
      case 'join': {
        if (!info?.authenticated) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Not authenticated.' }));
          break;
        }
        const oldRoom = info.room;
        const newColor = color || info.color || '#00e5a0';
        clients.set(ws, { ...info, room, color: newColor });
        // If switching rooms, update the old room's count too
        if (oldRoom && oldRoom !== room) broadcastUserList(oldRoom);
        broadcastUserList(room);
        if (DW_ROOMS.includes(room) || (oldRoom && DW_ROOMS.includes(oldRoom))) broadcastDWCounts();
        break;
      }

      // ── Regular chat message ───────────────────────────────
      case 'message': {
        if (!info?.authenticated || !info.room) break;
        const safeText = String(text).slice(0, 500);
        broadcastToRoom(info.room, { type: 'message', username: info.username, color: info.color, text: safeText });
        console.log(`[msg] ${info.username} → #${info.room}`);
        break;
      }

      // ── Update name color ──────────────────────────────────
      case 'update_color': {
        if (!info?.authenticated) break;
        clients.set(ws, { ...info, color: color || info.color });
        if (info.room && info.room !== GUILD_LOBBY) broadcastUserList(info.room);
        break;
      }

      // ── Change password ────────────────────────────────────
      case 'change_password': {
        if (!info?.authenticated) break;
        const account = accounts[info.username];
        if (!account) break;
        if (!bcrypt.compareSync(oldPassword || '', account.hash)) {
          ws.send(JSON.stringify({ type: 'pw_result', ok: false, text: 'Current password is incorrect.' }));
          break;
        }
        if (!newPassword || newPassword.length < 6) {
          ws.send(JSON.stringify({ type: 'pw_result', ok: false, text: 'New password must be at least 6 characters.' }));
          break;
        }
        account.hash = bcrypt.hashSync(newPassword, SALT_ROUNDS);
        saveAccounts();
        // Invalidate all old sessions for this user, issue a new one
        for (const [tok, uname] of sessions) if (uname === info.username) sessions.delete(tok);
        const newTok = generateToken();
        sessions.set(newTok, info.username);
        ws.send(JSON.stringify({ type: 'pw_result', ok: true, newToken: newTok }));
        console.log(`[pw-change] ${info.username}`);
        break;
      }

      // ── Leave room ─────────────────────────────────────────
      case 'leave': {
        if (!info) break;
        const oldRoom = info.room;
        clients.set(ws, { ...info, room: null });
        if (oldRoom) {
          broadcastUserList(oldRoom);
          if (guildRooms.has(oldRoom)) broadcastGuildRoomList();
        }
        break;
      }

      // ── Guild lobby ────────────────────────────────────────
      case 'guild_lobby': {
        if (!info?.authenticated) { ws.send(JSON.stringify({ type: 'auth_fail', text: 'Not authenticated.' })); break; }
        clients.set(ws, { ...info, room: GUILD_LOBBY });
        ws.send(JSON.stringify({ type: 'guild_rooms', rooms: getPublicGuildRooms() }));
        console.log(`[guild-lobby] ${info.username}`);
        break;
      }

      // ── Create guild room ──────────────────────────────────
      case 'guild_create': {
        if (!info?.authenticated) break;
        if (guildRooms.size >= MAX_GUILD_ROOMS) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: `Max ${MAX_GUILD_ROOMS} guild rooms reached.` }));
          break;
        }
        if (!name?.trim()) { ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: 'Room name is required.' })); break; }
        if (!password)     { ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: 'Password is required.' }));    break; }
        const newId = makeRoomId();
        guildRooms.set(newId, { name: name.trim().slice(0, 30), password, lastActivity: Date.now() });
        clients.set(ws, { ...info, room: newId });
        ws.send(JSON.stringify({ type: 'guild_joined', roomId: newId, roomName: name.trim() }));
        broadcastUserList(newId);
        broadcastGuildRoomList();
        console.log(`[guild-create] "${name}" by ${info.username}`);
        break;
      }

      // ── Join guild room ────────────────────────────────────
      case 'guild_join': {
        if (!info?.authenticated) break;
        const gr = guildRooms.get(roomId);
        if (!gr) { ws.send(JSON.stringify({ type: 'error', context: 'guild_join', text: 'Room no longer exists.' })); break; }
        if (gr.password !== password) { ws.send(JSON.stringify({ type: 'error', context: 'guild_join', text: 'Wrong password.' })); break; }
        gr.lastActivity = Date.now();
        clients.set(ws, { ...info, room: roomId });
        ws.send(JSON.stringify({ type: 'guild_joined', roomId, roomName: gr.name }));
        broadcastUserList(roomId);
        broadcastGuildRoomList();
        // Send current zone tracker state to the joining member
        if (guildZoneTrackers.has(roomId)) {
          ws.send(JSON.stringify({ type:'zone_tracker', zones: guildZoneTrackers.get(roomId) }));
        }
        // Send recent zone event history so they can see what was announced while away
        if (guildZoneEventHistory.has(roomId)) {
          const history = guildZoneEventHistory.get(roomId);
          for (const evt of history) {
            ws.send(JSON.stringify({ type: 'zone_event', text: evt.text, ts: evt.ts }));
          }
        }
        console.log(`[guild-join] ${info.username} → "${gr.name}"`);
        break;
      }

      // ── Boss alert (shared guild cooldown — first trigger wins) ──
      case 'boss_alert': {
        const info = clients.get(ws);
        if (!info?.authenticated || !info.room) break;
        // Only allowed in actual guild rooms, not DW rooms or lobby
        if (!guildRooms.has(info.room)) break;

        const safeText   = String(data.text    || '').slice(0, 300);
        const safeBossKey = String(data.bossKey || safeText).slice(0, 100);
        const cdKey = `${info.room}::${safeBossKey}`;
        const now = Date.now();

        // If another guild member already announced this boss within 2 min, silently drop
        if ((now - (guildBossCooldowns.get(cdKey) || 0)) < BOSS_ALERT_COOLDOWN) break;

        guildBossCooldowns.set(cdKey, now);
        broadcastToRoom(info.room, { type: 'boss_alert', text: safeText, ts: now });
        const room = guildRooms.get(info.room);
        console.log(`[boss-alert] "${room?.name}" — ${info.username}: ${safeText}`);
        break;
      }

      // ── Zone tracker update (live Olympus zone data) ──────
      case 'zone_update': {
        const info = clients.get(ws);
        if (!info?.authenticated || !guildRooms.has(info.room)) break;
        const zones = data.zones;
        if (!zones || typeof zones !== 'object') break;
        if (!guildZoneTrackers.has(info.room))    guildZoneTrackers.set(info.room, {});
        if (!guildZoneEventHistory.has(info.room)) guildZoneEventHistory.set(info.room, []);
        const tracker = guildZoneTrackers.get(info.room);
        const history = guildZoneEventHistory.get(info.room);
        const now = Date.now();

        for (const [zone, status] of Object.entries(zones)) {
          const safeZone  = String(zone).slice(0, 20);
          const prev      = tracker[safeZone];
          const event     = status.event || 'update';
          const prevState = prev?.bossState || 'unknown';

          let eventText = null;
          if (event === 'new_boss' && prevState !== 'boss') {
            const hp = status.hp ? formatHP(status.hp) : '?';
            eventText = `New boss in ${safeZone}! HP: ${hp}`;
          } else if (event === 'defeated' && prevState === 'boss') {
            eventText = `Boss defeated in ${safeZone}!`;
          }

          if (eventText) {
            const evt = { text: eventText, ts: now };
            history.push(evt);
            if (history.length > ZONE_EVENT_HISTORY_MAX) history.shift();
            broadcastToRoom(info.room, { type: 'zone_event', text: eventText, ts: now });
          }

          tracker[safeZone] = {
            hp:        typeof status.hp   === 'number' ? Math.floor(status.hp)                    : null,
            rage:      typeof status.rage === 'number' ? Math.min(100, Math.max(0, status.rage)) : null,
            bossId:    status.bossId  || null,
            bossState: status.hp ? 'boss' : status.rage ? 'raging' : 'clear',
            ts: now,
            by: info.username
          };
        }

        broadcastToRoom(info.room, { type: 'zone_tracker', zones: tracker });
        break;
      }

      // ── Guild chat message ─────────────────────────────────
      case 'guild_message': {
        if (!info?.authenticated || !guildRooms.has(info.room)) break;
        const gr = guildRooms.get(info.room);
        gr.lastActivity = Date.now();
        const safeText = String(text).slice(0, 500);
        broadcastToRoom(info.room, { type: 'message', username: info.username, color: info.color, text: safeText });
        console.log(`[guild-msg] ${info.username} → "${gr.name}"`);
        break;
      }
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
      clients.delete(ws);
      if (info.room && info.room !== GUILD_LOBBY) {
        broadcastUserList(info.room);
        if (DW_ROOMS.includes(info.room)) broadcastDWCounts();
        if (guildRooms.has(info.room)) broadcastGuildRoomList();
      }
      if (info.username) console.log(`[-] ${info.username} disconnected`);
    }
  });

  ws.on('error', (err) => { console.error('[error]', err.message); clients.delete(ws); });
});

console.log(`✅  Server running on port ${PORT}`);
