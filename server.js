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

function findAccount(username) {
  // Case-insensitive lookup; returns the stored key or null
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
        break;
      }

      // ── Join a chat room ───────────────────────────────────
      case 'join': {
        if (!info?.authenticated) {
          ws.send(JSON.stringify({ type: 'auth_fail', text: 'Not authenticated.' }));
          break;
        }
        const newColor = color || info.color || '#00e5a0';
        clients.set(ws, { ...info, room, color: newColor });
        broadcastUserList(room);
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
        console.log(`[guild-join] ${info.username} → "${gr.name}"`);
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
        if (guildRooms.has(info.room)) broadcastGuildRoomList();
      }
      if (info.username) console.log(`[-] ${info.username} disconnected`);
    }
  });

  ws.on('error', (err) => { console.error('[error]', err.message); clients.delete(ws); });
});

console.log(`✅  Server running on port ${PORT}`);
