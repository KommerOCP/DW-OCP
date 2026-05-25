const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss  = new WebSocket.Server({ port: PORT });

const clients      = new Map(); // ws -> { username, room, color }
const roomPasswords = new Map(); // regular room passwords
const guildRooms   = new Map(); // roomId -> { name, password, lastActivity }

const MAX_GUILD_ROOMS = 10;
const GUILD_TTL       = 7 * 24 * 60 * 60 * 1000; // 7 days
const GUILD_LOBBY     = '__guild_lobby__';

// ── Helpers ────────────────────────────────────────────────
function broadcastToRoom(room, data) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients) {
    if (info.room === room && ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function broadcast(room, data, excludeWs) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients) {
    if (info.room === room && ws !== excludeWs && ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function getUserList(room) {
  const users = [];
  for (const [, info] of clients) {
    if (info.room === room) users.push({ username: info.username, color: info.color || '#00e5a0' });
  }
  return users;
}

function broadcastUserList(room) {
  broadcastToRoom(room, { type: 'userlist', users: getUserList(room) });
}

function getPublicGuildRooms() {
  return Array.from(guildRooms.entries()).map(([id, r]) => ({
    id,
    name: r.name,
    count: [...clients.values()].filter(c => c.room === id).length,
    lastActivity: r.lastActivity,
  }));
}

function broadcastGuildRoomList() {
  const rooms = getPublicGuildRooms();
  const msg = JSON.stringify({ type: 'guild_rooms', rooms });
  for (const [ws, info] of clients) {
    if (info.room === GUILD_LOBBY && ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function makeRoomId() {
  return 'guild_' + Math.random().toString(36).slice(2, 9);
}

// ── Hourly cleanup: remove rooms inactive 7+ days ──────────
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

// ── Connection handler ─────────────────────────────────────
wss.on('connection', (ws) => {
  console.log(`[+] Connection (total: ${wss.clients.size})`);

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    const { type, username, room, text, password, color, name, roomId } = data;

    switch (type) {

      // Regular room join (DW chats)
      case 'join': {
        if (roomPasswords.has(room)) {
          if (roomPasswords.get(room) !== password) {
            ws.send(JSON.stringify({ type: 'error', text: 'Wrong password.' })); return;
          }
        } else if (password) {
          roomPasswords.set(room, password);
        }
        clients.set(ws, { username, room, color: color || '#00e5a0' });
        console.log(`[join] ${username} → #${room}`);
        broadcastUserList(room);
        break;
      }

      // Regular message
      case 'message': {
        const info = clients.get(ws);
        if (!info) break;
        const safeText = String(text).slice(0, 500);
        broadcastToRoom(info.room, { type: 'message', username: info.username, color: info.color, text: safeText });
        console.log(`[msg] ${info.username} → #${info.room}`);
        break;
      }

      // Leave
      case 'leave': {
        const info = clients.get(ws);
        if (!info) break;
        clients.delete(ws);
        if (guildRooms.has(info.room)) broadcastGuildRoomList();
        broadcastUserList(info.room);
        break;
      }

      // Join guild lobby (get room list)
      case 'guild_lobby': {
        clients.set(ws, { username, room: GUILD_LOBBY, color: color || '#00e5a0' });
        ws.send(JSON.stringify({ type: 'guild_rooms', rooms: getPublicGuildRooms() }));
        console.log(`[guild-lobby] ${username}`);
        break;
      }

      // Create a new guild room
      case 'guild_create': {
        if (guildRooms.size >= MAX_GUILD_ROOMS) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: `Maximum of ${MAX_GUILD_ROOMS} guild rooms reached. A room must expire before a new one can be created.` }));
          break;
        }
        if (!name || !name.trim()) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: 'Room name is required.' }));
          break;
        }
        if (!password) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_create', text: 'Password is required.' }));
          break;
        }
        const newId = makeRoomId();
        guildRooms.set(newId, { name: name.trim().slice(0, 30), password, lastActivity: Date.now() });
        clients.set(ws, { username, room: newId, color: color || '#00e5a0' });
        ws.send(JSON.stringify({ type: 'guild_joined', roomId: newId, roomName: name.trim() }));
        broadcastUserList(newId);
        broadcastGuildRoomList();
        console.log(`[guild-create] "${name}" by ${username}`);
        break;
      }

      // Join an existing guild room
      case 'guild_join': {
        const gr = guildRooms.get(roomId);
        if (!gr) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_join', text: 'Room no longer exists.' }));
          break;
        }
        if (gr.password !== password) {
          ws.send(JSON.stringify({ type: 'error', context: 'guild_join', text: 'Wrong password.' }));
          break;
        }
        gr.lastActivity = Date.now();
        clients.set(ws, { username, room: roomId, color: color || '#00e5a0' });
        ws.send(JSON.stringify({ type: 'guild_joined', roomId, roomName: gr.name }));
        broadcastUserList(roomId);
        broadcastGuildRoomList();
        console.log(`[guild-join] ${username} → "${gr.name}"`);
        break;
      }

      // Guild chat message
      case 'guild_message': {
        const info = clients.get(ws);
        if (!info || !guildRooms.has(info.room)) break;
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
      if (info.room !== GUILD_LOBBY) {
        broadcastUserList(info.room);
        if (guildRooms.has(info.room)) broadcastGuildRoomList();
      }
      console.log(`[-] ${info.username} left`);
    }
  });

  ws.on('error', (err) => { console.error('[error]', err.message); clients.delete(ws); });
});

console.log(`✅  Server running on ws://localhost:${PORT}`);
