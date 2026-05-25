const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const clients = new Map(); // ws -> { username, room, color }
const roomPasswords = new Map();

function broadcast(room, data, excludeWs = null) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients) {
    if (info.room === room && ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastToRoom(room, data) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients) {
    if (info.room === room && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
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

wss.on('connection', (ws) => {
  console.log(`[+] New connection (total: ${wss.clients.size})`);

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    const { type, username, room, text, password, color } = data;

    switch (type) {
      case 'join': {
        if (roomPasswords.has(room)) {
          if (roomPasswords.get(room) !== password) {
            ws.send(JSON.stringify({ type: 'error', text: 'Wrong password.' }));
            return;
          }
        } else if (password) {
          roomPasswords.set(room, password);
          console.log(`[lock] #${room} is now password protected`);
        }
        clients.set(ws, { username, room, color: color || '#00e5a0' });
        console.log(`[join] ${username} → #${room}`);
        broadcastUserList(room);
        break;
      }

      case 'message': {
        const info = clients.get(ws);
        if (!info) break;
        const safeText = String(text).slice(0, 500);
        console.log(`[msg] ${info.username} in #${info.room}: ${safeText}`);
        broadcastToRoom(info.room, {
          type: 'message',
          username: info.username,
          color: info.color || '#00e5a0',
          text: safeText
        });
        break;
      }

      case 'leave': {
        const info = clients.get(ws);
        if (!info) break;
        clients.delete(ws);
        broadcastUserList(info.room);
        console.log(`[leave] ${info.username} left #${info.room}`);
        break;
      }
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
      clients.delete(ws);
      broadcastUserList(info.room);
      console.log(`[-] ${info.username} disconnected`);
    }
  });

  ws.on('error', (err) => {
    console.error('[error]', err.message);
    clients.delete(ws);
  });
});

console.log(`✅  LiveChat server running on ws://localhost:${PORT}`);
