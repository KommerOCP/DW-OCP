const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Track clients: Map<ws, { username, room }>
const clients = new Map();

function broadcast(room, data, excludeWs = null) {
  const msg = JSON.stringify(data);
  for (const [ws, info] of clients) {
    if (info.room === room && ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function roomCount(room) {
  let count = 0;
  for (const [, info] of clients) {
    if (info.room === room) count++;
  }
  return count;
}

function sendCount(room) {
  const count = roomCount(room);
  const msg = JSON.stringify({ type: 'count', count });
  for (const [ws, info] of clients) {
    if (info.room === room && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

wss.on('connection', (ws) => {
  console.log(`[+] New connection (total: ${wss.clients.size})`);

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    const { type, username, room, text } = data;

    switch (type) {

      case 'join': {
        clients.set(ws, { username, room });
        console.log(`[join] ${username} → #${room}`);

        // Notify others in room
        broadcast(room, {
          type: 'system',
          text: `${username} joined the room`
        }, ws);

        // Send count to everyone in room
        sendCount(room);
        break;
      }

      case 'message': {
        const info = clients.get(ws);
        if (!info) break;

        const safeText = String(text).slice(0, 500);
        console.log(`[msg] ${info.username} in #${info.room}: ${safeText}`);

        // Echo to sender too
        const payload = { type: 'message', username: info.username, text: safeText };
        broadcast(info.room, payload);
        break;
      }

      case 'leave': {
        const info = clients.get(ws);
        if (!info) break;
        console.log(`[leave] ${info.username} left #${info.room}`);
        broadcast(info.room, { type: 'system', text: `${info.username} left the room` }, ws);
        clients.delete(ws);
        sendCount(info.room);
        break;
      }
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
      console.log(`[-] ${info.username} disconnected from #${info.room}`);
      clients.delete(ws);
      broadcast(info.room, { type: 'system', text: `${info.username} disconnected` });
      sendCount(info.room);
    } else {
      console.log(`[-] Anonymous connection closed`);
    }
  });

  ws.on('error', (err) => {
    console.error('[error]', err.message);
    clients.delete(ws);
  });
});

console.log(`✅  LiveChat server running on ws://localhost:${PORT}`);
