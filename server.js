const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  if (pathname !== '/' && pathname !== '/index.html') {
    res.writeHead(404); return res.end('Not found');
  }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('App file missing'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function leave(ws) {
  const room = ws.room;
  if (!room) return;
  const peers = rooms.get(room);
  if (peers) {
    peers.delete(ws);
    for (const peer of peers) send(peer, { type: 'peer-left' });
    if (peers.size === 0) rooms.delete(room);
  }
  ws.room = null;
}

wss.on('connection', ws => {
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'join') {
      const roomId = String(msg.room || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 40);
      if (!roomId) return send(ws, { type: 'error', message: 'Invalid room ID.' });
      let peers = rooms.get(roomId);
      const isNew = !peers || peers.size === 0;
      if (!peers) { peers = new Set(); rooms.set(roomId, peers); }
      if (peers.size >= 2) return send(ws, { type: 'room-full' });
      ws.room = roomId;
      peers.add(ws);
      send(ws, { type: isNew ? 'created' : 'joined' });
      if (!isNew) {
        for (const peer of peers) if (peer !== ws) send(peer, { type: 'peer-joined' });
      }
      return;
    }

    if (msg.type === 'leave') { leave(ws); return; }
    if (!ws.room) return;
    const peers = rooms.get(ws.room);
    if (!peers) return;
    const allowed = new Set(['ready', 'offer', 'answer', 'ice']);
    if (!allowed.has(msg.type)) return;
    for (const peer of peers) if (peer !== ws) send(peer, msg);
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

server.listen(PORT, () => console.log(`Orbit Video is running on http://localhost:${PORT}`));
