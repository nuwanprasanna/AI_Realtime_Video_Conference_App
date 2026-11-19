'use strict';

const http = require('node:http');
const { WebSocketServer, WebSocket } = require('ws');

const port = Number(process.env.PORT || 8080);
const httpServer = http.createServer((request, response) => {
  if (request.url === '/health') {
    response.writeHead(200, { 'content-type': 'text/plain' });
    response.end('ok');
    return;
  }
  response.writeHead(404);
  response.end('not found');
});
const wss = new WebSocketServer({ server: httpServer, maxPayload: 1024 * 1024 });
const clients = new Map(); // id -> { socket, name }
const calls = new Map(); // callId -> { callerId, calleeId }
const callsByUser = new Map(); // id -> Set<callId>

function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function sendTo(id, message) {
  const client = clients.get(id);
  if (client) send(client.socket, message);
}

function broadcastUsers() {
  const users = [...clients.entries()].map(([id, user]) => ({ id, name: user.name }));
  for (const { socket } of clients.values()) send(socket, { type: 'users', users });
}

function callFor(message, senderId) {
  if (typeof message.callId !== 'string') return null;
  const call = calls.get(message.callId);
  if (!call || (call.callerId !== senderId && call.calleeId !== senderId)) return null;
  return call;
}

function endCall(callId, state, notifyId) {
  const call = calls.get(callId);
  if (!call) return;
  if (notifyId) sendTo(notifyId, { type: 'call_state', callId, state });
  calls.delete(callId);
  for (const id of [call.callerId, call.calleeId]) {
    const set = callsByUser.get(id);
    if (set) {
      set.delete(callId);
      if (set.size === 0) callsByUser.delete(id);
    }
  }
}

wss.on('connection', (socket) => {
  let clientId = null;

  socket.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch (_) {
      send(socket, { type: 'error', message: 'Invalid JSON.' });
      return;
    }
    if (!message || typeof message.type !== 'string') return;

    if (message.type === 'register') {
      const id = typeof message.userId === 'string' ? message.userId.slice(0, 100) : '';
      const name = typeof message.name === 'string' ? message.name.trim().slice(0, 32) : '';
      if (!id || !name) {
        send(socket, { type: 'error', message: 'A user id and display name are required.' });
        socket.close(1008, 'Invalid registration');
        return;
      }
      const old = clients.get(id);
      if (old && old.socket !== socket) old.socket.close(4001, 'Reconnected elsewhere');
      clientId = id;
      clients.set(id, { socket, name });
      broadcastUsers();
      return;
    }

    if (!clientId || clients.get(clientId)?.socket !== socket) return;

    if (message.type === 'rename') {
      const name = typeof message.name === 'string' ? message.name.trim().slice(0, 32) : '';
      if (name) {
        clients.get(clientId).name = name;
        broadcastUsers();
      }
      return;
    }

    if (message.type === 'invite') {
      const to = typeof message.to === 'string' ? message.to : '';
      const callId = typeof message.callId === 'string' ? message.callId.slice(0, 120) : '';
      const offer = message.offer;
      if (!to || !callId || !offer || typeof offer.sdp !== 'string') return;
      const callee = clients.get(to);
      if (!callee) {
        send(socket, { type: 'call_state', callId, state: 'offline' });
        return;
      }
      if ((callsByUser.get(clientId)?.size || 0) > 0 || (callsByUser.get(to)?.size || 0) > 0) {
        send(socket, { type: 'call_state', callId, state: 'busy' });
        return;
      }
      calls.set(callId, { callerId: clientId, calleeId: to });
      for (const id of [clientId, to]) {
        if (!callsByUser.has(id)) callsByUser.set(id, new Set());
        callsByUser.get(id).add(callId);
      }
      send(callee.socket, {
        type: 'incoming', callId, fromId: clientId,
        fromName: clients.get(clientId).name, offer,
      });
      send(socket, { type: 'call_state', callId, state: 'ringing' });
      return;
    }

    if (message.type === 'decline' || message.type === 'hangup') {
      const call = callFor(message, clientId);
      if (!call) return;
      const otherId = call.callerId === clientId ? call.calleeId : call.callerId;
      endCall(message.callId, message.type === 'decline' ? 'declined' : 'ended', otherId);
      return;
    }

    const call = callFor(message, clientId);
    if (!call) return;
    const otherId = call.callerId === clientId ? call.calleeId : call.callerId;
    if (typeof message.to === 'string' && message.to !== otherId) return;

    if (message.type === 'answer') {
      if (!message.answer || typeof message.answer.sdp !== 'string') return;
      sendTo(otherId, { type: 'answer', callId: message.callId, answer: message.answer });
      sendTo(call.callerId, { type: 'call_state', callId: message.callId, state: 'connected' });
      sendTo(call.calleeId, { type: 'call_state', callId: message.callId, state: 'connected' });
    } else if (message.type === 'candidate') {
      if (message.candidate) sendTo(otherId, { type: 'candidate', callId: message.callId, candidate: message.candidate });
    } else if (message.type === 'mask') {
      sendTo(otherId, {
        type: 'mask', callId: message.callId,
        mask: typeof message.mask === 'string' ? message.mask.slice(0, 16) : '',
      });
    }
  });

  socket.on('close', () => {
    if (!clientId || clients.get(clientId)?.socket !== socket) return;
    clients.delete(clientId);
    for (const callId of [...(callsByUser.get(clientId) || [])]) {
      const call = calls.get(callId);
      const otherId = call?.callerId === clientId ? call.calleeId : call?.callerId;
      endCall(callId, 'ended', otherId);
    }
    broadcastUsers();
  });
});

httpServer.listen(port, '0.0.0.0', () => {
  console.log(`WebRTC signaling server listening on port ${port}`);
});
