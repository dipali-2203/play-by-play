import { WebSocketServer } from "ws";
import { WebSocket } from "ws";
import { wsArcject } from "../arcjet.js";

function sendJson(socket, payload) {
    if(socket.readyState !== WebSocket.OPEN) return;

    socket.send(JSON.stringify(payload));
}

function broadcast(wss, payload) {
    for(const client of wss.clients) {
        if(client.readyState !== WebSocket.OPEN) continue;

        client.send(JSON.stringify(payload));
    }
}

export function attachWebSocketServer(server) {
    const wss = new WebSocketServer({ noServer: true, maxPayload: 1024*1024 });

    server.on('upgrade', async (req, socket, head) => {
        const { pathname } = new URL(req.url, 'http://localhost');

        if(pathname !== '/ws') {
            socket.destroy();
            return;
        }

        try {
            if(wsArcject) {
                const decision = await wsArcject.protect(req);

                if(decision.isDenied()) {
                    const isRateLimited = decision.reason.isRateLimit();
                    const status = isRateLimited ? '429 Too Many Requests' : '403 Forbidden';
                    const body = JSON.stringify({ error: isRateLimited ? 'Too many requests.' : 'Forbidden.' });

                    socket.write(
                        `HTTP/1.1 ${status}\r\n` +
                        'Content-Type: application/json\r\n' +
                        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
                        'Connection: close\r\n\r\n' +
                        body
                    );
                    socket.destroy();
                    return;
                }
            }

            wss.handleUpgrade(req, socket, head, (ws) => {
                wss.emit('connection', ws, req);
            });
        } catch(e) {
            console.error('WS upgrade error', e);
            socket.destroy();
        }
    });

    wss.on('connection', (socket, req) => {
        socket.isAlive = true;
        socket.on('pong', () => { socket.isAlive = true; });

        sendJson(socket, { type: 'welcome' });

        socket.on('error', console.error);
    });

    const interval = setInterval(() => {
        wss.clients.forEach((ws) => {
            if(ws.isAlive === false) return ws.terminate();
            ws.isAlive = false;
            ws.ping();
        });
    }, 30000);

    wss.on('close', () => clearInterval(interval));

    function broadcastMatchCreated(match) {
        broadcast(wss, {type: 'match_created', data: match });
    }

    return { broadcastMatchCreated  }
}
