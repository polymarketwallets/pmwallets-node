import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import { PmwClient } from '../src/client.js';
import { FillStream, type StreamEvent } from '../src/stream.js';

/** a local stand-in for /v1/ws that accepts one key and greets like the real gateway */
async function server() {
  const http = createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(req.url?.startsWith('/v1/latency') ? '{"head":{"block":1}}' : '{"rows":[],"next":null,"subscriptions":1}');
  });
  const wss = new WebSocketServer({ noServer: true });
  http.on('upgrade', (req, socket, head) => {
    if (req.headers['x-api-key'] !== 'good') { socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n'); socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, (ws) => {
      ws.send(JSON.stringify({ type: 'hello', userId: 'u', session: 'S', seq: 0 }));
      ws.send(JSON.stringify({ type: 'fill', session: 'S', seq: 1, data: { eventId: 'e1', block: 2, logIndex: 0 } }));
    });
  });
  await new Promise<void>((r) => http.listen(0, '127.0.0.1', r));
  const port = (http.address() as AddressInfo).port;
  return { base: `http://127.0.0.1:${port}`, close: () => { wss.close(); http.close(); } };
}

describe('FillStream against a real ws server', () => {
  it('receives frames with a good key', async () => {
    const s = await server();
    const got: string[] = [];
    const stream = new FillStream({ client: new PmwClient({ apiKey: 'good', baseUrl: s.base }), onFill: (f) => { got.push(f.eventId); } });
    await stream.start();
    for (let i = 0; i < 50 && !got.length; i++) await new Promise((r) => setTimeout(r, 20));
    await stream.stop();
    s.close();
    expect(got).toEqual(['e1']);
  });

  it('stops with a fatal event on 401', async () => {
    const s = await server();
    const events: StreamEvent[] = [];
    const stream = new FillStream({ client: new PmwClient({ apiKey: 'bad', baseUrl: s.base }), onFill: () => {}, onEvent: (e) => events.push(e), minBackoffMs: 5 });
    await stream.start();
    for (let i = 0; i < 50 && !events.some((e) => e.type === 'fatal'); i++) await new Promise((r) => setTimeout(r, 20));
    await new Promise((r) => setTimeout(r, 100));
    await stream.stop();
    s.close();
    expect(events.filter((e) => e.type === 'fatal').length).toBe(1);
    expect(events.filter((e) => e.type === 'connecting').length).toBe(1);
  });
});
