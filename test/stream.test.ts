import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { FileStateStore, FillStream, MemoryStateStore, type SocketLike, type StreamEvent } from '../src/stream.js';
import type { PmwClient } from '../src/client.js';
import type { Fill, FillCursor } from '../src/types.js';

const fill = (block: number, logIndex: number): Fill => ({
  eventId: `137:${block}:0xh:0xt${block}:${logIndex}`, chain: 137, entityId: '0xe', wallet: '0xw', ts: '2026-09-24 00:00:00',
  block, blockHash: '0xh', txHash: `0xt${block}`, logIndex, exchange: 'pm_ctf_v2', side: 'BUY', role: 'taker',
  tokenId: '1', price: '0.5', shares: '1000000', usdc: '500000', fee: '0',
});

/** a scripted socket: the test pushes frames and closes it */
class FakeSocket implements SocketLike {
  handlers: Record<string, ((...a: any[]) => void)[]> = {};
  terminated = false;
  on(ev: string, cb: (...a: any[]) => void) { (this.handlers[ev] ??= []).push(cb); return this; }
  fire(ev: string, ...a: unknown[]) { for (const h of this.handlers[ev] ?? []) h(...a); }
  send(frame: object) { this.fire('message', Buffer.from(JSON.stringify(frame))); }
  ping() {}
  close(code = 1000) { this.fire('close', code, Buffer.from('')); }
  terminate() { this.terminated = true; this.fire('close', 1006, Buffer.from('')); }
}

function harness(ledger: Fill[], opts: { failOn?: string; head?: number; takeover?: boolean } = {}) {
  const sockets: FakeSocket[] = [];
  const urls: URL[] = [];
  const replays: FillCursor[] = [];
  const delivered: { id: string; source: string }[] = [];
  const events: StreamEvent[] = [];
  let failOnce = opts.failOn;
  const client = {
    apiKey: 'pmw_x_y',
    wsUrl: 'wss://example/v1/ws',
    async latency() { return { head: { block: opts.head ?? 209 } }; },
    async *fillsSince(c: FillCursor) {
      replays.push(c);
      for (const f of ledger) if (f.block > c.sinceBlock || (f.block === c.sinceBlock && f.logIndex > c.sinceLogIndex)) yield f;
    },
  } as unknown as PmwClient;
  const store = new MemoryStateStore();
  const stream = new FillStream({
    client, store, minBackoffMs: 1, maxBackoffMs: 2, takeover: opts.takeover,
    socketFactory: (url) => { urls.push(new URL(url)); const s = new FakeSocket(); sockets.push(s); queueMicrotask(() => s.fire('open')); return s; },
    onFill: (f, meta) => {
      if (failOnce === f.eventId) { failOnce = undefined; throw new Error('handler failed'); }
      delivered.push({ id: f.eventId, source: meta.source });
    },
    onEvent: (e) => events.push(e),
  });
  return { stream, sockets, replays, delivered, events, store, urls };
}

const tick = () => new Promise((r) => setTimeout(r, 10));

describe('FillStream', () => {
  it('delivers consecutive frames in order and tracks the cursor', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    const s = h.sockets[0]!;
    s.send({ type: 'hello', session: 'A', seq: 0 });
    s.send({ type: 'fill', session: 'A', seq: 1, data: fill(10, 1) });
    s.send({ type: 'fill', session: 'A', seq: 2, data: fill(10, 2) });
    await tick();
    expect(h.delivered.map((d) => d.id)).toEqual([fill(10, 1).eventId, fill(10, 2).eventId]);
    expect(h.replays).toEqual([]); // first hello ever: nothing to replay
    expect(h.stream.position).toMatchObject({ session: 'A', seq: 2, block: 10, logIndex: 2 });
    await h.stream.stop();
  });

  it('replays the gap on a seq skip, then drops the duplicate live frame', async () => {
    const ledger = [fill(10, 1), fill(11, 1), fill(12, 1)];
    const h = harness(ledger);
    await h.stream.start(); await tick();
    const s = h.sockets[0]!;
    s.send({ type: 'hello', session: 'A', seq: 0 });
    s.send({ type: 'fill', session: 'A', seq: 1, data: ledger[0] });
    s.send({ type: 'fill', session: 'A', seq: 3, data: ledger[2] }); // seq 2 (block 11) was dropped
    await tick();
    expect(h.replays).toEqual([{ sinceBlock: 10, sinceLogIndex: 1 }]);
    expect(h.delivered).toEqual([
      { id: ledger[0]!.eventId, source: 'ws' },
      { id: ledger[1]!.eventId, source: 'replay' },
      { id: ledger[2]!.eventId, source: 'replay' },
    ]);
    expect(h.stream.position).toMatchObject({ seq: 3, block: 12 });
    await h.stream.stop();
  });

  it('replays everything missed while disconnected when the next hello brings a new session', async () => {
    const ledger = [fill(10, 1), fill(11, 1)];
    const h = harness(ledger);
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'hello', session: 'A', seq: 0 });
    h.sockets[0]!.send({ type: 'fill', session: 'A', seq: 1, data: ledger[0] });
    await tick();
    h.sockets[0]!.close(1006);
    await tick();
    h.sockets[1]!.send({ type: 'hello', session: 'B', seq: 0 });
    await tick();
    expect(h.replays).toEqual([{ sinceBlock: 10, sinceLogIndex: 1 }]);
    expect(h.delivered.map((d) => d.id)).toEqual([ledger[0]!.eventId, ledger[1]!.eventId]);
    expect(h.stream.position.session).toBe('B');
    await h.stream.stop();
  });

  it('a throwing handler leaves the fill undelivered and it comes back after the reconnect', async () => {
    const ledger = [fill(10, 1), fill(11, 1)];
    const h = harness(ledger, { failOn: ledger[1]!.eventId });
    await h.stream.start(); await tick();
    const s = h.sockets[0]!;
    s.send({ type: 'hello', session: 'A', seq: 0 });
    s.send({ type: 'fill', session: 'A', seq: 1, data: ledger[0] });
    s.send({ type: 'fill', session: 'A', seq: 2, data: ledger[1] });
    await tick();
    expect(s.terminated).toBe(true);
    expect(h.stream.position).toMatchObject({ seq: 1, block: 10 }); // not advanced past the failure
    h.sockets[1]!.send({ type: 'hello', session: 'B', seq: 0 });
    await tick();
    expect(h.delivered.map((d) => d.id)).toEqual([ledger[0]!.eventId, ledger[1]!.eventId]);
    await h.stream.stop();
  });

  it('stops for good on 401 instead of hammering the server with a bad key', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    h.sockets[0]!.fire('unexpected-response', {}, { statusCode: 401 });
    await tick(); await tick();
    expect(h.events.some((e) => e.type === 'fatal')).toBe(true);
    expect(h.sockets.length).toBe(1);
    await h.stream.stop();
  });

  it('replaced by another client\'s takeover: stops for good instead of taking the stream back', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    h.sockets[0]!.fire('close', 1000, Buffer.from('replaced by a newer connection'));
    await tick(); await tick();
    expect(h.events.some((e) => e.type === 'replaced')).toBe(true);
    expect(h.events.some((e) => e.type === 'fatal')).toBe(true);
    expect(h.sockets.length, 'reconnecting would take the stream back and the two would fight').toBe(1);
    await h.stream.stop();
  });

  it('refused because another client holds the stream (4411): stops for good and says how to take over', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'blocked', reason: 'api_seat_taken' });
    h.sockets[0]!.fire('close', 4411, Buffer.from('another API client is connected'));
    await tick(); await tick();
    expect(h.events.some((e) => e.type === 'blocked')).toBe(true);
    const fatal = h.events.find((e) => e.type === 'fatal') as { error: Error } | undefined;
    expect(fatal?.error.message).toMatch(/takeover/);
    expect(h.sockets.length).toBe(1);
    await h.stream.stop();
  });

  it('a server restart (1012) reconnects at once, without waiting out a backoff', async () => {
    const h = harness([]);
    // a backoff long enough that a test would time out waiting for it
    (h.stream as unknown as { opts: { minBackoffMs: number; maxBackoffMs: number } }).opts.minBackoffMs = 60_000;
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'hello', session: 'A', seq: 0 });
    await tick();
    h.sockets[0]!.fire('close', 1012, Buffer.from('service restart; reconnect now'));
    await tick(); await tick();
    expect(h.events.some((e) => e.type === 'restarting')).toBe(true);
    expect(h.sockets.length).toBe(2);
    await h.stream.stop();
  });

  it('sends its clientId on every connection; takeover only until it is first seated', async () => {
    const h = harness([], { takeover: true });
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'hello', session: 'A', seq: 0 });
    await tick();
    h.sockets[0]!.fire('close', 1006, Buffer.from(''));
    await tick(); await tick();
    expect(h.urls.length).toBe(2);
    const id = h.urls[0]!.searchParams.get('clientId');
    expect(id).toMatch(/^[A-Za-z0-9_-]{8,64}$/);
    expect(h.urls[1]!.searchParams.get('clientId'), 'the same id, so the server knows it is us coming back').toBe(id);
    expect(h.urls[0]!.searchParams.get('takeover')).toBe('1');
    expect(h.urls[1]!.searchParams.get('takeover'), 'a reconnect must not snatch the stream from a client that took it meanwhile').toBeNull();
    await h.stream.stop();
  });

  it('never asks to take over unless told to', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    expect(h.urls[0]!.searchParams.get('takeover')).toBeNull();
    await h.stream.stop();
  });

  it('refuses a malformed clientId up front', () => {
    expect(() => new FillStream({ client: {} as PmwClient, onFill: () => undefined, clientId: 'short' })).toThrow(/clientId/);
  });
});

describe('FillStream starting point', () => {
  it('anchors behind the chain head on the first hello, so a disconnect before the first fill is still replayed', async () => {
    // head 300, lag 200 → replay covers blocks ≥ 100. Block 99 is history; block 150 was mined before
    // we connected but had not been pushed yet (the head ran ahead of the fill index).
    const ledger = [fill(99, 3), fill(150, 1), fill(301, 1)];
    const h = harness(ledger, { head: 300 });
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'hello', session: 'A', seq: 0 });
    await tick();
    expect(h.stream.position).toMatchObject({ block: 99, logIndex: 0xffffffff });
    expect(h.events).toContainEqual({ type: 'anchored', block: 100 });
    h.sockets[0]!.close(1006); await tick();
    h.sockets[1]!.send({ type: 'hello', session: 'B', seq: 0 });
    await tick();
    expect(h.replays).toEqual([{ sinceBlock: 99, sinceLogIndex: 0xffffffff }]);
    expect(h.delivered.map((d) => d.id)).toEqual([ledger[1]!.eventId, ledger[2]!.eventId]);
    await h.stream.stop();
  });
});

describe('FileStateStore', () => {
  it('round-trips and returns null when there is no file yet', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'pmw-'));
    const store = new FileStateStore(join(dir, 'sub', 'state.json'));
    expect(await store.load()).toBeNull();
    await store.save({ session: 's', seq: 3, block: 9, logIndex: 2 });
    expect(await store.load()).toEqual({ session: 's', seq: 3, block: 9, logIndex: 2 });
    expect(JSON.parse(await readFile(store.path, 'utf8')).block).toBe(9);
  });
});
