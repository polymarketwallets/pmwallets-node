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

function harness(ledger: Fill[], opts: { failOn?: string } = {}) {
  const sockets: FakeSocket[] = [];
  const replays: FillCursor[] = [];
  const delivered: { id: string; source: string }[] = [];
  const events: StreamEvent[] = [];
  let failOnce = opts.failOn;
  const client = {
    apiKey: 'pmw_x_y',
    wsUrl: 'wss://example/v1/ws',
    async *fillsSince(c: FillCursor) {
      replays.push(c);
      for (const f of ledger) if (f.block > c.sinceBlock || (f.block === c.sinceBlock && f.logIndex > c.sinceLogIndex)) yield f;
    },
  } as unknown as PmwClient;
  const store = new MemoryStateStore();
  const stream = new FillStream({
    client, store, minBackoffMs: 1, maxBackoffMs: 2,
    socketFactory: () => { const s = new FakeSocket(); sockets.push(s); queueMicrotask(() => s.fire('open')); return s; },
    onFill: (f, meta) => {
      if (failOnce === f.eventId) { failOnce = undefined; throw new Error('handler failed'); }
      delivered.push({ id: f.eventId, source: meta.source });
    },
    onEvent: (e) => events.push(e),
  });
  return { stream, sockets, replays, delivered, events, store };
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

  it('reports a takeover by another connection', async () => {
    const h = harness([]);
    await h.stream.start(); await tick();
    h.sockets[0]!.fire('close', 1000, Buffer.from('replaced by a newer connection'));
    await tick();
    expect(h.events.some((e) => e.type === 'replaced')).toBe(true);
    await h.stream.stop();
  });
});

describe('FillStream without a cursor', () => {
  it('does not replay all history when nothing was delivered yet (unless asked to)', async () => {
    const h = harness([fill(5, 1)]);
    await h.stream.start(); await tick();
    h.sockets[0]!.send({ type: 'hello', session: 'A', seq: 0 });
    await tick();
    h.sockets[0]!.close(1006); await tick();
    h.sockets[1]!.send({ type: 'hello', session: 'B', seq: 0 });
    await tick();
    expect(h.replays).toEqual([]);
    expect(h.events.some((e) => e.type === 'gap' && e.skipped === 'no_cursor')).toBe(true);
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
