import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { PassThrough } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { parseArgs } from '../src/args.js';
import type { PmwClient } from '../src/client.js';
import { MarketLookup, microToDecimal } from '../src/market.js';
import { serveMcp, type McpTool } from '../src/mcp.js';
import { CursorFile, newFills, pmwalletsTools } from '../src/tools.js';
import type { Fill, FillsPage } from '../src/types.js';
import { VERSION } from '../src/version.js';
import { noProxy } from '../src/proxy.js';

describe('microToDecimal', () => {
  it('converts 1e-6 integer units exactly', () => {
    expect(microToDecimal('1824000000')).toBe('1824.000000');
    expect(microToDecimal('1')).toBe('0.000001');
    expect(microToDecimal('0')).toBe('0.000000');
    expect(microToDecimal('-2500000')).toBe('-2.500000');
    expect(microToDecimal('123456789012345678901234567890')).toBe('123456789012345678901234.567890');
    expect(microToDecimal('1.5')).toBeNull();
    expect(microToDecimal('')).toBeNull();
  });
});

describe('MarketLookup', () => {
  const CID = `0x${'ab'.repeat(32)}`;
  function fake(fail = false) {
    const urls: string[] = [];
    const f = (async (url: string) => {
      urls.push(url);
      if (fail) return new Response('no', { status: 500 });
      if (url.includes('/markets-by-token/')) return Response.json({ condition_id: CID });
      return Response.json({ question: 'Xi out?', market_slug: 'xi', end_date_iso: '2027-01-01T00:00:00Z', closed: false, tokens: [{ token_id: '5', outcome: 'Yes' }, { token_id: '6', outcome: 'No' }] });
    }) as unknown as typeof fetch;
    return { urls, f };
  }

  it('finds the question and the outcome of a token, and caches it', async () => {
    const { urls, f } = fake();
    const m = new MarketLookup({ fetch: f });
    expect(await m.byToken('6')).toEqual({ conditionId: CID, question: 'Xi out?', outcome: 'No', slug: 'xi', endDate: '2027-01-01T00:00:00Z', closed: false });
    await m.byToken('6');
    expect(urls).toHaveLength(2);
  });

  it('never puts a non-canonical id into a URL, and answers null on failure', async () => {
    const { urls, f } = fake(true);
    const m = new MarketLookup({ fetch: f });
    expect(await m.byToken('../x')).toBeNull();
    expect(await m.byToken('0x12')).toBeNull();
    expect(await m.byToken('007')).toBeNull();
    expect(urls).toHaveLength(0);
    expect(await m.byToken('5')).toBeNull();
    expect(urls).toHaveLength(1);
  });
});

describe('parseArgs', () => {
  it('reads --a b, --a=b, boolean flags and positionals', () => {
    expect(parseArgs(['X', '--period', '7d', '--limit=5', '--takeover', '--exec', 'claude -p "hi"', 'Y'], ['takeover']))
      .toEqual({ pos: ['X', 'Y'], flags: { period: '7d', limit: '5', takeover: true, exec: 'claude -p "hi"' } });
    expect(parseArgs(['--dir'], []).flags).toEqual({ dir: true });
  });
});

async function rpc(tools: McpTool[], messages: object[]): Promise<any[]> {
  const input = new PassThrough();
  const output = new PassThrough();
  const chunks: string[] = [];
  output.on('data', (c) => chunks.push(String(c)));
  const done = serveMcp({ name: 't', version: '1', tools, input, output, instructions: 'hello' });
  for (const m of messages) input.write(typeof m === 'string' ? `${m}\n` : `${JSON.stringify(m)}\n`);
  input.end();
  await done;
  return chunks.join('').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
}

describe('serveMcp', () => {
  const echo: McpTool = { name: 'echo', description: 'd', inputSchema: { type: 'object' }, handler: async (a) => ({ got: a }) };
  const boom: McpTool = { name: 'boom', description: 'd', inputSchema: { type: 'object' }, handler: async () => { throw Object.assign(new Error('x'), { status: 402, body: { error: 'insufficient_balance' } }); } };

  it('speaks initialize / tools/list / tools/call, and answers no notification', async () => {
    const out = await rpc([echo, boom], [
      { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'c', version: '1' } } },
      { jsonrpc: '2.0', method: 'notifications/initialized' },
      { jsonrpc: '2.0', id: 2, method: 'tools/list' },
      { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'echo', arguments: { a: 1 } } },
      { jsonrpc: '2.0', id: 4, method: 'tools/call', params: { name: 'boom', arguments: {} } },
      { jsonrpc: '2.0', id: 5, method: 'tools/call', params: { name: 'nope' } },
      { jsonrpc: '2.0', id: 6, method: 'resources/list' },
      { jsonrpc: '2.0', id: 7, method: 'ping' },
      'not json',
    ]);
    const by = new Map(out.map((m) => [m.id, m]));
    expect(by.get(1).result).toMatchObject({ protocolVersion: '2025-03-26', capabilities: { tools: {} }, serverInfo: { name: 't' }, instructions: 'hello' });
    expect(by.get(2).result.tools.map((t: any) => t.name)).toEqual(['echo', 'boom']);
    expect(JSON.parse(by.get(3).result.content[0].text)).toEqual({ got: { a: 1 } });
    expect(by.get(4).result.isError).toBe(true);
    expect(by.get(4).result.content[0].text).toBe('HTTP 402: {"error":"insufficient_balance"}');
    expect(by.get(5).error.code).toBe(-32602);
    expect(by.get(6).error.code).toBe(-32601);
    expect(by.get(7).result).toEqual({});
    expect(by.get(null).error.code).toBe(-32700);
    expect(out).toHaveLength(8); // the notification got no answer
  });

  it('offers the latest protocol version to a client asking for an unknown one', async () => {
    const [m] = await rpc([], [{ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '1999-01-01' } }]);
    expect(m.result.protocolVersion).toBe('2025-06-18');
  });
});

describe('new_fills', () => {
  const row = (block: number, logIndex: number): Fill => ({
    eventId: `137:${block}:h:t:${logIndex}`, chain: 137, entityId: '0xe', wallet: '0xw', ts: '2026-10-14 00:00:00', block, blockHash: 'h', txHash: 't',
    logIndex, exchange: 'x', side: 'SELL', role: 'maker', tokenId: '9', price: '0.5', shares: '2000000', usdc: '1000000', fee: '0',
  });
  function fakeClient(rows: Fill[]) {
    const asked: { sinceBlock: number; sinceLogIndex: number; limit?: number }[] = [];
    const c = {
      async latency() { return { head: { block: 5000 } }; },
      async fills(q: { sinceBlock: number; sinceLogIndex: number; limit?: number }): Promise<FillsPage> {
        asked.push(q);
        const after = rows.filter((r) => r.block > q.sinceBlock || (r.block === q.sinceBlock && r.logIndex > q.sinceLogIndex));
        const page = after.slice(0, q.limit ?? 500);
        return { rows: page, next: after.length > page.length ? { sinceBlock: page.at(-1)!.block, sinceLogIndex: page.at(-1)!.logIndex } : null, subscriptions: 1 };
      },
    } as unknown as PmwClient;
    return { c, asked };
  }

  it('starts an hour back, hands each fill out once, and keeps its place in a file', async () => {
    const file = new CursorFile(join(mkdtempSync(join(tmpdir(), 'pmw-cur-')), 'c.json'));
    const { c, asked } = fakeClient([row(3000, 1), row(4000, 2), row(4000, 5), row(4500, 0)]);
    const first = await newFills(c, file, null, 2) as { fills: { block: number; usdcAmount: string }[]; more: boolean; note?: string };
    expect(asked[0]).toEqual({ sinceBlock: 3200, sinceLogIndex: 0, limit: 2 });
    expect(first.fills.map((f) => f.block)).toEqual([4000, 4000]);
    expect(first.fills[0]!.usdcAmount).toBe('1.000000');
    expect(first.more).toBe(true);
    expect(first.note).toMatch(/first call/);
    expect(JSON.parse(readFileSync(file.path, 'utf8'))).toEqual({ sinceBlock: 4000, sinceLogIndex: 5 });
    const second = await newFills(c, file, null, 2) as { fills: { block: number }[]; more: boolean };
    expect(second.fills.map((f) => f.block)).toEqual([4500]);
    expect(second.more).toBe(false);
    const third = await newFills(c, file, null, 2) as { fills: unknown[] };
    expect(third.fills).toEqual([]);
    expect(file.load()).toEqual({ sinceBlock: 4500, sinceLogIndex: 0 });
  });

  it('concurrent calls hand out disjoint fills, and the cursor never moves back', async () => {
    const file = new CursorFile(join(mkdtempSync(join(tmpdir(), 'pmw-cur-')), 'c.json'));
    file.save({ sinceBlock: 3999, sinceLogIndex: 0 });
    const { c } = fakeClient([row(4000, 1), row(4000, 2), row(4001, 0), row(4002, 0)]);
    const [a, b] = await Promise.all([newFills(c, file, null, 2), newFills(c, file, null, 2)]) as { fills: { eventId: string }[] }[];
    const ids = [...a!.fills, ...b!.fills].map((f) => f.eventId);
    expect(new Set(ids).size).toBe(4);
    // another process went further: an older answer must not rewind it
    file.save({ sinceBlock: 9000, sinceLogIndex: 0 });
    const { c: c2 } = fakeClient([row(9500, 0)]);
    file.save({ sinceBlock: 9600, sinceLogIndex: 0 });
    await newFills(c2, new CursorFile(file.path), null, 2);
    expect(file.load()).toEqual({ sinceBlock: 9600, sinceLogIndex: 0 });
  });

  it('the cursor moves only once the answer is complete (an interrupted market lookup loses nothing)', async () => {
    const file = new CursorFile(join(mkdtempSync(join(tmpdir(), 'pmw-cur-')), 'c.json'));
    file.save({ sinceBlock: 3999, sinceLogIndex: 0 });
    const { c } = fakeClient([row(4000, 1)]);
    const failing = { byToken: async () => { throw new Error('interrupted'); } } as unknown as MarketLookup;
    await expect(newFills(c, file, failing, 10)).rejects.toThrow(/interrupted/);
    expect(file.load()).toEqual({ sinceBlock: 3999, sinceLogIndex: 0 });
  });

  it('one file reached by two spellings of its path is still one queue', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-cur-'));
    const abs = join(dir, 'c.json');
    const rel = relative(process.cwd(), abs);
    new CursorFile(abs).save({ sinceBlock: 3999, sinceLogIndex: 0 });
    const { c } = fakeClient([row(4000, 1), row(4000, 2), row(4001, 0), row(4002, 0)]);
    const [a, b] = await Promise.all([newFills(c, new CursorFile(abs), null, 2), newFills(c, new CursorFile(rel), null, 2)]) as { fills: { eventId: string }[] }[];
    expect(new Set([...a!.fills, ...b!.fills].map((f) => f.eventId)).size).toBe(4);
  });

  it('every tool tells an agent how to fix a missing API key', async () => {
    const tools = pmwalletsTools(null, { markets: null });
    await expect(tools.find((t) => t.name === 'subscriptions')!.handler({})).rejects.toThrow(/PMW_API_KEY/);
  });
});

it('VERSION matches package.json', () => {
  expect(VERSION).toBe(JSON.parse(readFileSync(join(__dirname, '..', 'package.json'), 'utf8')).version);
});

describe('noProxy', () => {
  it('matches hosts, domain suffixes and *', () => {
    expect(noProxy('127.0.0.1', 'localhost,127.0.0.1')).toBe(true);
    expect(noProxy('api.pmwallets.com', '.pmwallets.com')).toBe(true);
    expect(noProxy('api.pmwallets.com', 'pmwallets.com')).toBe(true);
    expect(noProxy('evilpmwallets.com', 'pmwallets.com')).toBe(false);
    expect(noProxy('api.pmwallets.com', 'localhost')).toBe(false);
    expect(noProxy('x', '*')).toBe(true);
    expect(noProxy('x', '')).toBe(false);
  });
});
