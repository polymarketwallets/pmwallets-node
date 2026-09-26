import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { PmwClient, PmwError } from '../src/client.js';
import { verifyWebhook } from '../src/webhook.js';

function fakeFetch(pages: unknown[], status = 200) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = (async (url: URL, init: RequestInit) => {
    calls.push({ url: String(url), init });
    const body = pages.shift();
    return new Response(body === undefined ? '' : JSON.stringify(body), { status });
  }) as unknown as typeof fetch;
  return { fn, calls };
}

describe('PmwClient', () => {
  it('sends the key, builds the query and derives the ws url', async () => {
    const f = fakeFetch([{ rows: [], next: null, subscriptions: 0 }]);
    const c = new PmwClient({ apiKey: 'pmw_a_b', baseUrl: 'https://api.example.com/', fetch: f.fn });
    await c.fills({ sinceBlock: 5, sinceLogIndex: 2 });
    expect(f.calls[0]!.url).toBe('https://api.example.com/v1/account/fills?sinceBlock=5&sinceLogIndex=2&limit=500');
    expect((f.calls[0]!.init.headers as Record<string, string>)['x-api-key']).toBe('pmw_a_b');
    expect(c.wsUrl).toBe('wss://api.example.com/v1/ws');
  });

  it('walks every page of fills', async () => {
    const f = fakeFetch([
      { rows: [{ eventId: 'a' }, { eventId: 'b' }], next: { sinceBlock: 2, sinceLogIndex: 0 }, subscriptions: 1 },
      { rows: [{ eventId: 'c' }], next: null, subscriptions: 1 },
    ]);
    const c = new PmwClient({ apiKey: 'k', fetch: f.fn });
    const ids: string[] = [];
    for await (const r of c.fillsSince({ sinceBlock: 0, sinceLogIndex: 0 }, 2)) ids.push(r.eventId);
    expect(ids).toEqual(['a', 'b', 'c']);
    expect(f.calls[1]!.url).toContain('sinceBlock=2&sinceLogIndex=0&limit=2');
  });

  it('turns a non-2xx answer into PmwError with the parsed body', async () => {
    const f = fakeFetch([{ statusCode: 402, message: 'insufficient balance' }], 402);
    const c = new PmwClient({ apiKey: 'k', fetch: f.fn });
    const err = await c.subscribe({ entityId: '0xabc' }).catch((e) => e);
    expect(err).toBeInstanceOf(PmwError);
    expect(err.status).toBe(402);
    expect(err.body).toMatchObject({ message: 'insufficient balance' });
    expect(JSON.parse(f.calls[0]!.init.body as string)).toEqual({ channels: ['ws'], entityId: '0xabc' });
  });
});

describe('daily export files', () => {
  const W = `0x${'ab'.repeat(20)}`;
  it('reads the redirect instead of following it, so the key never reaches the storage host', async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const fn = (async (url: string, init: RequestInit) => {
      calls.push({ url: String(url), init });
      if (String(url).startsWith('https://api.example.com/')) return new Response(null, { status: 302, headers: { location: 'https://r2.example/signed' } });
      return new Response(new Uint8Array([1, 2, 3]), { status: 200 });
    }) as unknown as typeof fetch;
    const c = new PmwClient({ apiKey: 'pmw_a_b', baseUrl: 'https://api.example.com', fetch: fn });
    expect(await c.downloadExportFile(W, '2026-09-25')).toEqual(new Uint8Array([1, 2, 3]));
    expect(calls[0]!.url).toBe(`https://api.example.com/v1/account/data/${W}/2026-09-25`);
    expect(calls[0]!.init.redirect).toBe('manual');
    expect(calls[1]!.url).toBe('https://r2.example/signed');
    expect(JSON.stringify(calls[1]!.init.headers ?? {}), 'the key was sent to the storage host').not.toContain('pmw_a_b');
  });
  it('raises PmwError when there is no purchased file', async () => {
    const f = fakeFetch([{ statusCode: 404, message: 'no purchased file for this wallet and day' }], 404);
    const err = await new PmwClient({ apiKey: 'k', fetch: f.fn }).exportFileUrl(W, '2026-09-25').catch((e) => e);
    expect(err).toBeInstanceOf(PmwError);
    expect(err.status).toBe(404);
  });
  it('lists the files of an export', async () => {
    const f = fakeFetch([{ files: [{ wallet: W, day: '2026-09-25', rows: 3, bytes: 90, path: `/v1/account/data/${W}/2026-09-25` }] }]);
    const r = await new PmwClient({ apiKey: 'k', baseUrl: 'https://api.example.com', fetch: f.fn }).exportFiles('o1');
    expect(f.calls[0]!.url).toBe('https://api.example.com/v1/account/exports/o1/files');
    expect(r.files[0]!.day).toBe('2026-09-25');
  });
});

describe('verifyWebhook', () => {
  const body = '{"type":"fill","data":{}}';
  const sig = createHmac('sha256', 'sec').update(body).digest('hex');
  it('accepts the right signature and rejects everything else', () => {
    expect(verifyWebhook(body, sig, 'sec')).toBe(true);
    expect(verifyWebhook(Buffer.from(body), sig, 'sec')).toBe(true);
    expect(verifyWebhook(body + ' ', sig, 'sec')).toBe(false);
    expect(verifyWebhook(body, sig, 'other')).toBe(false);
    expect(verifyWebhook(body, 'zz', 'sec')).toBe(false);
    expect(verifyWebhook(body, undefined, 'sec')).toBe(false);
  });
});
