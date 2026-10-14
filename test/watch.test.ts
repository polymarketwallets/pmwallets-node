import { existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, readdirSync, utimesSync, writeFileSync } from 'node:fs';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { afterEach, describe, expect, it } from 'vitest';
import type { PmwClient } from '../src/client.js';
import type { MarketLookup } from '../src/market.js';
import type { SocketLike } from '../src/stream.js';
import type { Fill } from '../src/types.js';
import { Watcher, type WatchedFill } from '../src/watch.js';

const fill = (block: number, logIndex = 0): Fill => ({
  eventId: `137:${block}:0xh:0xt${block}:${logIndex}`, chain: 137, entityId: '0xe', wallet: '0xw', ts: '2026-10-14 00:00:00',
  block, blockHash: '0xh', txHash: `0xt${block}`, logIndex, exchange: 'pm_ctf_v2', side: 'BUY', role: 'taker',
  tokenId: '7', price: '0.42', shares: '3200000000', usdc: '1344000000', fee: '0',
});

class FakeSocket implements SocketLike {
  handlers: Record<string, ((...a: any[]) => void)[]> = {};
  on(ev: string, cb: (...a: any[]) => void) { (this.handlers[ev] ??= []).push(cb); return this; }
  fire(ev: string, ...a: unknown[]) { for (const h of this.handlers[ev] ?? []) h(...a); }
  send(frame: object) { this.fire('message', Buffer.from(JSON.stringify(frame))); }
  ping() {}
  close(code = 1000) { this.fire('close', code, Buffer.from('')); }
  terminate() { this.fire('close', 1006, Buffer.from('')); }
}

const client = {
  apiKey: 'pmw_x_y', wsUrl: 'wss://example/v1/ws',
  async latency() { return { head: { block: 1000 } }; },
  async *fillsSince() { /* nothing missed */ },
} as unknown as PmwClient;

const markets = { byToken: async () => ({ conditionId: '0xc', question: 'Will it rain?', outcome: 'Yes', slug: 'rain', endDate: null, closed: false }) } as unknown as MarketLookup;

const watchers: Watcher[] = [];
afterEach(async () => { for (const w of watchers.splice(0)) await w.stop(); });

function setup(opts: { exec?: string; batchMs?: number; execTimeoutMs?: number; dir?: string } = {}) {
  const dir = opts.dir ?? mkdtempSync(join(tmpdir(), 'pmw-watch-'));
  const sockets: FakeSocket[] = [];
  const lines: string[] = [];
  const events: Record<string, unknown>[] = [];
  const w = new Watcher({
    client, dir, exec: opts.exec, batchMs: opts.batchMs ?? 0, execTimeoutMs: opts.execTimeoutMs, markets,
    out: async (l) => { lines.push(l); },
    log: (e) => events.push(e),
    streamOptions: {
      minBackoffMs: 1, maxBackoffMs: 2,
      socketFactory: () => { const s = new FakeSocket(); sockets.push(s); queueMicrotask(() => s.fire('open')); return s; },
    },
  });
  watchers.push(w);
  let seq = 0;
  const push = (f: Fill) => sockets[sockets.length - 1]!.send({ type: 'fill', session: 's1', seq: ++seq, data: f });
  const hello = () => sockets[sockets.length - 1]!.send({ type: 'hello', session: 's1', seq: 0 });
  return { w, dir, lines, events, push, hello };
}

/** lock files held by this process (a released one stays on disk, marked released) */
const heldLocks = (dir: string) => readdirSync(dir).filter((n) => {
  if (!n.startsWith('watch.lock.')) return false;
  try { const o = JSON.parse(readFileSync(join(dir, n), 'utf8')) as { released?: boolean; pid?: number }; return !o.released && o.pid === process.pid; } catch { return false; }
});

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until(cond: () => boolean, ms = 5_000) {
  const end = Date.now() + ms;
  while (!cond()) { if (Date.now() > end) throw new Error('timed out'); await wait(10); }
}

/** a command that appends its stdin (and the batch file name) to $OUT, one block per batch */
const RECORD = `node -e "const fs=require('fs');fs.appendFileSync(process.env.OUT, JSON.stringify({file:process.env.PMW_BATCH_FILE,size:process.env.PMW_BATCH_SIZE,stdin:fs.readFileSync(0,'utf8')})+'\\n')"`;

describe('Watcher', () => {
  it('without a command prints one line per fill, with decimal amounts and the market', async () => {
    const h = setup();
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'connected'));
    h.hello();
    await until(() => h.events.some((e) => e['event'] === 'hello'));
    h.push(fill(1001));
    await until(() => h.lines.length === 1);
    const f = JSON.parse(h.lines[0]!) as WatchedFill;
    expect(f.eventId).toBe(fill(1001).eventId);
    expect(f.usdcAmount).toBe('1344.000000');
    expect(f.sharesAmount).toBe('3200.000000');
    expect(f.market?.question).toBe('Will it rain?');
    expect(f.market?.outcome).toBe('Yes');
    expect(f.source).toBe('ws');
  });

  it('hands fills to the command as JSON lines on stdin, and batches the ones that arrive while it runs', async () => {
    const out = join(mkdtempSync(join(tmpdir(), 'pmw-out-')), 'calls.jsonl');
    process.env['OUT'] = out;
    const slowRecord = `${RECORD} && node -e "setTimeout(()=>{},300)"`;
    const h = setup({ exec: slowRecord });
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'connected'));
    h.hello();
    await until(() => h.events.some((e) => e['event'] === 'hello'));
    h.push(fill(1001));
    await until(() => h.events.some((e) => e['event'] === 'batch_start'));
    h.push(fill(1002)); h.push(fill(1003));
    await until(() => h.events.filter((e) => e['event'] === 'batch_done').length === 2);
    const calls = readFileSync(out, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { stdin: string; size: string; file: string });
    expect(calls.map((c) => c.size)).toEqual(['1', '2']);
    expect(h.events.filter((e) => e['event'] === 'batch_start').map((e) => e['queued'])).toEqual([0, 0]);
    const second = calls[1]!.stdin.trim().split('\n').map((l) => JSON.parse(l) as WatchedFill);
    expect(second.map((f) => f.block)).toEqual([1002, 1003]);
    expect(calls[0]!.file).toMatch(/running-.*\.jsonl$/);
    // a finished batch leaves nothing behind
    expect(readdirSync(h.dir).filter((n) => /^(running|batch)-/.test(n))).toEqual([]);
    expect(readFileSync(join(h.dir, 'queue.jsonl'), 'utf8')).toBe('');
  });

  it('a failing or hanging command moves its batch to failed/ and the next batch still runs', async () => {
    const h = setup({ exec: 'node -e "process.exit(3)"' });
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'connected'));
    h.hello();
    await until(() => h.events.some((e) => e['event'] === 'hello'));
    h.push(fill(1001));
    await until(() => h.events.some((e) => e['event'] === 'batch_failed'));
    const failed = h.events.find((e) => e['event'] === 'batch_failed')!;
    expect(failed['reason']).toBe('exit code 3');
    expect(failed['eventIds']).toEqual([fill(1001).eventId]);
    expect(readdirSync(join(h.dir, 'failed'))).toHaveLength(1);

    const t = setup({ exec: 'node -e "setTimeout(()=>{},10000)"', execTimeoutMs: 200 });
    await t.w.start();
    await until(() => t.events.some((e) => e['event'] === 'connected'));
    t.hello();
    await until(() => t.events.some((e) => e['event'] === 'hello'));
    t.push(fill(1001));
    await until(() => t.events.some((e) => e['event'] === 'batch_failed'));
    expect(String(t.events.find((e) => e['event'] === 'batch_failed')!['reason'])).toMatch(/timed out/);
  });

  it('a timeout or a stop ends the whole command tree, and the lock is held until the batch is over', async () => {
    const pidFile = join(mkdtempSync(join(tmpdir(), 'pmw-pid-')), 'grandchild.pid');
    // a shell that starts a long-lived grandchild (as an agent CLI does) and waits for it
    const tree = `node -e "const c=require('child_process').spawn(process.execPath,['-e','setTimeout(()=>{},60000)'],{stdio:'ignore'});require('fs').writeFileSync('${pidFile}',String(c.pid));setTimeout(()=>{},60000)"`;
    const t = setup({ exec: tree, execTimeoutMs: 300 });
    await t.w.start();
    await until(() => t.events.some((e) => e['event'] === 'connected'));
    t.hello();
    await until(() => t.events.some((e) => e['event'] === 'hello'));
    t.push(fill(1001));
    await until(() => t.events.some((e) => e['event'] === 'batch_failed'), 8_000);
    const gc = Number(readFileSync(pidFile, 'utf8'));
    await until(() => { try { process.kill(gc, 0); return false; } catch { return true; } }, 8_000);

    const s = setup({ exec: tree, execTimeoutMs: 60_000 });
    await s.w.start();
    await until(() => s.events.some((e) => e['event'] === 'connected'));
    s.hello();
    await until(() => s.events.some((e) => e['event'] === 'hello'));
    s.push(fill(2001));
    await until(() => s.events.some((e) => e['event'] === 'batch_start'));
    await until(() => existsSync(pidFile) && Number(readFileSync(pidFile, 'utf8')) !== gc);
    const gc2 = Number(readFileSync(pidFile, 'utf8'));
    await s.w.stop();
    // by the time stop() returns: the batch is in failed/, the command tree is gone, and the lock is released
    expect(s.events.find((e) => e['event'] === 'batch_failed')?.['reason']).toMatch(/stopped with the watcher/);
    // the command is over: no SIGKILL may still be pending for its (soon reusable) process id
    expect((s.w as unknown as { escalations: Set<unknown> }).escalations.size).toBe(0);
    expect(readdirSync(join(s.dir, 'failed'))).toHaveLength(1);
    expect(readdirSync(s.dir).filter((n) => n.startsWith('running-'))).toEqual([]);
    expect(heldLocks(s.dir)).toEqual([]);
    await until(() => { try { process.kill(gc2, 0); return false; } catch { return true; } }, 8_000);
  }, 20_000);

  it('after a crash: a batch whose command had started is never run again; one that had not is queued again', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    mkdirSync(join(dir, 'failed'));
    const started = JSON.stringify({ ...fill(900), source: 'ws' });
    const notStarted = JSON.stringify({ ...fill(901), source: 'ws' });
    const queued = JSON.stringify({ ...fill(902), source: 'ws' });
    writeFileSync(join(dir, 'running-a.jsonl'), `${started}\n`);
    writeFileSync(join(dir, 'batch-b.jsonl'), `${notStarted}\n`);
    // the queue was not yet rewritten when it died: the same fill is in both, and must go out once
    writeFileSync(join(dir, 'queue.jsonl'), `${notStarted}\n${queued}\n`);
    const out = join(mkdtempSync(join(tmpdir(), 'pmw-out-')), 'calls.jsonl');
    process.env['OUT'] = out;
    const h = setup({ exec: RECORD, dir });
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'batch_done'));
    expect(h.events.some((e) => e['event'] === 'batch_interrupted')).toBe(true);
    expect(readdirSync(join(dir, 'failed'))).toEqual(['interrupted-a.jsonl']);
    const calls = readFileSync(out, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { stdin: string });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.stdin.trim().split('\n').map((l) => (JSON.parse(l) as Fill).block)).toEqual([901, 902]);
  });

  it('a fill already handed to the command is never handed again, even when the stream replays it', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    // the last run handed 1001 to the command and died before the stream cursor was saved
    writeFileSync(join(dir, 'handed.txt'), `${fill(1001).eventId}\n`);
    // and had taken 1003 off the queue, recorded it as handed, but not started the command yet
    writeFileSync(join(dir, 'batch-x.jsonl'), `${JSON.stringify({ ...fill(1003), source: 'ws' })}\n`);
    writeFileSync(join(dir, 'handed.txt'), `${fill(1001).eventId}\n${fill(1003).eventId}\n`);
    const out = join(mkdtempSync(join(tmpdir(), 'pmw-out-')), 'calls.jsonl');
    process.env['OUT'] = out;
    const h = setup({ exec: RECORD, dir });
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'connected'));
    h.hello();
    await until(() => h.events.some((e) => e['event'] === 'hello'));
    h.push(fill(1001));
    h.push(fill(1002));
    await until(() => h.events.some((e) => e['event'] === 'batch_done'));
    await wait(50);
    expect(h.events.some((e) => e['event'] === 'skipped_already_handed' && e['eventId'] === fill(1001).eventId)).toBe(true);
    const calls = readFileSync(out, 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { stdin: string });
    expect(calls.flatMap((c) => c.stdin.trim().split('\n').map((l) => (JSON.parse(l) as Fill).block))).toEqual([1002]);
    expect(readFileSync(join(dir, 'handed.txt'), 'utf8').trim().split('\n')).toContain(fill(1002).eventId);
    // 1003 was never run, and is kept for the user rather than dropped
    const unstarted = readdirSync(join(dir, 'failed')).filter((n) => n.startsWith('unstarted-'));
    expect(unstarted).toHaveLength(1);
    expect(readFileSync(join(dir, 'failed', unstarted[0]!), 'utf8')).toContain(fill(1003).eventId);
    expect(h.events.some((e) => e['event'] === 'batch_unstarted')).toBe(true);
  });

  it('takes over a lock left by a dead process with the next generation; two starters cannot both get it', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const dead = JSON.stringify({ pid: 2 ** 22 + 12345, host: hostname() });
    writeFileSync(join(dir, 'watch.lock.0'), dead);
    writeFileSync(join(dir, 'watch.lock.3'), dead);
    const h = setup({ dir });
    await h.w.start();
    // ours is the next one
    expect(heldLocks(dir)).toEqual(['watch.lock.4']);
    expect(JSON.parse(readFileSync(join(dir, 'watch.lock.4'), 'utf8')).pid).toBe(process.pid);
    // a second starter sees a live holder and refuses, without touching its file
    await expect(new Watcher({ client, dir, markets: null }).start()).rejects.toThrow(/another pmwallets watch/);
    expect(existsSync(join(dir, 'watch.lock.4'))).toBe(true);
    await h.w.stop();
    expect(heldLocks(dir)).toEqual([]);
    // released, not removed: every number stays taken, and the next watcher goes higher, never back to an old one
    const locks = () => readdirSync(dir).filter((n) => n.startsWith('watch.lock')).sort((a, b) => Number(a.split('.')[2]) - Number(b.split('.')[2]));
    expect(locks()).toEqual(['watch.lock.0', 'watch.lock.3', 'watch.lock.4']);
    const again = setup({ dir });
    await again.w.start();
    expect(locks()).toEqual(['watch.lock.0', 'watch.lock.3', 'watch.lock.4', 'watch.lock.5']);
    await again.w.stop();
    // a starter whose scan is stale tries a number already used: it exists, so it cannot be taken twice
    expect(() => openSync(join(dir, 'watch.lock.4'), 'wx')).toThrow(/EEXIST/);

    // a starter that saw the dead holder but lost the race for the next generation finds a live one and refuses
    writeFileSync(join(dir, 'watch.lock.17'), dead);
    writeFileSync(join(dir, 'watch.lock.18'), JSON.stringify({ pid: process.pid, host: hostname() }));
    await expect(new Watcher({ client, dir, markets: null }).start()).rejects.toThrow(/another pmwallets watch/);
  });

  it('stops a command the previous watcher left running before handing anything out; refuses when it cannot prove it is ours', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    mkdirSync(join(dir, 'failed'));
    const batchFile = join(dir, 'running-a.jsonl');
    writeFileSync(batchFile, `${JSON.stringify({ ...fill(900), source: 'ws' })}\n`);
    // the old watcher's command, still running in its own process group with the batch in its environment
    const orphan = spawn(process.execPath, ['-e', 'setTimeout(()=>{},60000)'], { detached: true, stdio: 'ignore', env: { ...process.env, PMW_BATCH_FILE: batchFile } });
    writeFileSync(join(dir, 'running-a.pid'), JSON.stringify({ pid: orphan.pid, host: hostname(), batchFile }));
    const h = setup({ exec: 'node -e ""', dir });
    await h.w.start();
    expect(h.events.some((e) => e['event'] === 'orphan_command_stopped')).toBe(true);
    await until(() => { try { process.kill(orphan.pid!, 0); return false; } catch { return true; } });
    expect(readdirSync(join(dir, 'failed'))).toEqual(['interrupted-a.jsonl']);
    expect(readdirSync(dir).filter((n) => n.endsWith('.pid'))).toEqual([]);
    await h.w.stop();

    // a live process group the record cannot be matched to: never killed, the start is refused and the lock released
    const stranger = spawn(process.execPath, ['-e', 'setTimeout(()=>{},60000)'], { detached: true, stdio: 'ignore' });
    writeFileSync(join(dir, 'running-b.jsonl'), `${JSON.stringify({ ...fill(901), source: 'ws' })}\n`);
    writeFileSync(join(dir, 'running-b.pid'), JSON.stringify({ pid: stranger.pid, host: hostname(), batchFile: join(dir, 'running-b.jsonl') }));
    const w2 = setup({ exec: 'node -e ""', dir });
    await expect(w2.w.start()).rejects.toThrow(/may still be running/);
    expect(heldLocks(dir)).toEqual([]);
    process.kill(-stranger.pid!, 'SIGKILL');
  });

  it('forgets handed fills only at or before the saved stream cursor', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const old = Array.from({ length: 40_001 }, (_, i) => fill(100 + i).eventId);
    const fresh = [fill(50_000, 1).eventId, fill(50_001).eventId];
    writeFileSync(join(dir, 'handed.txt'), `${[...old, ...fresh].join('\n')}\n`);
    // the stream has saved a position past the old fills but not the fresh ones (a replay still going on)
    writeFileSync(join(dir, 'stream.json'), JSON.stringify({ session: 's0', seq: 0, block: 50_000, logIndex: 0 }));
    const out = join(mkdtempSync(join(tmpdir(), 'pmw-out-')), 'calls.jsonl');
    process.env['OUT'] = out;
    // queued before start: a fill pushed through the stream would move the saved cursor past everything here first
    writeFileSync(join(dir, 'queue.jsonl'), `${JSON.stringify({ ...fill(60_000), source: 'ws' })}\n`);
    const h = setup({ exec: RECORD, dir });
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'batch_done'));
    const kept = readFileSync(join(dir, 'handed.txt'), 'utf8').trim().split('\n');
    expect(kept.sort()).toEqual([...fresh, fill(60_000).eventId].sort());
  }, 20_000);

  it('finds a leftover command by its batch path even when its process record was never written (Linux)', async () => {
    if (process.platform !== 'linux') return;
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const batchFile = join(dir, 'running-z.jsonl');
    writeFileSync(batchFile, `${JSON.stringify({ ...fill(800), source: 'ws' })}\n`);
    const orphan = spawn(process.execPath, ['-e', 'setTimeout(()=>{},60000)'], { detached: true, stdio: 'ignore', env: { ...process.env, PMW_BATCH_FILE: batchFile } });
    const h = setup({ dir });
    await h.w.start();
    expect(h.events.some((e) => e['event'] === 'orphan_command_stopped' && e['pid'] === orphan.pid)).toBe(true);
    await until(() => { try { process.kill(orphan.pid!, 0); return false; } catch { return true; } });
  });

  it('where processes cannot be inspected, a batch that may have a leftover command blocks the start', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    writeFileSync(join(dir, 'running-q.jsonl'), `${JSON.stringify({ ...fill(801), source: 'ws' })}\n`);
    const real = Object.getOwnPropertyDescriptor(process, 'platform')!;
    Object.defineProperty(process, 'platform', { value: 'darwin' });
    try {
      const h = setup({ exec: 'node -e ""', dir });
      await expect(h.w.start()).rejects.toThrow(/cannot be looked for on this system/);
      expect(heldLocks(dir)).toEqual([]);
    } finally { Object.defineProperty(process, 'platform', real); }
  });

  it('a stop that arrives while start is still stopping a leftover command keeps the lock until start has finished', async () => {
    if (process.platform !== 'linux') return;
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const batchFile = join(dir, 'running-s.jsonl');
    writeFileSync(batchFile, `${JSON.stringify({ ...fill(810), source: 'ws' })}\n`);
    // ignores SIGTERM, so stopping it takes the full 5 s grace
    const orphan = spawn(process.execPath, ['-e', "process.on('SIGTERM',()=>{});setTimeout(()=>{},60000)"], { detached: true, stdio: 'ignore', env: { ...process.env, PMW_BATCH_FILE: batchFile } });
    await wait(200);
    const h = setup({ exec: 'node -e ""', dir });
    const started = h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'orphan_command_stopped'));
    const stopping = h.w.stop();
    await wait(200);
    // start is still busy with the leftover: the directory is still ours, and recovery has not run
    expect(heldLocks(dir)).toHaveLength(1);
    expect(existsSync(batchFile)).toBe(true);
    await started;
    await stopping;
    expect(heldLocks(dir)).toEqual([]);
    expect(existsSync(batchFile)).toBe(true); // a stopped start does not touch the batch files
    await until(() => { try { process.kill(orphan.pid!, 0); return false; } catch { return true; } }, 8_000);
  }, 20_000);

  it('checks for a leftover command in every mode, not only with --exec', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const stranger = spawn(process.execPath, ['-e', 'setTimeout(()=>{},60000)'], { detached: true, stdio: 'ignore' });
    writeFileSync(join(dir, 'running-c.pid'), JSON.stringify({ pid: stranger.pid, host: hostname(), batchFile: join(dir, 'running-c.jsonl') }));
    const h = setup({ dir }); // stdout mode
    await expect(h.w.start()).rejects.toThrow(/may still be running/);
    process.kill(-stranger.pid!, 'SIGKILL');
  });

  it('a command whose process id cannot be recorded is stopped, and its batch failed', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    const at = Date.parse('2026-10-14T00:00:00Z');
    const id = `${new Date(at).toISOString().replace(/[:.]/g, '-')}-${process.pid}`;
    mkdirSync(join(dir, `running-${id}.pid`), { recursive: true }); // a directory where the record must go
    writeFileSync(join(dir, 'queue.jsonl'), `${JSON.stringify({ ...fill(700), source: 'ws' })}\n`);
    const out = join(mkdtempSync(join(tmpdir(), 'pmw-out-')), 'calls.jsonl');
    process.env['OUT'] = out;
    const h = setup({ exec: `node -e "setTimeout(()=>{},1000)" && ${RECORD}`, dir });
    (h.w as unknown as { now: () => number }).now = () => at;
    await h.w.start();
    await until(() => h.events.some((e) => e['event'] === 'batch_failed'));
    expect(String(h.events.find((e) => e['event'] === 'batch_failed')!['reason'])).toMatch(/could not record the command's process id/);
    await wait(1500);
    expect(existsSync(out)).toBe(false); // the command never got to act
  });

  it('without --exec, refuses a directory that still holds fills queued for a command', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    writeFileSync(join(dir, 'queue.jsonl'), `${JSON.stringify({ ...fill(900), source: 'ws' })}\n`);
    const h = setup({ dir });
    await expect(h.w.start()).rejects.toThrow(/queued for a command/);
    expect(heldLocks(dir)).toEqual([]);
  });

  it('a start that fails (unreadable stream position) releases the directory', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    writeFileSync(join(dir, 'stream.json'), '{not json');
    const h = setup({ dir });
    await expect(h.w.start()).rejects.toThrow();
    expect(heldLocks(dir)).toEqual([]);
  });

  it('a lock file left empty by a starter that died is taken over once it is clearly abandoned', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'pmw-watch-'));
    writeFileSync(join(dir, 'watch.lock.2'), '');
    const fresh = setup({ dir });
    await expect(fresh.w.start()).rejects.toThrow(/could not lock/); // may still be being written
    const old = (Date.now() - 60_000) / 1000;
    utimesSync(join(dir, 'watch.lock.2'), old, old);
    const h = setup({ dir });
    await h.w.start();
    expect(heldLocks(dir)).toEqual(['watch.lock.3']);
  });

  it('refuses a second watcher on the same directory', async () => {
    const h = setup();
    await h.w.start();
    const w2 = new Watcher({ client, dir: h.dir, markets: null });
    await expect(w2.start()).rejects.toThrow(/another pmwallets watch/);
    await h.w.stop();
    expect(heldLocks(h.dir)).toEqual([]);
  });
});
