import { spawn as nodeSpawn, spawnSync, type ChildProcess } from 'node:child_process';
import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, readdirSync, renameSync, statSync, unlinkSync, writeSync } from 'node:fs';
import { hostname } from 'node:os';
import { dirname, join, resolve as resolvePath } from 'node:path';
import type { PmwClient } from './client.js';
import { MarketLookup, microToDecimal, type MarketInfo } from './market.js';
import { FileStateStore, FillStream, type FillMeta, type StreamEvent } from './stream.js';
import type { Fill } from './types.js';

/** A fill as `pmwallets watch` hands it on: the API's fill plus what a reader needs to understand it. */
export interface WatchedFill extends Fill {
  source: FillMeta['source'];
  /** when this process received it (ISO, UTC) */
  receivedAt: string;
  /** `usdc` / `shares` / `fee` as decimal strings ("1824.000000") — the raw fields are 1e-6 integer units */
  usdcAmount: string | null;
  sharesAmount: string | null;
  feeAmount: string | null;
  /** the market and outcome the token belongs to; null when Polymarket could not be asked */
  market: MarketInfo | null;
}

export interface WatchOptions {
  client: PmwClient;
  /** holds the stream cursor, the queue of fills not yet handed to the command, and the lock */
  dir: string;
  /**
   * Shell command run for each batch of fills: the batch arrives on stdin as JSON lines, one fill per line (the
   * same lines are in the file named by PMW_BATCH_FILE). Without it, every fill is written to `out` as one JSON line.
   */
  exec?: string;
  /** how long to wait after the first queued fill before running the command, so fills that arrive close together go as one batch */
  batchMs?: number;
  /** most fills in one batch (default 50); the rest go in the next */
  maxBatch?: number;
  /** a command still running after this is killed and its batch counted as failed (default 10 min) */
  execTimeoutMs?: number;
  takeover?: boolean;
  /** null = do not look markets up */
  markets?: MarketLookup | null;
  out?: (line: string) => Promise<void>;
  /** progress and problems, one object per event (the CLI prints them to stderr as JSON lines) */
  log?: (event: Record<string, unknown>) => void;
  /** tests */
  spawn?: (command: string, env: NodeJS.ProcessEnv) => ChildProcess;
  streamOptions?: Partial<ConstructorParameters<typeof FillStream>[0]>;
  now?: () => number;
}

const QUEUE = 'queue.jsonl';
/** generation locks `watch.lock.<n>`: the highest n is the lock; see lock() */
const LOCK = 'watch.lock.';
/** eventIds already handed to the command, one per line: the at-most-once record */
const HANDED = 'handed.txt';
const HANDED_KEEP = 20_000;

/**
 * Follows the account's fill stream and hands every fill on — to stdout, or to a command (an AI agent, a script).
 *
 * Handing a fill to a command is AT MOST ONCE. Before the command starts, the batch's eventIds are appended to
 * `handed.txt` and synced to disk; a fill whose eventId is there is never queued or handed again — not when the
 * stream replays it because the process died before its cursor was saved, not when a batch is recovered. A
 * `running-*` file found at start-up (the process died while the command ran) is moved to `failed/`, never re-run:
 * the command may already have acted on it (placed an order). A batch whose command exits non-zero or times out
 * goes to `failed/` too. A fill is synced into the queue before the stream cursor moves past it, so nothing between
 * the stream and the command is lost across a restart.
 */
export class Watcher {
  private readonly o: Required<Pick<WatchOptions, 'batchMs' | 'maxBatch' | 'execTimeoutMs' | 'takeover'>> & WatchOptions;
  private readonly log: (event: Record<string, unknown>) => void;
  private readonly now: () => number;
  private stream: FillStream | null = null;
  private timer: NodeJS.Timeout | null = null;
  private child: Promise<void> | null = null;
  /** a start() in progress: stop() waits for it, so no step of it touches the directory after the lock is gone */
  private starting: Promise<void> | null = null;
  /** the command running now: stop() ends it (its whole process tree) before letting go of the directory */
  private proc: ChildProcess | null = null;
  /** pending SIGKILLs for the running command: cancelled when it is over, or they could hit a reused process id */
  private readonly escalations = new Set<NodeJS.Timeout>();
  private running = false;
  /** the lock file this process created, if it holds the directory */
  private lockFile: string | null = null;
  private readonly handed = new Set<string>();
  private handedLines = 0;
  private handedLimit = HANDED_KEEP * 2;
  /** something may be queued: kick() runs on every fill, and must not read the whole queue to find out */
  private pending = true;
  private stopped: (code: number) => void = () => {};
  /** resolves with the exit code once the watcher has stopped (fatal stream error, or stop()) */
  readonly done: Promise<number>;

  constructor(opts: WatchOptions) {
    // `??`, not spread defaults: an option passed as undefined must still get its default
    this.o = { ...opts, batchMs: opts.batchMs ?? 0, maxBatch: opts.maxBatch ?? 50, execTimeoutMs: opts.execTimeoutMs ?? 10 * 60_000, takeover: opts.takeover ?? false };
    if (!Number.isInteger(this.o.maxBatch) || this.o.maxBatch < 1) throw new Error('maxBatch must be a whole number ≥ 1');
    this.log = opts.log ?? (() => {});
    this.now = opts.now ?? Date.now;
    this.done = new Promise((r) => { this.stopped = r; });
  }

  private path(name: string): string { return join(this.o.dir, name); }

  start(): Promise<void> {
    this.starting = this.startNow().finally(() => { this.starting = null; });
    return this.starting;
  }

  private async startNow(): Promise<void> {
    mkdirSync(join(this.o.dir, 'failed'), { recursive: true });
    this.lock();
    this.running = true;
    try {
      // a command a dead watcher left running is stopped whatever this run's mode: it may still be trading
      await this.stopOrphans();
      if (!this.running) throw new Error('stopped while starting');
      if (this.o.exec) { this.loadHanded(); this.recover(); }
      else {
        // fills queued for a command by an earlier --exec run are past the stream cursor: printing from the stream
        // would silently skip them
        const waiting = readLines(this.path(QUEUE)).length + readdirSync(this.o.dir).filter((n) => n.startsWith('batch-')).length;
        if (waiting) throw new Error(`${this.o.dir} holds fills queued for a command by an earlier --exec run: start with --exec to hand them over, or move ${this.path(QUEUE)} (and any batch-* files) away to drop them`);
      }
      if (!this.running) throw new Error('stopped while starting');
      this.stream = new FillStream({
        client: this.o.client,
        store: new FileStateStore(this.path('stream.json')),
        takeover: this.o.takeover,
        onFill: (fill, meta) => this.onFill(fill, meta),
        onEvent: (e) => this.onEvent(e),
        ...this.o.streamOptions,
      });
      await this.stream.start();
      if (!this.running) throw new Error('stopped while starting');
    } catch (e) {
      // a start that fails (or is stopped midway) must not leave the directory locked, or a stream running
      const stoppedMidway = !this.running;
      this.running = false;
      await this.stream?.stop().catch(() => {});
      this.stream = null;
      this.unlock();
      if (stoppedMidway) { this.stopped(0); return; }
      throw e;
    }
    if (this.o.exec) this.kick();
  }

  async stop(code = 0): Promise<void> {
    if (!this.running) return;
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    // the command first: stopping the stream can take long (a replay in progress), and a command asked to stop
    // must not go on trading meanwhile. Its batch goes to failed/; it is stopped, not waited for.
    if (this.proc) {
      const proc = this.proc;
      killTree(proc, 'SIGTERM');
      // a command that ignores SIGTERM must not hold the directory (and maybe go on trading) until its timeout
      this.escalate(proc);
    }
    // a start still in progress sees the stop at its next step and cleans up after itself
    await this.starting?.catch(() => {});
    await this.stream?.stop();
    // the lock is held until the running batch is over: released earlier, a new watcher would take its file and
    // start a second command next to it
    await this.child;
    this.unlock();
    this.stopped(code);
  }

  private onEvent(e: StreamEvent): void {
    if (e.type === 'fatal') {
      this.log({ event: 'fatal', error: e.error.message });
      void this.stop(2);
      return;
    }
    if (e.type === 'error') this.log({ event: 'stream_error', error: e.error.message });
    else if (e.type === 'connecting') return;
    else this.log({ event: e.type, ...omit(e as Record<string, unknown>, ['type']) });
  }

  private async onFill(fill: Fill, meta: FillMeta): Promise<void> {
    const market = this.o.markets ? await this.o.markets.byToken(fill.tokenId) : null;
    const f = fill as Fill & { marketTitle?: string; conditionId?: string };
    const rec: WatchedFill = {
      ...fill,
      source: meta.source,
      receivedAt: new Date(this.now()).toISOString(),
      usdcAmount: microToDecimal(fill.usdc),
      sharesAmount: microToDecimal(fill.shares),
      feeAmount: microToDecimal(fill.fee),
      market: market ?? (f.marketTitle && f.conditionId
        ? { conditionId: f.conditionId, question: f.marketTitle, outcome: null, slug: null, endDate: null, closed: null }
        : null),
    };
    const line = JSON.stringify(rec);
    if (!this.o.exec) {
      await (this.o.out ?? writeStdout)(line);
      return;
    }
    // a replay of a fill the command already had (the cursor was not saved before a crash) must not reach it again
    if (this.handed.has(fill.eventId)) { this.log({ event: 'skipped_already_handed', eventId: fill.eventId }); return; }
    // synchronous and synced: the cursor moves only after this returns, so a queued fill is on disk first
    appendSynced(this.path(QUEUE), `${line}\n`);
    this.pending = true;
    this.kick();
  }

  /** run the command for the next batch, once the batch window has passed and no command is running */
  private kick(): void {
    if (!this.running || this.child || this.timer || !this.pending) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.child = this.dispatch()
        // a file the queue depends on could not be written: stop rather than run on with the queue in doubt
        .catch((e: unknown) => { this.log({ event: 'fatal', error: `batch dispatch failed: ${(e as Error).message}` }); void this.stop(1); })
        .finally(() => { this.child = null; this.kick(); });
    }, this.o.batchMs);
  }

  /** the queue, de-duplicated, without anything already handed to the command */
  private queued(): string[] {
    return dedupe(readLines(this.path(QUEUE))).filter((l) => !this.handed.has(eventIdOf(l)));
  }

  /**
   * A crash in the middle of an append can leave a last line without its newline. That fill was never confirmed (the
   * stream cursor did not move past it, so the stream sends it again), but the next append would be glued onto the
   * fragment and lost with it: cut the fragment off first.
   */
  private repairTail(name: string): void {
    const path = this.path(name);
    let text: string;
    try { text = readFileSync(path, 'utf8'); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return; throw e; }
    if (!text || text.endsWith('\n')) return;
    const keep = text.slice(0, text.lastIndexOf('\n') + 1);
    writeSynced(path, keep);
    this.log({ event: 'partial_line_removed', file: path, bytes: Buffer.byteLength(text) - Buffer.byteLength(keep) });
  }

  private loadHanded(): void {
    this.repairTail(HANDED);
    this.repairTail(QUEUE);
    const ids = readLines(this.path(HANDED));
    for (const id of ids) this.handed.add(id);
    this.handedLines = ids.length;
  }

  /** record a batch as handed over — synced to disk BEFORE the command can see it */
  private markHanded(ids: string[]): void {
    for (const id of ids) this.handed.add(id);
    this.handedLines += ids.length;
    if (this.handedLines > this.handedLimit) {
      // forget only what the stream can never replay: fills at or before its SAVED cursor (a replay starts strictly
      // after it). During a long replay the cursor is saved only at the end, so everything after it is kept.
      const cursor = this.savedCursor();
      const keep = [...this.handed].filter((id) => {
        const at = positionOf(id);
        return !cursor || !at || at.block > cursor.block || (at.block === cursor.block && at.logIndex > cursor.logIndex);
      });
      writeSynced(this.path(HANDED), `${keep.join('\n')}\n`);
      this.handed.clear();
      for (const id of keep) this.handed.add(id);
      this.handedLines = keep.length;
      this.handedLimit = Math.max(HANDED_KEEP * 2, keep.length * 2);
    } else {
      appendSynced(this.path(HANDED), `${ids.join('\n')}\n`);
    }
  }

  /** the stream cursor as it is on disk — synced first: the stream writes it without fsync, and ids are forgotten on its word */
  private savedCursor(): { block: number; logIndex: number } | null {
    try {
      const fd = openSync(this.path('stream.json'), 'r+'); // r+: Windows cannot fsync a read-only handle
      try { fsyncSync(fd); } finally { closeSync(fd); }
      syncDir(this.o.dir);
    } catch { return null; }
    try {
      const c = JSON.parse(readFileSync(this.path('stream.json'), 'utf8')) as { block?: unknown; logIndex?: unknown };
      return Number.isInteger(c.block) && Number.isInteger(c.logIndex) ? { block: c.block as number, logIndex: c.logIndex as number } : null;
    } catch { return null; }
  }

  private async dispatch(): Promise<void> {
    const lines = this.queued();
    if (!lines.length) { this.pending = false; return; }
    if (!this.running) return;
    const batch = lines.slice(0, this.o.maxBatch);
    const rest = lines.slice(this.o.maxBatch);
    const id = `${new Date(this.now()).toISOString().replace(/[:.]/g, '-')}-${process.pid}`;
    const pending = this.path(`batch-${id}.jsonl`);
    const runningFile = this.path(`running-${id}.jsonl`);
    // batch-* = taken from the queue but not started (put back at start-up); running-* = the command has it
    writeSynced(pending, `${batch.join('\n')}\n`);
    writeSynced(this.path(QUEUE), rest.length ? `${rest.join('\n')}\n` : '');
    this.pending = rest.length > 0;
    const eventIds = batch.map(eventIdOf);
    this.markHanded(eventIds);
    renameSync(pending, runningFile);
    syncDir(this.o.dir);

    const started = this.now();
    // `queued`: what is still waiting after this batch — a backlog that keeps growing means the command is too slow
    this.log({ event: 'batch_start', fills: batch.length, queued: rest.length, file: runningFile });
    const result = await this.runCommand(`${batch.join('\n')}\n`, runningFile, batch.length);
    const ms = this.now() - started;
    if (result.ok) {
      unlinkSync(runningFile);
      this.log({ event: 'batch_done', fills: batch.length, ms });
    } else {
      const failed = join(this.o.dir, 'failed', `${id}.jsonl`);
      renameSync(runningFile, failed);
      this.log({ event: 'batch_failed', fills: batch.length, ms, reason: result.reason, file: failed, eventIds });
    }
  }

  private runCommand(input: string, file: string, size: number): Promise<{ ok: boolean; reason?: string }> {
    return new Promise((resolve) => {
      // absolute: it is also how a later start recognises this command's processes (see stopOrphans)
      const env = { ...process.env, PMW_BATCH_FILE: resolvePath(file), PMW_BATCH_SIZE: String(size) };
      let child: ChildProcess;
      try {
        // its own process group (POSIX): an agent CLI starts children of its own, and a timeout or a stop must
        // end all of them, not only the shell — a surviving agent could go on trading
        child = this.o.spawn ? this.o.spawn(this.o.exec!, env)
          : nodeSpawn(this.o.exec!, { shell: true, env, stdio: ['pipe', 'inherit', 'inherit'], detached: process.platform !== 'win32' });
      } catch (e) { resolve({ ok: false, reason: `could not start the command: ${(e as Error).message}` }); return; }
      this.proc = child;
      // who is running this batch, for the next start if this process dies with the command still running
      const pidFile = file.replace(/\.jsonl$/, '.pid');
      let pidUnsaved: string | null = null;
      if (child.pid !== undefined) {
        try { writeSynced(pidFile, JSON.stringify({ pid: child.pid, host: hostname(), batchFile: resolvePath(file) })); } catch (e) { pidUnsaved = (e as Error).message; }
      }
      let timedOut = false;
      let settled = false;
      const finish = (r: { ok: boolean; reason?: string }) => {
        if (settled) return;
        settled = true;
        clearTimeout(kill);
        for (const t of this.escalations) clearTimeout(t);
        this.escalations.clear();
        this.proc = null;
        try { unlinkSync(pidFile); } catch { /* not written */ }
        // the batch is over when the command exits: anything it left running is stopped with it — its process group,
        // and on Linux any process that left the group but still carries this batch in its environment
        killTree(child, 'SIGKILL');
        if (process.platform === 'linux') for (const pgid of groupsRunning(resolvePath(file))) killTree({ pid: pgid } as ChildProcess, 'SIGKILL');
        resolve(r);
      };
      const kill = setTimeout(() => {
        timedOut = true;
        killTree(child, 'SIGTERM');
        this.escalate(child);
      }, this.o.execTimeoutMs);
      child.on('error', (e) => finish({ ok: false, reason: `could not start the command: ${e.message}` }));
      // unrecorded, a command could outlive a crash of this process unseen by the next start: do not let it run
      if (pidUnsaved !== null) { finish({ ok: false, reason: `could not record the command's process id (${pidUnsaved}); stopped it` }); return; }
      // 'exit', not 'close': a grandchild holding an inherited stream must not keep the batch open
      child.on('exit', (code, signal) => {
        if (timedOut) finish({ ok: false, reason: `timed out after ${Math.round(this.o.execTimeoutMs / 1000)} s` });
        else if (!this.running && code !== 0) finish({ ok: false, reason: `stopped with the watcher${signal ? ` (${signal})` : ''}` });
        else if (code === 0) finish({ ok: true });
        else finish({ ok: false, reason: signal ? `killed by ${signal}` : `exit code ${code}` });
      });
      // a command that never reads stdin (it uses PMW_BATCH_FILE) closes the pipe: that is not a failure
      child.stdin?.on('error', () => {});
      child.stdin?.end(input);
    });
  }

  /**
   * After a crash: a batch taken from the queue but never started goes back to the front of it; a batch whose
   * command had started is moved to failed/ — it may have been acted on, so it is never run again.
   */
  /**
   * A command of a watcher that died (SIGKILL, crash) may still be running — and trading. It is stopped before
   * anything new is handed out; one that cannot be confirmed as ours is left alone and the start refused.
   */
  private async stopOrphans(): Promise<void> {
    const names = readdirSync(this.o.dir);
    for (const n of names.filter((x) => x.startsWith('running-') && x.endsWith('.pid'))) {
      await this.stopOrphan(this.path(n));
      try { unlinkSync(this.path(n)); } catch { /* gone */ }
    }
    // On Linux every batch's processes are also found by the batch path in their environment — those of a batch
    // whose process record was never written, and any that left the command's process group (setsid). Elsewhere a
    // batch without a record cannot be checked, and it is said so.
    for (const n of names.filter((x) => x.startsWith('running-') && x.endsWith('.jsonl'))) {
      if (process.platform !== 'linux') {
        if (names.includes(n.replace(/\.jsonl$/, '.pid'))) continue;
        throw new Error(`a command may have been started for ${this.path(n)} just before the previous watcher died, and it cannot be looked for on this system. `
          + `Make sure no agent from the previous run is still running, then move that file into ${join(this.o.dir, 'failed')} and start again`);
      }
      for (const pgid of groupsRunning(resolvePath(this.path(n)))) {
        this.log({ event: 'orphan_command_stopped', pid: pgid, note: 'a command from the previous run was still running; it was stopped before anything new is handed out' });
        await stopGroup(pgid);
      }
    }
  }

  /** SIGKILL the command's tree in 5 s unless it is over by then */
  private escalate(proc: ChildProcess): void {
    const t = setTimeout(() => { this.escalations.delete(t); killTree(proc, 'SIGKILL'); }, 5_000);
    t.unref();
    this.escalations.add(t);
  }

  private recover(): void {
    const names = readdirSync(this.o.dir).sort();
    const batches = names.filter((x) => x.startsWith('batch-'));
    const lines = batches.flatMap((n) => readLines(this.path(n)));
    // recorded as handed but never started (the process died in between): never run, but never silently dropped
    const unstarted = lines.filter((l) => this.handed.has(eventIdOf(l)));
    const back = lines.filter((l) => !this.handed.has(eventIdOf(l)));
    if (unstarted.length) {
      const to = join(this.o.dir, 'failed', `unstarted-${new Date(this.now()).toISOString().replace(/[:.]/g, '-')}.jsonl`);
      writeSynced(to, `${dedupe(unstarted).join('\n')}\n`);
      this.log({ event: 'batch_unstarted', fills: unstarted.length, file: to, note: 'recorded as handed to the command, but the watcher stopped before running it: it is not run — check these fills yourself' });
    }
    if (back.length) {
      // the merged queue is on disk before any batch file goes: a crash in between only repeats this step
      writeSynced(this.path(QUEUE), `${dedupe([...back, ...readLines(this.path(QUEUE))]).join('\n')}\n`);
    }
    for (const n of batches) unlinkSync(this.path(n));
    for (const n of names.filter((x) => x.startsWith('running-') && x.endsWith('.jsonl'))) {
      const to = join(this.o.dir, 'failed', n.replace(/^running-/, 'interrupted-'));
      renameSync(this.path(n), to);
      this.log({ event: 'batch_interrupted', file: to, note: 'the command was running when the watcher stopped; it is not run again — check what it did' });
    }
  }

  private async stopOrphan(pidFile: string): Promise<void> {
    let rec: { pid?: number; host?: string; batchFile?: string };
    try { rec = JSON.parse(readFileSync(pidFile, 'utf8')); } catch { return; }
    if (typeof rec.pid !== 'number' || rec.host !== hostname()) return;
    const pid = rec.pid;
    const groupAlive = () => {
      try { process.kill(process.platform === 'win32' ? pid : -pid, 0); return true; } catch (e) { return (e as NodeJS.ErrnoException).code === 'EPERM'; }
    };
    if (!groupAlive()) return;
    if (!ownsBatch(pid, String(rec.batchFile ?? ''))) {
      throw new Error(`a command from the previous run may still be running (process group ${pid}) and could still be trading. `
        + `Check it, stop it (kill -- -${pid}), then delete ${pidFile} and start again`);
    }
    this.log({ event: 'orphan_command_stopped', pid, note: 'a command from the previous run was still running; it was stopped before anything new is handed out' });
    await stopGroup(pid);
  }

  /**
   * One watcher per directory: two would both hand every fill to the command. Locks are generations,
   * `watch.lock.<n>`, and the highest n is the lock. A new one may be created only when the highest is released or
   * held by a dead process on this host, and creating n+1 is exclusive — so of two starters that both saw it free,
   * exactly one gets it. Numbers only grow: a released lock stays on disk (marked released) as the highest until a
   * newer one exists, and lower ones are never removed — so a starter that paused on an old scan can never create a
   * number that was already used.
   */
  private lock(): void {
    for (let attempt = 0; attempt < 20; attempt++) {
      const top = this.generations().at(-1);
      if (top !== undefined) {
        const owner = this.lockOwner(top);
        if (owner === null) { pause(25); continue; } // being written: look again
        if (!owner.free) {
          throw new Error(`another pmwallets watch is using ${this.o.dir} (pid ${owner.pid ?? '?'} on ${owner.host ?? '?'}); stop it, or delete ${this.path(`${LOCK}${top}`)} if it is gone`);
        }
      }
      const gen = (top ?? -1) + 1;
      const file = this.path(`${LOCK}${gen}`);
      let fd: number;
      try { fd = openSync(file, 'wx'); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'EEXIST') continue; throw e; }
      try { writeSync(fd, JSON.stringify({ pid: process.pid, host: hostname(), startedAt: new Date().toISOString() })); fsyncSync(fd); } finally { closeSync(fd); }
      this.lockFile = file;
      // old generations are never removed: no number may be handed out twice, even to a starter whose scan is long
      // stale (each is a few bytes, one per start)
      return;
    }
    throw new Error(`could not lock ${this.o.dir}: other watchers keep starting there`);
  }

  private generations(): number[] {
    return readdirSync(this.o.dir).map((n) => (n.startsWith(LOCK) && /^\d+$/.test(n.slice(LOCK.length)) ? Number(n.slice(LOCK.length)) : Number.NaN))
      .filter((n) => Number.isSafeInteger(n)).sort((a, b) => a - b);
  }

  /** null = unreadable (being written); free = released, or its process is dead on this host */
  private lockOwner(gen: number): { free: boolean; pid?: number; host?: string } | null {
    const f = this.path(`${LOCK}${gen}`);
    let o: { pid?: unknown; host?: unknown; released?: unknown };
    try { o = JSON.parse(readFileSync(f, 'utf8')); } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return { free: true };
      // written within milliseconds of being created: still unreadable after 10 s, its writer died
      try { return Date.now() - statSync(f).mtimeMs > 10_000 ? { free: true } : null; } catch { return { free: true } }
    }
    if (o.released === true) return { free: true };
    const pid = typeof o.pid === 'number' ? o.pid : undefined;
    const host = typeof o.host === 'string' ? o.host : undefined;
    if (pid === undefined) { try { return Date.now() - statSync(f).mtimeMs > 10_000 ? { free: true } : null; } catch { return { free: true }; } }
    return { free: host === hostname() && !alive(pid), pid, host };
  }

  private unlock(): void {
    if (!this.lockFile) return;
    // released, not removed: the number stays taken, so no starter that scanned earlier can reuse it
    try { writeSynced(this.lockFile, JSON.stringify({ released: true, at: new Date().toISOString() })); } catch { /* gone */ }
    this.lockFile = null;
  }

}

/** signal a command and everything it started: its process group on POSIX, `taskkill /T` on Windows */
function killTree(child: ChildProcess, signal: NodeJS.Signals): void {
  if (child.pid === undefined) return;
  try {
    if (process.platform === 'win32') spawnSync('taskkill', ['/pid', String(child.pid), '/T', '/F'], { stdio: 'ignore' });
    else process.kill(-child.pid, signal);
  } catch { /* already gone */ }
}

/**
 * true only when a process of group `pgid` provably runs this batch: Linux, a member whose environment carries
 * PMW_BATCH_FILE=<batchFile>. Elsewhere it cannot be proven, and the caller refuses rather than kill a stranger.
 */
function ownsBatch(pgid: number, batchFile: string): boolean {
  return process.platform === 'linux' && !!batchFile && groupsRunning(batchFile).has(pgid);
}

/** Linux: the process groups with a member whose environment carries PMW_BATCH_FILE=<batchFile> */
function groupsRunning(batchFile: string): Set<number> {
  const out = new Set<number>();
  const marker = `PMW_BATCH_FILE=${batchFile}`;
  let pids: string[];
  try { pids = readdirSync('/proc').filter((d) => /^\d+$/.test(d)); } catch { return out; }
  for (const p of pids) {
    try {
      if (!readFileSync(`/proc/${p}/environ`, 'utf8').split('\0').includes(marker)) continue;
      const stat = readFileSync(`/proc/${p}/stat`, 'utf8');
      out.add(Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[2]));
    } catch { /* gone, or not ours to read */ }
  }
  return out;
}

/** SIGTERM a process group, wait up to 5 s, then SIGKILL */
async function stopGroup(pgid: number): Promise<void> {
  const alive = () => { try { process.kill(process.platform === 'win32' ? pgid : -pgid, 0); return true; } catch (e) { return (e as NodeJS.ErrnoException).code === 'EPERM'; } };
  killTree({ pid: pgid } as ChildProcess, 'SIGTERM');
  for (let i = 0; i < 50 && alive(); i++) await new Promise((r) => setTimeout(r, 100));
  if (alive()) killTree({ pid: pgid } as ChildProcess, 'SIGKILL');
}

/** (block, logIndex) from an eventId `chain:block:blockHash:txHash:logIndex` */
function positionOf(eventId: string): { block: number; logIndex: number } | null {
  const parts = eventId.split(':');
  const block = Number(parts[1]);
  const logIndex = Number(parts[parts.length - 1]);
  return parts.length === 5 && Number.isInteger(block) && Number.isInteger(logIndex) ? { block, logIndex } : null;
}

/** block the thread briefly (the lock is taken synchronously) */
function pause(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function eventIdOf(line: string): string {
  try { return String((JSON.parse(line) as { eventId?: unknown }).eventId ?? line); } catch { return line; }
}

/** a file's non-empty lines; a missing file is empty, any other read error is an error */
function readLines(path: string): string[] {
  try { return readFileSync(path, 'utf8').split('\n').filter(Boolean); } catch (e) {
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw e;
  }
}

function appendSynced(path: string, text: string): void {
  // a file this creates is not durable until its directory entry is: sync that too, before returning
  let fd: number;
  let created = false;
  try { fd = openSync(path, 'wx'); created = true; } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'EEXIST') throw e;
    fd = openSync(path, 'a');
  }
  try { writeSync(fd, text); fsyncSync(fd); } finally { closeSync(fd); }
  if (created) syncDir(dirname(path));
}

/** replace a file atomically and durably: temp file, sync, rename, sync the directory */
function writeSynced(path: string, text: string): void {
  const tmp = `${path}.tmp`;
  const fd = openSync(tmp, 'w');
  try { writeSync(fd, text); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(tmp, path);
  syncDir(dirname(path));
}

/** make renames and new files in `dir` durable; Windows cannot open a directory, and NTFS journals them anyway */
function syncDir(dir: string): void {
  if (process.platform === 'win32') return;
  const fd = openSync(dir, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function dedupe(lines: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const l of lines) {
    let id: string;
    try { id = String((JSON.parse(l) as { eventId?: unknown }).eventId ?? l); } catch { continue; }
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(l);
  }
  return out;
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (e) { return (e as NodeJS.ErrnoException).code === 'EPERM'; }
}

function omit(o: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  return Object.fromEntries(Object.entries(o).filter(([k]) => !keys.includes(k)));
}

function writeStdout(line: string): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${line}\n`, (err) => (err ? reject(err) : resolve()));
  });
}
