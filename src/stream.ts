import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import WebSocket, { type ClientOptions } from 'ws';
import type { PmwClient } from './client.js';
import type { Fill } from './types.js';

/**
 * Where the stream is: the WebSocket numbering it last accepted and the ledger position of the last
 * fill it delivered. Persist it (FileStateStore) and a restart resumes exactly where it stopped —
 * everything missed while the process was down is replayed from `block/logIndex`.
 */
export interface StreamState {
  session: string | null;
  seq: number;
  block: number;
  logIndex: number;
}

export interface StateStore {
  load(): Promise<StreamState | null>;
  save(state: StreamState): Promise<void>;
}

export class MemoryStateStore implements StateStore {
  private state: StreamState | null = null;
  async load() { return this.state ? { ...this.state } : null; }
  async save(state: StreamState) { this.state = { ...state }; }
}

/** JSON file, replaced atomically (write to a temp file, then rename) so a crash never leaves half a file. */
export class FileStateStore implements StateStore {
  constructor(readonly path: string) {}
  async load(): Promise<StreamState | null> {
    let text: string;
    try { text = await readFile(this.path, 'utf8'); } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw e;
    }
    const s = JSON.parse(text) as Partial<StreamState>;
    return { session: s.session ?? null, seq: s.seq ?? 0, block: s.block ?? 0, logIndex: s.logIndex ?? 0 };
  }
  async save(state: StreamState): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    const tmp = `${this.path}.tmp`;
    await writeFile(tmp, JSON.stringify(state));
    await rename(tmp, this.path);
  }
}

export type StreamEvent =
  | { type: 'connecting'; url: string }
  | { type: 'connected' }
  | { type: 'hello'; session: string; seq: number }
  | { type: 'gap'; reason: 'new_session' | 'seq_skip'; fromBlock: number; fromLogIndex: number; skipped?: 'no_cursor' }
  | { type: 'replayed'; delivered: number }
  | { type: 'disconnected'; code: number; reason: string }
  /** Another connection with the same account took over (one stream per account, newest wins). */
  | { type: 'replaced' }
  | { type: 'error'; error: Error }
  /** Unrecoverable (bad or revoked key): the stream has stopped. */
  | { type: 'fatal'; error: Error };

export interface FillMeta { source: 'ws' | 'replay' }

/** The subset of the `ws` WebSocket the stream uses — injectable for tests. */
export interface SocketLike {
  on(event: 'open', cb: () => void): unknown;
  on(event: 'message', cb: (data: WebSocket.RawData) => void): unknown;
  on(event: 'close', cb: (code: number, reason: Buffer) => void): unknown;
  on(event: 'error', cb: (err: Error) => void): unknown;
  on(event: 'pong', cb: () => void): unknown;
  on(event: 'unexpected-response', cb: (req: unknown, res: { statusCode?: number }) => void): unknown;
  ping(): void;
  close(code?: number): void;
  terminate(): void;
}
export type SocketFactory = (url: string, headers: Record<string, string>) => SocketLike;

export interface FillStreamOptions {
  client: PmwClient;
  /**
   * Called once per fill, in ledger order, never concurrently. A fill is marked delivered only after
   * this returns: if it throws, the connection is dropped and the fill is offered again after the
   * reconnect's replay — so make the handler idempotent on `eventId`, or never throw.
   */
  onFill: (fill: Fill, meta: FillMeta) => void | Promise<void>;
  onEvent?: (event: StreamEvent) => void;
  /** default: in memory (a restart starts from "now") */
  store?: StateStore;
  pingIntervalMs?: number;
  minBackoffMs?: number;
  maxBackoffMs?: number;
  /** how many recent eventIds to remember for de-duplication */
  seenCapacity?: number;
  socketFactory?: SocketFactory;
  /** extra options for the `ws` client, e.g. `{ agent }` to go through a proxy */
  wsOptions?: ClientOptions;
  /**
   * Before the first fill is delivered there is no position to replay from, and replaying from zero
   * returns every fill since each subscription began. Default false: skip that replay (a live
   * consumer such as a copy bot has no use for history). Set true to get the full backlog.
   */
  replayWithoutCursor?: boolean;
}

interface Frame { type?: string; session?: string; seq?: number; data?: Fill }

/**
 * The fills of every entity your account subscribes to, delivered exactly once and in order.
 *
 * The WebSocket is best effort: a frame dropped for a slow consumer, or everything sent while you were
 * reconnecting, is simply not resent. Every frame carries `session` + a consecutive `seq`, so a skip is
 * detectable — and on any skip or new session this stream pulls the gap from GET /v1/account/fills
 * (keyset-paged from the last delivered fill) before it moves on. Duplicates are dropped by `eventId`.
 */
export class FillStream {
  private readonly opts: Required<Omit<FillStreamOptions, 'store' | 'onEvent' | 'socketFactory' | 'wsOptions'>> & Pick<FillStreamOptions, 'onEvent'>;
  private readonly store: StateStore;
  private readonly socketFactory: SocketFactory;
  private state: StreamState = { session: null, seq: 0, block: 0, logIndex: 0 };
  private readonly seen = new Map<string, true>();
  private queue: Promise<void> = Promise.resolve();
  private running = false;
  private socket: SocketLike | null = null;
  private loopDone: Promise<void> | null = null;
  private wake: (() => void) | null = null;

  constructor(options: FillStreamOptions) {
    this.opts = {
      client: options.client,
      onFill: options.onFill,
      onEvent: options.onEvent,
      pingIntervalMs: options.pingIntervalMs ?? 20_000,
      minBackoffMs: options.minBackoffMs ?? 1_000,
      maxBackoffMs: options.maxBackoffMs ?? 30_000,
      seenCapacity: options.seenCapacity ?? 10_000,
      replayWithoutCursor: options.replayWithoutCursor ?? false,
    };
    this.store = options.store ?? new MemoryStateStore();
    this.socketFactory = options.socketFactory
      ?? ((url, headers) => new WebSocket(url, { ...options.wsOptions, headers: { ...options.wsOptions?.headers, ...headers } }) as unknown as SocketLike);
  }

  /** Current position (a copy). */
  get position(): StreamState { return { ...this.state }; }

  /** Loads the saved state and starts connecting. Returns immediately; call stop() to end. */
  async start(): Promise<void> {
    if (this.running) return;
    this.state = (await this.store.load()) ?? this.state;
    this.running = true;
    this.loopDone = this.loop();
  }

  /** Closes the socket and waits for the fill being handled (if any) to finish. */
  async stop(): Promise<void> {
    this.running = false;
    this.socket?.close(1000);
    this.wake?.();
    await this.loopDone;
    await this.queue;
  }

  private emit(e: StreamEvent) {
    try { this.opts.onEvent?.(e); } catch { /* a listener must not break the stream */ }
  }

  private async loop(): Promise<void> {
    let backoff = this.opts.minBackoffMs;
    while (this.running) {
      const healthy = await this.connectOnce();
      if (!this.running) break;
      backoff = healthy ? this.opts.minBackoffMs : Math.min(backoff * 2, this.opts.maxBackoffMs);
      await new Promise<void>((r) => { const t = setTimeout(r, backoff); this.wake = () => { clearTimeout(t); r(); }; });
      this.wake = null;
    }
  }

  /** One connection's life. Resolves when it closes; true when it got as far as a hello. */
  private connectOnce(): Promise<boolean> {
    return new Promise((resolve) => {
      const url = this.opts.client.wsUrl;
      this.emit({ type: 'connecting', url });
      let ws: SocketLike;
      try {
        ws = this.socketFactory(url, { 'x-api-key': this.opts.client.apiKey });
      } catch (e) {
        this.emit({ type: 'error', error: e as Error });
        resolve(false);
        return;
      }
      this.socket = ws;
      // frames of a connection that already failed must not be handled: its state is being thrown away
      const conn = { failed: false, greeted: false };
      let alive = true;
      let pinger: NodeJS.Timeout | null = null;

      ws.on('unexpected-response', (_req, res) => {
        const code = res?.statusCode ?? 0;
        if (code === 401 || code === 403) {
          this.running = false;
          this.emit({ type: 'fatal', error: new Error(`WebSocket upgrade refused with ${code}: check the API key`) });
        } else {
          this.emit({ type: 'error', error: new Error(`WebSocket upgrade refused with ${code}`) });
        }
        try { ws.terminate(); } catch { /* already gone */ }
      });
      ws.on('open', () => {
        this.emit({ type: 'connected' });
        pinger = setInterval(() => {
          // the server never pings; a half-open TCP connection is only found by asking
          if (!alive) { ws.terminate(); return; }
          alive = false;
          try { ws.ping(); } catch { /* close follows */ }
        }, this.opts.pingIntervalMs);
      });
      ws.on('pong', () => { alive = true; });
      ws.on('message', (raw) => {
        alive = true;
        let frame: Frame;
        try { frame = JSON.parse(raw.toString()) as Frame; } catch { return; }
        this.queue = this.queue
          .then(async () => {
            if (conn.failed) return;
            await this.handleFrame(frame);
            if (frame.type === 'hello') conn.greeted = true;
          })
          .catch((err: unknown) => {
            // leave the position where it is and reconnect: the next hello replays from it
            conn.failed = true;
            this.emit({ type: 'error', error: err instanceof Error ? err : new Error(String(err)) });
            try { ws.terminate(); } catch { /* already gone */ }
          });
      });
      ws.on('error', (err) => this.emit({ type: 'error', error: err }));
      ws.on('close', (code, reason) => {
        if (pinger) clearInterval(pinger);
        const why = reason?.toString() ?? '';
        if (code === 1000 && /replaced/i.test(why)) this.emit({ type: 'replaced' });
        this.emit({ type: 'disconnected', code, reason: why });
        if (this.socket === ws) this.socket = null;
        // wait for this connection's queued frames before the next connection's hello can run
        void this.queue.then(() => resolve(conn.greeted && !conn.failed));
      });
    });
  }

  private async handleFrame(m: Frame): Promise<void> {
    if (m.type === 'hello' && typeof m.session === 'string') {
      // A new session means the socket was down (or this process was): everything since the last
      // delivered fill may be missing. Replay BEFORE adopting the session — adopting first is what
      // silently swallows an outage.
      if (this.state.session !== null && m.session !== this.state.session) await this.replay('new_session');
      this.state.session = m.session;
      this.state.seq = m.seq ?? 0;
      await this.store.save(this.state);
      this.emit({ type: 'hello', session: m.session, seq: this.state.seq });
      return;
    }
    if (m.type !== 'fill' || !m.data || typeof m.seq !== 'number') return;
    if (m.session !== this.state.session || m.seq !== this.state.seq + 1) await this.replay('seq_skip');
    await this.deliver(m.data, 'ws');
    // only once the fill is handled: a position that ran ahead of a failed delivery would hide the gap
    this.state.session = m.session ?? null;
    this.state.seq = m.seq;
    await this.store.save(this.state);
  }

  private async replay(reason: 'new_session' | 'seq_skip'): Promise<void> {
    if (this.state.block === 0 && !this.opts.replayWithoutCursor) {
      this.emit({ type: 'gap', reason, fromBlock: 0, fromLogIndex: 0, skipped: 'no_cursor' });
      return;
    }
    this.emit({ type: 'gap', reason, fromBlock: this.state.block, fromLogIndex: this.state.logIndex });
    let delivered = 0;
    for await (const fill of this.opts.client.fillsSince({ sinceBlock: this.state.block, sinceLogIndex: this.state.logIndex })) {
      if (await this.deliver(fill, 'replay')) delivered++;
    }
    await this.store.save(this.state);
    this.emit({ type: 'replayed', delivered });
  }

  /** true when the fill was new and handed to onFill */
  private async deliver(fill: Fill, source: FillMeta['source']): Promise<boolean> {
    if (this.seen.has(fill.eventId)) return false;
    await this.opts.onFill(fill, { source });
    this.seen.set(fill.eventId, true);
    if (this.seen.size > this.opts.seenCapacity) this.seen.delete(this.seen.keys().next().value as string);
    // never move backwards: a live frame can be older than what the replay just walked past
    if (fill.block > this.state.block || (fill.block === this.state.block && fill.logIndex > this.state.logIndex)) {
      this.state.block = fill.block;
      this.state.logIndex = fill.logIndex;
    }
    return true;
  }
}
