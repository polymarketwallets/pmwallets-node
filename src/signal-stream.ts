import WebSocket, { type ClientOptions } from 'ws';
import type { PmwClient } from './client.js';
import type { SocketFactory } from './stream.js';

/** this socket was replaced by a newer one past the account's limit: reconnecting would evict another */
export const CLOSE_SIGNALS_REPLACED = 4412;

export interface SignalAlert { type: 'signal' | 'radar'; data: Record<string, unknown> }
export interface SignalStreamOptions {
  client: PmwClient;
  /** every alert of your signal and radar rules. Best effort: GET /v1/signals and /v1/radar hold what a gap missed */
  onAlert: (alert: SignalAlert) => void;
  /** connected, disconnected, or stopped for good (replaced, or a plan without signals) */
  onEvent?: (e: { type: 'connected' } | { type: 'disconnected'; code: number } | { type: 'stopped'; reason: string }) => void;
  wsOptions?: ClientOptions;
  /** @internal tests */
  socketFactory?: SocketFactory;
  /** @internal tests */
  maxBackoffMs?: number;
}

/** The live alerts of your rules (Pro and Max): connects, reconnects with backoff, stops when told not to come back. */
export class SignalStream {
  private stopped = false;
  private ws: ReturnType<SocketFactory> | null = null;
  private timer: NodeJS.Timeout | null = null;
  private backoff = 1000;
  constructor(private readonly o: SignalStreamOptions) {}

  start(): void { this.stopped = false; this.connect(); }
  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.ws?.close(1000);
  }

  private end(reason: string): void { this.stopped = true; this.o.onEvent?.({ type: 'stopped', reason }); }

  private connect(): void {
    if (this.stopped) return;
    const make: SocketFactory = this.o.socketFactory
      ?? ((url, headers) => new WebSocket(url, { ...this.o.wsOptions, headers: { ...this.o.wsOptions?.headers, ...headers } }) as never);
    const ws = make(this.o.client.signalsWsUrl, { 'x-api-key': this.o.client.apiKey });
    this.ws = ws;
    ws.on('open', () => { this.backoff = 1000; this.o.onEvent?.({ type: 'connected' }); });
    ws.on('message', (raw) => {
      let f: { type?: string; data?: Record<string, unknown> };
      try { f = JSON.parse(String(raw)) as typeof f; } catch { return; }
      if ((f.type === 'signal' || f.type === 'radar') && f.data) this.o.onAlert({ type: f.type, data: f.data });
    });
    ws.on('unexpected-response', (_q, res) => {
      if (res.statusCode === 401 || res.statusCode === 403) this.end(res.statusCode === 403 ? 'signals are part of the Pro and Max plans' : 'the API key was refused');
    });
    ws.on('error', () => undefined);   // a close follows
    ws.on('close', (code) => {
      this.o.onEvent?.({ type: 'disconnected', code });
      if (code === CLOSE_SIGNALS_REPLACED) return this.end('replaced by a newer connection');
      if (this.stopped) return;
      this.timer = setTimeout(() => this.connect(), code === 1012 ? 0 : this.backoff);
      this.backoff = Math.min(this.backoff * 2, this.o.maxBackoffMs ?? 15_000);
    });
  }
}
