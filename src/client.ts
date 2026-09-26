import type {
  ExportFile, ExportInput, Fill, FillCursor, FillsPage, Leaderboard, LeaderboardQuery, SubscribeInput, Subscription,
} from './types.js';

export const DEFAULT_BASE_URL = 'https://api.pmwallets.com';

/** A non-2xx answer from the API. `body` is the parsed JSON body when there was one. */
export class PmwError extends Error {
  constructor(readonly status: number, readonly body: unknown, message?: string) {
    super(message ?? `PMWallets API ${status}: ${typeof body === 'string' ? body : JSON.stringify(body)}`);
    this.name = 'PmwError';
  }
}

export interface PmwClientOptions {
  /** `pmw_<prefix>_<secret>`, created on https://pmwallets.com/keys */
  apiKey: string;
  baseUrl?: string;
  /** per-request timeout, default 15 s */
  timeoutMs?: number;
  /** inject a fetch implementation (tests, proxies) */
  fetch?: typeof fetch;
}

type Query = Record<string, string | number | boolean | undefined>;

/** REST client for https://api.pmwallets.com. Every method authenticates with the API key. */
export class PmwClient {
  readonly apiKey: string;
  readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: PmwClientOptions) {
    if (!opts?.apiKey) throw new Error('PmwClient: apiKey is required');
    this.apiKey = opts.apiKey;
    this.baseUrl = (opts.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, '');
    this.timeoutMs = opts.timeoutMs ?? 15_000;
    this.fetchImpl = opts.fetch ?? fetch;
  }

  /** The WebSocket URL derived from the base URL (https → wss). */
  get wsUrl(): string {
    return `${this.baseUrl.replace(/^http/, 'ws')}/v1/ws`;
  }

  async request<T>(method: string, path: string, opts: { query?: Query; body?: unknown } = {}): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(opts.query ?? {})) if (v !== undefined) url.searchParams.set(k, String(v));
    const headers: Record<string, string> = { 'x-api-key': this.apiKey, accept: 'application/json' };
    if (opts.body !== undefined) headers['content-type'] = 'application/json';
    const res = await this.fetchImpl(url, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const text = await res.text();
    let parsed: unknown = text;
    if (text) { try { parsed = JSON.parse(text); } catch { /* keep the raw text */ } }
    if (!res.ok) throw new PmwError(res.status, parsed);
    return (text ? parsed : undefined) as T;
  }

  // ── the board ────────────────────────────────────────────────────────────
  leaderboard(query: LeaderboardQuery = {}): Promise<Leaderboard> {
    return this.request('GET', '/v1/leaderboard', { query });
  }
  /** One entity by handle or address. The full address comes back only for entities you own. */
  entity(id: string, opts: { period?: string } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', `/v1/entities/${encodeURIComponent(id)}`, { query: opts });
  }
  latency(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/latency');
  }

  // ── addresses ────────────────────────────────────────────────────────────
  /** Buy the address behind a handle. `maxPriceCents` is a ceiling: the charge never exceeds it. */
  buyReveal(input: { entityId: string; maxPriceCents: number }): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/reveals', { body: input });
  }
  reveals(): Promise<Record<string, unknown>[]> {
    return this.request('GET', '/v1/account/reveals');
  }

  // ── subscriptions ────────────────────────────────────────────────────────
  /** Active and paused subscriptions, newest first. */
  subscriptions(): Promise<Subscription[]> {
    return this.request('GET', '/v1/account/subscriptions');
  }
  /** Starts billing per hour. 402 = balance too low, 409 = dormant entity (resend with acceptInactive). */
  subscribe(input: SubscribeInput): Promise<Subscription> {
    return this.request('POST', '/v1/account/subscriptions', { body: { channels: ['ws'], ...input } });
  }
  cancelSubscription(id: string): Promise<unknown> {
    return this.request('DELETE', `/v1/account/subscriptions/${encodeURIComponent(id)}`);
  }
  /** Charges another hour and resumes from the current head (the paused gap is not backfilled). */
  resumeSubscription(id: string): Promise<Subscription> {
    return this.request('POST', `/v1/account/subscriptions/${encodeURIComponent(id)}/resume`);
  }

  // ── fills ────────────────────────────────────────────────────────────────
  /** One page of fills strictly after the cursor, oldest first (all active subscriptions). */
  fills(cursor: Partial<FillCursor> & { limit?: number } = {}): Promise<FillsPage> {
    return this.request('GET', '/v1/account/fills', {
      query: { sinceBlock: cursor.sinceBlock ?? 0, sinceLogIndex: cursor.sinceLogIndex ?? 0, limit: cursor.limit ?? 500 },
    });
  }
  /** Every fill after the cursor, walking pages until the end. */
  async *fillsSince(cursor: FillCursor, limit = 500): AsyncGenerator<Fill> {
    let at: FillCursor | null = cursor;
    while (at) {
      const page: FillsPage = await this.fills({ ...at, limit });
      for (const row of page.rows) yield row;
      at = page.next;
    }
  }

  // ── trade-history exports ────────────────────────────────────────────────
  exportQuote(input: ExportInput): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/exports/quote', { body: input });
  }
  /** Charges the balance; the price is recomputed server-side. */
  createExport(input: ExportInput): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/exports', { body: input });
  }
  exports(): Promise<Record<string, unknown>[]> {
    return this.request('GET', '/v1/account/exports');
  }
  getExport(id: string): Promise<Record<string, unknown>> {
    return this.request('GET', `/v1/account/exports/${encodeURIComponent(id)}`);
  }
  /** The daily files an export grants: one per wallet per UTC day. */
  exportFiles(id: string): Promise<{ files: ExportFile[] }> {
    return this.request('GET', `/v1/account/exports/${encodeURIComponent(id)}/files`);
  }
  /**
   * A short-lived (5 minute) link to one purchased daily file. The API checks the purchase and
   * answers with a redirect; the redirect is read here rather than followed, so your key is never
   * sent to the storage host.
   */
  async exportFileUrl(wallet: string, day: string): Promise<string> {
    const res = await this.fetchImpl(`${this.baseUrl}/v1/account/data/${encodeURIComponent(wallet)}/${encodeURIComponent(day)}`, {
      headers: { 'x-api-key': this.apiKey, accept: 'application/json' },
      redirect: 'manual',
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const location = res.headers.get('location');
    if (res.status >= 300 && res.status < 400 && location) { await res.body?.cancel(); return location; }
    const text = await res.text();
    let parsed: unknown = text;
    if (text) { try { parsed = JSON.parse(text); } catch { /* keep the raw text */ } }
    throw new PmwError(res.status, parsed);
  }
  /** One purchased daily file: zstd-compressed CSV bytes (`.csv.zst`). */
  async downloadExportFile(wallet: string, day: string): Promise<Uint8Array> {
    const url = await this.exportFileUrl(wallet, day);
    const res = await this.fetchImpl(url, { signal: AbortSignal.timeout(Math.max(this.timeoutMs, 60_000)) });
    if (!res.ok) throw new PmwError(res.status, await res.text());
    return new Uint8Array(await res.arrayBuffer());
  }
}
