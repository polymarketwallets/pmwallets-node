import type {
  MyAddresses,
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

/** an array goes out as one comma-separated value (`bands=0,2`) */
type Query = Record<string, string | number | boolean | readonly number[] | undefined>;

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
  /** Where the alerts of your signal and radar rules arrive, as `{type: 'signal' | 'radar', data}`. */
  get signalsWsUrl(): string { return this.wsUrl.replace(/\/v1\/ws$/, '/v1/signals/ws'); }

  async request<T>(method: string, path: string, opts: { query?: Query; body?: unknown } = {}): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(opts.query ?? {})) if (v !== undefined) url.searchParams.set(k, Array.isArray(v) ? v.join(',') : String(v));
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
  /**
   * Every address you have unlocked — bought, taken free, or unlocked with your plan — as board rows, with how
   * each is held (`unlockedBy`; null = a free or plan unlock that lapsed and is masked again) and a paused push
   * subscription's `pausedReason`. `outside` (first page only) lists unlocked addresses with no fill in the window.
   * Spends no board rows.
   */
  myAddresses(query: LeaderboardQuery = {}): Promise<MyAddresses> {
    return this.request('GET', '/v1/leaderboard', { query: { ...query, mine: 1 } });
  }
  /** One entity by handle or address. The full address comes back only for entities you own. */
  entity(id: string, opts: { period?: string } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', `/v1/entities/${encodeURIComponent(id)}`, { query: opts });
  }
  /** The busiest markets of the 30d window, with counts and sums over traders. */
  markets(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/markets');
  }
  /** Whether ingest, push, the board, exports and smart-money files are current. A 503 is an answer here, so it returns the body instead of throwing. */
  async ready(): Promise<Record<string, unknown>> {
    try {
      return await this.request('GET', '/v1/ready');
    } catch (e) {
      if (e instanceof PmwError && e.status === 503 && typeof e.body === 'object' && e.body !== null) return e.body as Record<string, unknown>;
      throw e;
    }
  }
  /** Push latency over time and its histogram. `hours` is 1, 24, 168 or 720. */
  latencyShape(opts: { hours?: number } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/latency/shape', { query: opts });
  }
  /** Stretches of the last 30 days where the push median exceeded the threshold. */
  latencyIncidents(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/latency/incidents');
  }
  latency(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/latency');
  }
  /** The published statistics windows (e.g. 30d, 7d) and when each was computed. */
  periods(): Promise<Record<string, unknown>[]> {
    return this.request('GET', '/v1/periods');
  }

  // ── account ──────────────────────────────────────────────────────────────
  /** Your monthly plan (Base / Pro / Max), its period dates and what is used of its allowances. */
  plan(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/plan');
  }
  /** What moving to `tier` would do today. Charges nothing. */
  quotePlanChange(tier: 'base' | 'pro' | 'max'): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/plan/quote', { body: { tier } });
  }
  /**
   * Charges the balance for 30 days of a plan, which never renew by themselves. `maxCents` is a ceiling: the
   * charge never exceeds it. `requestId` is your idempotency key: keep it across retries of one purchase, or a
   * retry buys a second period; use a new one for a new purchase. Refused while a card plan is active.
   */
  buyPlan(input: { tier: 'base' | 'pro' | 'max'; maxCents: number; requestId: string }): Promise<Record<string, unknown>> {
    const { tier, maxCents, requestId } = input;
    return this.request('POST', '/v1/account/plan/balance', { body: { tier, maxCents, requestId } });
  }
  /** The account behind this key. Use it to check that a key works. */
  me(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/auth/me');
  }
  /** Your invite code, the campaign terms, your commission and how many people you invited. */
  invite(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/invite');
  }
  /** One page of the people you invited; pass the previous page's `next` as `cursor`. */
  invitees(opts: { limit?: number; cursor?: string } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/invite/invitees', { query: opts });
  }
  /** Bind an invite code to your account, once ever. Credits a non-refundable bonus while the campaign runs. */
  bindInviteCode(code: string): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/invite/bind', { body: { code } });
  }
  /** The plans on sale: price and allowances of Base / Pro / Max. */
  plans(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/plans');
  }
  /** Balance and top-up options. */
  billing(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/billing');
  }
  /** One page of the balance statement, newest first; pass the previous page's `next` as `cursor`. */
  ledger(opts: { limit?: number; cursor?: string } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/ledger', { query: opts });
  }

  // ── addresses ────────────────────────────────────────────────────────────
  /** Buy the address behind a handle. `maxPriceCents` is a ceiling: the charge never exceeds it. */
  buyReveal(input: { entityId: string; maxPriceCents: number; viaPlan?: boolean }): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/reveals', { body: input });
  }
  /** Unlock with your plan's allowance instead of the balance: never charges (409 when the allowance is used up). */
  unlockWithPlan(entityId: string): Promise<Record<string, unknown>> {
    return this.buyReveal({ entityId, maxPriceCents: 0, viaPlan: true });
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

  // ── signals and radar (Pro and Max) ──────────────────────────
  /** Markets where several smart-money wallets bought one side. `source: 'mine'` lists what your own rule fired. */
  signals(opts: { source?: 'official' | 'mine'; category?: string; q?: string; ids?: string; limit?: number; offset?: number } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/signals', { query: opts });
  }
  /** Large taker fills across the whole market. `minUsd` is one of 5000, 10000, 25000, 50000, 100000, 250000, 1000000. */
  radar(opts: { minUsd?: number; side?: 'BUY' | 'SELL'; smart?: boolean; minPnlUsd?: number; category?: string; q?: string; limit?: number; offset?: number } = {}): Promise<Record<string, unknown>> {
    const { smart, ...rest } = opts;
    return this.request('GET', '/v1/radar', { query: { ...rest, ...(smart ? { smart: '1' } : {}) } });
  }
  /** Your access level and both push rules. */
  signalRules(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/signal-rules');
  }
  /** Set a push rule; delivered over `signalsWsUrl` and your webhook. */
  saveSignalRule(kind: 'signal' | 'radar', rule: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.request('PUT', `/v1/account/signal-rules/${kind}`, { body: rule });
  }
  walletGroups(): Promise<Record<string, unknown>[]> {
    return this.request('GET', '/v1/account/wallet-groups');
  }
  /** The wallets you follow, as a group that keeps itself up to date. */
  followedWalletGroup(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/wallet-groups/subscriptions');
  }
  createWalletGroup(name: string): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/wallet-groups', { body: { name } });
  }
  /** Rename a group, or add and remove wallets: handles, or addresses you paid to unlock. */
  editWalletGroup(id: string, change: { name?: string; add?: string[]; remove?: string[] }): Promise<Record<string, unknown>> {
    return this.request('PATCH', `/v1/account/wallet-groups/${encodeURIComponent(id)}`, { body: change });
  }
  /** Fill a group with the board's best traders matching the filters; `count` is 10, 20, 50 or 100. */
  quickAddWallets(id: string, filters: Record<string, unknown> & { count: number }): Promise<Record<string, unknown>> {
    return this.request('POST', `/v1/account/wallet-groups/${encodeURIComponent(id)}/quick-add`, { body: filters });
  }
  deleteWalletGroup(id: string): Promise<unknown> {
    return this.request('DELETE', `/v1/account/wallet-groups/${encodeURIComponent(id)}`);
  }

  // ── webhooks ─────────────────────────────────────────────────────────────
  /** Your webhook endpoint. With an API key the URL comes back as its origin only; adding or removing one needs the website. */
  webhooks(): Promise<Record<string, unknown>[]> {
    return this.request('GET', '/v1/account/webhooks');
  }
  /** Re-enable an endpoint that was disabled after a day of failures, and resume the subscriptions it paused. */
  reactivateWebhook(id: string): Promise<Record<string, unknown>> {
    return this.request('POST', `/v1/account/webhooks/${encodeURIComponent(id)}/reactivate`);
  }
  /** One page of delivery attempts (webhook and WebSocket), newest first. */
  deliveries(opts: { limit?: number; cursor?: string } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/deliveries', { query: opts });
  }
  /** Delivery success and latency over the last `hours`. */
  deliveryStats(opts: { hours?: number } = {}): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/delivery-stats', { query: opts });
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
  /** From a Max plan's monthly allowance: the whole sellable window, nothing charged. */
  createPlanExport(entityIds: string[]): Promise<Record<string, unknown>> {
    return this.request('POST', '/v1/account/exports', { body: { entityIds, viaPlan: true } });
  }
  /** The days that can be bought right now (a contiguous range ending at the latest published day). */
  exportWindow(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/account/exports/window');
  }
  /** The per-day prices exports are charged at. */
  exportRateCard(): Promise<Record<string, unknown>> {
    return this.request('GET', '/v1/exports/rate-card');
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
