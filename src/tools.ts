import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import type { PmwClient } from './client.js';
import { MarketLookup, microToDecimal } from './market.js';
import type { McpTool } from './mcp.js';
import type { Fill, FillCursor } from './types.js';

/** A fill with its decimal amounts and its market: what an agent can read without knowing the 1e-6 units. */
export async function describeFill(fill: Fill, markets: MarketLookup | null): Promise<Record<string, unknown>> {
  const f = fill as Fill & { marketTitle?: string; conditionId?: string };
  const market = markets ? await markets.byToken(fill.tokenId) : null;
  return {
    ...fill,
    usdcAmount: microToDecimal(fill.usdc),
    sharesAmount: microToDecimal(fill.shares),
    feeAmount: microToDecimal(fill.fee),
    market: market ?? (f.marketTitle && f.conditionId ? { conditionId: f.conditionId, question: f.marketTitle, outcome: null, slug: null, endDate: null, closed: null } : null),
  };
}

/**
 * Where `new_fills` left off, in a small JSON file (default ~/.pmwallets/mcp-cursor.json, or PMW_MCP_STATE).
 * Advanced as soon as the fills are returned: each fill is handed out once — to ONE consumer per file. Calls in
 * one process are serialized; two processes on one file (the CLI and an MCP server, two agents) are not, and may
 * both receive a page or move the cursor back: give each its own PMW_MCP_STATE.
 */
export class CursorFile {
  constructor(readonly path: string) {}
  load(): FillCursor | null {
    try {
      const c = JSON.parse(readFileSync(this.path, 'utf8')) as Partial<FillCursor>;
      return Number.isInteger(c.sinceBlock) && Number.isInteger(c.sinceLogIndex) ? { sinceBlock: c.sinceBlock!, sinceLogIndex: c.sinceLogIndex! } : null;
    } catch { return null; }
  }
  save(c: FillCursor): void {
    mkdirSync(dirname(this.path), { recursive: true });
    const tmp = `${this.path}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(c));
    renameSync(tmp, this.path);
  }
}

export const defaultCursorPath = () => process.env['PMW_MCP_STATE'] || join(homedir(), '.pmwallets', 'mcp-cursor.json');

/** blocks before the head a first `new_fills` starts from: about one hour of Polygon */
const FIRST_LOOKBACK_BLOCKS = 1_800;

const after = (a: FillCursor, b: FillCursor) => a.sinceBlock > b.sinceBlock || (a.sinceBlock === b.sinceBlock && a.sinceLogIndex > b.sinceLogIndex);
/** one new_fills at a time per cursor file in this process: two concurrent calls would read the same position */
const cursorQueues = new Map<string, Promise<unknown>>();

/** Every fill after `cursor` up to `limit`, and where the next call should start. */
export function newFills(client: PmwClient, cursorFile: CursorFile, markets: MarketLookup | null, limit = 100): Promise<Record<string, unknown>> {
  const prev = cursorQueues.get(cursorFile.path) ?? Promise.resolve();
  const run = prev.then(() => newFillsNow(client, cursorFile, markets, limit), () => newFillsNow(client, cursorFile, markets, limit));
  const settled = run.then(() => undefined, () => undefined);
  cursorQueues.set(cursorFile.path, settled);
  void settled.then(() => { if (cursorQueues.get(cursorFile.path) === settled) cursorQueues.delete(cursorFile.path); });
  return run;
}

async function newFillsNow(client: PmwClient, cursorFile: CursorFile, markets: MarketLookup | null, limit: number): Promise<Record<string, unknown>> {
  let cursor = cursorFile.load();
  let anchored = false;
  if (!cursor) {
    const r = await client.latency();
    const head = Number((r['head'] as { block?: unknown } | undefined)?.block);
    if (!Number.isInteger(head) || head <= 0) throw new Error('could not read the chain head');
    cursor = { sinceBlock: Math.max(1, head - FIRST_LOOKBACK_BLOCKS), sinceLogIndex: 0 };
    anchored = true;
  }
  const n = Number.isFinite(limit) ? Math.min(Math.max(1, Math.floor(limit)), 500) : 100;
  const page = await client.fills({ ...cursor, limit: n });
  const last = page.rows[page.rows.length - 1];
  const next = last ? { sinceBlock: last.block, sinceLogIndex: last.logIndex } : cursor;
  // never move back over what is on disk (this narrows, but does not close, the window for two processes on one file)
  const onDisk = cursorFile.load();
  if (!onDisk || after(next, onDisk)) cursorFile.save(next);
  return {
    fills: await Promise.all(page.rows.map((f) => describeFill(f, markets))),
    more: page.next !== null,
    subscriptions: page.subscriptions,
    ...(anchored ? { note: 'first call: started about one hour back; later calls continue from here' } : {}),
  };
}

const str = (description: string) => ({ type: 'string', description });
const num = (description: string) => ({ type: 'number', description });
const obj = (properties: Record<string, unknown>, required: string[] = []) => ({ type: 'object', properties, required, additionalProperties: false });

const BOARD_FILTERS = {
  period: str('statistics window: "30d" (default) or "7d"'),
  sort: { type: 'string', enum: ['realizedPnl', 'roiOnBuys', 'eligible', 'winsLosses', 'nFills', 'conditions', 'grossBuyUsdc', 'unrealizedPnl', 'winLo', 'fillsPerDay', 'lastFill'], description: 'column to sort by (default realizedPnl)' },
  dir: { type: 'string', enum: ['asc', 'desc'] },
  limit: num('rows, at most 200 (rows count against a daily allowance of 20,000)'),
  offset: num('rows to skip (signed-in accounts; anonymous callers see a fixed sample)'),
  minPnl: num('floor on realized PnL in USD'),
  minRoi: num('floor on ROI on buys (0.1 = 10%)'),
  minWinLo: num('floor on the lower bound of the 95% win-rate interval (0–1): a win rate that is not luck'),
  minEligible: num('floor on markets that count toward the win rate'),
  minWinsLosses: num('floor on markets won or lost'),
  minFills: num('floor on number of fills'),
  minVolume: num('floor on traded volume in USD'),
  style: { type: 'string', enum: ['maker', 'taker', 'two-sided'] },
  category: str('market category, e.g. "politics", "sports", "crypto", "crypto-updown", "esports"'),
  minCategoryShare: num('with category: at least this share (0–1] of the trader\'s volume in it'),
  status: { type: 'string', enum: ['active', 'quiet', 'dormant'] },
  maxWallets: num('at most this many wallets in the entity'),
  bands: str('unlock price bands to include, comma-separated: 0 = free … 5'),
};

export const MCP_INSTRUCTIONS = `PMWallets: the Polymarket smart-money leaderboard (traders scored from the Polygon chain) and the live fills of the traders this account follows.
- leaderboard / entity: find traders. entityId is a 12-character handle until the address is unlocked.
- subscribe starts hourly billing on the account (unless the plan covers it); cancel_subscription stops it.
- new_fills returns the fills of followed traders since the previous call (each fill once). usdc/shares/fee are 1e-6 integer units; usdcAmount/sharesAmount are the same as decimals. market tells the question and outcome.
- Placing orders on Polymarket is not part of this server: use the pmwallets-copytrade tools (order_buy / order_sell) for that.`;

/** The PMWallets tools for an MCP host. `client` is null when no API key is set: every tool then says how to set one. */
export function pmwalletsTools(client: PmwClient | null, opts: { markets?: MarketLookup | null; cursor?: CursorFile } = {}): McpTool[] {
  const markets = opts.markets === undefined ? new MarketLookup() : opts.markets;
  const cursor = opts.cursor ?? new CursorFile(defaultCursorPath());
  const need = (): PmwClient => {
    if (!client) throw new Error('PMW_API_KEY is not set: create a key on https://pmwallets.com/keys and put it in the MCP server\'s env');
    return client;
  };
  const id = (v: unknown, name: string): string => {
    if (typeof v !== 'string' || !v.trim()) throw new Error(`${name} is required`);
    return v.trim();
  };
  return [
    {
      name: 'leaderboard',
      description: 'Polymarket traders ranked by realized PnL and other stats over a window, with filters. Each row is an entity (one trader, possibly several wallets).',
      inputSchema: obj(BOARD_FILTERS),
      handler: async (a) => need().leaderboard(a as never),
    },
    {
      name: 'entity',
      description: 'One trader\'s statistics by handle or address (full address only once unlocked).',
      inputSchema: obj({ id: str('12-character handle or 0x address'), period: str('"30d" or "7d"') }, ['id']),
      handler: async (a) => need().entity(id(a['id'], 'id'), a['period'] ? { period: String(a['period']) } : {}),
    },
    {
      name: 'subscriptions',
      description: 'The traders this account follows (active and paused subscriptions).',
      inputSchema: obj({}),
      handler: async () => need().subscriptions(),
    },
    {
      name: 'subscribe',
      description: 'Follow a trader: their fills start arriving from now on. BILLS THE ACCOUNT per hour (unless the plan covers it). 402 = balance too low; 409 = no fill for 7 days (retry with acceptInactive).',
      inputSchema: obj({ entityId: str('handle or 0x address'), acceptInactive: { type: 'boolean' } }, ['entityId']),
      handler: async (a) => need().subscribe({ entityId: id(a['entityId'], 'entityId'), channels: ['ws'], ...(a['acceptInactive'] === true ? { acceptInactive: true } : {}) }),
    },
    {
      name: 'cancel_subscription',
      description: 'Stop following (and stop paying for) a subscription, by its id from `subscriptions`.',
      inputSchema: obj({ id: str('subscription id') }, ['id']),
      handler: async (a) => need().cancelSubscription(id(a['id'], 'id')),
    },
    {
      name: 'new_fills',
      description: 'Fills of the followed traders since the previous call of this tool (first call: the last hour). Each fill is returned once to this server; the CLI `pmwallets new-fills` shares the position file, so do not poll with both at once (or give each its own PMW_MCP_STATE). Call again while `more` is true.',
      inputSchema: obj({ limit: num('at most this many fills (default 100, max 500)') }),
      handler: async (a) => newFills(need(), cursor, markets, a['limit'] === undefined ? 100 : Number(a['limit'])),
    },
    {
      name: 'fills',
      description: 'Fills of the followed traders strictly after a ledger position (block, logIndex), oldest first. Does not move the new_fills cursor.',
      inputSchema: obj({ sinceBlock: num('block'), sinceLogIndex: num('log index within the block (default 0)'), limit: num('max 500') }, ['sinceBlock']),
      handler: async (a) => {
        const int = (v: unknown, name: string, d: number) => { const n = v === undefined ? d : Number(v); if (!Number.isInteger(n) || n < 0) throw new Error(`${name} must be a whole number ≥ 0`); return n; };
        const page = await need().fills({ sinceBlock: int(a['sinceBlock'], 'sinceBlock', Number.NaN), sinceLogIndex: int(a['sinceLogIndex'], 'sinceLogIndex', 0), limit: Math.min(Math.max(1, int(a['limit'], 'limit', 100)), 500) });
        return { ...page, rows: await Promise.all(page.rows.map((f) => describeFill(f, markets))) };
      },
    },
    {
      name: 'market',
      description: 'The Polymarket market and outcome an outcome token id belongs to (question, outcome name, slug, end date).',
      inputSchema: obj({ tokenId: str('outcome token id (decimal)') }, ['tokenId']),
      handler: async (a) => (await (markets ?? new MarketLookup()).byToken(id(a['tokenId'], 'tokenId'))) ?? { error: 'not found' },
    },
    {
      name: 'account',
      description: 'The account\'s plan (allowances used) and balance.',
      inputSchema: obj({}),
      handler: async () => { const c = need(); const [plan, billing] = await Promise.all([c.plan(), c.billing()]); return { plan, billing }; },
    },
  ];
}
