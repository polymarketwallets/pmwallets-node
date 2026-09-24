/** One fill of an entity you subscribe to, exactly as the WebSocket frame and GET /v1/account/fills carry it. */
export interface Fill {
  /** `chain:block:blockHash:txHash:logIndex` — stable across retries and replays; deduplicate on it. */
  eventId: string;
  chain: number;
  /** The subscribed entity (0x address). */
  entityId: string;
  /** The address inside the entity that traded. */
  wallet: string;
  /** Block time, UTC, `YYYY-MM-DD HH:MM:SS`. */
  ts: string;
  block: number;
  blockHash: string;
  txHash: string;
  logIndex: number;
  exchange: string;
  side: 'BUY' | 'SELL';
  role: 'maker' | 'taker';
  /** Polymarket outcome token id (uint256 as a decimal string). */
  tokenId: string;
  /** Decimal string, e.g. "0.570000". */
  price: string;
  /** Integer string in 1e-6 share units ("3200000000" = 3,200 shares). */
  shares: string;
  /** Integer string in 1e-6 USDC units ("1824000000" = $1,824). */
  usdc: string;
  /** Integer string in 1e-6 USDC units. */
  fee: string;
}

export interface FillCursor { sinceBlock: number; sinceLogIndex: number }

export interface FillsPage { rows: Fill[]; next: FillCursor | null; subscriptions: number }

export interface Subscription {
  id: string;
  entityId: string;
  chain: number;
  channels: ('ws' | 'webhook')[];
  status: 'active' | 'paused' | 'canceled';
  fromBlock: number;
  createdAt: string;
  canceledAt: string | null;
  [k: string]: unknown;
}

export interface SubscribeInput {
  /** A 12-character handle or a full 0x address. */
  entityId: string;
  channels?: ('ws' | 'webhook')[];
  /** Subscribe even if the entity has had no fill for 7 days (otherwise the API answers 409). */
  acceptInactive?: boolean;
}

export interface LeaderboardQuery {
  period?: string;
  sort?: string;
  dir?: 'asc' | 'desc';
  limit?: number;
  offset?: number;
  minPnl?: number;
  minRoi?: number;
  minWinLo?: number;
  minEligible?: number;
  style?: 'maker' | 'taker' | 'two-sided';
  category?: string;
  status?: 'active' | 'quiet' | 'dormant';
  maxWallets?: number;
  [k: string]: string | number | undefined;
}

export interface LeaderboardRow {
  entityId: string;
  label: string;
  revealed: boolean;
  revealCents: number | null;
  subscribed: boolean;
  realizedPnl: number;
  unrealizedPnl: number;
  roiOnBuys: number | null;
  wins: number;
  losses: number;
  eligible: number;
  wr: number | null;
  lo: number | null;
  hi: number | null;
  pnlExTop: number | null;
  style: string;
  makerShare: number | null;
  wallets: number;
  nFills: number;
  status: string;
  [k: string]: unknown;
}

export interface Leaderboard {
  periodId: string;
  tier: string;
  tracked: number;
  rows: LeaderboardRow[];
  [k: string]: unknown;
}

export interface ExportInput { entityIds: string[]; from: string; to: string }
