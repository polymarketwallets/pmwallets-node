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
  /** Floor on markets won or lost (breakevens left out) — the floor the website uses. */
  minWinsLosses?: number;
  minFills?: number;
  minVolume?: number;
  style?: 'maker' | 'taker' | 'two-sided';
  category?: string;
  /** With `category`: at least this share (0–1] of the entity's traded value was in that category. */
  minCategoryShare?: number;
  status?: 'active' | 'quiet' | 'dormant';
  maxWallets?: number;
  /** Unlock price bands, any of them: 0 = free … 5 (the ladder is `pricing.bands`). Priced on 30d whatever the period. */
  bands?: readonly number[] | string;
  [k: string]: string | number | readonly number[] | undefined;
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

/** one of your addresses (see PmwClient.myAddresses): a board row plus how you hold it */
export interface MyAddressRow extends LeaderboardRow {
  /** 'paid' | 'free' | 'plan'; null = a free or plan unlock that lapsed — masked again, `revealCents` quotes it */
  unlockedBy: 'paid' | 'free' | 'plan' | null;
  /** set when your push subscription to it is paused, e.g. 'plan_ended', 'insufficient_balance' */
  pausedReason?: string;
}
/** unlocked but no fill in this window, so no figures to rank */
export interface MyAddressOutside { entityId: string; label: string; revealed: boolean; subscribed: boolean; unlockedBy: MyAddressRow['unlockedBy']; pausedReason?: string }
export interface MyAddresses extends Leaderboard {
  /** how many addresses you have unlocked */
  tracked: number;
  rows: MyAddressRow[];
  /** first page only */
  outside?: MyAddressOutside[];
}

export interface ExportInput { entityIds: string[]; from: string; to: string }
/** one daily file an export grants; `path` answers with a redirect to the file (see exportFileUrl) */
export interface ExportFile { wallet: string; day: string; rows: number; bytes: number; path: string }
