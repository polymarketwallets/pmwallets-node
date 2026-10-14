/**
 * What a fill was about, from Polymarket's public CLOB API: a fill carries an outcome token id, and an agent (or a
 * person) reading it needs the question and which side of it was bought. Best effort: any failure is `null`, never
 * an exception — a fill is still worth delivering without its market.
 */
export interface MarketInfo {
  conditionId: string;
  question: string;
  /** the outcome this token pays on, e.g. "Yes" / "No" / a candidate's name; null when the CLOB does not list it */
  outcome: string | null;
  slug: string | null;
  endDate: string | null;
  closed: boolean | null;
}

export interface MarketLookupOptions {
  clobUrl?: string;
  fetch?: typeof fetch;
  timeoutMs?: number;
  /** how long a found market is kept (default 10 min); a failed lookup is retried after 30 s */
  ttlMs?: number;
  maxEntries?: number;
}

/** a Polymarket outcome id: a canonical decimal uint256 — anything else is never put into a URL */
const TOKEN_ID = /^(0|[1-9][0-9]{0,77})$/;

export class MarketLookup {
  private readonly clobUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private readonly ttlMs: number;
  private readonly maxEntries: number;
  private readonly cache = new Map<string, { at: number; info: MarketInfo | null }>();

  constructor(opts: MarketLookupOptions = {}) {
    this.clobUrl = (opts.clobUrl ?? 'https://clob.polymarket.com').replace(/\/+$/, '');
    this.fetchImpl = opts.fetch ?? fetch;
    this.timeoutMs = opts.timeoutMs ?? 5_000;
    this.ttlMs = opts.ttlMs ?? 10 * 60_000;
    this.maxEntries = opts.maxEntries ?? 5_000;
  }

  async byToken(tokenId: string): Promise<MarketInfo | null> {
    if (typeof tokenId !== 'string' || !TOKEN_ID.test(tokenId)) return null;
    const hit = this.cache.get(tokenId);
    if (hit && Date.now() - hit.at < (hit.info ? this.ttlMs : 30_000)) return hit.info;
    let info: MarketInfo | null = null;
    try {
      const byToken = await this.getJson(`/markets-by-token/${tokenId}`);
      const cid = typeof byToken?.condition_id === 'string' ? byToken.condition_id : '';
      if (/^0x[0-9a-fA-F]{64}$/.test(cid)) {
        const m = await this.getJson(`/markets/${cid}`);
        if (m && typeof m === 'object' && typeof m.question === 'string') {
          const tok = Array.isArray(m.tokens) ? m.tokens.find((t: any) => String(t?.token_id ?? '') === tokenId) : undefined;
          info = {
            conditionId: cid,
            question: m.question,
            outcome: typeof tok?.outcome === 'string' ? tok.outcome : null,
            slug: typeof m.market_slug === 'string' && m.market_slug ? m.market_slug : null,
            endDate: typeof m.end_date_iso === 'string' && m.end_date_iso ? m.end_date_iso : null,
            closed: typeof m.closed === 'boolean' ? m.closed : null,
          };
        }
      }
    } catch { /* unknown: retried after 30 s */ }
    if (this.cache.size >= this.maxEntries) this.cache.delete(this.cache.keys().next().value as string);
    this.cache.set(tokenId, { at: Date.now(), info });
    return info;
  }

  private async getJson(path: string): Promise<any> {
    const res = await this.fetchImpl(this.clobUrl + path, { signal: AbortSignal.timeout(this.timeoutMs) });
    if (!res.ok) { await res.body?.cancel(); throw new Error(`CLOB ${res.status}`); }
    return res.json();
  }
}

/** "1824000000" (1e-6 units) → "1824.000000"; anything that is not a whole number comes back as null */
export function microToDecimal(micro: string): string | null {
  if (typeof micro !== 'string' || !/^-?[0-9]+$/.test(micro)) return null;
  const neg = micro.startsWith('-');
  const digits = (neg ? micro.slice(1) : micro).padStart(7, '0');
  return `${neg ? '-' : ''}${digits.slice(0, -6).replace(/^0+(?=\d)/, '')}.${digits.slice(-6)}`;
}
