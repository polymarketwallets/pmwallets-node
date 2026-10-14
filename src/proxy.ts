import { HttpsProxyAgent } from 'https-proxy-agent';
import { EnvHttpProxyAgent, setGlobalDispatcher } from 'undici';

/**
 * Honour HTTPS_PROXY / HTTP_PROXY / NO_PROXY for the CLI. Node's built-in fetch ignores them, and many users can
 * reach PMWallets or Polymarket only through a proxy: this covers fetch, and returns an agent for the WebSocket
 * to `wsUrl` unless NO_PROXY exempts its host.
 */
export function applyProxyFromEnv(wsUrl?: string, env: NodeJS.ProcessEnv = process.env): { agent?: HttpsProxyAgent<string>; proxy?: string } {
  const proxy = env['HTTPS_PROXY'] || env['https_proxy'] || env['HTTP_PROXY'] || env['http_proxy'];
  if (!proxy) return {};
  // undici 6 (kept so Node 20.0+ still installs) prints an 'experimental' warning for this agent on every run
  const emit = process.emitWarning;
  process.emitWarning = ((w: string | Error, ...rest: unknown[]) => {
    const opt = rest[0] as { code?: string } | undefined;
    if (opt?.code === 'UNDICI-EHPA' || rest[1] === 'UNDICI-EHPA') return;
    (emit as (...a: unknown[]) => void).call(process, w, ...rest);
  }) as typeof process.emitWarning;
  try { setGlobalDispatcher(new EnvHttpProxyAgent()); } finally { process.emitWarning = emit; }
  const shown = proxy.replace(/\/\/[^@/]*@/, '//***@');
  if (wsUrl && noProxy(new URL(wsUrl).hostname, env['NO_PROXY'] || env['no_proxy'] || '')) return { proxy: shown };
  return { agent: new HttpsProxyAgent(proxy), proxy: shown };
}

/** NO_PROXY: comma-separated hosts or domain suffixes (`.example.com` / `example.com`), `*` for all */
export function noProxy(host: string, list: string): boolean {
  const h = host.toLowerCase().replace(/^\[|\]$/g, '');
  return list.split(',').map((s) => s.trim().toLowerCase().replace(/:\d+$/, '')).filter(Boolean)
    .some((e) => e === '*' || h === e.replace(/^\./, '') || h.endsWith(e.startsWith('.') ? e : `.${e}`));
}
