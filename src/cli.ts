#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PmwClient } from './client.js';
import { MarketLookup } from './market.js';
import { serveMcp } from './mcp.js';
import { MCP_INSTRUCTIONS, pmwalletsTools } from './tools.js';
import { VERSION } from './version.js';
import { Watcher } from './watch.js';
import { parseArgs } from './args.js';
import { applyProxyFromEnv } from './proxy.js';

const HELP = `pmwallets ${VERSION} — PMWallets from the command line, for people and AI agents

Set PMW_API_KEY (create one on https://pmwallets.com/keys). Every command prints JSON.

  pmwallets watch [--exec "<command>"] [--dir ./pmw-watch] [--batch-sec 0] [--max-batch 50]
                  [--exec-timeout-sec 600] [--takeover] [--no-market]
      follow the fills of the traders you subscribe to, live. Without --exec: one JSON line per fill on stdout.
      With --exec: the command runs once per batch, the fills on its stdin as JSON lines (and in $PMW_BATCH_FILE);
      fills arriving while it runs make the next batch. A batch is handed over at most once.
  pmwallets leaderboard [--period 30d] [--sort realizedPnl] [--limit 20] [--minWinLo 0.55] [--category politics] …
  pmwallets entity <handle|address> [--period 30d]
  pmwallets subscriptions
  pmwallets subscribe <handle|address> [--accept-inactive]     starts hourly billing
  pmwallets unsubscribe <subscription-id>
  pmwallets new-fills [--limit 100]        fills since the previous call; the position is kept in ~/.pmwallets/mcp-cursor.json
                                           (PMW_MCP_STATE), shared with the MCP tool new_fills — one consumer per file
  pmwallets fills --since-block <n> [--since-log-index <n>] [--limit 100]
  pmwallets market <tokenId>               the Polymarket question and outcome of a token
  pmwallets account                        plan and balance
  pmwallets mcp                            MCP server on stdio (tools: leaderboard, entity, subscribe, new_fills, …)
  pmwallets skill                          print the guide for AI agents (SKILL.md)

Placing orders: pmwallets-copytrade (npm i -g pmwallets-copytrade), commands order / positions / balance.
Docs: https://pmwallets.com/docs`;

function positiveNumber(v: string | true | undefined, name: string, fallback: number): number {
  if (v === undefined) return fallback;
  const n = Number(v);
  if (v === true || !Number.isFinite(n) || n < 0) throw new Error(`--${name} must be a number ≥ 0`);
  return n;
}

function client(): PmwClient | null {
  const apiKey = process.env['PMW_API_KEY'];
  return apiKey ? new PmwClient({ apiKey, baseUrl: process.env['PMW_BASE_URL'] || undefined }) : null;
}

const print = (v: unknown) => console.log(JSON.stringify(v, null, 2));

async function main(argv: string[]): Promise<number> {
  const cmd = argv[0];
  const { pos, flags } = parseArgs(argv.slice(1), ['takeover', 'no-market', 'accept-inactive']);
  if (cmd === '--version' || cmd === 'version') { console.log(VERSION); return 0; }
  if (cmd === 'skill') { process.stdout.write(readFileSync(join(dirname(fileURLToPath(import.meta.url)), '..', 'SKILL.md'), 'utf8')); return 0; }
  const { agent } = applyProxyFromEnv(client()?.wsUrl);
  if (cmd === 'mcp') {
    // stdout belongs to the protocol: nothing else may be printed there
    await serveMcp({ name: 'pmwallets', version: VERSION, tools: pmwalletsTools(client()), instructions: MCP_INSTRUCTIONS });
    return 0;
  }
  if (cmd === 'watch') {
    if (flags['exec'] === true) throw new Error('--exec needs a command, e.g. --exec "claude -p \'…\'"');
    const c = client();
    if (!c) throw new Error('set PMW_API_KEY (create a key on https://pmwallets.com/keys)');
    const watcher = new Watcher({
      client: c,
      dir: typeof flags['dir'] === 'string' ? flags['dir'] : './pmw-watch',
      exec: typeof flags['exec'] === 'string' ? flags['exec'] : undefined,
      batchMs: positiveNumber(flags['batch-sec'], 'batch-sec', 0) * 1000,
      maxBatch: Math.max(1, Math.floor(positiveNumber(flags['max-batch'], 'max-batch', 50))),
      execTimeoutMs: positiveNumber(flags['exec-timeout-sec'], 'exec-timeout-sec', 600) * 1000,
      takeover: flags['takeover'] === true,
      markets: flags['no-market'] === true ? null : new MarketLookup(),
      log: (e) => process.stderr.write(`${JSON.stringify({ at: new Date().toISOString(), ...e })}\n`),
      streamOptions: agent ? { wsOptions: { agent } } : undefined,
    });
    process.on('SIGINT', () => void watcher.stop(0));
    process.on('SIGTERM', () => void watcher.stop(0));
    await watcher.start();
    return watcher.done;
  }

  const tools = new Map(pmwalletsTools(client()).map((t) => [t.name, t]));
  const call = async (name: string, args: Record<string, unknown>) => print(await tools.get(name)!.handler(args));
  switch (cmd) {
    case 'leaderboard': {
      const query: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(flags)) if (v !== true) query[k] = v;
      await call('leaderboard', query);
      return 0;
    }
    case 'entity': await call('entity', { id: pos[0], ...(typeof flags['period'] === 'string' ? { period: flags['period'] } : {}) }); return 0;
    case 'subscriptions': await call('subscriptions', {}); return 0;
    case 'subscribe': await call('subscribe', { entityId: pos[0], acceptInactive: flags['accept-inactive'] === true }); return 0;
    case 'unsubscribe': await call('cancel_subscription', { id: pos[0] }); return 0;
    case 'new-fills': await call('new_fills', { limit: positiveNumber(flags['limit'], 'limit', 100) }); return 0;
    case 'fills':
      if (flags['since-block'] === undefined) throw new Error('--since-block is required (or use new-fills)');
      await call('fills', { sinceBlock: positiveNumber(flags['since-block'], 'since-block', 0), sinceLogIndex: positiveNumber(flags['since-log-index'], 'since-log-index', 0), limit: positiveNumber(flags['limit'], 'limit', 100) });
      return 0;
    case 'market': await call('market', { tokenId: pos[0] }); return 0;
    case 'account': await call('account', {}); return 0;
    default:
      console.log(HELP);
      return cmd && cmd !== 'help' && cmd !== '--help' ? 1 : 0;
  }
}

main(process.argv.slice(2)).then(
  (code) => { process.exitCode = code; },
  (e: unknown) => {
    const err = e as { status?: number; body?: unknown; message?: string };
    process.stderr.write(`${JSON.stringify(err.status ? { error: `HTTP ${err.status}`, body: err.body } : { error: err.message ?? String(e) })}\n`);
    process.exitCode = 1;
  },
);
