# pmwallets — Node.js SDK for PMWallets

[![npm](https://img.shields.io/npm/v/pmwallets.svg)](https://www.npmjs.com/package/pmwallets) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Official Node.js / TypeScript client for [PMWallets](https://pmwallets.com): the **Polymarket smart-money
leaderboard** computed from the Polygon chain, and the **real-time fills of the Polymarket wallets you follow** —
delivered in order, exactly once, with every gap replayed.

[中文说明](README.zh.md) · Python SDK: [pmwallets-python](https://github.com/polymarketwallets/pmwallets-python) ·
Ready-to-run copy-trading bot built on it: [polymarket-copy-trading-bot](https://github.com/polymarketwallets/polymarket-copy-trading-bot)

## Install

```bash
npm install pmwallets     # Node.js ≥ 20
```

Create an API key on [pmwallets.com/keys](https://pmwallets.com/keys).

## Usage

```ts
import { PmwClient, FillStream, FileStateStore, verifyWebhook } from 'pmwallets';

const pmw = new PmwClient({ apiKey: process.env.PMW_API_KEY! });

// who is making money on Polymarket, and how
const board = await pmw.leaderboard({ minWinLo: 0.55, minEligible: 30, style: 'taker', status: 'active', limit: 50 });

// follow one (billed per entity per hour; 402 = balance too low, 409 = dormant)
await pmw.subscribe({ entityId: board.rows[0].entityId, channels: ['ws'] });

// every fill of every entity you follow
const stream = new FillStream({
  client: pmw,
  store: new FileStateStore('./stream.json'),    // a restart resumes exactly where it stopped
  onFill: async (fill, { source }) => console.log(source, fill.side, fill.price, fill.tokenId),
  onEvent: (e) => e.type === 'replaced' && console.warn('another connection took the stream'),
});
await stream.start();
```

## What is in it

| | |
|---|---|
| `PmwClient` | leaderboard, entities, address unlocks, subscriptions, fills replay (`fills`, `fillsSince`), trade-history exports |
| `FillStream` | WebSocket with reconnect and keep-alive; detects missed frames by `session`/`seq` and replays them from the last fill delivered; de-duplicates by `eventId`; anchors behind the chain head on first start; persisted cursor |
| `verifyWebhook(rawBody, signature, secret)` | checks `x-pmw-signature` (HMAC-SHA256 of the raw body) |

One stream per account. An API-key connection takes priority over the pmwallets.com feed page (the page never takes the stream from it); between two API connections the newest wins, so run one consumer per account.
`HTTPS_PROXY`: pass `wsOptions: { agent }` for the WebSocket and set a global fetch dispatcher.

PMWallets' servers are in the United Kingdom: a consumer hosted in the UK or elsewhere in Europe receives fills soonest.

## Resources

- [Polymarket smart-money leaderboard](https://pmwallets.com) — profitable Polymarket traders scored from the Polygon chain, with win-rate confidence intervals
- [Polymarket copy trading guide](https://pmwallets.com/copy-trading) — which wallets are worth following and how to get their fills in time
- [How to learn from Polymarket smart money](https://pmwallets.com/learn) — reading a trader's record: confidence intervals, maker vs taker, market specialism
- [PMWallets API documentation](https://pmwallets.com/docs) — WebSocket and webhook fill push, fills replay, trade-history exports
- [Measured fill-push latency](https://pmwallets.com/latency) — block-to-push p50 / p95, published live
- [Ways to follow Polymarket wallets, compared](https://pmwallets.com/compare) — official leaderboard, free trackers, SQL dashboards
- [FAQ](https://pmwallets.com/faq) · [中文站](https://pmwallets.com/zh)

## License

MIT
