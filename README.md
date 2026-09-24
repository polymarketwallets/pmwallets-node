# pmwallets

Official Node.js SDK for [PMWallets](https://pmwallets.com): the Polymarket smart-money board computed from the
Polygon chain, and the real-time fills of the traders you subscribe to.

```bash
npm install pmwallets
```

```ts
import { PmwClient, FillStream, FileStateStore } from 'pmwallets';

const pmw = new PmwClient({ apiKey: process.env.PMW_API_KEY! });   // https://pmwallets.com/keys

// who is making money, and how
const board = await pmw.leaderboard({ minWinLo: 0.55, minEligible: 30, status: 'active', limit: 50 });

// every fill of every entity you subscribe to — in order, exactly once, gaps replayed
const stream = new FillStream({
  client: pmw,
  store: new FileStateStore('./stream.json'),
  onFill: async (fill, { source }) => console.log(source, fill.side, fill.price, fill.tokenId),
  onEvent: (e) => e.type === 'replaced' && console.warn('another connection took the stream'),
});
await stream.start();
```

- `PmwClient` — leaderboard, entities, address unlocks, subscriptions, fills replay, exports.
- `FillStream` — WebSocket with reconnect, `session`/`seq` gap detection, keyset replay from the last fill
  delivered, de-duplication by `eventId`, a persisted cursor so a restart resumes where it stopped.
- `verifyWebhook(rawBody, signatureHeader, secret)` — check `x-pmw-signature`.

A copy-trading bot built on it: [`pmwallets-copytrade`](https://www.npmjs.com/package/pmwallets-copytrade).
API reference: <https://pmwallets.com/docs>. MIT.
