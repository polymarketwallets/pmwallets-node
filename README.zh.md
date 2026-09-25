# pmwallets —— PMWallets Node.js SDK

[![npm](https://img.shields.io/npm/v/pmwallets.svg)](https://www.npmjs.com/package/pmwallets) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[PMWallets](https://pmwallets.com/zh) 官方 Node.js / TypeScript 客户端：从 Polygon 链上计算的 **Polymarket 聪明钱排行榜**，
以及**你所跟踪的 Polymarket 钱包的实时成交** —— 按顺序、只送一次、漏掉的自动补发。

[English](README.md) · Python SDK：[pmwallets-python](https://github.com/polymarketwallets/pmwallets-python) ·
基于它的开箱即用跟单机器人：[polymarket-copy-trading-bot](https://github.com/polymarketwallets/polymarket-copy-trading-bot)

## 安装

```bash
npm install pmwallets     # Node.js ≥ 20
```

在 [pmwallets.com/keys](https://pmwallets.com/keys) 创建 API key。用法示例见 [README.md](README.md#usage)。

## 包含什么

| | |
|---|---|
| `PmwClient` | 排行榜、实体、地址解锁、订阅、成交补发（`fills`、`fillsSince`）、交易历史导出 |
| `FillStream` | 自动重连与保活的 WebSocket；按 `session`/`seq` 发现漏帧并从最后送达的成交补发；按 `eventId` 去重；首次启动锚定在链头之后；游标持久化 |
| `verifyWebhook(rawBody, signature, secret)` | 校验 `x-pmw-signature`（原始 body 的 HMAC-SHA256） |

每个账户只有一条推送流。API key 建立的连接优先于 pmwallets.com 的推送页（推送页不会抢走它的流）；两条 API 连接之间仍是后连的顶掉先连的，所以一个账户只跑一个消费者。

PMWallets 的服务器在英国。若还要在 Polymarket 下单，请部署在爱尔兰（AWS eu-west-1）：Polymarket 不接受来自英国、美国及部分欧盟国家的 API 订单。

## 相关链接

- [Polymarket 聪明钱排行榜](https://pmwallets.com/zh) —— 从 Polygon 链上计算的 Polymarket 盈利交易者，胜率带置信区间
- [Polymarket 跟单指南](https://pmwallets.com/zh/copy-trading) —— 哪些钱包值得跟，以及怎样及时拿到他们的成交
- [怎样向 Polymarket 聪明钱学习](https://pmwallets.com/zh/learn) —— 读懂一份战绩：置信区间、挂单与吃单、擅长的市场
- [PMWallets API 文档](https://pmwallets.com/zh/docs) —— WebSocket 与 Webhook 成交推送、补发接口、交易历史导出
- [成交推送实测延迟](https://pmwallets.com/zh/latency) —— 出块到推送的 p50 / p95，实时公布
- [追踪 Polymarket 钱包的几种做法对比](https://pmwallets.com/zh/compare) —— 官方榜单、免费追踪器、SQL 看板
- [常见问题](https://pmwallets.com/zh/faq) · [English site](https://pmwallets.com)

## 许可证

MIT
