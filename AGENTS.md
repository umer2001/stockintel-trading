# StockIntel Trading API Client (PSX)

This repository is an maintained fork of [`capitalstake/stockintel-trading`](https://github.com/capitalstake/stockintel-trading), published as the GitHub Packages package [`@umer2001/stockintel-trading`](https://github.com/umer2001/stockintel-trading/packages).

It provides a strongly typed TypeScript client, auto-reconnecting WebSocket engine, and pre-compiled Protocol Buffer definitions for the **StockIntel Trading API** (Pakistan Stock Exchange).

---

## 1. Capabilities

1. Connect to StockIntel Trading WebSocket gateway with authentication and optional OTP handling.
2. Submit orders (`PlaceOrder`) and cancellations (`CancelOrder`) with fire-and-acknowledge semantics.
3. Stream real-time execution events, market quotes, and 10-deep order books.
4. Query account balances, margin/buying power, and open positions.
5. Request historical OHLCV candle data across supported intervals.

---

## 2. System Architecture & Protocol

### 2.1 Transport & Wire Format
- **Transport**: Secure WebSocket (`wss://trading.stockintel.com/ws/v1`).
- **Subprotocol**: `capri.v1` (sent via `Sec-WebSocket-Protocol`).
- **Encoding**: **Google Protocol Buffers (proto3)** in binary frames (opcode `0x2`). Text frames are strictly rejected by the server.
- **Envelopes**:
  - Outbound (Client → Server): Always wrapped in `ClientFrame`.
  - Inbound (Server → Client): Always wrapped in `ServerFrame`.
- **Correlation**: Command requests carry a client-generated UUID `request_id`. The server echoes this exact `request_id` in its response or error. Server push events (`Welcome`, `ExecutionEvent`, `TradingSessionStatus`, `QuoteUpdate`, `OrderBookUpdate`) have an empty string `request_id`.

### 2.2 PSX Market Segments
The schema in `capri.proto` maps directly to PSX markets:
- `MARKET_REG` (1) — Regular Market
- `MARKET_BNB` (2) — Buy-Back / Book Building
- `MARKET_FUT` (3) — Deliverable Futures
- `MARKET_ODL` (4) — Odd Lots
- `MARKET_SQR` (5) — Square-off Market
- `MARKET_FSR` (6) — Futures Spread
- `MARKET_NDM` (7) — Negotiated Deal Market
- `MARKET_SIF` (8) — Stock Index Futures
- `MARKET_IDX` (11) — Indices (Quote only)

---

## 3. Repository & Package Layout

```
.github/workflows/
  sync-and-publish.yml   # Daily sync with upstream, build, and publish workflow
src/
  proto/                 # Generated TypeScript definitions from capri.proto
  client.ts              # StockIntelTradingClient implementation
  types.ts               # Options, request params, and typed event signatures
  index.ts               # Public exports
capri.proto              # Authoritative upstream Protocol Buffer schema
package.json             # @umer2001/stockintel-trading package definition
tsup.config.ts           # ESM & CommonJS dual build configuration
tsconfig.json            # TypeScript configuration
```

---

## 4. Development & Build Workflow

- **Package Manager**: `pnpm` (configured with `node-linker=hoisted` in `.npmrc`).
- **Compile Protobuf**:
  ```bash
  pnpm run proto:gen
  ```
- **Build Package (ESM + CJS + Types)**:
  ```bash
  pnpm run build
  ```
- **Typecheck**:
  ```bash
  pnpm run typecheck
  ```

---

## 5. Fork Sync & Automated Publishing

A GitHub Action (`.github/workflows/sync-and-publish.yml`) runs on a daily schedule (`02:00 UTC`) or via manual dispatch:
1. Fetches commits from `upstream` (`https://github.com/capitalstake/stockintel-trading.git`).
2. If new changes exist, merges `upstream/main` into `main`.
3. Runs `pnpm run build` and `pnpm run typecheck`.
4. Bumps the package version (patch) and tags the commit.
5. Pushes updates back to `origin` (`https://github.com/umer2001/stockintel-trading.git`).
6. Publishes the updated package to GitHub Packages (`npm.pkg.github.com`) using `GITHUB_TOKEN`.
7. Generates a GitHub Release.

---

## 6. Standards & Rules

- **Conventional Commits**: Commits must follow conventional commits (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`).
- **Clean Code & DRY**: Avoid duplication, keep helper functions modular and well-documented.
- **Safety**: Do not commit secrets (`.env`, tokens).
