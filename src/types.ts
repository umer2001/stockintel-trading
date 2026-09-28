import type {
  Welcome,
  ExecutionEvent,
  QuoteUpdate,
  OrderBookUpdate,
  TradingSessionStatus,
  Error as CapriProtoError,
  Market,
  OrderType,
  OrderSide,
  TimeInForce,
  OrderStatus,
  SymbolRef
} from './proto/capri.js';

export type SymbolInput = string | SymbolRef;

export interface StockIntelClientOptions {
  /**
   * API Token (si_sb_... for Sandbox, si_lv_... for Live)
   */
  token: string;

  /**
   * WebSocket endpoint. Defaults to wss://trading.stockintel.com/ws/v1
   */
  url?: string;

  /**
   * Optional OTP to submit automatically upon connection if OTP gate is active.
   * Sandbox OTP is typically "54321".
   */
  otp?: string;

  /**
   * Request timeout in milliseconds (default 10,000 ms)
   */
  requestTimeoutMs?: number;

  /**
   * Whether to automatically reconnect on unexpected disconnects.
   */
  autoReconnect?: boolean;

  /**
   * Delay before reconnection attempts in milliseconds (default 3,000 ms)
   */
  reconnectIntervalMs?: number;
}

export interface PlaceOrderParams {
  brokerCode: string;
  clientCode: string;
  symbol: string;
  side: OrderSide;
  quantity: number;
  market?: Market;
  type?: OrderType;
  price?: number;
  stopPrice?: number;
  timeInForce?: TimeInForce;
  pin?: string;
}

export interface CancelOrderParams {
  brokerCode: string;
  clientCode: string;
  brokerOrderId?: string;
  exchangeOrderId?: string;
  pin?: string;
}

export interface ListOrdersParams {
  brokerCode: string;
  clientCode: string;
  status?: OrderStatus;
  symbol?: string;
  market?: Market;
  side?: OrderSide;
  type?: OrderType;
  dateFrom?: string;
  dateTo?: string;
  count?: number;
  offset?: number;
}

export interface GetHistoricalParams {
  symbol: string;
  market?: Market;
  interval: string;
  timeFrom: Date | string | number;
  timeTo: Date | string | number;
}

export class StockIntelApiError extends Error {
  public readonly code: number;
  public readonly reason: string;
  public readonly retryAfterMs?: number;

  constructor(error: CapriProtoError) {
    super(`StockIntel API Error [${error.code}]: ${error.message} (${error.reason})`);
    this.name = 'StockIntelApiError';
    this.code = error.code;
    this.reason = error.reason;
    this.retryAfterMs = error.retryInfo?.retryAfterMs;
  }
}

export type StockIntelEventMap = {
  welcome: (welcome: Welcome) => void;
  execution: (event: ExecutionEvent) => void;
  quote: (quote: QuoteUpdate) => void;
  orderbook: (orderbook: OrderBookUpdate) => void;
  session_status: (status: TradingSessionStatus) => void;
  error: (err: StockIntelApiError | Error) => void;
  open: () => void;
  close: (code: number, reason: string) => void;
};
