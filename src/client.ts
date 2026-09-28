import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import { v4 as uuidv4 } from 'uuid';
import {
  ClientFrame,
  ServerFrame,
  Welcome,
  SubmitOtpResponse,
  PlaceOrderResponse,
  CancelOrderResponse,
  ListOrdersResponse,
  ListAccountsResponse,
  GetAccountResponse,
  GetSessionStatusResponse,
  GetHistoricalResponse,
  SubscriptionResponse,
  SymbolRef,
  Market,
  OrderType,
  OrderSide,
  OrderStatus,
  TimeInForce
} from './proto/capri.js';
import { Timestamp } from './proto/google/protobuf/timestamp.js';
import {
  StockIntelClientOptions,
  PlaceOrderParams,
  CancelOrderParams,
  ListOrdersParams,
  GetHistoricalParams,
  StockIntelApiError,
  StockIntelEventMap,
  SymbolInput
} from './types.js';

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (reason: any) => void;
  timer: NodeJS.Timeout;
}

function dateToTimestamp(d: Date | string | number): Timestamp {
  const date = typeof d === 'string' || typeof d === 'number' ? new Date(d) : d;
  const millis = date.getTime();
  const seconds = Math.floor(millis / 1000);
  const nanos = (millis % 1000) * 1000000;
  return { seconds: String(seconds), nanos };
}

function normalizeSymbolRefs(symbols: SymbolInput[], defaultMarket: Market = Market.REG): SymbolRef[] {
  return symbols.map((s) => {
    if (typeof s === 'string') {
      return { market: defaultMarket, symbol: s };
    }
    return s;
  });
}

export class StockIntelTradingClient extends EventEmitter {
  private readonly token: string;
  private readonly url: string;
  private readonly otp?: string;
  private readonly requestTimeoutMs: number;
  private readonly autoReconnect: boolean;
  private readonly reconnectIntervalMs: number;

  private ws: WebSocket | null = null;
  private pending = new Map<string, PendingRequest>();
  private isConnecting = false;
  private isExplicitlyClosed = false;
  private welcomeData: Welcome | null = null;

  constructor(options: StockIntelClientOptions) {
    super();
    if (!options.token) {
      throw new Error('API token is required to initialize StockIntelTradingClient');
    }
    this.token = options.token;
    this.url = options.url || 'wss://trading.stockintel.com/ws/v1';
    this.otp = options.otp;
    this.requestTimeoutMs = options.requestTimeoutMs || 10000;
    this.autoReconnect = options.autoReconnect ?? false;
    this.reconnectIntervalMs = options.reconnectIntervalMs || 3000;
  }

  public get isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  public get welcome(): Welcome | null {
    return this.welcomeData;
  }

  /**
   * Open WebSocket connection and await Welcome message.
   */
  public async connect(): Promise<Welcome> {
    if (this.isConnected && this.welcomeData) {
      return this.welcomeData;
    }
    if (this.isConnecting) {
      return new Promise((resolve, reject) => {
        this.once('welcome', resolve);
        this.once('error', reject);
      });
    }

    this.isConnecting = true;
    this.isExplicitlyClosed = false;

    return new Promise<Welcome>((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url, 'capri.v1', {
          headers: {
            Authorization: `Bearer ${this.token}`,
          },
        });

        this.ws.binaryType = 'nodebuffer';

        const connectTimeout = setTimeout(() => {
          if (this.isConnecting) {
            this.cleanup();
            reject(new Error('Connection timed out waiting for Welcome frame'));
          }
        }, this.requestTimeoutMs);

        this.ws.on('open', () => {
          this.emit('open');
        });

        this.ws.on('message', async (data: WebSocket.RawData) => {
          try {
            const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data as any);
            const frame = ServerFrame.fromBinary(buffer);
            await this.handleServerFrame(frame, (welcome) => {
              clearTimeout(connectTimeout);
              this.isConnecting = false;
              resolve(welcome);
            });
          } catch (err) {
            this.emit('error', err instanceof Error ? err : new Error(String(err)));
          }
        });

        this.ws.on('error', (err) => {
          this.emit('error', err);
          if (this.isConnecting) {
            clearTimeout(connectTimeout);
            this.isConnecting = false;
            reject(err);
          }
        });

        this.ws.on('close', (code, reason) => {
          const reasonStr = reason.toString();
          this.emit('close', code, reasonStr);
          this.cleanup();

          if (this.autoReconnect && !this.isExplicitlyClosed) {
            setTimeout(() => {
              this.connect().catch((err) => this.emit('error', err));
            }, this.reconnectIntervalMs);
          }
        });
      } catch (err) {
        this.isConnecting = false;
        reject(err);
      }
    });
  }

  /**
   * Close the WebSocket connection.
   */
  public close(): void {
    this.isExplicitlyClosed = true;
    if (this.ws) {
      this.ws.close();
      this.cleanup();
    }
  }

  private cleanup(): void {
    for (const [rid, req] of this.pending.entries()) {
      clearTimeout(req.timer);
      req.reject(new Error(`Connection closed before response received for request ${rid}`));
    }
    this.pending.clear();
    this.isConnecting = false;
    this.ws = null;
  }

  private async handleServerFrame(frame: ServerFrame, onWelcomeConnect?: (w: Welcome) => void): Promise<void> {
    const rid = frame.requestId;

    // Check if this frame answers a pending command request
    if (rid && this.pending.has(rid)) {
      const pendingReq = this.pending.get(rid)!;
      this.pending.delete(rid);
      clearTimeout(pendingReq.timer);

      if (frame.payload.oneofKind === 'error') {
        pendingReq.reject(new StockIntelApiError(frame.payload.error));
        return;
      }

      const kind = frame.payload.oneofKind;
      if (kind) {
        pendingReq.resolve((frame.payload as any)[kind]);
      } else {
        pendingReq.resolve(frame);
      }
      return;
    }

    // Server push messages (or welcome frame)
    switch (frame.payload.oneofKind) {
      case 'welcome': {
        this.welcomeData = frame.payload.welcome;
        this.emit('welcome', frame.payload.welcome);

        if (this.welcomeData.otpRequired && this.otp) {
          try {
            await this.submitOtp(this.otp);
          } catch (err) {
            this.emit('error', err instanceof Error ? err : new Error(String(err)));
          }
        }

        if (onWelcomeConnect) {
          onWelcomeConnect(this.welcomeData);
        }
        break;
      }
      case 'executionEvent':
        this.emit('execution', frame.payload.executionEvent);
        break;
      case 'quoteUpdate':
        this.emit('quote', frame.payload.quoteUpdate);
        break;
      case 'orderBookUpdate':
        this.emit('orderbook', frame.payload.orderBookUpdate);
        break;
      case 'sessionStatus':
        this.emit('session_status', frame.payload.sessionStatus);
        break;
      case 'error':
        this.emit('error', new StockIntelApiError(frame.payload.error));
        break;
      default:
        break;
    }
  }

  private sendCommand<T>(command: ClientFrame['command']): Promise<T> {
    if (!this.isConnected || !this.ws) {
      return Promise.reject(new Error('WebSocket is not connected. Call connect() first.'));
    }

    const requestId = uuidv4();
    const frame: ClientFrame = {
      requestId,
      command
    };

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`Request timed out after ${this.requestTimeoutMs}ms (request_id: ${requestId})`));
      }, this.requestTimeoutMs);

      this.pending.set(requestId, { resolve, reject, timer });

      try {
        const bytes = ClientFrame.toBinary(frame);
        this.ws!.send(bytes);
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(err);
      }
    });
  }

  /**
   * Submit One-Time Password for live tokens or sandbox accounts.
   */
  public submitOtp(code: string): Promise<SubmitOtpResponse> {
    return this.sendCommand<SubmitOtpResponse>({
      oneofKind: 'submitOtp',
      submitOtp: { code }
    });
  }

  /**
   * Place a new order on PSX.
   */
  public placeOrder(params: PlaceOrderParams): Promise<PlaceOrderResponse> {
    return this.sendCommand<PlaceOrderResponse>({
      oneofKind: 'placeOrder',
      placeOrder: {
        brokerCode: params.brokerCode,
        clientCode: params.clientCode,
        symbol: params.symbol,
        side: params.side,
        quantity: params.quantity,
        market: params.market ?? Market.REG,
        type: params.type ?? OrderType.MARKET,
        price: params.price ?? 0,
        stopPrice: params.stopPrice ?? 0,
        timeInForce: params.timeInForce ?? TimeInForce.DAY,
        pin: params.pin ?? ''
      }
    });
  }

  /**
   * Cancel an open order on PSX.
   */
  public cancelOrder(params: CancelOrderParams): Promise<CancelOrderResponse> {
    return this.sendCommand<CancelOrderResponse>({
      oneofKind: 'cancelOrder',
      cancelOrder: {
        brokerCode: params.brokerCode,
        clientCode: params.clientCode,
        brokerOrderId: params.brokerOrderId ?? '',
        exchangeOrderId: params.exchangeOrderId ?? '',
        pin: params.pin ?? ''
      }
    });
  }

  /**
   * List orders for an account.
   */
  public listOrders(params: ListOrdersParams): Promise<ListOrdersResponse> {
    return this.sendCommand<ListOrdersResponse>({
      oneofKind: 'listOrders',
      listOrders: {
        brokerCode: params.brokerCode,
        clientCode: params.clientCode,
        status: params.status ?? OrderStatus.UNSPECIFIED,
        symbol: params.symbol ?? '',
        market: params.market ?? Market.UNSPECIFIED,
        side: params.side ?? OrderSide.UNSPECIFIED,
        type: params.type ?? OrderType.UNSPECIFIED,
        dateFrom: params.dateFrom ?? '',
        dateTo: params.dateTo ?? '',
        count: params.count ?? 25,
        offset: params.offset ?? 0
      }
    });
  }

  /**
   * List linked brokerage accounts.
   */
  public listAccounts(): Promise<ListAccountsResponse> {
    return this.sendCommand<ListAccountsResponse>({
      oneofKind: 'listAccounts',
      listAccounts: {}
    });
  }

  /**
   * Get balances, margin, and open positions for an account.
   */
  public getAccount(brokerCode: string, clientCode: string): Promise<GetAccountResponse> {
    return this.sendCommand<GetAccountResponse>({
      oneofKind: 'getAccount',
      getAccount: { brokerCode, clientCode }
    });
  }

  /**
   * Get current market session status for a broker.
   */
  public getSessionStatus(brokerCode: string, market: Market = Market.REG): Promise<GetSessionStatusResponse> {
    return this.sendCommand<GetSessionStatusResponse>({
      oneofKind: 'getSessionStatus',
      getSessionStatus: { brokerCode, market }
    });
  }

  /**
   * Get historical OHLCV candle data.
   */
  public getHistorical(params: GetHistoricalParams): Promise<GetHistoricalResponse> {
    return this.sendCommand<GetHistoricalResponse>({
      oneofKind: 'getHistorical',
      getHistorical: {
        symbol: params.symbol,
        market: params.market ?? Market.REG,
        interval: params.interval,
        timeFrom: dateToTimestamp(params.timeFrom),
        timeTo: dateToTimestamp(params.timeTo)
      }
    });
  }

  /**
   * Subscribe to real-time quotes for symbols.
   */
  public subscribeQuotes(symbols: SymbolInput[], defaultMarket: Market = Market.REG): Promise<SubscriptionResponse> {
    return this.sendCommand<SubscriptionResponse>({
      oneofKind: 'subscribeQuotes',
      subscribeQuotes: {
        symbols: normalizeSymbolRefs(symbols, defaultMarket)
      }
    });
  }

  /**
   * Unsubscribe from quotes for symbols. Empty array unsubscribes all.
   */
  public unsubscribeQuotes(symbols: SymbolInput[] = [], defaultMarket: Market = Market.REG): Promise<SubscriptionResponse> {
    return this.sendCommand<SubscriptionResponse>({
      oneofKind: 'unsubscribeQuotes',
      unsubscribeQuotes: {
        symbols: normalizeSymbolRefs(symbols, defaultMarket)
      }
    });
  }

  /**
   * Subscribe to real-time 10-deep order books for symbols.
   */
  public subscribeOrderBook(symbols: SymbolInput[], defaultMarket: Market = Market.REG): Promise<SubscriptionResponse> {
    return this.sendCommand<SubscriptionResponse>({
      oneofKind: 'subscribeOrderBook',
      subscribeOrderBook: {
        symbols: normalizeSymbolRefs(symbols, defaultMarket)
      }
    });
  }

  /**
   * Unsubscribe from order books for symbols. Empty array unsubscribes all.
   */
  public unsubscribeOrderBook(symbols: SymbolInput[] = [], defaultMarket: Market = Market.REG): Promise<SubscriptionResponse> {
    return this.sendCommand<SubscriptionResponse>({
      oneofKind: 'unsubscribeOrderBook',
      unsubscribeOrderBook: {
        symbols: normalizeSymbolRefs(symbols, defaultMarket)
      }
    });
  }

  // Type-safe event emitter methods
  override on<K extends keyof StockIntelEventMap>(event: K, listener: StockIntelEventMap[K]): this {
    return super.on(event, listener as any);
  }

  override once<K extends keyof StockIntelEventMap>(event: K, listener: StockIntelEventMap[K]): this {
    return super.once(event, listener as any);
  }

  override emit<K extends keyof StockIntelEventMap>(event: K, ...args: Parameters<StockIntelEventMap[K]>): boolean {
    return super.emit(event, ...args);
  }
}
