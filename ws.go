package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// 币安 U 本位「用户数据流」WebSocket 客户端
//
// 为什么需要它：REST 里最贵的是 GET /fapi/v1/openOrders（不带 symbol 时权重 40），
// 加上 account(5)+positionRisk(5)，一轮就是 50 权重；5 秒一轮 = 600/分钟，
// 叠加分析脚本就会顶穿 2400/分钟的 IP 限额。
//
// 用户数据流把账户/持仓/订单变更**推**过来（ACCOUNT_UPDATE / ORDER_TRADE_UPDATE），
// 于是未成交订单不再需要轮询（权重 40 → 0），账户与持仓也能按事件即时更新。
//
// 生命周期：
//   POST /fapi/v1/listenKey  创建（签名）→ wss://fstream.binance.com/ws/<key> 连接
//   PUT  /fapi/v1/listenKey  每 25 分钟保活（listenKey 60 分钟过期）
//   断线/过期 → 指数退避重连（1s→60s）
// ---------------------------------------------------------------------------

const wsBaseURL = "wss://fstream.binance.com/ws/"

type WSBalance struct {
	Asset              string
	WalletBalance      string
	CrossWalletBalance string
	Updated            time.Time
}

type WSPositionLite struct {
	Symbol           string
	PositionSide     string
	Amount           string
	EntryPrice       string
	UnrealizedProfit string
	MarginType       string
	IsolatedWallet   string
	Updated          time.Time
}

type WSFill struct {
	Symbol       string
	Side         string
	PositionSide string
	OrderType    string
	LastQty      string
	LastPrice    string
	RealizedPnl  string
	Time         time.Time
}

// UserDataStream 维护用户数据流的连接与实时状态
type UserDataStream struct {
	client *Client

	mu         sync.RWMutex
	listenKey  string
	conn       *websocket.Conn
	connected  bool
	lastEvent  time.Time
	lastErr    string
	events     int64
	reconnects int64

	balances  map[string]WSBalance
	positions map[string]WSPositionLite
	orders    map[string]FuturesOrder // key: symbol|orderId
	fills     []WSFill                // 最近成交事件（最多 100 条）
}

func NewUserDataStream(c *Client) *UserDataStream {
	return &UserDataStream{
		client:    c,
		balances:  map[string]WSBalance{},
		positions: map[string]WSPositionLite{},
		orders:    map[string]FuturesOrder{},
	}
}

// Start 后台运行：创建 listenKey → 连接 → 读事件 → 断了就重连
func (s *UserDataStream) Start(ctx context.Context) {
	go func() {
		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err := s.runOnce(ctx)
			if ctx.Err() != nil {
				return
			}
			s.mu.Lock()
			s.connected = false
			s.reconnects++
			if err != nil {
				s.lastErr = err.Error()
			}
			s.mu.Unlock()
			if err != nil {
				log.Printf("用户数据流断开：%v；%s 后重连", err, backoff)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
		}
	}()
}

func (s *UserDataStream) runOnce(ctx context.Context) error {
	key, err := s.createListenKey(ctx)
	if err != nil {
		return fmt.Errorf("创建 listenKey 失败: %w", err)
	}
	s.mu.Lock()
	s.listenKey = key
	s.mu.Unlock()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsBaseURL+key, nil)
	if err != nil {
		return fmt.Errorf("连接 WebSocket 失败: %w", err)
	}
	s.mu.Lock()
	s.conn, s.connected, s.lastEvent = conn, true, time.Now()
	s.mu.Unlock()
	log.Printf("用户数据流已连接（listenKey %s…）", key[:min(6, len(key))])

	defer func() {
		conn.Close()
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
	}()

	// 先补一次全量未成交订单（权重 40，但只在连接建立时做一次）
	s.seedOpenOrders(ctx)

	// 保活
	kaCtx, kaCancel := context.WithCancel(ctx)
	defer kaCancel()
	go s.keepaliveLoop(kaCtx)

	conn.SetReadDeadline(time.Now().Add(24 * time.Hour))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		s.handle(msg)
	}
}

func (s *UserDataStream) keepaliveLoop(ctx context.Context) {
	t := time.NewTicker(25 * time.Minute)
	reseed := time.NewTicker(5 * time.Minute) // 周期性用 REST 校准一次，防止事件丢失导致漂移
	defer t.Stop()
	defer reseed.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.RLock()
			key := s.listenKey
			s.mu.RUnlock()
			if key == "" {
				continue
			}
			if err := s.putListenKey(ctx, key); err != nil {
				log.Printf("listenKey 保活失败：%v", err)
			}
		case <-reseed.C:
			s.seedOpenOrders(ctx)
		}
	}
}

// ---------------- listenKey REST ----------------

func (s *UserDataStream) createListenKey(ctx context.Context) (string, error) {
	var out struct {
		ListenKey string `json:"listenKey"`
	}
	// 注意：创建 listenKey 必须是 POST（用 GET 会返回 404 HTML 页面）
	if err := s.client.post(ctx, s.client.fapiURL, "/fapi/v1/listenKey", nil, &out); err != nil {
		return "", err
	}
	if out.ListenKey == "" {
		return "", fmt.Errorf("返回的 listenKey 为空")
	}
	return out.ListenKey, nil
}

func (s *UserDataStream) putListenKey(ctx context.Context, key string) error {
	params := map[string][]string{"listenKey": {key}}
	return s.client.put(ctx, s.client.fapiURL, "/fapi/v1/listenKey", params, nil)
}

// seedOpenOrders 连接建立后拉一次未成交订单做基线（之后由事件维护）
func (s *UserDataStream) seedOpenOrders(ctx context.Context) {
	rows, err := s.client.FuturesOpenOrders(ctx)
	if err != nil {
		log.Printf("用户数据流：初始化未成交订单失败（将由事件补齐）：%v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders = map[string]FuturesOrder{}
	for _, o := range rows {
		s.orders[orderKey(o.Symbol, o.OrderID)] = o
	}
	log.Printf("用户数据流：已载入 %d 条未成交订单作为基线", len(rows))
}

// ---------------- 事件处理 ----------------

type wsEnvelope struct {
	E string `json:"e"`
	T int64  `json:"T"`
	A struct {
		M string `json:"m"`
		B []struct {
			A  string `json:"a"`
			WB string `json:"wb"`
			CW string `json:"cw"`
		} `json:"B"`
		P []struct {
			S  string `json:"s"`
			PA string `json:"pa"`
			EP string `json:"ep"`
			UP string `json:"up"`
			MT string `json:"mt"`
			IW string `json:"iw"`
			PS string `json:"ps"`
		} `json:"P"`
	} `json:"a"`
	O struct {
		S    string `json:"s"`
		C    string `json:"c"`
		Side string `json:"S"`
		Type string `json:"o"`
		TIF  string `json:"f"`
		Q    string `json:"q"`
		P    string `json:"p"`
		AP   string `json:"ap"`
		SP   string `json:"sp"`
		X    string `json:"x"` // 执行类型
		St   string `json:"X"` // 订单状态
		I    int64  `json:"i"`
		L    string `json:"l"`
		Z    string `json:"z"`
		Lp   string `json:"L"`
		Tt   int64  `json:"T"`
		R    bool   `json:"R"`  // reduceOnly
		Ps   string `json:"ps"` // positionSide
		Rp   string `json:"rp"` // realized pnl
	} `json:"o"`
}

func (s *UserDataStream) handle(msg []byte) {
	var ev wsEnvelope
	if err := json.Unmarshal(msg, &ev); err != nil {
		return
	}
	now := time.Now()
	s.mu.Lock()
	s.lastEvent = now
	s.events++
	s.mu.Unlock()

	switch ev.E {
	case "listenKeyExpired":
		log.Printf("用户数据流：listenKey 已过期，将重新创建")
		if c := s.conn; c != nil {
			c.Close() // 触发重连
		}
	case "ACCOUNT_UPDATE":
		s.mu.Lock()
		for _, b := range ev.A.B {
			s.balances[b.A] = WSBalance{Asset: b.A, WalletBalance: b.WB, CrossWalletBalance: b.CW, Updated: now}
		}
		for _, p := range ev.A.P {
			s.positions[p.S+"|"+p.PS] = WSPositionLite{
				Symbol: p.S, PositionSide: p.PS, Amount: p.PA, EntryPrice: p.EP,
				UnrealizedProfit: p.UP, MarginType: p.MT, IsolatedWallet: p.IW, Updated: now,
			}
		}
		s.mu.Unlock()
	case "ORDER_TRADE_UPDATE":
		s.applyOrderEvent(ev, now)
	}
}

func orderKey(symbol string, id int64) string {
	return symbol + "|" + strconv.FormatInt(id, 10)
}

// applyOrderEvent 用订单事件维护"未成交订单"集合
func (s *UserDataStream) applyOrderEvent(ev wsEnvelope, now time.Time) {
	o := ev.O
	if o.S == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := orderKey(o.S, o.I)
	switch o.St {
	case "NEW", "PARTIALLY_FILLED":
		s.orders[key] = FuturesOrder{
			Symbol: o.S, OrderID: o.I, ClientOrderID: o.C,
			Price: o.P, AvgPrice: o.AP, OrigQty: o.Q, ExecutedQty: o.Z,
			CumQuote: mulStr(o.Z, o.AP), Status: o.St, TimeInForce: o.TIF,
			Type: o.Type, Side: o.Side, StopPrice: o.SP, ReduceOnly: o.R,
			PositionSide: o.Ps, Time: o.Tt, UpdateTime: now.UnixMilli(),
		}
	case "CANCELED", "EXPIRED", "FILLED", "EXPIRED_IN_MATCH":
		delete(s.orders, key)
	default:
		// CALCULATED / NEW_INSURANCE 等：忽略
	}

	// 成交事件（挂保护单/被止损止盈打掉都会在这里出现）
	if o.X == "TRADE" || o.Lp != "" || o.Rp != "" {
		s.fills = append(s.fills, WSFill{
			Symbol: o.S, Side: o.Side, PositionSide: o.Ps, OrderType: o.Type,
			LastQty: o.L, LastPrice: o.Lp, RealizedPnl: o.Rp, Time: time.UnixMilli(o.Tt),
		})
		if len(s.fills) > 100 {
			s.fills = s.fills[len(s.fills)-100:]
		}
	}
}

func mulStr(a, b string) string {
	x, err1 := strconv.ParseFloat(a, 64)
	y, err2 := strconv.ParseFloat(b, 64)
	if err1 != nil || err2 != nil {
		return "0"
	}
	return strconv.FormatFloat(x*y, 'f', 8, 64)
}

// ---------------- 对外查询 ----------------

// FuturesOrders 返回实时未成交订单（与 REST 的 FuturesOpenOrders 同结构，可直接替换）
func (s *UserDataStream) FuturesOrders() []FuturesOrder {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]FuturesOrder, 0, len(s.orders))
	for _, o := range s.orders {
		out = append(out, o)
	}
	return out
}

// Usable 数据流是否可用于替代 REST：只看连接状态。
// （ACCOUNT_UPDATE 只在账户/持仓变动时推送，不能要求"最近有事件"）
func (s *UserDataStream) Usable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// Fresh 最近是否有事件（仅用于展示）
func (s *UserDataStream) Fresh(d time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected && !s.lastEvent.IsZero() && time.Since(s.lastEvent) < d
}

// Status 供页面/接口展示
func (s *UserDataStream) Status() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	orders := len(s.orders)
	last := ""
	if !s.lastEvent.IsZero() {
		last = s.lastEvent.In(cstZone()).Format("15:04:05")
	}
	return map[string]any{
		"connected":  s.connected,
		"events":     s.events,
		"reconnects": s.reconnects,
		"lastEvent":  last,
		"openOrders": orders,
		"positions":  len(s.positions),
		"lastError":  s.lastErr,
	}
}

// Fills 最近成交事件（给页面展示"止损/止盈触发了"）
func (s *UserDataStream) Fills(limit int) []WSFill {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit > len(s.fills) {
		limit = len(s.fills)
	}
	out := make([]WSFill, limit)
	copy(out, s.fills[len(s.fills)-limit:])
	return out
}
