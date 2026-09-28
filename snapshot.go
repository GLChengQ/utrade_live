package main

import (
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 快照聚合：把现货账户、合约账户、行情、当前委托、合约持仓汇总成一份给 H5 用的 JSON
// ---------------------------------------------------------------------------

// 与美元 1:1 锚定的资产，直接按 1 计价，避免因为没有 XXXUSDT 交易对而漏算。
var stableAssets = map[string]bool{
	"USDT": true, "USDC": true, "FDUSD": true, "BUSD": true, "TUSD": true,
	"DAI": true, "USDP": true, "USD1": true, "SUSD": true, "AEUR": true,
}

type AssetRow struct {
	Asset    string  `json:"asset"`
	Free     float64 `json:"free"`
	Locked   float64 `json:"locked"`
	Total    float64 `json:"total"`
	Price    float64 `json:"price"`
	HasPrice bool    `json:"hasPrice"`
	Value    float64 `json:"value"`
	Pct      float64 `json:"pct"`
	Source   string  `json:"source"` // 计价来源，例如 BTCUSDT / 稳定币
}

type OrderRow struct {
	Market        string  `json:"market"` // 现货 / 合约
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"`
	SideText      string  `json:"sideText"`
	Type          string  `json:"type"`
	Price         float64 `json:"price"`
	StopPrice     float64 `json:"stopPrice"`
	Qty           float64 `json:"qty"`
	Filled        float64 `json:"filled"`
	Remaining     float64 `json:"remaining"`
	QuoteQty      float64 `json:"quoteQty"`
	Notional      float64 `json:"notional"`
	Status        string  `json:"status"`
	TimeInForce   string  `json:"timeInForce"`
	ReduceOnly    bool    `json:"reduceOnly"`
	PositionSide  string  `json:"positionSide"`
	OrderID       int64   `json:"orderId"`
	ClientOrderID string  `json:"clientOrderId"`
	Time          int64   `json:"time"`
	UpdateTime    int64   `json:"updateTime"`
	Conditional   bool    `json:"conditional"` // true = 条件单（止损/止盈）
}

type PositionRow struct {
	Symbol       string  `json:"symbol"`
	SideText     string  `json:"sideText"`
	Amount       float64 `json:"amount"`
	EntryPrice   float64 `json:"entryPrice"`
	MarkPrice    float64 `json:"markPrice"`
	LiqPrice     float64 `json:"liqPrice"`
	Notional     float64 `json:"notional"`
	Unrealized   float64 `json:"unrealized"`
	RoePct       float64 `json:"roePct"`
	Leverage     string  `json:"leverage"`
	MarginType   string  `json:"marginType"`
	PositionSide string  `json:"positionSide"`
	UpdateTime   int64   `json:"updateTime"`
}

type SpotSummary struct {
	Enabled     bool     `json:"enabled"`
	CanTrade    bool     `json:"canTrade"`
	AccountType string   `json:"accountType"`
	Permissions []string `json:"permissions"`
	EquityUSDT  float64  `json:"equityUSDT"`
	AssetCount  int      `json:"assetCount"`
	UpdateTime  int64    `json:"updateTime"`
	Error       string   `json:"error"`
}

type FuturesSummary struct {
	Enabled       bool    `json:"enabled"`
	WalletBalance float64 `json:"walletBalance"`
	Unrealized    float64 `json:"unrealized"`
	MarginBalance float64 `json:"marginBalance"`
	Available     float64 `json:"available"`
	InitialMargin float64 `json:"initialMargin"`
	MaintMargin   float64 `json:"maintMargin"`
	PositionCount int     `json:"positionCount"`
	OrderCount    int     `json:"orderCount"`
	Error         string  `json:"error"`
}

type Totals struct {
	SpotUSDT       float64 `json:"spotUSDT"`
	FuturesMargin  float64 `json:"futuresMargin"`
	FuturesUnreal  float64 `json:"futuresUnreal"`
	GrandTotal     float64 `json:"grandTotal"`
	OpenOrders     int     `json:"openOrders"`
	Positions      int     `json:"positions"`
	UnpricedAssets int     `json:"unpricedAssets"`
	LockedSpotUSDT float64 `json:"lockedSpotUSDT"`
}

type Snapshot struct {
	OK        bool      `json:"ok"`
	Demo      bool      `json:"demo"`
	Error     string    `json:"error"`
	Warnings  []string  `json:"warnings"`
	FetchedAt time.Time `json:"fetchedAt"`
	FetchedMS int64     `json:"fetchedAtMs"`
	LatencyMS int64     `json:"latencyMs"`
	PollMS    int       `json:"pollMs"`
	Source    string    `json:"source"` // 现货 / 合约 接口 host

	// 限流相关
	CoolingDown        bool   `json:"coolingDown"`        // 正在限流冷却，暂停向上游请求
	CooldownSeconds    int    `json:"cooldownSeconds"`    // 预计还需冷却多少秒
	UsedWeight1M       int64  `json:"usedWeight1M"`       // 币安报告的 1 分钟已用权重
	FuturesOrdersAgeMS int64  `json:"futuresOrdersAgeMs"` // 合约委托数据的年龄（降频复用时 > 0）
	OrdersSource       string `json:"ordersSource"`       // 合约委托来源：websocket / rest

	Spot      SpotSummary     `json:"spot"`
	Futures   *FuturesSummary `json:"futures"`
	Totals    Totals          `json:"totals"`
	Assets    []AssetRow      `json:"assets"`
	Orders    []OrderRow      `json:"orders"`
	Positions []PositionRow   `json:"positions"`
	Odds      *OddsBlock      `json:"odds,omitempty"`
	Alerts    *AlertStatus    `json:"alerts,omitempty"`
}

// OddsBlock 是"持仓概率"：每个持仓一块（多单算上方目标，空单算下方目标）
type OddsBlock struct {
	Ready     bool      `json:"ready"`
	Note      string    `json:"note,omitempty"`
	Horizons  []string  `json:"horizons"`
	Bars      int       `json:"bars"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Samples   int       `json:"samples"`
	Positions []OddsPos `json:"positions"`
}

// OddsPos 单个持仓的概率
type OddsPos struct {
	Ready    bool         `json:"ready"`
	Note     string       `json:"note,omitempty"`
	Symbol   string       `json:"symbol"`
	Side     string       `json:"side"`
	Long     bool         `json:"long"`
	Amount   float64      `json:"amount"`
	Notional float64      `json:"notional"`
	Entry    float64      `json:"entry"`
	Price    float64      `json:"price"`
	Liq      float64      `json:"liqPrice"`
	LiqValid bool         `json:"liqValid"`   // 强平价方向是否与该持仓的逆行方向一致
	LiqDist  float64      `json:"liqDistPct"` // 距强平价的百分比（正数；LiqValid=false 时无意义）
	LiqTouch []float64    `json:"liqTouch"`   // 各周期内触及强平价的概率
	Targets  []OddsTarget `json:"targets"`
}

type OddsTarget struct {
	Label    string    `json:"label"`   // 档位标签（价格或 ±x%）
	Level    float64   `json:"level"`   // 档位价格
	Dist     float64   `json:"distPct"` // 距离现价的百分比
	Touch    []float64 `json:"touch"`   // 各周期内摸到该价的概率
	First    []float64 `json:"first"`   // 先到该价（而非先被强平）的概率
	LiqFirst []float64 `json:"liqFirst"`
}

func pf(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return v
}

func sideText(side string) string {
	switch strings.ToUpper(side) {
	case "BUY":
		return "买入"
	case "SELL":
		return "卖出"
	}
	return side
}

func posSideText(side string, amt float64) string {
	switch strings.ToUpper(side) {
	case "LONG":
		return "多头"
	case "SHORT":
		return "空头"
	}
	if amt < 0 {
		return "空头"
	}
	return "多头"
}

func round(v float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(v*p) / p
}

// priceOf 计算某资产的 USDT 价：稳定币=1，其次 XXXUSDT，再次 XXXBTC × BTCUSDT。
func priceOf(asset string, prices map[string]float64) (price float64, ok bool, source string) {
	if stableAssets[strings.ToUpper(asset)] {
		return 1, true, "稳定币"
	}
	if v, hit := prices[asset+"USDT"]; hit && v > 0 {
		return v, true, asset + "USDT"
	}
	if v, hit := prices[asset+"BTC"]; hit && v > 0 {
		if btc, hit2 := prices["BTCUSDT"]; hit2 && btc > 0 {
			return v * btc, true, asset + "BTC×BTCUSDT"
		}
	}
	return 0, false, ""
}

// buildSnapshot 并发拉取所有数据源。单项失败不影响其它项，错误分别展示。
// fetchFutOrders=false 时跳过合约当前委托（该接口不带 symbol 时权重 40，最贵），
// 由调用方复用上一轮结果。
func buildSnapshot(ctx context.Context, c *Client, pollMS int, enableFutures, fetchFutOrders bool) *Snapshot {
	start := time.Now()
	snap := &Snapshot{OK: true, PollMS: pollMS, FetchedAt: time.Now()}

	var (
		wg         sync.WaitGroup
		spotAcc    *SpotAccount
		prices     map[string]float64
		spotOrders []SpotOrder
		futAcc     *FuturesAccount
		futPos     []FuturesPosition
		futOrders  []FuturesOrder
		algoOrders []AlgoOrder

		errSpot, errPrices, errSpotOrders  error
		errFutAcc, errFutPos, errFutOrders error
	)

	wg.Add(3)
	go func() { defer wg.Done(); spotAcc, errSpot = c.SpotAccount(ctx) }()
	go func() { defer wg.Done(); prices, errPrices = c.SpotPrices(ctx) }()
	go func() { defer wg.Done(); spotOrders, errSpotOrders = c.SpotOpenOrders(ctx) }()
	if enableFutures {
		wg.Add(2)
		go func() { defer wg.Done(); futAcc, errFutAcc = c.FuturesAccount(ctx) }()
		go func() { defer wg.Done(); futPos, errFutPos = c.FuturesPositions(ctx) }()
		// 条件单（止损/止盈）不在 openOrders 也不走数据流推送，单独查（权重 1）
		wg.Add(1)
		go func() { defer wg.Done(); algoOrders, _ = c.AlgoOpenOrders(ctx) }()
		// 用户数据流在线时，未成交订单由 WebSocket 事件维护，跳过权重 40 的 REST 轮询
		if fetchFutOrders && !(wsEngine != nil && wsEngine.Usable()) {
			wg.Add(1)
			go func() { defer wg.Done(); futOrders, errFutOrders = c.FuturesOpenOrders(ctx) }()
		}
	}
	wg.Wait()

	// 用户数据流在线：未成交订单始终取自推送（每轮都是最新的），REST 只在断线时兜底
	if wsEngine != nil && wsEngine.Usable() {
		futOrders = wsEngine.FuturesOrders()
		errFutOrders = nil
		snap.OrdersSource = "websocket"
	} else {
		snap.OrdersSource = "rest"
	}

	if errPrices != nil {
		prices = map[string]float64{}
		snap.Warnings = append(snap.Warnings, "行情报价获取失败，非稳定币资产无法折算 USDT："+friendly(errPrices))
	}

	// ---- 现货 ----
	snap.Spot.Enabled = errSpot == nil
	if errSpot != nil {
		snap.Spot.Error = friendly(errSpot)
	} else {
		snap.Spot.CanTrade = spotAcc.CanTrade
		snap.Spot.AccountType = spotAcc.AccountType
		snap.Spot.Permissions = spotAcc.Permissions
		snap.Spot.UpdateTime = spotAcc.UpdateTime
	}
	if errSpotOrders != nil {
		snap.Warnings = append(snap.Warnings, "现货当前委托获取失败："+friendly(errSpotOrders))
	}

	assets := make([]AssetRow, 0, 32)
	if spotAcc != nil {
		for _, b := range spotAcc.Balances {
			free, locked := pf(b.Free), pf(b.Locked)
			total := free + locked
			if total <= 0 {
				continue // 只展示有余额的币种
			}
			row := AssetRow{Asset: b.Asset, Free: free, Locked: locked, Total: total}
			if p, ok, src := priceOf(b.Asset, prices); ok {
				row.Price, row.HasPrice, row.Source = p, true, src
				row.Value = total * p
				snap.Totals.SpotUSDT += row.Value
				snap.Totals.LockedSpotUSDT += locked * p
			} else {
				snap.Totals.UnpricedAssets++
			}
			assets = append(assets, row)
		}
	}
	// 按 USDT 估值倒序，无价格的排在最后
	sort.SliceStable(assets, func(i, j int) bool { return assets[i].Value > assets[j].Value })
	for i := range assets {
		if snap.Totals.SpotUSDT > 0 {
			assets[i].Pct = round(assets[i].Value/snap.Totals.SpotUSDT*100, 4)
		}
	}
	// 估值统一保留 2 位小数，避免前端出现过长小数
	for i := range assets {
		assets[i].Value = round(assets[i].Value, 4)
	}
	snap.Spot.EquityUSDT = round(snap.Totals.SpotUSDT, 4)
	snap.Spot.AssetCount = len(assets)
	snap.Assets = assets

	// ---- 现货委托 ----
	for _, o := range spotOrders {
		price, qty := pf(o.Price), pf(o.OrigQty)
		filled, quote := pf(o.ExecutedQty), pf(o.CummulativeQuoteQty)
		remaining := qty - filled
		if remaining < 0 {
			remaining = 0
		}
		notional := quote
		if price > 0 {
			notional = price * remaining
		}
		snap.Orders = append(snap.Orders, OrderRow{
			Market: "现货", Symbol: o.Symbol, Side: o.Side, SideText: sideText(o.Side),
			Type: o.Type, Price: price, StopPrice: pf(o.StopPrice), Qty: qty, Filled: filled,
			Remaining: remaining, QuoteQty: quote, Notional: round(notional, 4),
			Status: o.Status, TimeInForce: o.TimeInForce, OrderID: o.OrderID,
			ClientOrderID: o.ClientOrderID, Time: o.Time, UpdateTime: o.UpdateTime,
		})
	}

	// ---- 合约 ----
	if enableFutures {
		fs := &FuturesSummary{Enabled: errFutAcc == nil}
		if errFutAcc != nil {
			fs.Error = friendly(errFutAcc)
			snap.Warnings = append(snap.Warnings, "合约账户获取失败（若未开通合约可忽略）："+friendly(errFutAcc))
		} else {
			fs.WalletBalance = pf(futAcc.TotalWalletBalance)
			fs.Unrealized = pf(futAcc.TotalUnrealizedProfit)
			fs.MarginBalance = pf(futAcc.TotalMarginBalance)
			fs.Available = pf(futAcc.AvailableBalance)
			fs.InitialMargin = pf(futAcc.TotalInitialMargin)
			fs.MaintMargin = pf(futAcc.TotalMaintMargin)
			snap.Totals.FuturesMargin = fs.MarginBalance
			snap.Totals.FuturesUnreal = fs.Unrealized
		}
		if errFutPos != nil {
			snap.Warnings = append(snap.Warnings, "合约持仓获取失败："+friendly(errFutPos))
		}
		for _, p := range futPos {
			amt := pf(p.PositionAmt)
			if amt == 0 {
				continue
			}
			mark := pf(p.MarkPrice)
			entry := pf(p.EntryPrice)
			notional := pf(p.Notional)
			if notional == 0 {
				notional = amt * mark
			}
			unreal := pf(p.UnRealizedProfit)
			lev := pf(p.Leverage)
			// v3 接口不返回 marginType，用 isolated 字段兜底
			marginType := p.MarginType
			if marginType == "" {
				if p.Isolated {
					marginType = "isolated"
				} else {
					marginType = "cross"
				}
			}
			row := PositionRow{
				Symbol: p.Symbol, SideText: posSideText(p.PositionSide, amt), Amount: amt,
				EntryPrice: entry, MarkPrice: mark, LiqPrice: pf(p.LiquidationPrice),
				Notional: round(abs(notional), 4), Unrealized: round(unreal, 4),
				Leverage: p.Leverage, MarginType: marginType, PositionSide: p.PositionSide,
				UpdateTime: p.UpdateTime,
			}
			if lev > 0 && entry > 0 && amt != 0 {
				margin := abs(amt) * entry / lev
				if margin > 0 {
					row.RoePct = round(unreal/margin*100, 4)
				}
			}
			snap.Positions = append(snap.Positions, row)
		}
		sort.SliceStable(snap.Positions, func(i, j int) bool {
			return abs(snap.Positions[i].Notional) > abs(snap.Positions[j].Notional)
		})
		fs.PositionCount = len(snap.Positions)

		for _, o := range futOrders {
			price, qty := pf(o.Price), pf(o.OrigQty)
			filled, quote := pf(o.ExecutedQty), pf(o.CumQuote)
			remaining := qty - filled
			if remaining < 0 {
				remaining = 0
			}
			notional := quote
			if price > 0 {
				notional = price * remaining
			}
			snap.Orders = append(snap.Orders, OrderRow{
				Market: "合约", Symbol: o.Symbol, Side: o.Side, SideText: sideText(o.Side),
				Type: o.Type, Price: price, StopPrice: pf(o.StopPrice), Qty: qty, Filled: filled,
				Remaining: remaining, QuoteQty: quote, Notional: round(notional, 4),
				Status: o.Status, TimeInForce: o.TimeInForce, ReduceOnly: o.ReduceOnly,
				PositionSide: o.PositionSide, OrderID: o.OrderID, ClientOrderID: o.ClientOrderID,
				Time: o.Time, UpdateTime: o.UpdateTime,
			})
		}
		// 条件单（止损/止盈）：单独来源，标记为"条件单"，让页面能看见保护单
		for _, a := range algoOrders {
			qty := pf(a.Quantity)
			trig := pf(a.TriggerPrice)
			snap.Orders = append(snap.Orders, OrderRow{
				Market: "合约", Symbol: a.Symbol, Side: a.Side, SideText: sideText(a.Side),
				Type: a.OrderType, Price: 0, StopPrice: trig, Qty: qty, Filled: 0,
				Remaining: qty, Notional: round(trig*qty, 4), Status: a.AlgoStatus,
				TimeInForce: a.WorkingType, ReduceOnly: a.ReduceOnly,
				PositionSide: a.PositionSide, OrderID: a.AlgoID, ClientOrderID: a.ClientAlgoID,
				Time: a.CreateTime, UpdateTime: a.UpdateTime, Conditional: true,
			})
		}
		fs.OrderCount = len(futOrders)
		if errFutOrders != nil {
			snap.Warnings = append(snap.Warnings, "合约当前委托获取失败："+friendly(errFutOrders))
		}
		snap.Futures = fs
	}

	snap.Totals.OpenOrders = len(snap.Orders)
	snap.Totals.Positions = len(snap.Positions)
	snap.Totals.GrandTotal = round(snap.Totals.SpotUSDT+snap.Totals.FuturesMargin, 4)
	snap.Totals.SpotUSDT = round(snap.Totals.SpotUSDT, 4)

	// 全部数据源都失败 → 页面显示错误横幅
	if errSpot != nil && (!enableFutures || errFutAcc != nil) {
		snap.OK = false
		parts := []string{"现货账户: " + friendly(errSpot)}
		if enableFutures && errFutAcc != nil {
			parts = append(parts, "合约账户: "+friendly(errFutAcc))
		}
		snap.Error = strings.Join(parts, "； ")
	}
	snap.Source = c.baseURL
	snap.LatencyMS = time.Since(start).Milliseconds()
	snap.FetchedMS = snap.FetchedAt.UnixMilli()
	snap.UsedWeight1M = c.UsedWeight1M()
	if oddsEngine != nil && len(snap.Positions) > 0 {
		snap.Odds = oddsEngine.block(ctx, c, snap.Positions)
	}
	// 告警：用本轮新鲜数据评估规则（内部有冷却，不会重复轰炸）
	if alertEngineRef != nil {
		alertEngineRef.evaluate(snap)
		snap.Alerts = alertEngineRef.Status()
	}
	return snap
}

// oddsEngine 由 main 初始化（含配置的止盈档）
var oddsEngine *oddsStore

// wsEngine 是由 main 初始化的用户数据流（WebSocket）
var wsEngine *UserDataStream

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// friendly 把任意错误转成适合展示的中文短句。
func friendly(err error) string {
	if err == nil {
		return ""
	}
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Friendly()
	}
	var ce *CooldownError
	if errors.As(err, &ce) {
		return "币安接口限流冷却中，约 " + strconv.Itoa(int(ce.Remaining.Seconds())+1) +
			" 秒后自动恢复（本地已暂停请求，不会继续消耗额度）"
	}
	msg := err.Error()
	if strings.Contains(msg, "context deadline exceeded") {
		return "请求超时，请检查网络/代理是否可访问币安接口"
	}
	if strings.Contains(msg, "no such host") || strings.Contains(msg, "connectex") ||
		strings.Contains(msg, "connection") || strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "reset") {
		return "无法连接币安接口（网络被阻断或需要代理）：" + msg
	}
	return msg
}

// ---------------------------------------------------------------------------
// -demo 演示数据：不需要 API Key，用来验证页面与程序是否正常。
// ---------------------------------------------------------------------------

func demoSnapshot(pollMS int, futuresEnabled bool) *Snapshot {
	now := time.Now()
	snap := &Snapshot{
		OK: true, Demo: true, PollMS: pollMS, FetchedAt: now, FetchedMS: now.UnixMilli(),
		LatencyMS: 12, Source: "demo",
	}
	snap.Warnings = append(snap.Warnings, "当前为演示数据（-demo 模式），未连接币安账户接口")

	assets := []AssetRow{
		{Asset: "USDT", Free: 5231.42, Locked: 800, Total: 6031.42, Price: 1, HasPrice: true, Source: "稳定币"},
		{Asset: "BTC", Free: 0.0812, Locked: 0.01, Total: 0.0912, Price: 96500, HasPrice: true, Source: "BTCUSDT"},
		{Asset: "ETH", Free: 1.35, Locked: 0, Total: 1.35, Price: 3320.5, HasPrice: true, Source: "ETHUSDT"},
		{Asset: "BNB", Free: 3.2, Locked: 0.5, Total: 3.7, Price: 705.2, HasPrice: true, Source: "BNBUSDT"},
		{Asset: "SOL", Free: 12.5, Locked: 0, Total: 12.5, Price: 188.4, HasPrice: true, Source: "SOLUSDT"},
	}
	var spot float64
	for i := range assets {
		assets[i].Value = round(assets[i].Total*assets[i].Price, 4)
		spot += assets[i].Value
	}
	for i := range assets {
		assets[i].Pct = round(assets[i].Value/spot*100, 4)
	}
	sort.SliceStable(assets, func(i, j int) bool { return assets[i].Value > assets[j].Value })
	snap.Assets = assets
	snap.Totals.SpotUSDT = round(spot, 4)
	snap.Totals.LockedSpotUSDT = round(800+0.01*96500+0.5*705.2, 4)
	snap.Spot = SpotSummary{
		Enabled: true, CanTrade: true, AccountType: "SPOT",
		Permissions: []string{"SPOT"}, EquityUSDT: round(spot, 4),
		AssetCount: len(assets), UpdateTime: now.UnixMilli(),
	}

	if futuresEnabled {
		fs := &FuturesSummary{
			Enabled: true, WalletBalance: 4200, Unrealized: 318.65, MarginBalance: 4518.65,
			Available: 3120.4, InitialMargin: 1398.25, MaintMargin: 92.35,
		}
		snap.Futures = fs
		snap.Totals.FuturesMargin = fs.MarginBalance
		snap.Totals.FuturesUnreal = fs.Unrealized
		snap.Positions = []PositionRow{
			{Symbol: "BTCUSDT", SideText: "多头", Amount: 0.045, EntryPrice: 94120.5, MarkPrice: 96500,
				LiqPrice: 81230.4, Notional: 4342.5, Unrealized: 107.08, RoePct: 25.28,
				Leverage: "10", MarginType: "cross", PositionSide: "BOTH", UpdateTime: now.UnixMilli()},
			{Symbol: "ETHUSDT", SideText: "空头", Amount: -2.1, EntryPrice: 3412.8, MarkPrice: 3320.5,
				LiqPrice: 3980.2, Notional: 6973.05, Unrealized: 193.83, RoePct: 27.05,
				Leverage: "5", MarginType: "isolated", PositionSide: "BOTH", UpdateTime: now.UnixMilli()},
		}
		fs.PositionCount = len(snap.Positions)
		fs.OrderCount = 2
	}

	snap.Orders = []OrderRow{
		{Market: "现货", Symbol: "BTCUSDT", Side: "BUY", SideText: "买入", Type: "LIMIT",
			Price: 92000, Qty: 0.02, Filled: 0.005, Remaining: 0.015, QuoteQty: 460,
			Notional: 1380, Status: "PARTIALLY_FILLED", TimeInForce: "GTC",
			OrderID: 100000001, ClientOrderID: "web_demo_1", Time: now.Add(-42 * time.Minute).UnixMilli(),
			UpdateTime: now.Add(-6 * time.Minute).UnixMilli()},
		{Market: "现货", Symbol: "SOLUSDT", Side: "SELL", SideText: "卖出", Type: "LIMIT",
			Price: 205.5, Qty: 12.5, Filled: 0, Remaining: 12.5, QuoteQty: 0,
			Notional: 2568.75, Status: "NEW", TimeInForce: "GTC",
			OrderID: 100000002, ClientOrderID: "web_demo_2", Time: now.Add(-12 * time.Minute).UnixMilli(),
			UpdateTime: now.Add(-12 * time.Minute).UnixMilli()},
		{Market: "合约", Symbol: "ETHUSDT", Side: "SELL", SideText: "卖出", Type: "LIMIT",
			Price: 3450, Qty: 1.2, Filled: 0, Remaining: 1.2, QuoteQty: 0,
			Notional: 4140, Status: "NEW", TimeInForce: "GTC", ReduceOnly: true,
			PositionSide: "BOTH", OrderID: 200000001, ClientOrderID: "web_demo_3",
			Time: now.Add(-3 * time.Minute).UnixMilli(), UpdateTime: now.Add(-3 * time.Minute).UnixMilli()},
		{Market: "合约", Symbol: "BTCUSDT", Side: "BUY", SideText: "买入", Type: "STOP_MARKET",
			StopPrice: 93000, Qty: 0.045, Filled: 0, Remaining: 0.045,
			Notional: 4185, Status: "NEW", TimeInForce: "GTC", ReduceOnly: true,
			PositionSide: "BOTH", OrderID: 200000002, ClientOrderID: "web_demo_4",
			Time: now.Add(-90 * time.Second).UnixMilli(), UpdateTime: now.Add(-90 * time.Second).UnixMilli()},
	}
	snap.Totals.OpenOrders = len(snap.Orders)
	snap.Totals.Positions = len(snap.Positions)
	snap.Totals.GrandTotal = round(snap.Totals.SpotUSDT+snap.Totals.FuturesMargin, 4)
	// -demo 模式给出示例概率（多单 + 空单各一块），便于本地核对页面渲染
	snap.Odds = &OddsBlock{
		Ready: true, Horizons: []string{"24h", "3d", "7d"},
		Bars: 45000, From: "2025-05-01", To: "2026-09-25", Samples: 44000,
		Positions: []OddsPos{
			{
				Ready: true, Symbol: "BTCUSDT", Side: "多头", Long: true,
				Amount: 0.045, Notional: 4342.5, Entry: 94120.5,
				Price: 96500, Liq: 81230.4, LiqDist: 15.8,
				LiqTouch: []float64{3.2, 7.1, 11.4},
				Targets: []OddsTarget{
					{Label: "10.40", Level: 10.40, Dist: 7.8, Touch: []float64{22.4, 48.1, 62.3}, First: []float64{20.9, 43.7, 54.1}, LiqFirst: []float64{3.0, 6.8, 10.6}},
					{Label: "9.95", Level: 9.95, Dist: 3.1, Touch: []float64{41.7, 68.9, 79.2}, First: []float64{40.1, 64.2, 71.8}, LiqFirst: []float64{2.9, 6.5, 10.1}},
				},
			},
			{
				Ready: true, Symbol: "ETHUSDT", Side: "空头", Long: false,
				Amount: -2.1, Notional: 6973.05, Entry: 3412.8,
				Price: 3320.5, Liq: 3980.2, LiqDist: 19.9,
				LiqTouch: []float64{2.8, 6.2, 10.2},
				Targets: []OddsTarget{
					{Label: "-2%", Level: 3254.09, Dist: 2.0, Touch: []float64{38.2, 61.4, 71.9}, First: []float64{36.9, 58.1, 66.4}, LiqFirst: []float64{2.6, 5.9, 9.6}},
					{Label: "-4%", Level: 3187.68, Dist: 4.0, Touch: []float64{17.1, 33.8, 45.2}, First: []float64{16.5, 31.9, 41.3}, LiqFirst: []float64{2.5, 5.6, 9.1}},
					{Label: "-6%", Level: 3121.27, Dist: 6.0, Touch: []float64{7.4, 16.2, 24.1}, First: []float64{7.2, 15.4, 22.2}, LiqFirst: []float64{2.4, 5.4, 8.8}},
					{Label: "-8%", Level: 3054.86, Dist: 8.0, Touch: []float64{3.1, 7.6, 12.3}, First: []float64{3.0, 7.2, 11.4}, LiqFirst: []float64{2.3, 5.2, 8.5}},
				},
			},
		},
	}
	return snap
}
