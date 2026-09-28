package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 币安 REST 客户端（只依赖标准库，HMAC-SHA256 签名在服务端完成，Secret 永不发给浏览器）
// 文档: https://developers.binance.com/docs/binance-spot-api-docs
// ---------------------------------------------------------------------------

// APIError 表示币安返回的业务错误，例如 -2015 (无效 API Key / IP 限制)。
type APIError struct {
	Status int    `json:"-"`
	Code   int    `json:"code"`
	Msg    string `json:"msg"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("binance error %d (http %d): %s", e.Code, e.Status, e.Msg)
}

// Friendly 把常见错误码翻译成中文提示，方便页面上直接展示。
func (e *APIError) Friendly() string {
	switch e.Code {
	case -1021:
		return "请求时间戳超出 recvWindow，请校准本机系统时间（" + e.Msg + "）"
	case -1022:
		return "签名校验失败，请检查 config.json 中的 apiSecret 是否正确（" + e.Msg + "）"
	case -2014:
		return "API Key 格式错误或缺少必要参数（" + e.Msg + "）"
	case -2015:
		return "API Key 无效、被禁用、IP 不在白名单，或没有“读取”权限（" + e.Msg + "）"
	case -2008:
		return "该 API Key 的 IP 白名单不包含当前公网 IP（" + e.Msg + "）"
	case -1003:
		if d := BanDuration(e.Msg); d > 0 {
			return "当前 IP 已被币安临时封禁（限流升级），约 " + d.Round(time.Second).String() +
				"后解封；程序不会在此期间重试。建议改用 WebSocket 或降低刷新频率（" + e.Msg + "）"
		}
		return "请求过于频繁，已被限流。程序会自动进入冷却退避，也可以把 pollMs 调大（" + e.Msg + "）"
	case -1102, -1100, -1104:
		return "请求参数错误（" + e.Msg + "）"
	}
	if e.Status == http.StatusTeapot {
		return "当前网络/IP 被币安地域限制（HTTP 451），需要换网络出口或使用代理（" + e.Msg + "）"
	}
	return fmt.Sprintf("币安接口报错 %d: %s", e.Code, e.Msg)
}

// CooldownError 表示本地主动进入限流冷却：这段时间内不再向上游发任何请求，
// 避免在被币安限流后继续“顶着”打，把权重烧光（-1003 / HTTP 429 / 418）。
type CooldownError struct {
	Remaining time.Duration
	Note      string
}

func (e *CooldownError) Error() string {
	return fmt.Sprintf("本地限流冷却中，剩余 %s；%s", e.Remaining.Round(time.Second), e.Note)
}

// Client 是币安现货 + U 本位合约的轻量客户端。
type Client struct {
	baseURL   string // 现货: https://api.binance.com
	fapiURL   string // U 本位合约: https://fapi.binance.com
	apiKey    string
	apiSecret string
	hc        *http.Client

	mu           sync.Mutex
	cool         map[string]hostCool // 按 host 分别冷却：现货被限流时合约仍可正常查询
	okPath       map[string]string   // 各 fapi 接口已验证可用的版本路径
	usedWeight1m int64               // 币安返回的 X-MBX-USED-WEIGHT-1M 最大值（现货/合约各自口径）
}

type hostCool struct {
	until time.Time
	note  string
}

func NewClient(apiKey, apiSecret, baseURL, fapiURL, proxy string) *Client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 12 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	if strings.TrimSpace(proxy) != "" {
		if pu, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(pu)
		}
	}
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		fapiURL:   strings.TrimRight(fapiURL, "/"),
		apiKey:    strings.TrimSpace(apiKey),
		apiSecret: strings.TrimSpace(apiSecret),
		hc:        &http.Client{Transport: tr, Timeout: 20 * time.Second},
	}
}

// Cooldown 返回所有 host 中最长的剩余冷却时间与原因；为 0 表示都能正常请求。
func (c *Client) Cooldown() (time.Duration, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best time.Duration
	var note string
	for _, hc := range c.cool {
		if d := time.Until(hc.until); d > best {
			best, note = d, hc.note
		}
	}
	return best, note
}

// coolFor 返回指定 host（现货 / 合约）的剩余冷却时间。
func (c *Client) coolFor(base string) (time.Duration, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hc, ok := c.cool[base]
	if !ok || !time.Now().Before(hc.until) {
		return 0, ""
	}
	return time.Until(hc.until), hc.note
}

// AllHostsCooling 判断需要用的 host 是否全都在冷却（此时连一轮请求都不用发）。
func (c *Client) AllHostsCooling(includeFutures bool) bool {
	hosts := []string{c.baseURL}
	if includeFutures {
		hosts = append(hosts, c.fapiURL)
	}
	for _, h := range hosts {
		if d, _ := c.coolFor(h); d <= 0 {
			return false
		}
	}
	return true
}

// UsedWeight1M 返回最近一次币安响应里报告的 1 分钟已用权重（现货与合约取较大值）。
func (c *Client) UsedWeight1M() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usedWeight1m
}

// enterCooldown 让某个 host 进入冷却：只延长不缩短，避免并发请求互相覆盖。
func (c *Client) enterCooldown(base string, d time.Duration, note string) {
	if d < 10*time.Second {
		d = 10 * time.Second
	}
	if d > 24*time.Hour { // 币安封禁最长可达数天，这里最多冷却 24 小时
		d = 24 * time.Hour
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cool == nil {
		c.cool = map[string]hostCool{}
	}
	if until := time.Now().Add(d); until.After(c.cool[base].until) {
		c.cool[base] = hostCool{until: until, note: note}
	}
}

// banRe 匹配币安 418 提示里的封禁时间：“banned until 1790306630180”
var banRe = regexp.MustCompile(`banned until (\d{10,16})`)

// BanDuration 从币安提示里解析 IP 封禁剩余时间（毫秒时间戳，最权威）。
func BanDuration(msg string) time.Duration {
	m := banRe.FindStringSubmatch(msg)
	if len(m) != 2 {
		return 0
	}
	ms, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	if d := time.Until(time.UnixMilli(ms)); d > 0 {
		return d
	}
	return 0
}

func (c *Client) noteUsedWeight(h http.Header) {
	raw := h.Get("X-MBX-USED-WEIGHT-1M")
	if raw == "" {
		return
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return
	}
	c.mu.Lock()
	if v > c.usedWeight1m {
		c.usedWeight1m = v
	}
	c.mu.Unlock()
}

// retryAfter 解析 Retry-After 头（秒），币安在 429/418 时会带上。
func retryAfter(h http.Header) time.Duration {
	raw := strings.TrimSpace(h.Get("Retry-After"))
	if raw == "" {
		return 0
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(raw); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// get 发起一次 GET 请求；signed=true 时自动补 timestamp/recvWindow 并做 HMAC-SHA256 签名。
// 签名必须针对最终发送的 query string（params.Encode() 按 key 排序），因此这里统一构造。
func (c *Client) get(ctx context.Context, base, path string, params url.Values, signed bool, out any) error {
	return c.request(ctx, http.MethodGet, base, path, params, signed, out)
}

// put 发起一次 PUT 请求（目前只用于 listenKey 保活）
func (c *Client) put(ctx context.Context, base, path string, params url.Values, out any) error {
	return c.request(ctx, http.MethodPut, base, path, params, true, out)
}

// post 发起一次 POST 请求（目前只用于创建 listenKey）
func (c *Client) post(ctx context.Context, base, path string, params url.Values, out any) error {
	return c.request(ctx, http.MethodPost, base, path, params, true, out)
}

func (c *Client) request(ctx context.Context, method, base, path string, params url.Values, signed bool, out any) error {
	// 冷却期内直接短路，一个请求都不发出去（按 host 判断：现货被封不影响合约）
	if wait, note := c.coolFor(base); wait > 0 {
		return &CooldownError{Remaining: wait, Note: note}
	}
	if params == nil {
		params = url.Values{}
	}
	if signed {
		params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
		if params.Get("recvWindow") == "" {
			params.Set("recvWindow", "5000")
		}
	}
	raw := params.Encode()
	full := base + path
	if raw != "" {
		full += "?" + raw
	}
	if signed {
		mac := hmac.New(sha256.New, []byte(c.apiSecret))
		mac.Write([]byte(raw))
		full += "&signature=" + hex.EncodeToString(mac.Sum(nil))
	}

	req, err := http.NewRequestWithContext(ctx, method, full, nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-MBX-APIKEY", c.apiKey)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "binance-equity-viewer/1.0")

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("网络请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}
	c.noteUsedWeight(resp.Header)

	// HTTP 429/418：币安明确要求退避，按其 Retry-After / 封禁时间进入冷却
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		ae := &APIError{Status: resp.StatusCode, Code: -1003}
		if json.Unmarshal(body, ae) != nil || ae.Msg == "" {
			if ae.Code == 0 {
				ae.Code = -1003
			}
			ae.Msg = strings.TrimSpace(string(body))
		}
		d := retryAfter(resp.Header)
		if b := BanDuration(ae.Msg); b > 0 { // 币安给出的解封时间最准确，优先采用
			d = b
		}
		if d == 0 {
			d = 60 * time.Second
		}
		c.enterCooldown(base, d, ae.Msg)
		return ae
	}
	if resp.StatusCode >= 400 {
		ae := &APIError{Status: resp.StatusCode}
		if json.Unmarshal(body, ae) != nil || ae.Msg == "" {
			ae.Msg = strings.TrimSpace(string(body))
			if ae.Msg == "" {
				ae.Msg = resp.Status
			}
		}
		if ae.Code == -1003 { // 有些场景会以 400 + -1003 返回
			d := retryAfter(resp.Header)
			if b := BanDuration(ae.Msg); b > 0 {
				d = b
			}
			if d == 0 {
				d = 60 * time.Second
			}
			c.enterCooldown(base, d, ae.Msg)
		}
		return ae
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}
	return nil
}

// futuresGet 依次尝试多个 fapi 版本路径（币安把 /fapi/v2/account 迁到了 /fapi/v3/account），
// 只有 404 才继续降级，其它错误直接返回，避免掩盖权限/签名问题。
// 成功的版本会按 key 记住，下一轮直接命中，省掉每次“探测不存在的版本”白白消耗的权重。
func (c *Client) futuresGet(ctx context.Context, key string, paths []string, params url.Values, out any) error {
	ordered := paths
	if rp := c.getPath(key); rp != "" {
		ordered = append([]string{rp}, paths...)
	}
	var lastErr error
	seen := make(map[string]bool, len(ordered))
	for _, p := range ordered {
		if seen[p] {
			continue
		}
		seen[p] = true
		cp := url.Values{}
		for k, v := range params {
			cp[k] = append([]string(nil), v...)
		}
		err := c.get(ctx, c.fapiURL, p, cp, true, out)
		if err == nil {
			c.setPath(key, p)
			return nil
		}
		lastErr = err
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == http.StatusNotFound {
			continue
		}
		return err
	}
	return lastErr
}

// getPath / setPath 记住各接口实际可用的 fapi 版本路径。
func (c *Client) getPath(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.okPath[key]
}

func (c *Client) setPath(key, path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.okPath == nil {
		c.okPath = map[string]string{}
	}
	c.okPath[key] = path
}

// Kline 是概率模型需要的 K 线字段
type Kline struct {
	T                       int64
	O, H, L, C, Vol, TakerB float64
}

// Klines 拉取公开 K 线（无需签名），用于历史概率统计
func (c *Client) Klines(ctx context.Context, symbol, interval string, start, end int64, limit int) ([]Kline, error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("interval", interval)
	params.Set("limit", strconv.Itoa(limit))
	if start > 0 {
		params.Set("startTime", strconv.FormatInt(start, 10))
	}
	if end > 0 {
		params.Set("endTime", strconv.FormatInt(end, 10))
	}
	var raw [][]any
	if err := c.get(ctx, c.fapiURL, "/fapi/v1/klines", params, false, &raw); err != nil {
		return nil, err
	}
	out := make([]Kline, 0, len(raw))
	for _, r := range raw {
		if len(r) < 11 {
			continue
		}
		f := func(i int) float64 {
			v, _ := strconv.ParseFloat(r[i].(string), 64)
			return v
		}
		out = append(out, Kline{T: int64(r[0].(float64)), O: f(1), H: f(2), L: f(3), C: f(4), Vol: f(5), TakerB: f(9)})
	}
	return out, nil
}

// ---------------- 返回结构（只取页面需要的字段） ----------------

// SpotAccount GET /api/v3/account
type SpotAccount struct {
	CanTrade    bool     `json:"canTrade"`
	CanWithdraw bool     `json:"canWithdraw"`
	AccountType string   `json:"accountType"`
	UpdateTime  int64    `json:"updateTime"`
	Permissions []string `json:"permissions"`
	Balances    []struct {
		Asset  string `json:"asset"`
		Free   string `json:"free"`
		Locked string `json:"locked"`
	} `json:"balances"`
}

// Ticker GET /api/v3/ticker/price （全市场最新价，权重 2）
type Ticker struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// SpotOrder 现货当前委托，GET /api/v3/openOrders
type SpotOrder struct {
	Symbol                  string `json:"symbol"`
	OrderID                 int64  `json:"orderId"`
	ClientOrderID           string `json:"clientOrderId"`
	Price                   string `json:"price"`
	OrigQty                 string `json:"origQty"`
	ExecutedQty             string `json:"executedQty"`
	CummulativeQuoteQty     string `json:"cummulativeQuoteQty"`
	Status                  string `json:"status"`
	TimeInForce             string `json:"timeInForce"`
	Type                    string `json:"type"`
	Side                    string `json:"side"`
	StopPrice               string `json:"stopPrice"`
	Time                    int64  `json:"time"`
	UpdateTime              int64  `json:"updateTime"`
	IsWorking               bool   `json:"isWorking"`
	SelfTradePreventionMode string `json:"selfTradePreventionMode"`
}

// FuturesAccount GET /fapi/v3/account （字段为字符串）
type FuturesAccount struct {
	TotalWalletBalance      string `json:"totalWalletBalance"`
	TotalUnrealizedProfit   string `json:"totalUnrealizedProfit"`
	TotalMarginBalance      string `json:"totalMarginBalance"`
	TotalInitialMargin      string `json:"totalInitialMargin"`
	TotalMaintMargin        string `json:"totalMaintMargin"`
	AvailableBalance        string `json:"availableBalance"`
	TotalCrossWalletBalance string `json:"totalCrossWalletBalance"`
	Assets                  []struct {
		Asset            string `json:"asset"`
		WalletBalance    string `json:"walletBalance"`
		UnrealizedProfit string `json:"unrealizedProfit"`
		MarginBalance    string `json:"marginBalance"`
		AvailableBalance string `json:"availableBalance"`
	} `json:"assets"`
}

// FuturesPosition GET /fapi/v2/positionRisk
// 注意：v3 版的 positionRisk 不再返回 leverage / marginType / positionSide，
// 因此优先用 v2，只有在 v2 不可用时才降级到 v3。
type FuturesPosition struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	BreakEvenPrice   string `json:"breakEvenPrice"`
	MarkPrice        string `json:"markPrice"`
	UnRealizedProfit string `json:"unRealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MarginType       string `json:"marginType"`
	Isolated         bool   `json:"isolated"`
	PositionSide     string `json:"positionSide"`
	Notional         string `json:"notional"`
	UpdateTime       int64  `json:"updateTime"`
}

// FuturesOrder GET /fapi/v1/openOrders
type FuturesOrder struct {
	Symbol        string `json:"symbol"`
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	CumQuote      string `json:"cumQuote"`
	Status        string `json:"status"`
	TimeInForce   string `json:"timeInForce"`
	Type          string `json:"type"`
	Side          string `json:"side"`
	StopPrice     string `json:"stopPrice"`
	ReduceOnly    bool   `json:"reduceOnly"`
	PositionSide  string `json:"positionSide"`
	Time          int64  `json:"time"`
	UpdateTime    int64  `json:"updateTime"`
}

// ---------------- 具体接口封装 ----------------

func (c *Client) SpotAccount(ctx context.Context) (*SpotAccount, error) {
	var out SpotAccount
	if err := c.get(ctx, c.baseURL, "/api/v3/account", nil, true, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) SpotPrices(ctx context.Context) (map[string]float64, error) {
	var list []Ticker
	if err := c.get(ctx, c.baseURL, "/api/v3/ticker/price", nil, false, &list); err != nil {
		return nil, err
	}
	m := make(map[string]float64, len(list))
	for _, t := range list {
		if v, err := strconv.ParseFloat(t.Price, 64); err == nil {
			m[t.Symbol] = v
		}
	}
	return m, nil
}

func (c *Client) SpotOpenOrders(ctx context.Context) ([]SpotOrder, error) {
	var out []SpotOrder
	if err := c.get(ctx, c.baseURL, "/api/v3/openOrders", nil, true, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) FuturesAccount(ctx context.Context) (*FuturesAccount, error) {
	var out FuturesAccount
	if err := c.futuresGet(ctx, "futuresAccount", []string{"/fapi/v3/account", "/fapi/v2/account"}, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) FuturesPositions(ctx context.Context) ([]FuturesPosition, error) {
	var out []FuturesPosition
	if err := c.futuresGet(ctx, "futuresPositions", []string{"/fapi/v2/positionRisk", "/fapi/v3/positionRisk"}, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) FuturesOpenOrders(ctx context.Context) ([]FuturesOrder, error) {
	var out []FuturesOrder
	if err := c.get(ctx, c.fapiURL, "/fapi/v1/openOrders", nil, true, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// AlgoOrder 是条件单（止损/止盈）——它们不在 openOrders 里，也不走用户数据流推送，
// 必须单独查。权重只有 1，很便宜。
type AlgoOrder struct {
	AlgoID       int64  `json:"algoId"`
	ClientAlgoID string `json:"clientAlgoId"`
	Symbol       string `json:"symbol"`
	Side         string `json:"side"`
	PositionSide string `json:"positionSide"`
	OrderType    string `json:"orderType"`
	AlgoStatus   string `json:"algoStatus"`
	Quantity     string `json:"quantity"`
	TriggerPrice string `json:"triggerPrice"`
	ClosePos     bool   `json:"closePosition"`
	ReduceOnly   bool   `json:"reduceOnly"`
	WorkingType  string `json:"workingType"`
	CreateTime   int64  `json:"createTime"`
	UpdateTime   int64  `json:"updateTime"`
}

func (c *Client) AlgoOpenOrders(ctx context.Context) ([]AlgoOrder, error) {
	var out []AlgoOrder
	if err := c.get(ctx, c.fapiURL, "/fapi/v1/openAlgoOrders", nil, true, &out); err != nil {
		return nil, err
	}
	return out, nil
}
