package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

// Config 支持三种来源，优先级：命令行参数 > 环境变量 > config.json
type Config struct {
	APIKey           string         `json:"apiKey"`
	APISecret        string         `json:"apiSecret"`
	BaseURL          string         `json:"baseURL"`
	FAPIBaseURL      string         `json:"fapiBaseURL"`
	Addr             string         `json:"addr"`
	PollMS           int            `json:"pollMs"`
	FuturesOrdersSec int            `json:"futuresOrdersSec"` // 合约“当前委托”查询间隔（该接口权重 40，故降频）
	TPLevels         []float64      `json:"tpLevels"`         // 多单止盈档（页面概率用）
	TPLevelsShort    []float64      `json:"tpLevelsShort"`    // 空单止盈档（留空则按 -2/-4/-6/-8% 自动生成）
	AlertRules       []AlertRule    `json:"alertRules"`       // 告警规则
	AlertChannels    []AlertChannel `json:"alertChannels"`    // 告警通道（含语音电话）
	EnableFutures    bool           `json:"enableFutures"`
	Demo             bool           `json:"demo"`
	Proxy            string         `json:"proxy"`
}

func defaultConfig() Config {
	return Config{
		BaseURL:          "https://api.binance.com",
		FAPIBaseURL:      "https://fapi.binance.com",
		Addr:             "127.0.0.1:8080",
		PollMS:           5000, // 币安按 IP 限 2400 权重/分钟，5 秒一轮约消耗 1/4 额度，留足余量
		FuturesOrdersSec: 15,
		TPLevels:         []float64{10.40, 9.95},
		EnableFutures:    true,
	}
}

func loadConfigFile(path string) (Config, bool) {
	cfg := defaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, false
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Printf("警告: 解析 %s 失败 (%v)，将忽略该文件", path, err)
		return defaultConfig(), false
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.binance.com"
	}
	if cfg.FAPIBaseURL == "" {
		cfg.FAPIBaseURL = "https://fapi.binance.com"
	}
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:8080"
	}
	if cfg.PollMS < 2000 {
		cfg.PollMS = 5000
	}
	if cfg.FuturesOrdersSec < 5 {
		cfg.FuturesOrdersSec = 15
	}
	if len(cfg.TPLevels) == 0 {
		cfg.TPLevels = []float64{10.40, 9.95}
	}
	return cfg, true
}

// parseLevels 解析 "9.31,9.11,8.80" 形式的档位列表
func parseLevels(s string) []float64 {
	var out []float64
	for _, part := range strings.Split(s, ",") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(part), 64); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

// 快照缓存：多个浏览器标签页/快速刷新时只在服务端打一次币安接口，避免触发限流。
type snapshotCache struct {
	mu           sync.Mutex
	last         *Snapshot // 最近一次成功的数据（限流冷却时继续展示它）
	lastFail     *Snapshot // 最近一次失败的结果
	lastAt       time.Time // 最近一次向上游发起请求的时间
	minGap       time.Duration
	futOrdersGap time.Duration
	futOrdersAt  time.Time
	client       *Client
	pollMS       int
	futures      bool
	demo         bool
	cfgErr       string // 配置缺失时的提示，直接返回给页面，不做无意义的签名请求
}

func (sc *snapshotCache) Get(ctx context.Context) *Snapshot {
	if sc.cfgErr != "" {
		now := time.Now()
		return &Snapshot{
			OK: false, PollMS: sc.pollMS, FetchedAt: now, FetchedMS: now.UnixMilli(),
			Error: sc.cfgErr, Source: "未配置",
		}
	}

	// 限流冷却中：一个上游请求都不发，继续展示上一次成功的数据并说明原因
	// （只在现货与合约都冷却时才这样；只封了一边时另一边照常刷新）
	if sc.client.AllHostsCooling(sc.futures) {
		if wait, note := sc.client.Cooldown(); wait > 0 {
			secs := int(wait.Seconds()) + 1
			msg := "币安接口限流中（" + note + "）。已暂停向上游请求，" + strconv.Itoa(secs) + " 秒后自动恢复"
			sc.mu.Lock()
			defer sc.mu.Unlock()
			if sc.last != nil {
				cp := *sc.last
				cp.CoolingDown, cp.CooldownSeconds = true, secs
				cp.Warnings = append(append([]string{}, sc.last.Warnings...), msg)
				return &cp
			}
			now := time.Now()
			return &Snapshot{
				OK: false, CoolingDown: true, CooldownSeconds: secs, PollMS: sc.pollMS,
				FetchedAt: now, FetchedMS: now.UnixMilli(), Error: msg, Source: sc.client.baseURL,
			}
		}
	}

	sc.mu.Lock()
	if src := sc.last; src != nil && time.Since(sc.lastAt) < sc.minGap {
		sc.mu.Unlock()
		return src
	}
	if sc.last == nil && sc.lastFail != nil && time.Since(sc.lastAt) < sc.minGap {
		s := sc.lastFail
		sc.mu.Unlock()
		return s
	}
	prev := sc.last
	fetchFutOrders := sc.futures && (prev == nil || time.Since(sc.futOrdersAt) >= sc.futOrdersGap)
	sc.mu.Unlock()

	var snap *Snapshot
	if sc.demo {
		snap = demoSnapshot(sc.pollMS, sc.futures)
	} else {
		snap = buildSnapshot(ctx, sc.client, sc.pollMS, sc.futures, fetchFutOrders)
	}

	// 合约委托降频：本轮没查就复用上一轮的结果（权重 40，不值得每轮都拉）
	// 注意：条件单（止损/止盈）每轮都是新查的，不能一起复用，否则会重复累加
	if sc.futures && !fetchFutOrders && prev != nil {
		carried := 0
		for _, o := range prev.Orders {
			if o.Market == "合约" && !o.Conditional {
				snap.Orders = append(snap.Orders, o)
				carried++
			}
		}
		if carried > 0 {
			snap.Totals.OpenOrders = len(snap.Orders)
			snap.FuturesOrdersAgeMS = time.Since(sc.futOrdersAt).Milliseconds()
			if snap.Futures != nil {
				snap.Futures.OrderCount = carried
			}
		}
	}

	// 本轮刚被限流：立刻把冷却状态带上，页面马上能显示“限流暂停”而不是干等下一次刷新
	if wait, note := sc.client.Cooldown(); wait > 0 {
		secs := int(wait.Seconds()) + 1
		snap.CoolingDown, snap.CooldownSeconds = true, secs
		snap.Warnings = append([]string{
			"币安接口限流中（" + note + "），已暂停向上游请求，" + strconv.Itoa(secs) + " 秒后自动恢复",
		}, snap.Warnings...)
		if !snap.OK {
			snap.Error = "币安接口限流：短时间内请求过多，已暂停请求并进入冷却退避，" +
				strconv.Itoa(secs) + " 秒后自动重试（" + note + "）"
		}
	}

	built := snap
	// 上游失败时不要给页面一堆 0：只要上一次成功的数据还够新，就继续展示它并说明情况
	if !built.OK && prev != nil && prev.OK && time.Since(prev.FetchedAt) < 5*time.Minute {
		cp := *prev
		cp.CoolingDown, cp.CooldownSeconds = built.CoolingDown, built.CooldownSeconds
		cp.Error, cp.LatencyMS = built.Error, built.LatencyMS
		cp.Warnings = append(append([]string{}, built.Warnings...),
			"本轮刷新失败，当前展示的是 "+time.Since(prev.FetchedAt).Round(time.Second).String()+"前的数据")
		snap = &cp
	}

	sc.mu.Lock()
	sc.lastAt = time.Now()
	if built.OK {
		sc.last = built // 只保存干净的成功快照，避免告警信息在后续几轮里累积
	} else {
		sc.lastFail = built
	}
	if fetchFutOrders {
		sc.futOrdersAt = time.Now()
	}
	sc.mu.Unlock()
	return snap
}

func main() {
	cfg, fromFile := loadConfigFile("config.json")

	// 环境变量覆盖（便于不落盘保存密钥）
	envOverride := func(dst *string, keys ...string) {
		for _, k := range keys {
			if v := strings.TrimSpace(os.Getenv(k)); v != "" {
				*dst = v
				return
			}
		}
	}
	envOverride(&cfg.APIKey, "BINANCE_API_KEY", "BINANCE_APIKEY")
	envOverride(&cfg.APISecret, "BINANCE_API_SECRET", "BINANCE_SECRET")
	envOverride(&cfg.BaseURL, "BINANCE_BASE_URL")
	envOverride(&cfg.FAPIBaseURL, "BINANCE_FAPI_BASE_URL")
	envOverride(&cfg.Proxy, "BINANCE_PROXY", "HTTPS_PROXY", "https_proxy")

	// 命令行参数（显式传入时优先级最高）
	addr := flag.String("addr", cfg.Addr, "H5 页面监听地址，默认 127.0.0.1:8080（仅本机可访问）")
	pollMS := flag.Int("pollms", cfg.PollMS, "页面自动刷新间隔（毫秒，最小 2000）")
	demo := flag.Bool("demo", cfg.Demo, "使用内置演示数据，不请求币安接口")
	noFutures := flag.Bool("no-futures", !cfg.EnableFutures, "关闭 U 本位合约查询，只显示现货")
	baseURL := flag.String("base-url", cfg.BaseURL, "现货 REST 地址")
	proxy := flag.String("proxy", cfg.Proxy, "HTTP 代理，例如 http://127.0.0.1:7890（也可用 HTTPS_PROXY 环境变量）")
	futOrdersSec := flag.Int("futures-orders-sec", cfg.FuturesOrdersSec, "合约“当前委托”的查询间隔秒数（权重 40，默认 15 秒一次）")
	tpFlag := flag.String("tp", "", "多单止盈档位（逗号分隔，如 10.40,9.95），用于页面概率计算；默认读 config.json")
	tpShortFlag := flag.String("tp-short", "", "空单止盈档位（逗号分隔，如 9.31,9.11,8.80）；留空则按 -2/-4/-6/-8% 自动生成")
	flag.Parse()

	cfg.Addr, cfg.PollMS, cfg.Demo = *addr, *pollMS, *demo
	cfg.EnableFutures = !*noFutures
	cfg.BaseURL, cfg.Proxy = *baseURL, *proxy
	cfg.FuturesOrdersSec = *futOrdersSec
	if cfg.PollMS < 2000 {
		cfg.PollMS = 2000
	}
	if cfg.FuturesOrdersSec < 5 {
		cfg.FuturesOrdersSec = 5
	}
	if *tpFlag != "" {
		if lv := parseLevels(*tpFlag); len(lv) > 0 {
			cfg.TPLevels = lv
		}
	}
	if *tpShortFlag != "" {
		if lv := parseLevels(*tpShortFlag); len(lv) > 0 {
			cfg.TPLevelsShort = lv
		}
	}

	log.SetFlags(log.LstdFlags)
	log.Printf("币安账户实时看板启动中…")
	if fromFile {
		log.Printf("已加载 config.json")
	}
	if cfg.Demo {
		log.Printf("模式: DEMO（演示数据，未连接币安）")
	} else {
		// 按 rune 处理，避免把 UTF-8 多字节字符截断成乱码
		mask := func(s string) string {
			r := []rune(s)
			if len(r) == 0 {
				return "(未配置)"
			}
			if len(r) <= 8 {
				return strings.Repeat("*", len(r))
			}
			return string(r[:4]) + "****" + string(r[len(r)-4:])
		}
		log.Printf("API Key: %s  Secret: %s", mask(cfg.APIKey), mask(cfg.APISecret))
		if cfg.Proxy != "" {
			log.Printf("使用代理: %s", cfg.Proxy)
		}
	}

	client := NewClient(cfg.APIKey, cfg.APISecret, cfg.BaseURL, cfg.FAPIBaseURL, cfg.Proxy)
	cache := &snapshotCache{
		minGap:       time.Duration(cfg.PollMS) * time.Millisecond,
		futOrdersGap: time.Duration(cfg.FuturesOrdersSec) * time.Second,
		client:       client, pollMS: cfg.PollMS, futures: cfg.EnableFutures, demo: cfg.Demo,
	}
	if !cfg.Demo {
		var missing []string
		if cfg.APIKey == "" {
			missing = append(missing, "apiKey")
		}
		if cfg.APISecret == "" {
			missing = append(missing, "apiSecret")
		}
		if len(missing) > 0 {
			cache.cfgErr = "尚未配置 " + strings.Join(missing, " 和 ") +
				"。请在 config.json 中填写（或用环境变量 BINANCE_API_KEY / BINANCE_API_SECRET 传入），保存后刷新页面即可。"
		}
	}
	// 一个刷新周期最多向币安请求一轮：即使开多个标签页、或频繁点“立即刷新”，
	// 也不会成倍消耗接口权重（fapi 的 openOrders 不带 symbol 时权重 40，最贵）。
	if cache.minGap < 1000*time.Millisecond {
		cache.minGap = 1000 * time.Millisecond
	}
	log.Printf("刷新间隔 %d ms；合约委托每 %d 秒查询一次", cfg.PollMS, cfg.FuturesOrdersSec)

	// 持仓概率模型（后台拉历史 K 线，页面会显示“构建中”直到就绪）
	if !cfg.Demo {
		oddsEngine = newOddsStore(cfg.TPLevels, cfg.TPLevelsShort, []float64{2, 4, 6, 8})
		log.Printf("已启用持仓概率模型（%s K线）：多单档 %v / 空单档 %v（空档则自动 ±2/4/6/8%%）",
			oddsInterval, cfg.TPLevels, cfg.TPLevelsShort)
		// 用户数据流（WebSocket）：实时推送账户/持仓/订单，替代权重 40 的 openOrders 轮询
		wsEngine = NewUserDataStream(client)
		wsEngine.Start(context.Background())
		log.Printf("已启动用户数据流（WebSocket），合约未成交订单改由推送维护")
	}
	// 告警引擎（规则 + 通道；通道可留空，此时只在页面显示历史）
	if len(cfg.AlertRules) > 0 || len(cfg.AlertChannels) > 0 {
		alertEngineRef = newAlertEngine(cfg.AlertRules, cfg.AlertChannels)
		enabledCh := 0
		for _, c := range cfg.AlertChannels {
			if c.Enabled {
				enabledCh++
			}
		}
		log.Printf("已启用告警引擎：%d 条规则 / %d 个通道（含 %d 个已启用；通道用 /api/alert/test 测试）",
			len(cfg.AlertRules), len(cfg.AlertChannels), enabledCh)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/index.html" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		timeout := 25 * time.Second
		if cfg.Demo {
			timeout = 3 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		snap := cache.Get(ctx)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(snap); err != nil {
			log.Printf("写响应失败: %v", err)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	// 告警通道自检：/api/alert/test 普通，/api/alert/test?urgent=1 会真的打电话
	mux.HandleFunc("/api/alert/test", func(w http.ResponseWriter, r *http.Request) {
		if alertEngineRef == nil {
			http.Error(w, "告警引擎未启用（请在 config.json 配置 alertRules / alertChannels）", http.StatusBadRequest)
			return
		}
		urgent := r.URL.Query().Get("urgent") == "1"
		res := alertEngineRef.Test(urgent)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("测试告警已触发：" + res))
	})

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// 启动前给出配置检查提示（不阻塞启动，页面里也会显示同样的错误）
	if !cfg.Demo {
		if cfg.APIKey == "" || cfg.APISecret == "" {
			log.Printf("警告: 未配置 apiKey / apiSecret，页面将显示鉴权错误。请填写 config.json 或设置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET")
		}
	}

	go func() {
		log.Printf("H5 页面地址: http://%s/", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("启动失败: %v", err)
		}
	}()

	// 等待 Ctrl+C，优雅退出
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("正在退出…")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path == "/api/snapshot" {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}
