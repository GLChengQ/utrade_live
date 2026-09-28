package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 告警引擎：按规则监测持仓/价格，触发后通过多个通道推送（含"打电话"）
//
// 通道说明：
//   bark       iOS 推送，level=critical 会强行响铃（最接近打电话，免费）
//   telegram   Telegram Bot，免费、秒级、国际通用
//   twilio     **真正的语音电话**（TTS 朗读），需要 Twilio 账号 + 被叫号码验证
//   webhook    通用 GET 模板，适配 PushPlus / Server酱 / ntfy / 企业微信机器人
//   console    只写日志（本地验证用）
// ---------------------------------------------------------------------------

type AlertRule struct {
	Name        string  `json:"name"`
	Metric      string  `json:"metric"` // price_le / price_ge / liq_dist_le / equity_le / unrealized_le / drop5m_ge
	Value       float64 `json:"value"`
	Symbol      string  `json:"symbol"`      // 留空 = 自动用名义最大的持仓
	CooldownSec int     `json:"cooldownSec"` // 同一规则最短重复间隔
	Enabled     bool    `json:"enabled"`
	Message     string  `json:"message"` // 支持 {price} {liqDist} {equity} {unrealized} {rule}
	Urgent      bool    `json:"urgent"`  // true = 走语音电话/临界推送

	lastFired time.Time
	fireCount int
}

type AlertChannel struct {
	Type    string `json:"type"` // bark / telegram / twilio / webhook / console
	Enabled bool   `json:"enabled"`

	// Bark
	BarkServer string `json:"barkServer"` // 默认 https://api.day.app
	BarkKey    string `json:"barkKey"`

	// Telegram
	TGToken  string `json:"telegramToken"`
	TGChatID string `json:"telegramChatId"`

	// Twilio（语音电话）
	TwilioSID   string `json:"twilioSid"`
	TwilioToken string `json:"twilioToken"`
	TwilioFrom  string `json:"twilioFrom"` // 你的 Twilio 号码，如 +1234567890
	TwilioTo    string `json:"twilioTo"`   // 你的手机号，如 +8613800000000
	Voice       string `json:"voice"`      // 例如 Polly.Zhiyu（中文女声）

	// Webhook（通用；{title} {body} 会被替换并 URL 编码）
	WebhookURL string `json:"webhookUrl"`

	lastErr string
	sent    int
}

type AlertEvent struct {
	ID      int64  `json:"id"`
	Time    int64  `json:"time"`
	Rule    string `json:"rule"`
	Message string `json:"message"`
	Urgent  bool   `json:"urgent"`
	Results string `json:"results"` // 各通道发送结果
}

type alertEngine struct {
	mu       sync.Mutex
	rules    []AlertRule
	channels []AlertChannel
	history  []AlertEvent
	nextID   int64

	prices []pricePoint // 最近价格（用于跌幅判断）
}

type pricePoint struct {
	t     time.Time
	price float64
}

func newAlertEngine(rules []AlertRule, channels []AlertChannel) *alertEngine {
	return &alertEngine{rules: rules, channels: channels}
}

// AlertStatusProvider 供外部（快照/接口）读取告警状态
var alertEngineRef *alertEngine

// evaluate 用最新快照评估所有规则（只在拿到新数据时调用）
func (e *alertEngine) evaluate(snap *Snapshot) {
	if e == nil || snap == nil {
		return
	}
	// 取"主持仓"：名义最大的、有有效强平价的
	var main *OddsPos
	if snap.Odds != nil {
		for i := range snap.Odds.Positions {
			p := &snap.Odds.Positions[i]
			if main == nil || abs(p.Notional) > abs(main.Notional) {
				main = p
			}
		}
	}
	var price float64
	var liqDist float64
	if main != nil {
		price = main.Price
		if main.LiqValid {
			liqDist = main.LiqDist
		}
	} else if len(snap.Positions) > 0 {
		price = snap.Positions[0].MarkPrice
	}
	if price > 0 {
		e.mu.Lock()
		e.prices = append(e.prices, pricePoint{time.Now(), price})
		if len(e.prices) > 600 { // 约 50 分钟 @5s
			e.prices = e.prices[len(e.prices)-600:]
		}
		e.mu.Unlock()
	}

	equity, unreal := 0.0, 0.0
	if snap.Futures != nil {
		equity = snap.Futures.MarginBalance
		unreal = snap.Futures.Unrealized
	}

	now := time.Now()
	for i := range e.rules {
		r := &e.rules[i]
		if !r.Enabled {
			continue
		}
		var hit bool
		var detail string
		switch r.Metric {
		case "price_le":
			hit, detail = price > 0 && price <= r.Value, fmt.Sprintf("标记价 %.4f ≤ %.4f", price, r.Value)
		case "price_ge":
			hit, detail = price > 0 && price >= r.Value, fmt.Sprintf("标记价 %.4f ≥ %.4f", price, r.Value)
		case "liq_dist_le":
			hit, detail = liqDist > 0 && liqDist <= r.Value, fmt.Sprintf("距强平 %.2f%% ≤ %.2f%%", liqDist, r.Value)
		case "equity_le":
			hit, detail = equity > 0 && equity <= r.Value, fmt.Sprintf("账户权益 %.2f ≤ %.2f", equity, r.Value)
		case "unrealized_le":
			hit, detail = unreal <= r.Value, fmt.Sprintf("未实现盈亏 %.2f ≤ %.2f", unreal, r.Value)
		case "drop5m_ge":
			d := e.dropPct(5 * time.Minute)
			hit, detail = d >= r.Value, fmt.Sprintf("5 分钟跌幅 %.2f%% ≥ %.2f%%", d, r.Value)
		default:
			continue
		}
		if !hit {
			continue
		}
		cd := time.Duration(r.CooldownSec) * time.Second
		if cd <= 0 {
			cd = 5 * time.Minute
		}
		if now.Sub(r.lastFired) < cd {
			continue
		}
		r.lastFired = now
		r.fireCount++
		msg := r.Message
		if msg == "" {
			msg = detail
		}
		msg = strings.NewReplacer(
			"{price}", fmt.Sprintf("%.4f", price),
			"{liqDist}", fmt.Sprintf("%.2f", liqDist),
			"{equity}", fmt.Sprintf("%.2f", equity),
			"{unrealized}", fmt.Sprintf("%.2f", unreal),
			"{rule}", r.Name,
		).Replace(msg)
		e.dispatch(r, msg+"\n"+detail)
	}
}

func (e *alertEngine) dropPct(window time.Duration) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.prices) < 2 {
		return 0
	}
	latest := e.prices[len(e.prices)-1]
	// 找 window 之前最近的一个点
	base := 0.0
	for i := len(e.prices) - 1; i >= 0; i-- {
		if latest.t.Sub(e.prices[i].t) >= window {
			base = e.prices[i].price
			break
		}
	}
	if base <= 0 {
		base = e.prices[0].price
	}
	if base <= 0 {
		return 0
	}
	return (base - latest.price) / base * 100
}

// dispatch 向所有启用的通道发送；urgent 规则会额外触发"打电话"
func (e *alertEngine) dispatch(rule *AlertRule, msg string) {
	title := "ETC 告警 · " + rule.Name
	ev := AlertEvent{Time: time.Now().UnixMilli(), Rule: rule.Name, Message: msg, Urgent: rule.Urgent}

	var results []string
	for i := range e.channels {
		ch := &e.channels[i]
		if !ch.Enabled {
			continue
		}
		// 非紧急规则不打电话，避免吼你
		if ch.Type == "twilio" && !rule.Urgent {
			continue
		}
		if ch.Type == "bark" && !rule.Urgent {
			// 非紧急的 Bark 用普通级别
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := sendToChannel(ctx, ch, title, msg, rule.Urgent)
		cancel()
		if err != nil {
			ch.lastErr = err.Error()
			results = append(results, fmt.Sprintf("%s:失败(%v)", ch.Type, err))
		} else {
			ch.sent++
			results = append(results, ch.Type+":成功")
		}
	}
	ev.Results = strings.Join(results, " ")

	e.mu.Lock()
	e.nextID++
	ev.ID = e.nextID
	e.history = append(e.history, ev)
	if len(e.history) > 50 {
		e.history = e.history[len(e.history)-50:]
	}
	e.mu.Unlock()

	log.Printf("【告警】%s | %s | %s", rule.Name, strings.ReplaceAll(msg, "\n", " "), ev.Results)
}

func sendToChannel(ctx context.Context, ch *AlertChannel, title, body string, urgent bool) error {
	hc := &http.Client{Timeout: 20 * time.Second}
	switch ch.Type {
	case "console":
		return nil

	case "bark":
		server := ch.BarkServer
		if server == "" {
			server = "https://api.day.app"
		}
		if ch.BarkKey == "" {
			return fmt.Errorf("未配置 barkKey")
		}
		// level=critical → 绕过静音、强行响铃（最接近打电话）
		q := url.Values{}
		if urgent {
			q.Set("level", "critical")
			q.Set("volume", "10")
			q.Set("sound", "alarm")
		} else {
			q.Set("level", "active")
		}
		q.Set("group", "ETC交易")
		u := fmt.Sprintf("%s/%s/%s/%s?%s", strings.TrimRight(server, "/"),
			url.PathEscape(ch.BarkKey), url.PathEscape(title), url.PathEscape(body), q.Encode())
		return doGet(ctx, hc, u)

	case "telegram":
		if ch.TGToken == "" || ch.TGChatID == "" {
			return fmt.Errorf("未配置 telegramToken / telegramChatId")
		}
		u := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", ch.TGToken)
		form := url.Values{}
		form.Set("chat_id", ch.TGChatID)
		form.Set("text", title+"\n\n"+body)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP %d %s", resp.StatusCode, string(b))
		}
		return nil

	case "twilio":
		if ch.TwilioSID == "" || ch.TwilioToken == "" || ch.TwilioFrom == "" || ch.TwilioTo == "" {
			return fmt.Errorf("未配置 twilioSid/token/from/to")
		}
		voice := ch.Voice
		if voice == "" {
			voice = "Polly.Zhiyu"
		}
		say := strings.ReplaceAll(body, "\n", "，")
		say = strings.ReplaceAll(say, "<", " ")
		twiml := fmt.Sprintf(`<Response><Say voice="%s" language="zh-CN">%s</Say><Say voice="%s" language="zh-CN">重复一遍，%s</Say></Response>`,
			voice, say, voice, say)
		u := fmt.Sprintf("https://api.twilio.com/2010-04-01/Accounts/%s/Calls.json", ch.TwilioSID)
		form := url.Values{}
		form.Set("From", ch.TwilioFrom)
		form.Set("To", ch.TwilioTo)
		form.Set("Twiml", twiml)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(form.Encode()))
		req.SetBasicAuth(ch.TwilioSID, ch.TwilioToken)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := hc.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP %d %s", resp.StatusCode, string(b))
		}
		return nil

	case "webhook":
		if ch.WebhookURL == "" {
			return fmt.Errorf("未配置 webhookUrl")
		}
		u := strings.NewReplacer(
			"{title}", url.QueryEscape(title),
			"{body}", url.QueryEscape(body),
		).Replace(ch.WebhookURL)
		return doGet(ctx, hc, u)
	}
	return fmt.Errorf("未知通道类型 %s", ch.Type)
}

func doGet(ctx context.Context, hc *http.Client, u string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// ---------------- 对外查询 ----------------

type AlertStatus struct {
	Rules    []AlertRule     `json:"rules"`
	Channels []ChannelStatus `json:"channels"`
	History  []AlertEvent    `json:"history"`
}

type ChannelStatus struct {
	Type    string `json:"type"`
	Enabled bool   `json:"enabled"`
	Sent    int    `json:"sent"`
	LastErr string `json:"lastErr"`
}

func (e *alertEngine) Status() *AlertStatus {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	st := &AlertStatus{}
	st.Rules = append(st.Rules, e.rules...)
	for _, ch := range e.channels {
		st.Channels = append(st.Channels, ChannelStatus{Type: ch.Type, Enabled: ch.Enabled, Sent: ch.sent, LastErr: ch.lastErr})
	}
	st.History = append(st.History, e.history...)
	return st
}

// Test 发送一条测试告警（给页面上的"测试"按钮用）
func (e *alertEngine) Test(urgent bool) string {
	r := &AlertRule{Name: "测试", Urgent: urgent}
	e.dispatch(r, "这是一条测试告警，收到说明通道配置正确。")
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.history) == 0 {
		return "未触发"
	}
	return e.history[len(e.history)-1].Results
}
