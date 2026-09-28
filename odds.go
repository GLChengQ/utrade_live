package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 持仓概率模型：用历史 15m K 线统计"先到止盈还是先到强平价"的经验概率。
//
// 思路：把每一根历史上的 K 线当作一个"当前状态"，向前看 7 天，记录
//   - 先摸到上涨 u% 的最早时间
//   - 先摸到下跌 d% 的最早时间
// 于是对任意 (止盈距离 u, 强平距离 d) 都能查出历史频率：
//   P(摸到 u) / P(摸到 d) / P(先到 u) / P(先到 d)
// 距离按 0.5%/0.2% 网格统计，查询时线性插值。
// ---------------------------------------------------------------------------

const (
	oddsInterval = "15m"
	oddsBarsWant = 45000 // 约 1.3 年
	oddsFwdBars  = 672   // 7 天（15m × 672 = 168 小时）
	oddsUpMin    = 0.5   // 上涨目标从 0.5% 起
	oddsUpStep   = 0.5   // 步长 0.5%
	oddsUpN      = 40    // 到 20%
	oddsDnMin    = 0.2   // 下跌止损从 0.2% 起
	oddsDnStep   = 0.2   // 步长 0.2%
	oddsDnN      = 50    // 到 10%
)

var oddsHorizons = []struct {
	Name string
	Bars int
}{{"24h", 96}, {"3d", 288}, {"7d", 672}}

type OddsModel struct {
	Symbol   string
	Bars     int
	From, To time.Time
	Total    int32
	// [h][iu][id]
	upFirst []int32
	dnFirst []int32
	// [h][iu]
	upTouch []int32
	// [h][id]
	dnTouch []int32
}

// Prob 返回 (P(摸到上涨 upPct), P(摸到下跌 dnPct), P(先到上涨), P(先到下跌))
func (m *OddsModel) Prob(upPct, dnPct float64, hIdx int) (float64, float64, float64, float64) {
	if m == nil || m.Total == 0 || hIdx < 0 || hIdx >= len(oddsHorizons) {
		return 0, 0, 0, 0
	}
	tot := float64(m.Total)

	// 上涨维度插值
	uIdx := (upPct - oddsUpMin) / oddsUpStep
	if uIdx < 0 {
		uIdx = 0
	}
	if uIdx > oddsUpN-1 {
		uIdx = oddsUpN - 1
	}
	iu0 := int(uIdx)
	iu1 := iu0 + 1
	if iu1 > oddsUpN-1 {
		iu1 = oddsUpN - 1
	}
	fu := uIdx - float64(iu0)

	// 下跌维度插值
	dIdx := (dnPct - oddsDnMin) / oddsDnStep
	if dIdx < 0 {
		dIdx = 0
	}
	if dIdx > oddsDnN-1 {
		dIdx = oddsDnN - 1
	}
	id0 := int(dIdx)
	id1 := id0 + 1
	if id1 > oddsDnN-1 {
		id1 = oddsDnN - 1
	}
	fd := dIdx - float64(id0)

	lerp := func(a, b, f float64) float64 { return a + (b-a)*f }
	base := hIdx * oddsUpN * oddsDnN
	up1 := func(iu, id int) float64 {
		return float64(m.upFirst[base+iu*oddsDnN+id]) / tot * 100
	}
	dn1 := func(iu, id int) float64 {
		return float64(m.dnFirst[base+iu*oddsDnN+id]) / tot * 100
	}
	// 双线性
	upF := lerp(lerp(up1(iu0, id0), up1(iu1, id0), fu), lerp(up1(iu0, id1), up1(iu1, id1), fu), fd)
	dnF := lerp(lerp(dn1(iu0, id0), dn1(iu1, id0), fu), lerp(dn1(iu0, id1), dn1(iu1, id1), fu), fd)

	tb := hIdx * oddsUpN
	upT := lerp(float64(m.upTouch[tb+iu0])/tot*100, float64(m.upTouch[tb+iu1])/tot*100, fu)
	db := hIdx * oddsDnN
	dnT := lerp(float64(m.dnTouch[db+id0])/tot*100, float64(m.dnTouch[db+id1])/tot*100, fd)
	return upT, dnT, upF, dnF
}

// buildOdds 拉历史 K 线并统计概率表（较慢，放后台跑）
func buildOdds(ctx context.Context, c *Client, symbol string, barsWant int) (*OddsModel, error) {
	step := int64(15 * 60 * 1000)
	now := time.Now().UnixMilli()
	var all []Kline
	for got := 0; got < barsWant; {
		n := 1500
		if barsWant-got < n {
			n = barsWant - got
		}
		end := now - int64(got)*step
		start := end - int64(n)*step
		ks, err := c.Klines(ctx, symbol, oddsInterval, start, end, n)
		if err != nil || len(ks) == 0 {
			if err != nil && len(all) == 0 {
				return nil, err
			}
			break
		}
		all = append(ks, all...)
		got += len(ks)
	}
	if len(all) < 2000 {
		return nil, fmt.Errorf("%s 历史数据不足（%d 根）", symbol, len(all))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].T < all[j].T })
	uniq := all[:0]
	var lastT int64 = -1
	for _, k := range all {
		if k.T != lastT {
			uniq = append(uniq, k)
			lastT = k.T
		}
	}
	all = uniq

	m := &OddsModel{
		Symbol:  symbol,
		Bars:    len(all),
		From:    time.UnixMilli(all[0].T),
		To:      time.UnixMilli(all[len(all)-1].T),
		upFirst: make([]int32, len(oddsHorizons)*oddsUpN*oddsDnN),
		dnFirst: make([]int32, len(oddsHorizons)*oddsUpN*oddsDnN),
		upTouch: make([]int32, len(oddsHorizons)*oddsUpN),
		dnTouch: make([]int32, len(oddsHorizons)*oddsDnN),
	}
	timeUp := make([]int32, oddsUpN)
	timeDn := make([]int32, oddsDnN)

	for i := 0; i+2 < len(all); i++ {
		c0 := all[i].C
		if c0 <= 0 {
			continue
		}
		for k := range timeUp {
			timeUp[k] = -1
		}
		for k := range timeDn {
			timeDn[k] = -1
		}
		maxUp, maxDn := 0.0, 0.0
		nextU, nextD := 0, 0
		lim := i + oddsFwdBars
		if lim >= len(all) {
			lim = len(all) - 1
		}
		for j := i + 1; j <= lim; j++ {
			if u := (all[j].H - c0) / c0 * 100; u > maxUp {
				maxUp = u
			}
			if d := (c0 - all[j].L) / c0 * 100; d > maxDn {
				maxDn = d
			}
			for nextU < oddsUpN && maxUp >= oddsUpMin+float64(nextU)*oddsUpStep {
				timeUp[nextU] = int32(j - i)
				nextU++
			}
			for nextD < oddsDnN && maxDn >= oddsDnMin+float64(nextD)*oddsDnStep {
				timeDn[nextD] = int32(j - i)
				nextD++
			}
			if nextU >= oddsUpN && nextD >= oddsDnN {
				break
			}
		}
		for h, hz := range oddsHorizons {
			ub := h * oddsUpN
			db := h * oddsDnN
			base := h * oddsUpN * oddsDnN
			for iu := 0; iu < oddsUpN; iu++ {
				tu := timeUp[iu]
				hitU := tu > 0 && int(tu) <= hz.Bars
				if hitU {
					m.upTouch[ub+iu]++
				}
				row := base + iu*oddsDnN
				for id := 0; id < oddsDnN; id++ {
					td := timeDn[id]
					hitD := td > 0 && int(td) <= hz.Bars
					switch {
					case hitU && hitD:
						if tu < td {
							m.upFirst[row+id]++
						} else {
							m.dnFirst[row+id]++
						}
					case hitU:
						m.upFirst[row+id]++
					case hitD:
						m.dnFirst[row+id]++
					}
				}
			}
			for id := 0; id < oddsDnN; id++ {
				if td := timeDn[id]; td > 0 && int(td) <= hz.Bars {
					m.dnTouch[db+id]++
				}
			}
		}
		m.Total++
	}
	return m, nil
}

// ---------------- 模型缓存 ----------------

type oddsStore struct {
	mu       sync.Mutex
	models   map[string]*OddsModel
	building map[string]bool
	failed   map[string]string
	tpLong   []float64
	tpShort  []float64
	autoPcts []float64
	horizons []string
}

func newOddsStore(tpLong, tpShort, autoPcts []float64) *oddsStore {
	s := &oddsStore{
		models:   map[string]*OddsModel{},
		building: map[string]bool{},
		failed:   map[string]string{},
		tpLong:   tpLong,
		tpShort:  tpShort,
		autoPcts: autoPcts,
	}
	for _, h := range oddsHorizons {
		s.horizons = append(s.horizons, h.Name)
	}
	return s
}

// get 返回已就绪的模型；未就绪则触发后台构建并返回 nil
func (s *oddsStore) get(ctx context.Context, c *Client, symbol string) *OddsModel {
	s.mu.Lock()
	if m := s.models[symbol]; m != nil {
		s.mu.Unlock()
		return m
	}
	if s.building[symbol] {
		s.mu.Unlock()
		return nil
	}
	if _, bad := s.failed[symbol]; bad {
		s.mu.Unlock()
		return nil
	}
	s.building[symbol] = true
	s.mu.Unlock()

	go func() {
		bg, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		t0 := time.Now()
		m, err := buildOdds(bg, c, symbol, oddsBarsWant)
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.building, symbol)
		if err != nil {
			s.failed[symbol] = err.Error()
			log.Printf("概率模型构建失败 %s: %v", symbol, err)
			return
		}
		s.models[symbol] = m
		log.Printf("概率模型就绪 %s: %d 根 %s K线（%s ~ %s），耗时 %s",
			symbol, m.Bars, oddsInterval,
			m.From.In(cstZone()).Format("2006-01-02"), m.To.In(cstZone()).Format("2006-01-02"),
			time.Since(t0).Round(time.Second))
	}()
	return nil
}

func (s *oddsStore) failReason(symbol string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed[symbol]
}

// block 为当前所有持仓分别生成概率：多单算"上方目标 vs 下方强平"，空单算"下方目标 vs 上方强平"
func (s *oddsStore) block(ctx context.Context, c *Client, positions []PositionRow) *OddsBlock {
	if len(positions) == 0 {
		return nil
	}
	blk := &OddsBlock{Horizons: s.horizons}
	for _, p := range positions {
		if p.Amount == 0 || p.MarkPrice <= 0 || p.LiqPrice <= 0 {
			continue
		}
		long := p.Amount > 0
		op := OddsPos{
			Symbol: p.Symbol, Side: p.SideText, Long: long,
			Amount: p.Amount, Notional: p.Notional, Entry: p.EntryPrice,
			Price: p.MarkPrice, Liq: p.LiqPrice,
		}
		// 对冲模式下币安只给一个"标的级"强平价（由净敞口决定），
		// 只有强平价落在该持仓的逆行方向时才可用：
		//   多单 → 强平价应在现价下方；空单 → 强平价应在现价上方
		op.LiqValid = (long && p.LiqPrice < p.MarkPrice) || (!long && p.LiqPrice > p.MarkPrice)
		if op.LiqValid {
			if long {
				op.LiqDist = (1 - p.LiqPrice/p.MarkPrice) * 100
			} else {
				op.LiqDist = (p.LiqPrice/p.MarkPrice - 1) * 100
			}
		} else {
			op.Note = "该腿方向与标的级强平价相反（对冲持仓），强平价由净敞口决定，本腿不做强平对比"
		}
		blk.Positions = append(blk.Positions, op)
	}
	if len(blk.Positions) == 0 {
		return nil
	}

	allReady := true
	for i := range blk.Positions {
		p := &blk.Positions[i]
		m := s.get(ctx, c, p.Symbol)
		if m == nil {
			allReady = false
			p.Note = s.failReason(p.Symbol)
			if p.Note == "" {
				p.Note = "概率模型构建中（拉取历史 K 线，约 10~40 秒）…"
			}
			continue
		}
		p.Ready = true
		if blk.Bars == 0 {
			blk.Bars = m.Bars
			blk.From = m.From.In(cstZone()).Format("2006-01-02")
			blk.To = m.To.In(cstZone()).Format("2006-01-02")
			blk.Samples = int(m.Total)
		}

		// 强平价触及概率：多单看下跌方向，空单看上涨方向（仅当强平价方向一致时）
		if p.LiqValid {
			for h := range oddsHorizons {
				upT, dnT, _, _ := m.Prob(p.LiqDist, p.LiqDist, h)
				if p.Long {
					p.LiqTouch = append(p.LiqTouch, round2(dnT))
				} else {
					p.LiqTouch = append(p.LiqTouch, round2(upT))
				}
			}
		}

		// 止盈档：多单只取现价上方的配置档，空单只取现价下方的配置档
		levels := s.tpLong
		if !p.Long {
			levels = s.tpShort
		}
		targets := make([]OddsTarget, 0, 10)
		for _, lv := range levels {
			if p.Long && lv <= p.Price {
				continue
			}
			if !p.Long && lv >= p.Price {
				continue
			}
			targets = append(targets, OddsTarget{Label: strconv.FormatFloat(lv, 'f', -1, 64), Level: lv})
		}
		// 配置档位在该方向都不适用时，退化成按百分比自动生成（±2/4/6/8%）
		if len(targets) == 0 {
			for _, pct := range s.autoPcts {
				lv := p.Price * (1 + pct/100)
				sign := "+"
				if !p.Long {
					lv = p.Price * (1 - pct/100)
					sign = "-"
				}
				targets = append(targets, OddsTarget{
					Label: fmt.Sprintf("%s%.0f%%", sign, pct), Level: round5(lv),
				})
			}
		}
		for ti := range targets {
			t := &targets[ti]
			if p.Long {
				t.Dist = (t.Level/p.Price - 1) * 100
			} else {
				t.Dist = (1 - t.Level/p.Price) * 100
			}
			for h := range oddsHorizons {
				if p.LiqValid {
					// 多单：上涨=目标，下跌=强平；空单：下跌=目标，上涨=强平
					var upT, dnT, upF, dnF float64
					if p.Long {
						upT, dnT, upF, dnF = m.Prob(t.Dist, p.LiqDist, h)
					} else {
						upT, dnT, upF, dnF = m.Prob(p.LiqDist, t.Dist, h)
					}
					if p.Long {
						t.Touch = append(t.Touch, round2(upT))
						t.First = append(t.First, round2(upF))
						t.LiqFirst = append(t.LiqFirst, round2(dnF))
					} else {
						t.Touch = append(t.Touch, round2(dnT))
						t.First = append(t.First, round2(dnF))
						t.LiqFirst = append(t.LiqFirst, round2(upF))
					}
				} else {
					// 无有效强平价：只算"能否摸到该档"的一维概率
					upT, dnT, _, _ := m.Prob(t.Dist, t.Dist, h)
					if p.Long {
						t.Touch = append(t.Touch, round2(upT))
					} else {
						t.Touch = append(t.Touch, round2(dnT))
					}
				}
			}
		}
		p.Targets = targets
	}
	blk.Ready = allReady
	if !allReady {
		blk.Note = "部分持仓的概率模型仍在构建中…"
	}
	return blk
}

func round5(v float64) float64 { return math.Round(v*100000) / 100000 }

func round2(v float64) float64 { return math.Round(v*10) / 10 }

func cstZone() *time.Location {
	if z, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return z
	}
	return time.FixedZone("CST", 8*3600)
}
