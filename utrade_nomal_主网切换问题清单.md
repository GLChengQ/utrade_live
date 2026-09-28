# utrade_nomal 测试网 → 主网 差异与风险清单

| 项目 | 内容 |
| --- | --- |
| 被审对象 | [GLChengQ/utrade_nomal](https://github.com/GLChengQ/utrade_nomal)（Binance USDⓈ-M 永续合约量化系统） |
| 审查分支 | `main`（raw 文件直读） |
| 审查日期 | 2026-09-25 |
| 审查方式 | 静态代码审查 + 币安官方接口文档核对（**未实际运行该程序**，未在测试网/主网下单验证） |
| 已读文件 | `README.md`、`config.py`、`.env.example`、`bot.py`、`backtest.py`、`binance_futures/client.py`、`binance_futures/utils.py`、`quant/engine.py`、`quant/risk.py`、`quant/state.py`、`quant/db.py`、`quant/backtest.py`、`dashboard/server.py`、`requirements.txt` |
| 文档用途 | 切主网前的风险登记与整改清单；每条含"测试网为何没暴露"与"建议动作" |

> **一句话结论**：端点、签名、精度这些"技术切换"你已经在 `config.py` 里做对了；真正会在主网出问题的是三类 —— ①**持仓模式与最小下单量**（会直接开不了仓）、②**熔断开关 / 5000 基线 / 重置检测**（风控和账目是假的）、③**保护单的真空窗口 + 状态失真**（程序以为有止损，交易所上其实没有）。

---

## 0. 严重度总览

| 编号 | 严重度 | 问题 | 文件位置 | 主网后果 |
| --- | --- | --- | --- | --- |
| R-01 | **P0 阻断** | 下单从不传 `positionSide`，双向持仓(Hedge)模式下全部被拒 | `quant/engine.py` `_open` / `_place_protective` | 开不了仓、保护单挂不上（-4061） |
| R-02 | **P0 阻断** | 保护单"先撤后挂"，且挂单失败只 log 不重试，状态却已更新 | `quant/engine.py` `_place_protective` / `_update_trailing_stop` | 真实持仓可能长时间**裸奔无止损** |
| R-03 | **P0 风控失效** | 熔断器默认**关闭**，仅在 `state/breaker_on.flag` 存在时生效 | `quant/engine.py` `run_cycle` | 15% 回撤 / 3% 日亏熔断**完全不生效** |
| R-04 | **P1 拒单** | 未校验 `MIN_NOTIONAL`（`utils.min_notional` 是死代码） | `quant/engine.py` `_open` / `binance_futures/utils.py` | 名义价值不足被拒（-4164），小资金最易命中 |
| R-05 | **P1 拒单** | 未校验 `MARKET_LOT_SIZE`，市价单只按 `LOT_SIZE` 取整 | `quant/engine.py` `_open` | 部分币种市价单被拒 |
| R-06 | **P1 账目失真** | 净权益曲线基线**硬编码 5000**（测试网初始额度） | `quant/db.py` `rebuild_daily_summary` / `fetch_net_equity` | 收益率全算错，整体偏移 5000 |
| R-07 | **P1 账目失真** | 20% 钱包跌幅被判定为"账户重置"，并把差额**加回**净权益 | `quant/engine.py` `run_cycle` + `quant/db.py` | 真实亏损被打成"重置影响"，日汇总/日历失真 |
| R-08 | **P1 状态污染** | `state/trading_state.json` 沿用测试网数据 | `quant/state.py` | 熔断基线错、幽灵仓位被写入成交历史 |
| R-09 | **P1 盈亏口径** | 落库 `pnl` 只取 `REALIZED_PNL`，不含手续费与资金费 | `quant/engine.py` `_realized_pnl_since` | 面板盈亏**系统性偏好** |
| R-10 | **P1 实盘/回测不一致** | 实盘用"未收盘 K 线"判断突破（重绘），回测用已收盘 K 线 | `quant/engine.py` `_klines` vs `quant/backtest.py` | 实盘≠回测，假突破上真金白银入场 |
| R-11 | **P1 参数不一致** | 实盘与回测默认参数不同（`entry_period` 15 vs 20、`min_atr_pct` 0.001 vs 0.002） | `config.py` vs `backtest.py` | 回测结论不能代表实盘参数 |
| R-12 | **P1 数据源** | 回测与实盘共用同一 client → 若 `TESTNET=true` 则**用测试网 K 线调参** | `backtest.py` | 参数被稀疏/插针数据喂出来 |
| R-13 | **P1 成本遗漏** | 回测手续费 0.04%（主网 VIP0 taker 0.05%）、**未建模资金费与滑点** | `quant/backtest.py` | 实盘收益低于回测 |
| R-14 | **P2 运维** | 买入量上限、保证金模式未设置（`change_margin_type` 从未调用） | `quant/engine.py` | 与风控公式假设不符（逐仓/全仓混用） |
| R-15 | **P2 运维** | 面板 `0.0.0.0:8080` 无鉴权，且带真实平仓/熔断开关接口 | `dashboard/server.py` | 主网账户可被任意网络访问者平仓 |
| R-16 | **P2 运维** | 环境变量名 `BINANCE_TESTNET_API_KEY/_SECRET` 主网也照读 | `config.py` | 误用测试网 key（-2015）或误把主网 key 留在测试网配置 |
| R-17 | **P2 运维** | 无 429/418 退避、无 server-time 偏移补偿 | `binance_futures/client.py` | -1021 签名失败、限流升级为封禁 |
| R-18 | **P2 信息** | README 说"按成交量自动选前 40"，实际是硬编码列表，`liquid_symbols()` 未调用 | `bot.py` / `config.py` | 测试网缺失币种被静默跳过，主网"突然开始交易" |
| R-19 | **P2 信息** | `requirements.txt` 无 websocket 库 → listenKey/用户数据流是死代码 | `requirements.txt` / `client.py` | 只能 60s REST 轮询，无成交流事件 |
| R-20 | **P2 信息** | 多资产模式(Multi-Assets)未区分 | `quant/engine.py` `_equity` | 保证金/可用余额口径不同 |

---

## 1. 已经做对的部分（切主网无需改动）

| 项 | 说明 |
| --- | --- |
| 端点切换 | `config.py`：`TESTNET=true` → `https://testnet.binancefuture.com` + `wss://stream.binancefuture.com`；`false` → `https://fapi.binance.com` + `wss://fstream.binance.com`。REST 与 WS 都切了，没有漏。 |
| 精度自适应 | `binance_futures/utils.py` 的 tick/step 从**目标环境**的 `exchangeInfo` 动态获取，主网精度不同也能自适应。 |
| 签名方式 | HMAC-SHA256 + `timestamp` + `recvWindow=5000`，与主网要求一致；`X-MBX-APIKEY` 仅在签名请求带。 |
| 保护单接口 | `POST /fapi/v1/algoOrder`（`algoType=CONDITIONAL`、`triggerPrice`、`workingType`）是**主网正式文档接口**，非测试网专有；`closePosition=true` 时未传 `quantity`/`reduceOnly`，符合文档约束。 |
| 平仓方式 | `close_position_market` 用 `reduceOnly + 全量 quantity`（而非 `closePosition`+MARKET），符合币安限制（-4136）。 |
| 熔断状态持久化 | `quant/state.py` 把 peak/daily 基准落盘，重启不丢（前提是开启熔断，见 R-03）。 |

> **注**：`client.py` 注释称"STOP_MARKET / TAKE_PROFIT_MARKET 已从 `/fapi/v1/order` 移走"并不准确 —— Algo Order 是 2025-06 新增的**独立入口**，不是唯一入口。该注释不影响主网可用性，但建议修正以免误导。

---

## 2. P0 阻断级问题

### R-01 双向持仓模式下无法下单（最高优先级）

**现象**：`quant/engine.py` 中所有下单都不带 `positionSide`：

```python
# _open()
self.client.place_order(symbol, self._order_side(side), "MARKET", quantity=qty_str)
# _place_protective()
self.client.place_algo_order(symbol, close_side, "STOP_MARKET", sl_str,
                            close_position=True, working_type=self.cfg.working_type)
```

**依据**：币安 [New Algo Order 文档](https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order) 明确：`positionSide` —— *"Default `BOTH` for One-way Mode; `LONG` or `SHORT` for Hedge Mode. **It must be sent in Hedge Mode.**"*

**测试网为何没暴露**：新开的测试网账户默认**单向持仓(One-way)**，`positionSide` 缺省即 `BOTH`，所以一切正常。

**主网后果**：主网账户只要开过双向持仓（手动交易者很常见），**所有开仓、止损、止盈都会被拒**（-4061 *Order's position side does not match user's setting*）。表现是"策略有信号但永远开不了仓，日志里一片 error"。

**建议动作**：
1. 启动自检：`GET /fapi/v1/positionSide/dual`，把结果打进日志与面板。
2. 若为 Hedge，二选一：调用 `set_position_mode(false)`（注意：**有持仓/挂单时无法切换**）或让引擎显式传 `positionSide`（此时 `closePosition=true` 还另有"LONG 不能 BUY / SHORT 不能 SELL"的约束）。
3. 顺带在自检里校验 `multiAssetsMargin`、每币 `marginType`。

### R-02 保护单"真空窗口" + 状态失真（真会没钱）

**现象**：`_place_protective` 的流程是"**先 `cancel_all_algo_orders`，再挂新单**"：

```python
try:
    self.client.cancel_all_algo_orders(symbol)      # ① 先撤掉全部保护单
except Exception as exc:
    log.warning(...)
try:
    self.client.place_algo_order(..., "STOP_MARKET", sl_str, close_position=True, ...)  # ② 再挂
except Exception as exc:
    log.error("%s STOP-LOSS placement failed: %s", symbol, exc)   # 只记录，不重试、不告警、不中断
```

叠加 `_update_trailing_stop` 的写入顺序：

```python
pos_state["sl"] = new_sl                      # ① 先把本地状态改成新止损
if not self.dry_run:
    self._place_protective(...)                # ② 再尝试挂单（可能失败）
self.state.save()
```

**后果链**：
1. 每次移动止损（以及每次加仓）都会有一段**交易所上没有止损**的真空窗口；
2. 若第 ② 步失败，`pos_state["sl"]` 已是新值 → **程序认为有保护，实际没有**；
3. `improved` 判断以刚写入的 `cur_sl` 为基准 → 必须等价格再创新高/新低才会再挂一次，可能等很久；
4. 重启后只对账"仓位是否存在"（`_position(symbol) is None`），**从不校验保护单是否存在**；`repair_tp.py` / `_protect.py` 是手动脚本。

**主网后果**：一次瞬时失败 = 一个长时间无止损的真实仓位。测试网无所谓，主网是裸奔。

**建议动作**：
1. 改为"**先挂新单，成功后再撤旧单**"（或直接用 `/fapi/v1/order` 的 modify/单腿替换）；
2. 挂单失败 → 立即重试（指数退避）+ 面板告警 + **禁止该币加仓**；
3. 每轮对账：拉 `GET /fapi/v1/openAlgoOrders`，凡有持仓无止损者立刻补挂并记事件；
4. 仅在**确认新止损已生效**后再更新 `pos_state["sl"]`。

### R-03 熔断器默认关闭

**现象**：`quant/engine.py` `run_cycle`：

```python
self.breaker_enabled = (self.state.path.parent / "breaker_on.flag").exists()
if not self.breaker_enabled:
    halt_reason = None            # ← 熔断结果被直接丢弃
```

README 亦承认"测试环境默认关闭"。

**主网后果**：不手动创建 `state/breaker_on.flag`，则 `BOT_MAX_DRAWDOWN_PCT=0.15` 与 `BOT_DAILY_LOSS_LIMIT_PCT=0.03` **完全不生效**；面板"熔断开关"默认也是关的；`resume.flag` 形同虚设。

**建议动作**：主网默认**开启**（反过来：用 flag 关闭而非开启），并在启动日志与面板显式展示"熔断：ON/OFF"。

---

## 3. 账目与风控口径问题（会误导你）

### R-04 `MIN_NOTIONAL` 未校验

`binance_futures/utils.py` 定义了 `min_notional` 属性，但**全局无任何调用点**；`quant/engine.py::_open` 只用到 `filters.step_size` 与 `filters.min_qty`。

**主网后果**：主网要求订单名义价值 ≥ `MIN_NOTIONAL.notional`（USDⓈ-M 常见为 5 USDT，**以目标环境 `exchangeInfo` 为准**），不足直接拒单 `-4164`。小权益 + 宽 ATR 止损（`notional = 风险额 / 止损距离`）最容易命中。

**建议**：`position_size` 返回前增加 `qty * entry >= min_notional` 校验；不满足则跳过并记录原因（而不是发单被拒）。

### R-05 `MARKET_LOT_SIZE` 未校验

市价单在主网需满足 `MARKET_LOT_SIZE`（其 step/min/max 与 `LOT_SIZE` 未必相同）。当前只按 `LOT_SIZE.stepSize` 取整。

**建议**：市价单用 `MARKET_LOT_SIZE` 过滤；限价单用 `LOT_SIZE`。

### R-06 净权益基线硬编码 5000

`quant/db.py`：

```python
net = 5000.0        # rebuild_daily_summary()
net = 5000.0        # fetch_net_equity()
```

这是测试网初始额度。**主网入金多少就是多少**，于是"净权益曲线"整体偏移 5000，`daily_summary.net_equity`、面板"净权益"与任何收益率计算全部错位。

**建议**：从 `state/trading_state.json` 的"主网首日权益"或环境变量 `BOT_INITIAL_EQUITY` 读取基线，并把 5000 作为回退默认值。

### R-07 "账户重置"检测在主网=把亏损洗掉

`quant/engine.py` `run_cycle`：

```python
if last_wallet and wallet < last_wallet * (1 - _cfg.RESET_DETECT_PCT / 100.0):
    self.store.record_reset(..., note=f"wallet dropped {drop_pct:.1f}% (likely testnet account reset)")
```

`quant/db.py`：

```python
net += realized + impact      # impact = new_wallet - prev_wallet（负数亏损被"加回"）
```

**主网后果**：主网没有账户重置，20% 的钱包跌幅是**真实亏损**；但它会被记成 `resets` 事件，`reset_impact` 又把亏损加回净权益 → **净权益曲线与实际权益曲线同时存在，但"净权益"那根被人为抬高**，日历视图的日盈亏也被污染。

**建议**：主网关闭重置检测（`RESET_DETECT_PCT=0` 或用 `TESTNET` 门控），历史 `resets` 表清空/归档。

### R-08 状态文件沿用测试网数据

`state/trading_state.json` 含 `peak_equity`、`daily_start_equity`、`day_open_equity`、`positions`、`last_wallet`。

**主网后果**：
1. 熔断峰值基线是测试网数字（可能是几十万），真实 15% 回撤永远触发不了（或反之立刻触发）；
2. `positions` 里的测试网仓位在主网不存在 → 对账循环 `self._position(symbol) is None` → `_record_close(symbol, "sl/tp")` 把**幽灵仓位**写成成交记录（还会落 MySQL）。

**建议**：切主网时**删除 `state/`**（或写一个 `--reset-state` 开关），并清空/重建 MySQL 库。

### R-09 落库盈亏不含手续费与资金费

`quant/engine.py::_realized_pnl_since` 只取 `incomeType="REALIZED_PNL"`。币安 `income` 流水中 `COMMISSION`（手续费）与 `FUNDING_FEE`（资金费）是**独立的 incomeType**。

**主网后果**：`trades.pnl`、`daily_summary.realized_pnl`、面板成交明细与日历**系统性偏好**（少了手续费+资金费）。测试网这两项不重要，主网很实在。

**建议**：一并汇总 `REALIZED_PNL + COMMISSION + FUNDING_FEE`，或分列展示。

### R-10 实盘用"未收盘 K 线"，回测用已收盘 K 线

`quant/engine.py::_klines` 取 `client.klines(...)` 的最后一根即**当前正在形成的 K 线**，随后用 `closes[-1]` 判突破、用 `highs[-1]/lows[-1]` 算 ATR；而 `quant/backtest.py` 是逐根**已收盘** bar 推进。

**主网后果**：盘中出现的"突破"可能在收盘前消失（**信号重绘**），实盘会在假突破上真实成交；回测胜率/盈亏比无法代表实盘。测试网因成交无成本，这个偏差看不出来。

**建议**：信号只认已收盘 bar（用 `closes[-2]` 或丢弃最后一根），或明确改为"收盘后下一根开盘成交"。

### R-11 实盘与回测默认参数不一致

| 参数 | 实盘 `config.py` | 回测 `backtest.py` 默认 | README 描述 |
| --- | --- | --- | --- |
| `entry_period` | 15 | `--entry-period 20` | 20 |
| `min_atr_pct` | 0.001 (0.1%) | `--min-atr-pct 0.002` (0.2%) | 0.1% |
| 初始权益 | 账户实际 | `--equity 10000` | — |

**建议**：让回测默认值从 `BotConfig.from_env()` 取值，做到"回测=实盘参数"；README 同步更正。

### R-12 回测数据源跟随 `TESTNET`

`backtest.py` 用 `config.REST_BASE_URL` 建 client → `BINANCE_TESTNET=true` 时，回测拉的是**测试网 K 线**。测试网数据稀疏、缺口、插针明显。

**建议**：回测支持显式 `--mainnet-data`（或独立用 `data-api`/主网拉取历史 K 线），并记录数据来源与区间到回测输出里。

---

## 4. 主网真实成本与成交差异（回测/测试网都不体现）

### R-13 手续费、资金费、滑点

`quant/backtest.py` 模块 docstring 与参数：

```python
fee_rate: float = 0.0004,   # 0.04%
# "Taker fees are charged on entry and exit. Funding is not modeled."
```

| 成本 | 回测/测试网 | 主网实际 |
| --- | --- | --- |
| 手续费 | 0.04% 单边 | USDⓈ-M VIP0 taker **0.05%**（maker 0.02%，BNB 抵扣约 0.045%/0.018%）——**以账户 VIP 等级为准** |
| 资金费 | 未建模 | 每 8 小时结算（部分币种 4 小时），趋势策略持仓数小时~数天，累积可观 |
| 滑点 | 未建模（按信号 bar 收盘价成交） | 市价单有真实滑点与点差，`1000BONK`/`ORDI`/`WIF` 之类更明显 |
| 成交 | 测试网基本"按最新价瞬间全额成交" | 真实盘口深度、部分成交、极端行情拒单（`PERCENT_PRICE`/STP） |

**建议**：回测手续费默认提到 0.0005，增加"资金费按持仓时长估算"与"滑点(按 ATR 或点差比例)"两个参数；输出中单列成本合计。

### 其它主网专属行为

- **资金规模导致行为不同**：`max_position_pct=0.5 × max_leverage=5` 给出单仓名义上限 2.5×权益，但实际由 `1.5% 风险 ÷ 止损距离` 决定；小资金主网上并发 10 仓开不出来，会频繁 `insufficient margin; skip`（测试网余额充足永不触发）。
- **杠杆档位**：`change_leverage(5)` 在主网受 `leverageBracket` 限制（个别币种最大杠杆低于 5 时失败，代码仅 `warning` 后继续）。
- **保证金模式**：`change_margin_type` 已实现但**从未调用**（R-14）；主网该 symbol 可能是逐仓，而风控按 5x 全仓估算保证金。
- **维护窗口/结算时点**：主网有定期维护与资金费结算时的临时限流；测试网几乎遇不到。

---

## 5. 运维与安全

### R-15 面板无鉴权 + 真实操作接口

`dashboard/server.py`：`ThreadingHTTPServer(("0.0.0.0", port), Handler)`，且提供：

| 接口 | 作用 | 主网风险 |
| --- | --- | --- |
| `GET /api/summary`、`/api/history`、`/api/calendar`、`/api/curves` | 资产、持仓、成交、权益曲线 | 账户资金信息泄露 |
| `POST /api/close?symbol=` | **真实市价平仓** | 任意网络访问者可平你的仓 |
| `POST /api/resume` | 解除熔断（重设基线） | 绕过风控 |
| `POST /api/toggle_breaker` | **关闭熔断** | 直接关掉保护 |

README 已提示"只走 Tailscale"，但**代码层没有任何约束**。

**建议**：加 token 鉴权（Header/Bearer）、默认只绑 `127.0.0.1` 或 Tailscale 网段、破坏性接口二次确认。

### R-16 密钥变量名陷阱

`config.py`：`API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "")`，**切主网后读的还是这两个名字**，没有 `BINANCE_MAINNET_*`。

**风险**：`BINANCE_TESTNET=false` 但忘了换 key → 主网用测试网 key → `-2015`；更危险的是把**主网 key 填进测试网配置**长期留存。

**建议**：改为 `BINANCE_API_KEY/SECRET`（按环境各自存放），并在启动时校验 key 与目标环境是否匹配（例如打印 `GET /fapi/v1/time` + 自检失败即拒绝启动）。

### R-17 无限流退避与时间同步

`binance_futures/client.py::_parse` 对 429/418/-1003 只是抛 `BinanceAPIError`，没有 `Retry-After` 退避，也没有记录 `X-MBX-USED-WEIGHT-1M`；`timestamp` 直接取本机时间，无 server-time 偏移补偿。

**主网后果**：限流后仍按 60s 周期继续请求，可能升级为 `418 IP banned`；本机时钟漂移 > `recvWindow(5000ms)` 时全部签名失败（-1021）。

**建议**：解析 `Retry-After` 与 `banned until`、按 host 冷却退避、启动时用 `/fapi/v1/time` 计算偏移量。

### R-18 文档与实现不符（watchlist）

README 称"默认自动选取前 40 个流动性主流合约"，但 `bot.py` 与 `dashboard/server.py` 都用 `list(config.DEFAULT_SYMBOLS[:cfg.max_symbols])`，`client.liquid_symbols()` **无调用点**。

**主网后果**：测试网缺失的币种会因 `len(closes) < warmup` 被静默 `continue`；切主网后这些币**突然开始交易**，实际并发持仓与保证金占用比测试网大。

**建议**：明确二选一并同步文档；若启用 `liquid_symbols()`，注意它无 symbol 拉全量 `ticker/24hr`（主网权重 40）。

### R-19 用户数据流是死代码

`requirements.txt` 只有 `requests` / `python-dotenv` / `pymysql`，无 websocket 客户端；`client.py` 的 `start_user_data_stream()` 等未被调用。

**主网后果**：只能 60s REST 轮询，无法及时感知"止损已被交易所打掉"，成交/权益靠下一轮对账补记。

**建议**：接入 listenKey + `wss://fstream.binance.com` 用户数据流（有 websocket-client 或 websockets），或明确接受 60s 延迟。

### R-20 多资产模式未区分

`quant/engine.py::_equity` 直接取 `totalMarginBalance` / `availableBalance`。主网若开启 Multi-Assets Mode，保证金与可用余额口径与单一资产模式不同。

**建议**：自检 `/fapi/v1/multiAssetsMargin`，不匹配时警告。

---

## 6. 切主网 Checklist

- [ ] **R-01** 启动自检持仓模式；单向(Hedge→One-way)或显式传 `positionSide`
- [ ] **R-03** 创建 `state/breaker_on.flag`（或改为默认开启熔断）
- [ ] **R-08** 清理 `state/trading_state.json`（peak/daily/positions/last_wallet 全部重置）
- [ ] **R-07** 关闭账户重置检测；清空 `resets` 表
- [ ] **R-06** 把 `5000` 基线改成主网真实入金（`BOT_INITIAL_EQUITY`）
- [ ] **R-02** 保护单改"先挂后撤" + 失败重试告警 + 每轮 `openAlgoOrders` 对账
- [ ] **R-04/R-05** 补 `MIN_NOTIONAL` 与 `MARKET_LOT_SIZE` 校验
- [ ] **R-09** 盈亏口径补 `COMMISSION` + `FUNDING_FEE`
- [ ] **R-10** 信号改用已收盘 K 线
- [ ] **R-11/R-12/R-13** 回测换主网数据、参数与实盘对齐、手续费 0.0005 + 资金费 + 滑点
- [ ] **R-14** 逐币校验/设置 `marginType` 与 `leverageBracket`
- [ ] **R-15** 面板加鉴权、只绑内网、破坏性接口确认
- [ ] **R-16** 另建主网 Key（勾选合约交易、IP 白名单、**禁提现**）
- [ ] **R-17** 加 429/418 退避与 server-time 偏移补偿
- [ ] 全流程 `--dry-run` 跑 ≥3 天 → 单币种最小仓位 → 逐步放开
- [ ] 主网首日人工盯盘：确认"开仓→挂保护单→移动止损"三步在交易所端可见

---

## 7. 附录 A：核对来源与验证状态

| 结论 | 来源 | 验证状态 |
| --- | --- | --- |
| `/fapi/v1/algoOrder` + `algoType=CONDITIONAL` + `triggerPrice` 为主网正式接口 | [Binance New Algo Order 文档](https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order) | ✅ 已核对文档（含 `closePosition` 与 `quantity`/`reduceOnly` 互斥、双仓模式下 `positionSide` 必传） |
| Hedge Mode 下必须传 `positionSide` | 同上文档 `positionSide` 字段说明 | ✅ 已核对 |
| `/fapi/v2/positionRisk` 主网仍可用（返回 `leverage`/`marginType`） | 本机实测（同 IP 直连 `fapi.binance.com`） | ✅ 实测可用（但长期以 v3 为准） |
| 手续费 VIP0 taker 0.05% / maker 0.02% | 币安公开费率表 | ⚠️ 未在本次核对，请按账户 VIP 等级确认 |
| `MIN_NOTIONAL` 常见 5 USDT | 币安 exchangeInfo 过滤器 | ⚠️ 以目标环境 `exchangeInfo` 实际值为准 |
| 测试网会定期清零账户 | 该仓库 README 自述 | ⚠️ 采信仓库自述，未独立验证 |
| 测试网资金费/成交是否与主网一致 | — | ❌ 本次未核实，故正文中按"回测未建模资金费"陈述，未断言测试网资金费行为 |

> 本清单为**静态审查**结论：未运行该程序、未在其测试网账户下单，因此 R-01/R-02/R-06/R-07 等"行为类"结论建议在测试网上用最小仓位复现确认后再切主网。

## 8. 附录 B：建议先跑的自检

```bash
# 1) 逐接口连通性与账户口径（该仓库自带）
python main.py status

# 2) 关键前置条件（建议写成 check_env.py，切主网前必跑）
#    - GET /fapi/v1/positionSide/dual      -> dualSidePosition 是否 false
#    - GET /fapi/v1/multiAssetsMargin      -> multiAssetsMargin 是否 false
#    - GET /fapi/v1/exchangeInfo           -> 每个 watchlist 币的
#         MIN_NOTIONAL.notional / LOT_SIZE / MARKET_LOT_SIZE / PRICE_FILTER
#    - GET /fapi/v1/leverageBracket        -> 每个币是否允许 max_leverage=5
#    - GET /fapi/v1/positionRisk?symbol=X  -> marginType 是否 cross
#    - GET /fapi/v1/time 与本机时间差       -> 是否 < 1000ms
#    - GET /fapi/v1/openAlgoOrders         -> 当前真实存在的保护单（对账基线）
```

## 9. 附录 C：本次未覆盖的部分

- `quant/strategy.py`（各策略 `levels()` / 加仓细节）未逐行审阅，仅依据 `engine.py` 调用方式与 README 描述。
- `dashboard/index.html`（22KB）未审阅，前端交互与鉴权改造点未评估。
- `quant/session.py` 仅按 README 描述（09:00~01:00 UTC+8）理解，未逐行核对跨午夜/时区边界。
- `main.py`、`optimize.py`、`repair_tp.py`、`_protect.py`、`_vgas_check.py` 未逐行审阅。
- 资金费/滑点在主网对**本策略**的实际影响幅度，需用主网数据回测估量，本清单只指出"未建模"这一事实。

---

*文档路径：`D:\Demo\utrade_nomal_主网切换问题清单.md` · 生成于 2026-09-25*
