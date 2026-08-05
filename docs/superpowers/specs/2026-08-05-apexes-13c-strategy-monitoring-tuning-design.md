# Apexes 子项目 #13c：策略监控与实时调参面板设计
### （含前置任务：行 8.3 消息总线最小可用版本 + 会话多进程架构重构）

> 本文档覆盖 `docs/FEATURES.md` 行 `9.3`（策略监控与实时调参面板），建立在
> 前置重构 `2026-08-06-apexes-trading-loop-unification-design.md`（已完成：
> `run_backtest`/`run_paper_trading`/`run_live_trading` 现在共用同一个
> `apexes_backtest::unified_loop::run_unified_event_loop` 核心）之上。
>
> 设计过程中确认：13c 依赖的"UI 进程托管交易会话"这个初始方案不可取
> （详见 §一.3），改为多进程架构；这又直接需要行 `8.3`（消息总线，
> "genuine inter-process/multi-machine support"，此前在子项目 `#12`
> 明确标注为独立、推迟的子项目）的一个最小可用版本作为前置基础设施。
> 三者耦合紧密，合并为一份设计文档，而不是拆成互相引用的多份——拆开
> 反而难以理解整体架构为何长这样。

---

## 一、范围与背景

### 1.1 目标

在一个正在运行的 backtest/paper/live 会话上：
1. 实时监控（仓位/余额/权益/已实现盈亏/保证金状态）。
2. 实时调参（策略参数热更新，下一次读取立即生效，三种运行模式行为
   完全一致）。
3. 暂停/恢复/终止会话。
4. 一个简单的会话启动器（选运行模式、选策略、选交易对、填凭据）。

### 1.2 前置确认（与用户逐项对齐的决策，本节汇总，细节见对应小节）

- 调参是**真正的进程内热更新**，不是"改参数后重启新会话"。
- 三种运行模式（backtest/paper/live）的调参行为完全一致，包括回测。
- 调参对象覆盖 **Drift Script 策略 + Rust 原生策略**两者，需要在
  `Strategy` trait 本身新增可选的读/写参数接口。
- 快照内容 = 仓位/余额/权益/已实现盈亏 + 保证金状态（`margin_status`，
  `StrategyContext` 里已有现成数据）。
- 需要暂停/恢复：暂停期间策略仍消费行情事件（盘口/仓位/PnL 照常更新），
  但 `on_market_event` 不再被调用（不产生新 intent）。
- 快照推送频率：统一循环内部节流，可配置最小推送间隔，默认 `500ms`。
- 会话启动器覆盖全部 `{backtest, paper, live} × {Drift Script 文件,
  Rust 原生策略}` 六种组合。
- 凭据来源：启动器从环境变量/本地配置文件读取，传给
  `BinanceGatewayConfig` 等构造器（不改网关 crate 自己"从不读环境变量"
  的既有约定）。

### 1.3 为什么放弃"UI 进程托管会话"，改为多进程

设计过程中最初提议让 `apexes-ui` 进程直接 `tokio::spawn` 一个
`run_backtest`/`run_paper_trading`/`run_live_trading` 任务，在同一进程内
把控制通道直接传递给 `egui` 渲染循环。这个方案更容易实现，但用户否决
了它，理由（也是本设计采纳的理由）：

1. **崩溃隔离**：一个持有真实资金的 live 会话必须能在 UI 崩溃（`egui`
   panic、`apexes-chart` 的 `wgpu` GPU 驱动异常）后继续存活，反之亦然。
   同进程耦合两者的生命周期是真实风险，不是假设性的。
2. **调度隔离**：交易引擎需要绑定 CPU 核心、免受 `egui` 帧驱动重绘和
   GPU 工作对同一进程线程的争抢。
3. **独立生命周期**：live 会话应该能无人值守长期运行，UI 重启/升级不
   应该需要连带关闭一个正在跑的 live 会话。
4. **既有设计已经假设了这一点**：`apexes-oms::OrderManager`/
   `apexes-ems::PostgresEms` 的"崩溃可恢复、靠 `reconcile()` 对账"设计
   本来就假设交易引擎的生命周期独立于监控它的任何东西；把它塞进 UI
   进程会破坏这个假设。

结论：**每个会话是独立 OS 进程**（新二进制 crate `apexes-engine`，可设
CPU 亲和性），`apexes-ui` 变成纯客户端（WebSocket 连接到一个或多个引擎
进程 + 一个共享的行情网关进程），不再在自己进程内持有任何交易逻辑。

### 1.4 不包含（明确超出范围）

- 消息总线的完整生产级形态（NATS JetStream、真正的持久化 WAL 格式、
  跨机器部署）——本设计只做最小可用版本，见 §二.2。
- 多用户/多租户权限控制——单机单用户场景，不做认证授权。
- 引擎进程自动重启之外的高可用（多活、Raft）——`docs/FEATURES.md` 行
  `14`（系统监控与运维）的范围，本设计不涉及。
- Drift Script 参数以外的策略代码热重载（脚本逻辑本身改了必须重新
  `launch`，只有 `param()` 声明的数值参数支持热更新）。
- 行 `9.4`（订单/持仓/风控管理 UI）、行 `9.5`（回测报告可视化）、行
  `9.6`（多屏监控大屏）——`#13` 后续子阶段，不在本设计范围。

---

## 二、整体架构

完整通信模型图（两版都保留，供对照）：
- **v1（已废弃，同进程方案）**：
  `docs/superpowers/specs/assets/2026-08-06-apexes-13c-communication-model-v1-inprocess-superseded.svg`
  ——`apexes-ui` 直接 `tokio::spawn` 会话任务的最初方案，因 §1.3 所述
  理由被否决，仅作历史记录保留。
- **v2（当前，多进程方案）**：
  `docs/superpowers/specs/assets/2026-08-05-apexes-13c-communication-model-v2-multiprocess.svg`
  ——本文档描述的实际架构，三个独立 OS 进程边界均已在图中显式画出。

### 2.1 四类进程

```
┌─────────────────┐   WS    ┌──────────────────────────┐
│   apexes-ui      │────────▶│ apexes-market-data-gateway│◀── 真实上游
│  (纯客户端)       │         │  (常驻，共享行情源)        │    (Binance)
└────────┬─────────┘         └──────────┬───────────────┘
         │ spawn (Command)               │ 持久化(异步)
         │ + WS (session control)        ▼
         │                    ┌──────────────────┐
         ▼                    │  apexes-storage   │
┌─────────────────┐   WS      │  (ClickHouse,已有) │
│  apexes-engine   │──────────▶└──────────────────┘
│  (每会话一个进程) │  订阅行情
│  可设 CPU 亲和性  │
└─────────────────┘
```

- **`apexes-market-data-gateway`**（新二进制 crate，本设计的前置任务）：
  常驻进程，持有真实的 `MarketDataProvider`（今天只有
  `BinanceMarketDataProvider`），通过 WebSocket 把 `MarketEvent` 广播
  给所有连接的客户端（引擎进程 + `apexes-ui` 自己的 `/ws` 行情看板都
  连它，而不是各自独立订阅上游）。收到的事件异步批量写入
  `apexes-storage::StorageWriter`（复用行 `8.1` 已完成的 ClickHouse
  持久化，不发明新格式）。新客户端连接时可选传一个"从某个历史时间点
  开始"参数，网关先用 `apexes-storage::StorageReader` 把这段历史回放
  给它，再无缝切到实时转发——这就是行 `8.3`"消息总线支持重放"要求
  的最小实现，直接复用已有存储层，不新增 WAL 格式。
- **`apexes-engine`**（新二进制 crate）：一个会话 = 一个进程。启动参数
  决定运行模式（backtest/paper/live）与具体调用哪个 `run_*`
  （`apexes-backtest::run_backtest`/`apexes-paper::run_paper_trading`/
  `apexes-live-trading::run_live_trading`，均已就绪，无需改动其公开
  签名）。自带一个 `axum` WebSocket 服务（`/session/ws`），把
  §四 的控制/快照协议暴露出来。可选设置 CPU 亲和性（`core_affinity`
  crate，新工作区依赖，见 §三.3）。
- **`apexes-ui`**（既有 crate，13a 已交付，本设计扩展其职责）：不再
  持有任何交易逻辑。新增：(a) 负责 `spawn` 网关进程（若尚未运行，见
  §三.4）与引擎进程（每次 `launch` 一个）；(b) 新增"会话启动器"标签页
  （选模式/策略/交易对/凭据引用）；(c) 新增"监控/调参"标签页，连接到
  已启动引擎进程的 `/session/ws`。
- **`apexes-storage`**（既有 crate，无需改动）：网关进程的持久化后端。

### 2.2 行 8.3 最小可用版本的精确边界

**做什么**：进程间实时转发（网关 → 多个客户端，WebSocket，
`tokio::sync::broadcast` 语义的跨进程版本）+ 复用已有 ClickHouse 存储
实现"新订阅者可选从历史时间点回放"。

**不做什么**：不引入 NATS JetStream 或任何外部消息中间件；不发明新的
WAL/持久化格式；不做真正的"发布-订阅多主题"通用消息总线（只有
`MarketEvent` 这一种载荷类型，走一个固定的 `/market` WS 端点）；不做
跨机器部署验证（本设计仍只在本机 `127.0.0.1` 验证，为跨机器预留 WS
本身的天然跨网络能力，但不专门测试）。

行 `8.3` 未来若需要升级到 NATS JetStream 等生产级方案，本设计交付的
`apexes-market-data-gateway`（尤其是它对外暴露的 WS 协议）可以作为
过渡期的实现，或被替换为 NATS 客户端而不改变下游（引擎/`apexes-ui`）
的订阅接口——但这个演进路径本身不是本设计要构建的，只是设计时的一个
兼容性考量。

---

## 三、`apexes-market-data-gateway`（新二进制 crate）

### 3.1 依赖与结构

```
crates/apexes-market-data-gateway/
  Cargo.toml
  src/
    main.rs        -- CLI 参数解析、CPU 亲和性设置、启动 axum 服务
    gateway.rs      -- 持有 MarketDataProvider，管理订阅者列表
    replay.rs       -- 新连接时的历史回放逻辑（读 apexes-storage）
    protocol.rs      -- WS 消息信封类型（见 §六，与 apexes-engine 共享，
                         提到公共 crate 或直接在这里定义再被 engine 复用）
```

依赖：`apexes-core`、`apexes-market-data`（`BinanceMarketDataProvider`）、
`apexes-storage`（持久化+回放）、`axum`、`tokio-tungstenite`（若网关自己
也要主动连接上游）、`core_affinity`（新工作区依赖）、`clap`（CLI 参数）。

### 3.2 行为

- 启动时：绑定固定端口（默认值 + CLI 覆盖，与 13a `apexes-ui` 的
  `--port`/`APEXES_UI_PORT` 约定一致的风格）；写入本地 PID 文件（供
  `apexes-ui` 探活，见 §三.4）；构造并持有一个
  `BinanceMarketDataProvider`（今天唯一的真实实现）；对配置的交易对
  列表调用一次 `subscribe`，持有 receiver 存活于进程生命周期。
- 每个连接的 WS 客户端：可选携带一个 `replay_from: Option<DateTime<Utc>>`
  查询参数。若提供，网关对**每个订阅的 symbol × 每种相关 event type**
  分别调用 `StorageReader::read_batch(symbol, venue, date_range, event_type)`
  ——这是 `apexes-storage` 现有 API 的真实形状（按 symbol/venue/
  event_type 分别查询，`date_range` 是 UTC 天粒度的 `NaiveDate` 区间，
  不是任意精度时间戳；`replay_from` 到 `date_range.start` 之间需要
  做一次"取 `replay_from` 所在自然日"的转换），不是一次能查"某时间戳
  之后所有事件"的通用调用。多个 `read_batch` 结果按 `event_timestamp`
  合并排序后依次回放为 WS 帧，再无缝切换到实时转发（衔接点：回放
  结束时刻与实时流开始被消费的时刻之间如果有微小间隙，允许极少量
  重复事件，不做精确去重——客户端按 `(symbol, timestamp)` 天然幂等
  处理即可，这是本设计的一个明确权衡，不追求精确一次语义）。
- 持久化：网关内部维护一个按 `MarketEvent` 变体分组的小批量缓冲区，
  达到批量大小或超时（例如 100 条或 1 秒，取先到者）后调用
  `StorageWriter::write_batch`——复用 `apexes-storage` 现有的"要求
  同构批次"约定，不新增缓冲/批处理逻辑之外的复杂度。持久化失败只
  记录 `tracing::error!`，不影响实时转发路径（监控可用性优先于历史
  完整性，与 `apexes-message-bus`"记录并跳过，从不 panic"的既有约定
  一致）。

### 3.3 CPU 亲和性（`apexes-engine` 同样适用）

新工作区依赖 `core_affinity`（`crates.io` 上维护良好的跨平台 CPU
亲和性绑定库，之前工作区从未引入过这类系统调用依赖，本设计是首次
引入）。启动参数 `--cpu-cores 2,3`（逗号分隔的核心编号列表）；若未
提供则不设置亲和性（保持操作系统默认调度，与今天完全一致的行为，
向后兼容）。设置失败（例如核心编号超出系统实际核心数）记录
`tracing::warn!` 并继续以默认调度运行，不视为致命错误——这是一项
性能优化，不是正确性前提。

### 3.4 `apexes-ui` 如何发现/启动网关进程

- `apexes-ui` 在首次需要发起任何会话 `launch` 之前，检查一个约定
  路径的 PID 文件（例如 `~/.apexes/market-data-gateway.pid`）。
- 若文件存在且其中记录的 PID 当前存活（跨平台"进程是否存活"检查，
  例如 `sysinfo` crate 或平台特定的 `kill(pid, 0)` 等价物——具体实现
  留给实现计划阶段选型）：判定网关已在运行，直接连接其已知端口
  （端口号可以约定为固定默认值，或同样写入 PID 文件旁的一个小
  元数据文件）。
- 若不存在或 PID 已死：`apexes-ui` 通过 `std::process::Command` 
  `spawn` 一个新的 `apexes-market-data-gateway` 进程（同样可携带
  `--cpu-cores` 参数，来源见下），网关自己负责写入新的 PID 文件。
- 后续所有会话（无论 backtest/paper/live）复用同一个已启动的网关
  实例，不重复 spawn。

---

## 四、`apexes-engine`（新二进制 crate）

### 4.1 结构

```
crates/apexes-engine/
  Cargo.toml
  src/
    main.rs           -- CLI 参数解析、CPU 亲和性设置、选择并调用
                          run_backtest/run_paper_trading/run_live_trading
    launch_config.rs   -- LaunchConfig：反序列化自 apexes-ui 传入的启动
                          参数（JSON，通过 CLI 参数或临时文件传递，
                          具体传输方式留给实现计划——两者都可行，
                          CLI 参数更简单但有长度限制，临时文件更
                          robust，倾向于后者但非本设计强制）
    strategy_loader.rs -- 根据 LaunchConfig.strategy_ref 加载 Drift
                          Script 文件或查 Rust 原生策略表
    control.rs          -- ControlHandle（见 §五），wired 进
                          UnifiedLoopConfig
    server.rs            -- axum WS 服务，/session/ws 端点
```

### 4.2 `LaunchConfig`（`apexes-ui` → `apexes-engine` 的启动参数）

```rust
pub struct LaunchConfig {
    pub mode: SessionMode,           // Backtest | Paper | Live
    pub strategy_ref: StrategyRef,   // DriftScript { path } | RustNative { name }
    pub instruments: Vec<Instrument>,
    pub initial_balances: HashMap<String, Decimal>,
    /// 回测专属：日期范围。paper/live 忽略此字段。
    pub backtest_date_range: Option<Range<NaiveDate>>,
    /// live 专属：网关凭据引用（不是凭据本身——见 §四.4）。
    pub credentials_ref: Option<String>,
    pub cpu_cores: Option<Vec<usize>>,
    pub snapshot_min_interval: Duration, // 默认 500ms
    // margin_tiers/market_condition_config/risk_free_return 等既有
    // BacktestConfig/PaperTradingConfig/LiveTradingConfig 字段，
    // 原样透传，不在此处重复列出。
}
```

### 4.3 策略加载（`strategy_loader.rs`）

- `StrategyRef::DriftScript { path }`：直接调用既有
  `DriftScriptStrategy::from_file(path)`（或
  `from_file_with_params` 若启动时就带初始 override，两者都已存在，
  无需改动）。
- `StrategyRef::RustNative { name }`：查一个**编译期固定的表**
  `HashMap<&'static str, fn() -> Box<dyn Strategy>>`——这是 Rust
  没有运行时反射的必然约束（不是遗漏），每新增一个可被选择的 Rust
  原生策略，开发者需要在这张表里手动加一行。`GET /session/strategies`
  返回这张表的 key 列表 + 磁盘上扫描到的 `.drift` 文件名列表拼在一起。

### 4.4 凭据处理

- `LaunchConfig.credentials_ref` 只是一个引用字符串（例如
  `"binance-main"`），不是凭据本身——凭据从不通过网络/WS 传输。
- `apexes-engine` 启动时（`main.rs`）根据这个引用去读环境变量（例如
  `APEXES_CRED_BINANCE_MAIN_API_KEY`/`..._API_SECRET`）或本地配置
  文件，构造 `BinanceGatewayConfig::new(..)`。`apexes-gateway-binance`
  crate 自己"从不读环境变量"的既有约定不变——读环境变量这一步只发生
  在 `apexes-engine` 这个新的上层调用方里。

---

## 五、控制通道：`ControlHandle`（统一循环内部）

### 5.1 `apexes-strategy` 新增：`OrderExecutor`/`LiquidationPolicy`/
`SupplementalFillSource` 之外的第四个概念——不是 trait，是一对
channel，直接集成进 `UnifiedLoopConfig`/`run_unified_event_loop`
本身（用户明确要求：直接集成，不做成第四个可插拔 trait）。

```rust
// apexes-backtest::unified_loop 新增
pub struct ControlHandle {
    /// 循环内部持有发送端，每 tick 尝试推一次快照，节流到
    /// `min_interval`（未到间隔的 tick 直接跳过，不排队积压）。
    snapshot_tx: watch::Sender<SessionSnapshot>,
    /// 循环内部持有接收端，每 tick 顶部（或 tick 中的固定检查点）
    /// 非阻塞地 poll 一次，处理所有已入队命令。
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
    min_interval: Duration,
    last_pushed: Option<Instant>,
}

pub enum SessionCommand {
    SetParam { name: String, value: Decimal },
    Pause,
    Resume,
    Stop,
}

pub struct SessionSnapshot {
    pub positions: HashMap<String, Position>,
    pub balances: HashMap<String, Decimal>,
    pub equity: Decimal,
    pub realized_pnl: HashMap<String, Decimal>,
    pub margin_status: HashMap<String, PositionMarginStatus>,
    pub params: HashMap<String, Decimal>, // 来自 Strategy::current_params()
    pub paused: bool,
    pub timestamp: DateTime<Utc>,
}
```

`UnifiedLoopConfig` 新增一个 `pub control: Option<ControlHandle>` 字段
——`None`（`apexes-backtest`/`apexes-paper`/`apexes-live-trading` 所有
既有测试与非 `apexes-engine` 调用方的默认值）是完全静默的 no-op，
与本项目一贯的"新增能力默认关闭、不影响既有行为"约定一致。

### 5.2 每 tick 的处理位置（统一循环内）

紧邻既有的 equity-point 记录之后（`unified_loop.rs` 市场事件分支的
末尾）：

1. 若 `control.is_some()`：
   a. 非阻塞 drain `command_rx` 里所有待处理命令（一个 tick 内可能
      积压多条，全部按到达顺序处理，不只处理一条）。
      - `SetParam`：调用 `strategy.set_param(name, value)`（见 §5.3，
        默认实现是 no-op）。
      - `Pause`/`Resume`：翻转一个循环内部的 `paused: bool` 局部
        状态。
      - `Stop`：等价于 `stop_signal` 触发，循环在这次迭代结束后退出。
   b. 若 `paused`：**跳过** `strategy.on_market_event(&event, &ctx)`
      这一步（不产生新 intent），但本 tick 之前的所有步骤（资金结算、
      保证金检查、强平/降风险、行情簿维护）照常执行——这正是用户
      确认的"暂停语义"：盘口/仓位/PnL 继续更新，只是不再产生新决策。
   c. 若 `Instant::now() - last_pushed >= min_interval`：采样当前
      `ledger`/`margin_status`/`strategy.current_params()` 构造一个
      `SessionSnapshot`，`snapshot_tx.send(..)`（`watch` channel 语义：
      多个未及时消费的快照会被覆盖，只保留最新一份——这正是"节流"
      的自然实现，不需要额外的丢弃逻辑）。

### 5.3 `Strategy` trait 新增两个可选方法（默认实现，向后兼容）

```rust
pub trait Strategy: Send {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent>;

    /// 读取当前所有可调参数的值，用于快照展示。默认返回空表——
    /// 不声明任何可调参数的策略（今天几乎所有既有 Strategy 实现）
    /// 不需要改动即可继续工作。
    fn current_params(&self) -> HashMap<String, Decimal> {
        HashMap::new()
    }

    /// 热更新一个参数。默认实现是 no-op（返回 Ok(()) 但什么也不做）
    /// ——不支持热更新的策略不需要改动。返回 Err 表示这个策略不认识
    /// 这个参数名（供调用方在 UI 上提示"未知参数"）。
    fn set_param(&mut self, _name: &str, _value: Decimal) -> Result<(), StrategyError> {
        Ok(())
    }
}
```

`DriftScriptStrategy` 实现两者：`current_params`/`set_param` 操作一个
新增的内部 `Arc<Mutex<HashMap<String, Decimal>>>`（替代/扩展今天
`params.rs`'s `register_to` 里那个不可变的 `overrides: HashMap`——
改为共享可变状态）。

### 5.4 Drift Script `param()` 语义变化（用户已确认的改动）

- **旧写法**（脚本顶层 `let x = param("x", 20);`）：仍然合法、仍然
  工作，但**不支持热更新**——顶层语句只在脚本加载时执行一次，`param()`
  这次调用之后 `x` 就是一个固定的 `DriftValue`，`set_param` 更新的
  是 `overrides` map，但没有任何代码会再次读它。
- **新写法**（脚本在 `on_market_event` 函数体内部调用
  `param("x", 20)`）：每个 tick 都重新调用，读到 `set_param` 写入的
  最新值——这是热更新真正生效的唯一途径。
- 两种写法在同一份脚本里可以共存（互不冲突，`param()` 本身的实现
  不区分调用位置）。这是脚本作者需要理解的一个真实约束，需要在
  `docs/superpowers/specs/` 的某个 Drift Script 使用说明或
  `apexes-strategy` crate 文档里明确写清楚（本设计范围内，作为一个
  小的文档任务）。

---

## 六、WS 线协议

### 6.1 网关的 `/market` 端点（复用 13a `/ws` 的帧格式，端点改名以区分）

原始 `MarketEvent` JSON（今天 13a `/ws` 已有的格式，不变），新增可选
查询参数 `?replay_from=<RFC3339 timestamp>`。

### 6.2 引擎的 `/session/ws` 端点（新，双向）

新增一个消息信封类型，区分三类消息（用户已确认的方案）：

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SessionMessage {
    /// 引擎 -> UI，周期性快照（见 §五 SessionSnapshot）。
    Snapshot(SessionSnapshot),
    /// UI -> 引擎，控制命令（见 §五 SessionCommand）。
    Command(SessionCommand),
}
```

### 6.3 引擎的 REST 端点

- `GET /session/strategies`：返回可选策略列表（Drift Script 文件名 +
  Rust 原生策略名，见 §四.3）。**注意**：这个端点实际上更适合放在
  `apexes-engine` 里，还是需要一个"策略从哪查"的独立轻量端点——由于
  `apexes-engine` 是"每会话一个进程"，在会话真正启动前还没有进程可以
  服务这个查询。因此**实际实现中，`GET /session/strategies` 由
  `apexes-ui` 自己直接扫描本地策略目录+读自己编译期链接的 Rust 原生
  策略表来回答，不需要经过任何引擎/网关进程**（`apexes-ui` 和
  `apexes-engine` 共享同一份 `strategy_loader.rs` 逻辑，作为一个小的
  共享内部 crate 或直接复制维护——具体做法留给实现计划）。这是设计
  阶段发现的一个真实简化，记录在此以免实现时疑惑为什么这个端点没有
  引擎进程可打。
- `POST /session/launch`（`apexes-ui` 自己处理，不是网络请求——见
  §四，`apexes-ui` 直接 `spawn` 子进程，"launch"是 UI 内部触发的一次
  本地操作，不是发给谁的 HTTP 请求。上一版通信图里把它画成 HTTP 端点
  是设计早期的错误，本版文字描述予以更正）。
- `POST /session/stop`：`apexes-ui` 通过已建立的 `/session/ws` 连接
  发送 `SessionCommand::Stop`，或在进程失联时直接 kill 掉子进程
  （兜底路径）。

---

## 七、`apexes-ui` 侧改动

### 7.1 新增标签页

- **会话启动器**：模式（下拉 backtest/paper/live）、策略选择（下拉，
  数据源见 §六.3）、交易对多选、初始余额输入、（live 专属）凭据引用
  下拉、（可选）CPU 核心输入。点击"启动"：本地拼装 `LaunchConfig`，
  `spawn` 一个 `apexes-engine` 子进程（若网关还没起也先拉起它，见
  §三.4），记录子进程 PID/`/session/ws` 端口，连接上去，切换到
  "监控/调参"标签页。
- **监控/调参**：表格展示 `SessionSnapshot` 的仓位/余额/权益/已实现
  盈亏/保证金状态；一个参数编辑表（`params` map 的每一项可编辑，
  编辑后发送 `SessionCommand::SetParam`）；暂停/恢复/停止三个按钮。

### 7.2 引擎进程崩溃处理（用户已确认的两层逻辑）

- `/session/ws` 连接断开：先尝试重连，退避参数复用 13a `client.rs`
  已有的 500ms 倍增封顶 30s 那套状态机（直接复用现有实现，不重新
  发明）。
- 达到固定重试次数上限（例如 5 次）后判定引擎进程已经真的挂了：
  UI 用记录的原始 `LaunchConfig` 重新 `spawn` 一个新的引擎进程
  （注意：这只能恢复"用相同参数重新开始一个新会话"，不是"恢复到
  崩溃前的确切状态"——对 live 会话，新引擎进程启动后 `OrderManager`/
  `PostgresEms` 自己的 `reconcile()` 机制会对账出真实持仓状态，这是
  既有崩溃恢复设计的自然延伸，本设计不需要另外发明状态恢复逻辑）。

---

## 八、测试策略

- `apexes-market-data-gateway`：单元测试（回放窗口边界计算、批量
  持久化的批次/超时触发逻辑），集成测试（真实 `127.0.0.1:0` 绑定，
  假 `MarketDataProvider`，验证多客户端收到相同事件、`replay_from`
  正确拼接历史+实时）。
- `apexes-engine`：单元测试（`LaunchConfig` 反序列化、策略加载
  分派逻辑、`ControlHandle`'s 节流/命令处理逻辑——用假
  `SupplementalFillSource`/`OrderExecutor` 隔离测试，不需要真实
  Postgres/Binance）。集成测试（真实跑一个短 backtest 会话，通过
  `/session/ws` 发送 `SetParam`，断言下一次快照里的 `params` 反映
  新值；发送 `Pause`，断言若干 tick 内 `equity` 仍变化但没有新增
  `trades`；发送 `Stop`，断言进程优雅退出）。
- `apexes-strategy`：`DriftScriptStrategy::current_params`/`set_param`
  的单元测试，新旧两种 `param()` 调用位置写法的行为差异回归测试
  （新写法热更新生效，旧写法不生效——两个测试都要有，防止未来有人
  "修复"旧写法使其意外热更新，这本身是一个明确的设计选择）。
- 端到端：一个手动/半自动的验证脚本（不一定是自动化测试，可以是
  实现计划里的一个手动验证步骤）：启动网关+一个 paper 会话，UI 连上，
  改一个参数，观察策略行为变化——证明整条链路真实打通。

---

## 九、与既有设计的关系

| | 本设计前 | 本设计后 |
|---|---|---|
| 会话托管 | 无（`run_*` 只是库函数，从未打包成可独立运行的进程） | `apexes-engine`，每会话一个 OS 进程 |
| 行情来源 | 各调用方独立构造 `MarketEventBus`（`apexes-ui`/`apexes-paper`/`apexes-live-trading` 各自一份上游连接） | `apexes-market-data-gateway` 常驻共享，所有客户端复用一个上游连接 |
| 消息总线（行 8.3） | 未开始 | 最小可用版本：跨进程实时转发 + 复用 `apexes-storage` 的可选历史回放 |
| 调参 | 不存在（`param()` 只在脚本加载时解析一次，无热更新概念） | `Strategy` trait 新增可选读/写接口，`ControlHandle` 集成进统一循环，三种模式行为一致 |
| `apexes-ui` 职责 | 只读行情看板（13a） | 增加：进程 spawn/生命周期管理、会话启动器、监控/调参客户端 |
| CPU 亲和性 | 无相关依赖 | 新增 `core_affinity` 工作区依赖，网关/引擎进程均可选设置 |
