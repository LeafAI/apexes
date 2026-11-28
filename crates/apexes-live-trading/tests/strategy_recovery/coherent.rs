//! Real unified-loop capture and atomic publication of current-run callback
//! progress. Exchange and market transport are mocked; PostgreSQL is real.
use super::*;
use apexes_live_trading::run_live_trading_with_checkpoint_on_stop;
use apexes_strategy::{
    FillOutcome, IntentId, OrderOutcome, OrderOutcomeKind, TerminalKind, TerminalOutcome,
};

struct ExecutionProbe {
    state: Probe,
    submitted: bool,
    stop_on_attempt: bool,
    stop: Option<oneshot::Sender<()>>,
}

impl ExecutionProbe {
    fn new(stop: oneshot::Sender<()>, stop_on_attempt: bool) -> Self {
        Self {
            state: Probe::new(0),
            submitted: false,
            stop_on_attempt,
            stop: Some(stop),
        }
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Strategy for ExecutionProbe {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &StrategyContext) -> Vec<OrderIntent> {
        self.state.on_market_event(event, ctx);
        if matches!(event, MarketEvent::Trade(_)) && !self.submitted {
            self.submitted = true;
            vec![callback_probe_intent(false)]
        } else {
            vec![]
        }
    }

    fn on_order_outcome(&mut self, outcome: &OrderOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        assert!(
            matches!(outcome.kind, OrderOutcomeKind::Returned { .. }),
            "{:?}",
            outcome.kind
        );
        self.state.value += 10;
        if self.stop_on_attempt {
            self.stop();
        }
    }

    fn on_fill(&mut self, outcome: &FillOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        self.state.value += 100;
    }

    fn on_order_terminal(&mut self, outcome: &TerminalOutcome<'_>) {
        assert_eq!(outcome.intent_id, Some(IntentId(1)));
        assert!(matches!(outcome.kind, TerminalKind::Live { .. }));
        self.state.value += 1000;
        self.stop();
    }

    fn checkpoint_schema_version(&self) -> Option<u32> {
        self.state.checkpoint_schema_version()
    }

    fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
        self.state.checkpoint_payload()
    }

    fn restore_checkpoint(&mut self, state: &ValidatedState<'_>) -> Result<(), CheckpointError> {
        self.state.restore_checkpoint(state)
    }
}

fn execution_startup(
    selection: RecoverySelection,
    stop: oneshot::Sender<()>,
    stop_on_attempt: bool,
) -> StrategyStartup<ExecutionProbe> {
    StrategyStartup::Restore {
        active: ExecutionProbe {
            state: Probe::new(999),
            submitted: false,
            stop_on_attempt: false,
            stop: None,
        },
        candidate: ExecutionProbe::new(stop, stop_on_attempt),
        selection: Box::new(selection),
    }
}

async fn assert_old_head(fixture: &Fixture, selection: &RecoverySelection) {
    let mut store = fixture.store().await;
    let bytes = store
        .load_strategy_checkpoint(Some(selection.receipt()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bytes,
        capture(&Probe::new(42), identity(), cursor()).unwrap()
    );
    drop(store);
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM strategy_checkpoint_boundary")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn completed_direct_callbacks_and_checkpoint_commit_as_one_exact_boundary() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let registry = registry_with(
        gateway.clone(),
        Arc::new(MockMarketDataProvider {
            events: sample_events(),
            keep_open: true,
        }),
    );
    let risk_limits = apexes_risk::RiskLimits {
        max_daily_trade_count: Some(2),
        ..Default::default()
    };
    let make_checker = || {
        let (margin_pre, margin_position) = apexes_risk::MarginMonitor::new(dec!(1.5)).split();

        apexes_risk::CompositeRiskChecker::new()
            .with_pre_trade_check(Box::new(apexes_risk::LimitChecker::new(
                risk_limits.clone(),
            )))
            .with_pre_trade_check(Box::new(apexes_risk::SymbolVenueList::new(
                apexes_risk::ListMode::Whitelist(["BTCUSDT".to_string()].into_iter().collect()),
                apexes_risk::ListMode::Whitelist([Venue::Binance].into_iter().collect()),
            )))
            .with_pre_trade_check(Box::new(apexes_risk::SelfTradePreventer))
            .with_pre_trade_check(Box::new(margin_pre))
            .with_position_monitor(Box::new(margin_position))
    };
    let checker = Arc::new(make_checker());
    let candidate = make_checker();
    let candidate_snapshot = candidate.inspect_registered_state().unwrap();
    registry
        .register::<Arc<apexes_risk::CompositeRiskChecker>, ()>((), checker.clone())
        .unwrap();
    let (manager, mut config, health) = fixture.manager(&registry).await;
    let mut independent_opening_balances = config
        .initial_balances
        .iter()
        .map(|(a, v)| (a.clone(), *v))
        .collect::<Vec<_>>();
    independent_opening_balances.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected_ledger_balances = config.initial_balances.clone();
    *expected_ledger_balances.entry("BTC".into()).or_default() += dec!(1);
    *expected_ledger_balances.entry("USDT".into()).or_default() -= dec!(100);
    let mut expected_ledger_balances = expected_ledger_balances.into_iter().collect::<Vec<_>>();
    expected_ledger_balances.sort_by(|a, b| a.0.cmp(&b.0));

    let market_config = apexes_risk::MarketConditionConfig {
        volatility_threshold: dec!(1000),
        jump_threshold_pct: dec!(0.10),
        sensitivity: dec!(1),
        volatility_window: 3,
        price_source: apexes_risk::PriceSource::Trade,
        account_equity_volatility_threshold: None,
        include_spot: false,
    };
    config.market_condition_config = Some(market_config.clone());
    let table = apexes_strategy::MarginTierTable {
        tiers: vec![apexes_strategy::MarginTier {
            notional_floor: dec!(0),
            max_leverage: dec!(10),
            maintenance_margin_rate: dec!(0.01),
            maintenance_amount: dec!(0),
        }],
    };
    config.margin_tiers.insert("BTCUSDT".into(), table.clone());
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let driver_pool = pool.clone();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    let driver = tokio::spawn(async move {
        let (id, exchange_id, _) =
            wait_for_callback_order(&driver_pool, uuid::Uuid::nil(), false).await;
        // Preserve an independent copy of the synthetic exchange fixture;
        // do not reconstruct the comparison evidence from the database.
        let trade = ExchangeFill {
            exchange_trade_id: "coherent-fill".into(),
            price: dec!(100),
            quantity: dec!(1),
            fee: dec!(0),
            fee_currency: None,
            filled_at: Utc::now(),
        };
        gateway.set_fills_for_order(id, vec![trade.clone()]);
        gateway.set_exchange_state(id, OrderStatus::Filled);
        updates
            .unbounded_send(ExchangeUpdate {
                client_order_id: id,
                exchange_order_id: exchange_id.clone(),
                status: OrderStatus::Filled,
            })
            .unwrap();
        (
            id,
            apexes_oms::TerminalOrderEvidence {
                client_order_id: id,
                venue: Venue::Binance,
                asset_class: AssetClass::Spot,
                symbol: "BTCUSDT".into(),
                side: Side::Buy,
                remote: apexes_oms::RemoteTerminalEvidence::Found {
                    exchange_order_id: exchange_id.unwrap(),
                    execution: apexes_oms::OrderExecutionSnapshot {
                        status: OrderStatus::Filled,
                        executed_quantity: dec!(1),
                    },
                    trades: vec![trade],
                },
            },
        )
    });
    let (stop, receiver) = oneshot::channel();
    let (report, receipt, captured) = tokio::time::timeout(
        Duration::from_secs(10),
        apexes_live_trading::run_live_trading_with_observation_capture_on_stop(
            config,
            &registry,
            execution_startup(selection.clone(), stop, false),
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let (id, evidence) = driver.await.unwrap();
    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].order_id, id);
    assert!(!health.is_engaged(), "{:?}", health.reason());
    assert_eq!(receipt.last_intent, 1);
    assert_eq!((receipt.order_count, receipt.fill_count), (1, 1));
    assert!(receipt.fill_sequence > 0 && receipt.terminal_sequence > 0);
    assert_eq!(receipt.checkpoint.cursor.ordinal, cursor().ordinal + 1);
    assert_eq!(
        receipt.checkpoint.cursor.observed_at,
        cursor().observed_at + chrono::TimeDelta::seconds(1)
    );
    assert_eq!(receipt.checkpoint.identity, identity());
    let (stored_scope, stored_terminal): (uuid::Uuid, i64) =
        sqlx::query_as("SELECT intent_scope,terminal_delivery_seq FROM orders WHERE id=$1")
            .bind(id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored_scope, receipt.run_scope);
    assert_eq!(stored_terminal, receipt.terminal_sequence);
    let stored_fill: i64 = sqlx::query_scalar("SELECT delivery_seq FROM fills WHERE order_id=$1")
        .bind(id.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored_fill, receipt.fill_sequence);

    let mut store = fixture.store().await;
    store.verify_coherent_boundary(&receipt).await.unwrap();
    let bytes = store
        .load_strategy_checkpoint(Some(&receipt.checkpoint))
        .await
        .unwrap()
        .unwrap();
    let mut restored = Probe::new(0);
    apexes_strategy::checkpoint::restore(
        &mut restored,
        Probe::new(0),
        &bytes,
        &identity(),
        receipt.checkpoint.cursor,
    )
    .unwrap();
    // market decision + direct attempt + fill + terminal callback.
    assert_eq!(restored.value, 42 + 1 + 10 + 100 + 1000);
    assert!(
        store
            .load_strategy_checkpoint(Some(selection.receipt()))
            .await
            .is_err()
    );
    // An exact plain retry cannot overwrite/detach the coherent evidence.
    assert!(
        store
            .publish_strategy_checkpoint(
                Some(selection.receipt()),
                &identity(),
                receipt.checkpoint.cursor,
                &bytes
            )
            .await
            .is_err()
    );
    let mut false_receipt = receipt.clone();
    false_receipt.last_intent += 1;
    assert!(
        store
            .verify_coherent_boundary(&false_receipt)
            .await
            .is_err()
    );
    // Select the actual acknowledged coherent receipt independently
    // of the database and verify a post-execution preview after reopening.
    let coherent =
        apexes_live_trading::CoherentRecoverySelection::new(selection.clone(), receipt.clone())
            .unwrap();
    let dir = std::env::temp_dir().join(format!(
        "apexes-coherent-selection-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("coherent.json");
    coherent.write_new(&path).unwrap();
    let coherent = apexes_live_trading::CoherentRecoverySelection::read(&path).unwrap();
    // Actual completed iteration capture, compared to independent
    // mock-source fixtures. The first depth event was filtered.
    use apexes_backtest::observation_receipt::{CompletedObservation, ObservationClock};
    let ledger_evidence = captured.ledger.as_ref().unwrap();
    assert_eq!(
        ledger_evidence.scope,
        vec![(Venue::Binance, "BTCUSDT".into())]
    );
    assert_eq!(ledger_evidence.balances, expected_ledger_balances);
    assert_eq!(ledger_evidence.positions.len(), 1);
    assert_eq!(ledger_evidence.positions[0].quantity, dec!(1));
    assert_eq!(ledger_evidence.positions[0].average_entry_price, dec!(100));
    assert!(ledger_evidence.realized_pnl.is_empty());
    let attribution = ledger_evidence.fee_valuation.as_ref().unwrap();
    assert_eq!(attribution.accounts.len(), 1);
    let account = &attribution.accounts[0];
    assert_eq!(account.symbol, "BTCUSDT");
    assert_eq!(account.venue, Venue::Binance);
    assert_eq!(account.base, "BTC");
    assert_eq!(account.quote, "USDT");
    assert_eq!(account.gross_quantity, dec!(1));
    assert_eq!(account.entry, dec!(100));
    assert_eq!(account.gross_realized, dec!(0));
    assert_eq!(account.historical_fee, dec!(0));
    assert_eq!(account.missing_execution_valuations, 0);
    assert!(account.fees.is_empty());
    assert!(account.external_fees.is_empty());
    let decisions = attribution.decisions.as_ref().unwrap();
    assert_eq!(decisions.len(), 1);
    let observed_fill_id = decisions[0].fill_id;
    assert_eq!(
        captured.fill_observation_prefixes.as_deref(),
        Some(
            &[apexes_live_trading::FillObservationPrefix {
                fill_id: report.trades[0].id,
                observations: 1,
            }][..]
        )
    );
    assert_eq!(decisions[0].symbol, "BTCUSDT");
    assert_eq!(decisions[0].price, dec!(100));
    assert_eq!(decisions[0].fee, dec!(0));
    assert_eq!(decisions[0].value, Some(dec!(0)));
    assert_eq!(
        decisions[0].source,
        apexes_backtest::fee_evidence::FeeDecisionSource::Zero
    );

    assert_eq!(attribution.prices.len(), 1);
    assert_eq!(attribution.prices[0].observations.len(), 1);
    assert_eq!(attribution.prices[0].observations[0].1, dec!(100));

    let registered = captured.registered_risk.as_ref().unwrap();
    assert!(!registered.has_unsupported_slots());
    let aliases = registered.aliases.as_ref().unwrap();
    assert_eq!(aliases.pre_trade, vec![None, None, None, Some(0)]);
    assert_eq!(aliases.position, vec![Some(0)]);
    assert!(aliases.in_trade.is_empty());
    assert!(aliases.market_condition.is_empty());

    assert!(!captured.market_monitor_unqualified);
    assert!(!captured.margin_monitor_unqualified);
    let market = captured.market_monitor.as_ref().unwrap();
    assert_eq!(market.config, market_config);
    assert_eq!(market.symbols.len(), 1);
    assert_eq!(market.symbols[0].symbol, "BTCUSDT");
    assert_eq!(market.symbols[0].volatility.sample_count(), 1);
    assert_eq!(
        captured.margin_tiers.as_ref().unwrap(),
        &vec![("BTCUSDT".into(), table.clone())]
    );

    assert_eq!(registered, &checker.inspect_registered_state().unwrap());
    assert!(
        matches!(&registered.pre_trade[0], apexes_risk::RegisteredRiskState::Limits(s)
        if s.trade_count==1 && s.traded_volume==dec!(1) && s.limits==risk_limits)
    );
    assert_eq!(registered.pre_trade.len(), 4);
    assert!(
        matches!(&registered.pre_trade[1], apexes_risk::RegisteredRiskState::SymbolVenueList(s)
        if s.symbols == apexes_risk::ListState::Whitelist(vec!["BTCUSDT".into()])
        && s.venues == apexes_risk::ListState::Whitelist(vec![Venue::Binance]))
    );
    assert_eq!(
        registered.pre_trade[2],
        apexes_risk::RegisteredRiskState::SelfTradePreventer { version: 1 }
    );
    assert!(
        matches!(&registered.pre_trade[3], apexes_risk::RegisteredRiskState::Margin(s)
        if s.warning_threshold == dec!(1.5) && s.tripped_symbols.is_empty())
    );
    assert_eq!(
        registered.position.as_ref().unwrap(),
        &vec![registered.pre_trade[3].clone()]
    );
    assert_eq!(captured.receipt.clock, ObservationClock::SourceAvailability);
    assert_eq!(captured.receipt.predecessor, selection.receipt().cursor);
    assert_eq!(captured.receipt.through, receipt.checkpoint.cursor);
    let expected_records = vec![CompletedObservation {
        cursor: receipt.checkpoint.cursor,
        event: sample_events().pop().unwrap(),
    }];
    let scope = vec![(Venue::Binance, "BTCUSDT".to_string())];
    assert_eq!(captured.scope, scope);
    assert_eq!(captured.records, expected_records);
    captured.receipt.verify(&scope, &expected_records).unwrap();
    assert!(receipt.capture_sha256.is_some());
    let recovered = coherent
        .inspect_durable_capture(&mut store, &fixture.url)
        .await
        .unwrap();
    assert_eq!(recovered, captured);
    let candidate_market = apexes_risk::MarketConditionMonitor::new(market_config.clone())
        .inspect_checkpoint()
        .unwrap();
    let independent_scope = vec![(Venue::Binance, "BTCUSDT".to_string())];
    let independent_tiers = vec![("BTCUSDT".to_string(), table)];
    recovered
        .qualify_offline_risk_candidate(
            &independent_scope,
            Some(&candidate_snapshot),
            Some(&candidate_market),
            Some(&independent_tiers),
        )
        .unwrap();
    recovered
        .qualify_isolated_risk_restoration(
            &independent_scope,
            Some(&candidate_snapshot),
            Some(&candidate_market),
            Some(&independent_tiers),
        )
        .unwrap();
    assert!(
        recovered
            .qualify_isolated_risk_restoration(
                &independent_scope,
                Some(registered),
                Some(&candidate_market),
                Some(&independent_tiers),
            )
            .is_err()
    );
    assert!(
        recovered
            .qualify_isolated_risk_restoration(
                &independent_scope,
                Some(&candidate_snapshot),
                None,
                Some(&independent_tiers),
            )
            .is_err()
    );
    assert_eq!(
        candidate_snapshot,
        candidate.inspect_registered_state().unwrap()
    );
    assert!(
        recovered
            .qualify_offline_risk_candidate(
                &independent_scope,
                Some(registered),
                Some(&candidate_market),
                Some(&independent_tiers)
            )
            .is_err()
    );
    assert!(
        recovered
            .qualify_offline_risk_candidate(
                &independent_scope,
                Some(&candidate_snapshot),
                None,
                Some(&independent_tiers)
            )
            .is_err()
    );
    assert!(
        recovered
            .qualify_offline_risk_candidate(
                &independent_scope,
                Some(&candidate_snapshot),
                Some(&candidate_market),
                None
            )
            .is_err()
    );
    assert!(
        recovered
            .qualify_offline_risk_candidate(
                &[(Venue::Binance, "ETHUSDT".into())],
                Some(&candidate_snapshot),
                Some(&candidate_market),
                Some(&independent_tiers)
            )
            .is_err()
    );

    let mut invalid_prefix = captured.clone();
    invalid_prefix.fill_observation_prefixes.as_mut().unwrap()[0].observations = 2;
    assert!(invalid_prefix.encode_durable().is_err());
    invalid_prefix
        .fill_observation_prefixes
        .as_mut()
        .unwrap()
        .clear();
    assert!(invalid_prefix.encode_durable().is_err());
    let mut legacy = captured.clone();
    legacy.fill_observation_prefixes = None;
    let legacy_bytes = legacy.encode_durable().unwrap();
    assert_eq!(
        apexes_backtest::observation_receipt::CapturedObservations::decode_durable(
            &legacy_bytes,
            selection.receipt().cursor,
            receipt.checkpoint.cursor,
        )
        .unwrap(),
        legacy,
    );
    let original = captured.encode_durable().unwrap();
    for damaged in [Some(vec![1u8]), None] {
        sqlx::query("UPDATE strategy_checkpoint_boundary SET capture=$1 WHERE singleton")
            .bind(damaged)
            .execute(&pool)
            .await
            .unwrap();
        assert!(store.verify_coherent_boundary(&receipt).await.is_err());
        assert!(
            coherent
                .inspect_durable_capture(&mut store, &fixture.url)
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE strategy_checkpoint_boundary SET capture=$1 WHERE singleton")
        .bind(original)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        coherent
            .inspect_durable_capture(&mut store, &fixture.url)
            .await
            .unwrap(),
        captured
    );
    let records = captured.records;
    let history = captured.receipt;
    let retained = apexes_live_trading::ObservationBoundaryEvidence::new(
        OWNER_SHA256.into(),
        selection.receipt().clone(),
        receipt.clone(),
        history,
    )
    .unwrap();
    let observation_path = dir.join("observation.json");
    retained.write_new(&observation_path).unwrap();
    assert!(retained.write_new(&observation_path).is_err());
    let retained =
        apexes_live_trading::ObservationBoundaryEvidence::read(&observation_path).unwrap();
    let independent_observation_receipt = retained.clone();
    coherent
        .inspect_observation_evidence(&mut store, &fixture.url, &retained, &scope, &records)
        .await
        .unwrap();
    let mut altered = records.clone();
    if let MarketEvent::Trade(t) = &mut altered[0].event {
        t.price = dec!(101);
    }
    assert!(matches!(
        coherent
            .inspect_observation_evidence(&mut store, &fixture.url, &retained, &scope, &altered)
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Observation(
            apexes_live_trading::ObservationEvidenceError::History(
                apexes_backtest::observation_receipt::ObservationError::Mismatch
            )
        ))
    ));
    assert!(
        coherent
            .inspect_observation_evidence(&mut store, &fixture.url, &retained, &scope, &[])
            .await
            .is_err()
    );

    // Synthetic independent risk evidence; the current stop path does
    // not yet atomically capture its actual registered risk monitors.
    let captured_at = Utc::now();
    let limit = apexes_risk::LimitStateSnapshot {
        version: 1,
        limits: apexes_risk::RiskLimits {
            max_daily_trade_count: Some(1),
            ..Default::default()
        },
        day: Some(captured_at.date_naive()),
        trade_count: 1,
        traded_volume: dec!(1),
    };
    let risk = apexes_live_trading::RiskBoundaryEvidence::new(
        OWNER_SHA256.into(),
        receipt.clone(),
        captured_at,
        limit.clone(),
        None,
    )
    .unwrap();
    let risk_path = dir.join("risk.json");
    risk.write_new(&risk_path).unwrap();
    let risk = apexes_live_trading::RiskBoundaryEvidence::read(&risk_path).unwrap();
    coherent
        .inspect_risk_evidence(&mut store, &fixture.url, &risk, captured_at, (&limit, None))
        .await
        .unwrap();
    let reset = apexes_risk::LimitStateSnapshot {
        day: None,
        trade_count: 0,
        traded_volume: dec!(0),
        ..limit.clone()
    };
    assert!(matches!(
        coherent
            .inspect_risk_evidence(&mut store, &fixture.url, &risk, captured_at, (&reset, None))
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Risk(
            apexes_live_trading::RiskEvidenceError::State(apexes_risk::RiskStateError::Mismatch)
        ))
    ));

    // Independently supplied synthetic coverage for the offline preview.
    // No live exchange lookup is performed by the inspection API.
    let started = Utc::now();
    let end = started + chrono::TimeDelta::seconds(1);
    let inspected = end + chrono::TimeDelta::seconds(1);
    let window = apexes_live_trading::RecoveryEvidenceWindow {
        terminal_window: apexes_live_trading::EvidenceWindow {
            started_at: started,
            finished_at: end,
        },
        terminal_owner_fingerprint: OWNER_SHA256.into(),
        terminal_evidence_complete: true,
        terminal_orders: vec![evidence.clone()],
        open_orders: apexes_live_trading::OpenOrderCoverage {
            window: apexes_live_trading::EvidenceWindow {
                started_at: end,
                finished_at: inspected,
            },
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            owner_fingerprint: OWNER_SHA256.into(),
            account_wide_complete: true,
            open_order_count: 0,
            foreign_order_count: 0,
        },
    };
    coherent
        .inspect_evidence_window(&mut store, &fixture.url, &window, inspected)
        .await
        .unwrap();
    assert!(matches!(
        coherent
            .inspect_evidence_window(
                &mut store,
                &fixture.url,
                &window,
                inspected + chrono::TimeDelta::seconds(61)
            )
            .await,
        Err(
            apexes_live_trading::CoherentInspectionError::EvidenceWindow(
                apexes_live_trading::EvidenceWindowError::Stale
            )
        )
    ));
    let mut incomplete = window.clone();
    incomplete.open_orders.account_wide_complete = false;
    assert!(
        coherent
            .inspect_evidence_window(&mut store, &fixture.url, &incomplete, inspected)
            .await
            .is_err()
    );
    let mut foreign = window.clone();
    foreign.open_orders.foreign_order_count = 1;
    assert!(
        coherent
            .inspect_evidence_window(&mut store, &fixture.url, &foreign, inspected)
            .await
            .is_err()
    );

    coherent
        .inspect_terminal_evidence(&mut store, &fixture.url, std::slice::from_ref(&evidence))
        .await
        .unwrap();
    let mut revised = evidence.clone();
    if let apexes_oms::RemoteTerminalEvidence::Found { trades, .. } = &mut revised.remote {
        trades[0].fee = dec!(0.01);
    }
    assert!(matches!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[revised])
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Terminal(
            apexes_oms::TerminalAuditError::Trade
        ))
    ));
    assert!(matches!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[])
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Terminal(
            apexes_oms::TerminalAuditError::MissingOrder
        ))
    ));

    let wallet = HashMap::from([
        (
            "BTC".into(),
            apexes_live_trading::spot_recovery::SpotWalletBalance {
                free: dec!(5),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            apexes_live_trading::spot_recovery::SpotWalletBalance {
                free: dec!(99900),
                locked: dec!(0),
            },
        ),
    ]);
    let instruments = base_config().instruments;
    let wallet_evidence = apexes_live_trading::WalletEvidence {
        window: apexes_live_trading::EvidenceWindow {
            started_at: inspected,
            finished_at: inspected + chrono::TimeDelta::seconds(1),
        },
        owner_fingerprint: OWNER_SHA256.into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        account_wide_complete: true,
        balances: wallet.clone(),
    };
    let wallet_at = wallet_evidence.window.finished_at;
    let source = CollectionSource {
        terminal: Some(evidence.clone()),
        owner: OWNER_SHA256.into(),
        open: 0,
        wallet: Some(
            wallet
                .iter()
                .map(|(asset, balance)| {
                    (
                        asset.clone(),
                        apexes_oms::SpotEvidenceBalance {
                            free: balance.free,
                            locked: balance.locked,
                        },
                    )
                })
                .collect(),
        ),
        calls: Default::default(),
    };
    let collection_strategy = serde_json::json!({"RustNative":{"name":"fixture"}});
    let collected_receipt = coherent
        .collect_spot_evidence_receipt(
            &mut store,
            &fixture.url,
            &source,
            &collection_strategy,
            &instruments,
        )
        .await
        .unwrap();
    let path = dir.join("filled-collection.json");
    collected_receipt.write_new(&path).unwrap();
    let reopened =
        apexes_live_trading::SpotCollectionReceipt::read(&path, collected_receipt.sha256())
            .unwrap();
    let historical = coherent
        .inspect_spot_collection_receipt(
            &mut store,
            &fixture.url,
            &instruments,
            &reopened,
            collected_receipt.sha256(),
        )
        .await
        .unwrap();
    assert_eq!(historical.window.terminal_orders.len(), 1);
    assert_eq!(historical.wallet.balances, wallet);
    assert_eq!(
        *source.calls.lock().unwrap(),
        vec!["terminal", "open", "wallet"]
    );
    let mut opening_wallet = wallet_evidence.clone();
    opening_wallet.window = apexes_live_trading::EvidenceWindow {
        started_at: cursor().observed_at,
        finished_at: cursor().observed_at,
    };
    opening_wallet.balances.get_mut("BTC").unwrap().free = dec!(4);
    opening_wallet.balances.get_mut("USDT").unwrap().free = dec!(100000);
    let opening = apexes_live_trading::OpeningWalletEvidence {
        predecessor: selection.receipt().clone(),
        wallet: opening_wallet,
    };
    let ledger = coherent
        .inspect_ledger_continuity(
            &mut store,
            &fixture.url,
            (Probe::new(999), Probe::new(0)),
            &instruments,
            (&opening, &window, &wallet_evidence, wallet_at),
        )
        .await
        .unwrap();
    assert_eq!(ledger.owned_positions["BTCUSDT"].quantity, dec!(1));
    // Independently expected mock fill quantity and price, not a copy
    // of the reconstructed journal preview.
    let retained = apexes_live_trading::ClosingPositionEvidence {
        version: 1,
        owner_fingerprint: wallet_evidence.owner_fingerprint.clone(),
        boundary: receipt.clone(),
        captured_at: wallet_evidence.window.finished_at,
        symbols: vec!["BTCUSDT".into()],
        positions: vec![apexes_live_trading::ClosingSpotPosition {
            symbol: "BTCUSDT".into(),
            quantity: dec!(1),
            average_entry_price: dec!(100),
        }],
    };
    coherent
        .inspect_closing_position_evidence(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&opening, &window, &wallet_evidence, wallet_at),
            &retained,
        )
        .await
        .unwrap();
    // The existing account fixture holds four BTC outside the session.
    // It must not be accepted as an exact local-ledger reconciliation.
    assert!(matches!(
        coherent
            .inspect_captured_ledger_reconciliation(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&opening, &window, &wallet_evidence, wallet_at),
                &retained
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Ledger(
            apexes_live_trading::LedgerContinuityError::CapturedLedgerMismatch
        ))
    ));
    // Independent complete account fixture for the session-only case.
    let mut exact_opening = opening.clone();
    exact_opening.wallet.balances.get_mut("BTC").unwrap().free = dec!(0);
    let mut exact_wallet = wallet_evidence.clone();
    exact_wallet.balances.get_mut("BTC").unwrap().free = dec!(1);
    coherent
        .inspect_captured_ledger_reconciliation(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            &retained,
        )
        .await
        .unwrap();
    coherent
        .inspect_captured_realized_pnl(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            &retained,
        )
        .await
        .unwrap();
    coherent
        .inspect_captured_direct_fees(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            &retained,
        )
        .await
        .unwrap();
    let third_asset_rates = apexes_live_trading::ThirdAssetFeeEvidence {
        version: 1,
        boundary: receipt.clone(),
        owner_fingerprint: wallet_evidence.owner_fingerprint.clone(),
        rates: vec![],
    };
    coherent
        .inspect_captured_third_asset_fees(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &third_asset_rates),
        )
        .await
        .unwrap();
    coherent
        .inspect_captured_fee_decisions(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &third_asset_rates),
        )
        .await
        .unwrap();
    coherent
        .inspect_captured_fee_bearing_pnl(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &third_asset_rates),
        )
        .await
        .unwrap();
    let marks = apexes_live_trading::ClosingFeeMarkEvidence {
        version: 1,
        boundary: receipt.clone(),
        owner_fingerprint: exact_opening.wallet.owner_fingerprint.clone(),
        rates: vec![],
        symbols: vec![apexes_live_trading::ClosingFeeRevaluation {
            symbol: "BTCUSDT".into(),
            fee_at_execution: dec!(0),
            fee_at_mark: dec!(0),
            fee_revaluation: dec!(0),
        }],
    };
    coherent
        .inspect_captured_fee_revaluation(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &third_asset_rates, &marks),
        )
        .await
        .unwrap();
    let allocation = apexes_live_trading::OpeningTrackedAssetEvidence {
        version: 1,
        predecessor: exact_opening.predecessor.clone(),
        owner_fingerprint: exact_opening.wallet.owner_fingerprint.clone(),
        captured_at: exact_opening.wallet.window.finished_at,
        balances: independent_opening_balances,
    };
    coherent
        .inspect_external_fee_ownership(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &allocation),
        )
        .await
        .unwrap();
    coherent
        .inspect_combined_fee_evidence(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (&retained, &allocation, &third_asset_rates, &marks),
        )
        .await
        .unwrap();
    let scope = apexes_live_trading::ConversionScopeEvidence {
        version: 1,
        boundary: receipt.clone(),
        owner_fingerprint: exact_opening.wallet.owner_fingerprint.clone(),
        instruments: vec![apexes_live_trading::EvidenceInstrumentScope {
            venue: Venue::Binance,
            symbol: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            role: apexes_live_trading::EvidenceInstrumentRole::StrategyInventory,
        }],
    };
    coherent
        .inspect_conversion_scope_evidence(&mut store, &fixture.url, &instruments, &scope)
        .await
        .unwrap();
    coherent
        .inspect_conversion_price_provenance(
            &mut store,
            &fixture.url,
            &instruments,
            (&scope, &independent_observation_receipt),
        )
        .await
        .unwrap();
    coherent
        .inspect_retained_fill_observation_availability(
            &mut store,
            &fixture.url,
            &instruments,
            (&scope, &independent_observation_receipt),
        )
        .await
        .unwrap();
    coherent
        .inspect_isolated_ledger_application(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &instruments,
            (&exact_opening, &window, &exact_wallet, wallet_at),
            (
                (&retained, &allocation, &third_asset_rates, &marks),
                (&scope, &independent_observation_receipt),
            ),
        )
        .await
        .unwrap();
    let mut transition = apexes_live_trading::TransitionEvidence {
        instruments: &instruments,
        opening: &exact_opening,
        window: &window,
        closing: &exact_wallet,
        inspected_at: wallet_at,
        positions: &retained,
        allocation: &allocation,
        fees: &third_asset_rates,
        marks: &marks,
        conversion: &scope,
        observations: &independent_observation_receipt,
        registered_risk: Some(&candidate_snapshot),
        market_monitor: Some(&candidate_market),
        margin_tiers: Some(&independent_tiers),
    };
    let witness = coherent
        .inspect_offline_transition(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &transition,
        )
        .await
        .unwrap();
    let witness_path = dir.join("offline-transition.json");
    witness.write_new(&witness_path).unwrap();
    assert!(witness.write_new(&witness_path).is_err());
    let reopened = apexes_live_trading::OfflineTransitionWitness::read(&witness_path).unwrap();
    let independently_requalified = coherent
        .inspect_offline_transition(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &transition,
        )
        .await
        .unwrap();
    reopened.compare(&independently_requalified).unwrap();
    transition.inspected_at += chrono::TimeDelta::milliseconds(1);
    let later = coherent
        .inspect_offline_transition(
            &mut store,
            &fixture.url,
            (Probe::new(0), Probe::new(0)),
            &transition,
        )
        .await
        .unwrap();
    assert!(reopened.compare(&later).is_err());
    transition.market_monitor = None;
    assert!(
        coherent
            .inspect_offline_transition(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &transition,
            )
            .await
            .is_err()
    );
    let mut changed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&witness_path).unwrap()).unwrap();
    changed["body"]["evidence_sha256"] = serde_json::Value::String("0".repeat(64));
    let changed_path = dir.join("altered-transition.json");
    std::fs::write(&changed_path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(apexes_live_trading::OfflineTransitionWitness::read(&changed_path).is_err());
    let mut wrong_allocation = allocation.clone();
    wrong_allocation.balances[0].1 += dec!(1);
    assert!(
        coherent
            .inspect_isolated_ledger_application(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&exact_opening, &window, &exact_wallet, wallet_at),
                (
                    (&retained, &wrong_allocation, &third_asset_rates, &marks),
                    (&scope, &independent_observation_receipt)
                ),
            )
            .await
            .is_err()
    );
    // Select the independent opening history, ending at the original
    // checkpoint. This fee-free fixture cannot prove external-fee continuity.
    let opening_cursor = apexes_strategy::checkpoint::StateCursor {
        ordinal: cursor().ordinal - 1,
        observed_at: cursor().observed_at - chrono::TimeDelta::minutes(6),
    };
    let mut opening_event = records[0].event.clone();
    if let MarketEvent::Trade(t) = &mut opening_event {
        t.timestamp = cursor().observed_at;
    } else {
        panic!("expected trade fixture");
    }
    let opening_records = vec![CompletedObservation {
        cursor: cursor(),
        event: opening_event,
    }];
    let opening_history = apexes_backtest::observation_receipt::ObservationReceipt::build(
        opening_cursor,
        cursor(),
        ObservationClock::SourceAvailability,
        &[(Venue::Binance, "BTCUSDT".into())],
        &opening_records,
    )
    .unwrap();
    let opening_receipt = apexes_live_trading::OpeningObservationEvidence::new(
        OWNER_SHA256.into(),
        selection.receipt().clone(),
        opening_history,
    )
    .unwrap();
    let opening_path = dir.join("opening-observations.json");
    opening_receipt.write_new(&opening_path).unwrap();
    assert!(opening_receipt.write_new(&opening_path).is_err());
    let opening_receipt =
        apexes_live_trading::OpeningObservationEvidence::read(&opening_path).unwrap();
    let preview = coherent
        .inspect_opening_valuation_preview(
            &mut store,
            &fixture.url,
            &instruments,
            (
                &scope,
                &independent_observation_receipt,
                &opening_receipt,
                &opening_records,
            ),
        )
        .await
        .unwrap();
    assert_eq!(preview.compared_fills, 1);
    assert_eq!(preview.missing_after_warmup, 0);
    assert!(preview.differences.is_empty());
    let mut altered_opening = opening_records.clone();
    if let MarketEvent::Trade(t) = &mut altered_opening[0].event {
        t.price += dec!(1);
    }
    assert!(
        coherent
            .inspect_opening_valuation_preview(
                &mut store,
                &fixture.url,
                &instruments,
                (
                    &scope,
                    &independent_observation_receipt,
                    &opening_receipt,
                    &altered_opening
                ),
            )
            .await
            .is_err()
    );
    let prefix = apexes_live_trading::FillObservationAvailability {
        version: 1,
        boundary: receipt.clone(),
        owner_fingerprint: exact_opening.wallet.owner_fingerprint.clone(),
        prefixes: vec![apexes_live_trading::FillObservationPrefix {
            fill_id: observed_fill_id,
            observations: 1,
        }],
    };
    coherent
        .inspect_fill_observation_availability(
            &mut store,
            &fixture.url,
            &instruments,
            (&scope, &independent_observation_receipt, &prefix),
        )
        .await
        .unwrap();
    let mut changed = retained.clone();
    changed.positions[0].average_entry_price = dec!(101);
    assert!(matches!(
        coherent
            .inspect_closing_position_evidence(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&opening, &window, &wallet_evidence, wallet_at),
                &changed
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Ledger(
            apexes_live_trading::LedgerContinuityError::PositionMismatch
        ))
    ));
    let mut changed = retained.clone();
    changed.positions.clear();
    assert!(
        coherent
            .inspect_closing_position_evidence(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&opening, &window, &wallet_evidence, wallet_at),
                &changed
            )
            .await
            .is_err()
    );
    let mut changed = retained;
    changed.symbols.push("ETHUSDT".into());
    assert!(
        coherent
            .inspect_closing_position_evidence(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&opening, &window, &wallet_evidence, wallet_at),
                &changed
            )
            .await
            .is_err()
    );

    let mut changed_wallet = wallet_evidence.clone();
    changed_wallet.balances.get_mut("USDT").unwrap().free += dec!(1);
    assert!(matches!(
        coherent
            .inspect_ledger_continuity(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&opening, &window, &changed_wallet, wallet_at)
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Ledger(
            apexes_live_trading::LedgerContinuityError::Mismatch
        ))
    ));
    let qualified = coherent
        .inspect_wallet_continuity(
            &mut store,
            &fixture.url,
            (Probe::new(999), Probe::new(0)),
            &instruments,
            (&window, &wallet_evidence, wallet_at),
        )
        .await
        .unwrap();
    assert_eq!(qualified.owned_positions["BTCUSDT"].quantity, dec!(1));
    assert_eq!(qualified.wallet_balances["BTC"].free, dec!(5));
    let mut missing = wallet_evidence.clone();
    missing.balances.remove("USDT");
    assert!(matches!(
        coherent
            .inspect_wallet_continuity(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&window, &missing, wallet_at)
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Wallet(
            apexes_live_trading::WalletEvidenceError::MissingAsset
        ))
    ));
    let mut short = wallet_evidence.clone();
    short.balances.get_mut("BTC").unwrap().free = dec!(0.5);
    assert!(matches!(
        coherent
            .inspect_wallet_continuity(
                &mut store,
                &fixture.url,
                (Probe::new(0), Probe::new(0)),
                &instruments,
                (&window, &short, wallet_at)
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Inventory(_))
    ));
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    let preview = coherent
        .inspect(
            &mut store,
            &fixture.url,
            Probe::new(999),
            candidate,
            &instruments,
            &wallet,
        )
        .await
        .unwrap();
    assert_eq!(preview.cursor, receipt.checkpoint.cursor);
    assert_eq!(
        (
            preview.historical_order_count,
            preview.historical_fill_count
        ),
        (1, 1)
    );
    assert_eq!(preview.owned_positions["BTCUSDT"].quantity, dec!(1));
    assert_eq!(
        preview.owned_positions["BTCUSDT"].average_entry_price,
        dec!(100)
    );
    assert_eq!(preview.wallet_balances["BTC"].free, dec!(5));
    assert!(observed.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 1);

    let mut short = wallet.clone();
    short.get_mut("BTC").unwrap().free = dec!(0.5);
    assert!(matches!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &short
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Inventory(_))
    ));
    let mut invalid_candidate = Probe::new(0);
    invalid_candidate.fail_restore = true;
    assert!(matches!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                invalid_candidate,
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Restore(_))
    ));
    assert!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &[],
                &wallet
            )
            .await
            .is_err()
    );
    assert!(
        coherent
            .inspect(
                &mut store,
                "different database selection",
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await
            .is_err()
    );
    let wrong_owner =
        RecoverySelection::new("0".repeat(64), selection.receipt().clone(), HashMap::new())
            .unwrap();
    let wrong =
        apexes_live_trading::CoherentRecoverySelection::new(wrong_owner, receipt.clone()).unwrap();
    assert!(matches!(
        wrong
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Store(
            apexes_oms::CheckpointStoreError::Owner
        ))
    ));
    let mut other_receipt = selection.receipt().clone();
    other_receipt.envelope_sha256 = "0".repeat(64);
    let wrong_predecessor =
        RecoverySelection::new(OWNER_SHA256.into(), other_receipt, HashMap::new()).unwrap();
    let wrong =
        apexes_live_trading::CoherentRecoverySelection::new(wrong_predecessor, receipt.clone())
            .unwrap();
    assert!(matches!(
        wrong
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await,
        Err(apexes_live_trading::CoherentInspectionError::Store(
            apexes_oms::CheckpointStoreError::Conflict
        ))
    ));
    // A preview never enables a repeatable live startup.
    std::fs::remove_dir_all(dir).unwrap();
    // Modification with unchanged row counts and marker watermarks must
    // still invalidate the exact witness.
    sqlx::query("UPDATE orders SET updated_at=updated_at+interval '1 second' WHERE id=$1")
        .bind(id.0)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.verify_coherent_boundary(&receipt).await,
        Err(apexes_oms::CheckpointStoreError::JournalChanged)
    ));
    assert!(
        coherent
            .inspect(
                &mut store,
                &fixture.url,
                Probe::new(0),
                Probe::new(0),
                &instruments,
                &wallet
            )
            .await
            .is_err()
    );
    assert!(
        coherent
            .inspect_terminal_evidence(&mut store, &fixture.url, &[evidence])
            .await
            .is_err()
    );
    drop(store);
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn working_order_refuses_coherent_publication_and_preserves_predecessor() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let (stop, receiver) = oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            execution_startup(selection.clone(), stop, true),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(health.is_engaged());
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM orders WHERE status='New' AND submission_state='Accepted'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    pool.close().await;
    assert_old_head(&fixture, &selection).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn commit_failure_rolls_back_both_head_and_boundary_receipt() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    sqlx::query("CREATE FUNCTION refuse_boundary_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected boundary commit failure'; END; $$")
        .execute(&pool).await.unwrap();
    sqlx::query("CREATE CONSTRAINT TRIGGER boundary_commit_failure AFTER INSERT ON strategy_checkpoint_boundary DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_boundary_commit()")
        .execute(&pool).await.unwrap();
    let reg = registry();
    let (manager, config, health) = fixture.manager(&reg).await;
    let candidate = Probe::new(0);
    let observed = candidate.observed.clone();
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stop.send(());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        apexes_live_trading::run_live_trading_with_observation_capture_on_stop(
            config,
            &reg,
            restore_startup(selection.clone(), candidate),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    driver.abort();
    assert!(
        matches!(result, Err(ApexesError::InvalidBacktestConfig { ref reason })
        if reason.contains("checkpoint database operation failed"))
    );
    assert!(health.is_engaged());
    assert_eq!(*observed.lock().unwrap(), vec![43]);
    assert_old_head(&fixture, &selection).await;
    pool.close().await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn no_completed_market_observation_does_not_publish_a_checkpoint() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let reg = registry_with(
        Arc::new(MockExecutionGateway::new()),
        Arc::new(MockMarketDataProvider {
            events: vec![sample_events().remove(0)],
            keep_open: true,
        }),
    );
    let (manager, config, health) = fixture.manager(&reg).await;
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = stop.send(());
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        apexes_live_trading::run_live_trading_with_observation_capture_on_stop(
            config,
            &reg,
            restore_startup(selection.clone(), Probe::new(0)),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap();
    driver.abort();
    assert!(result.is_err());
    assert!(health.is_engaged());
    assert_old_head(&fixture, &selection).await;
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn incomplete_fill_and_terminal_callback_prefixes_never_publish() {
    // Each interruption uses a separate owner and one-time claim. An
    // incomplete prefix may not be promoted merely by inventing an envelope.
    for interruption in 0..4 {
        let fixture = Fixture::new().await;
        let selection = fixture.seed().await;
        let gateway = Arc::new(MockExecutionGateway::new());
        let registry = registry_with(
            gateway.clone(),
            Arc::new(MockMarketDataProvider {
                events: vec![],
                keep_open: true,
            }),
        );
        let (mut manager, config, health) = fixture.manager(&registry).await;
        manager
            .claim_strategy_startup(selection.receipt(), OWNER_SHA256, &config.ems_postgres_url)
            .await
            .unwrap();
        health.startup_ready();
        let scope = uuid::Uuid::new_v4();
        let id = manager
            .submit_correlated_order(
                callback_probe_intent(false),
                &HashMap::new(),
                &config.initial_balances,
                apexes_oms::IntentLink {
                    run_scope: scope,
                    intent_id: IntentId(1),
                },
            )
            .await
            .unwrap();
        let exchange_id = manager.open_orders()[&id].exchange_order_id.clone();
        let (updates, _stream) = futures::channel::mpsc::unbounded();
        publish_mock_fill(
            &gateway,
            &updates,
            id,
            exchange_id.clone(),
            "interrupted-fill",
        );
        manager
            .apply_exchange_update(id, exchange_id, OrderStatus::Filled)
            .await
            .unwrap();
        assert!(manager.open_orders().is_empty());
        let staged = manager.durable_fill_batch().await.unwrap();
        assert_eq!(staged.len(), 1);
        let ids = [staged[0].id];
        if interruption >= 1 {
            manager.acknowledge_fill_batch(&ids, true).await;
        }
        if interruption >= 2 {
            manager.acknowledge_fill_notifications(&ids, true);
        }
        if interruption >= 3 {
            let batch = manager.ready_terminal_batch().await;
            assert_eq!(batch.len(), 1);
        }
        assert!(!health.is_engaged());
        let next = StateCursor {
            ordinal: cursor().ordinal + 1,
            observed_at: cursor().observed_at + chrono::TimeDelta::seconds(1),
        };
        let envelope = capture(&Probe::new(43), identity(), next).unwrap();
        assert!(
            matches!(
                manager
                    .publish_coherent_strategy_checkpoint(
                        selection.receipt(),
                        next,
                        scope,
                        1,
                        &envelope
                    )
                    .await,
                Err(apexes_oms::CheckpointStoreError::Boundary)
            ),
            "interruption {interruption}"
        );
        assert!(health.is_engaged());
        drop(manager);
        assert_old_head(&fixture, &selection).await;
        fixture.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn zero_fill_terminal_callback_is_included_in_the_coherent_boundary() {
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let gateway = Arc::new(MockExecutionGateway::new());
    let reg = registry_with(
        gateway.clone(),
        Arc::new(MockMarketDataProvider {
            events: sample_events(),
            keep_open: true,
        }),
    );
    let (manager, config, health) = fixture.manager(&reg).await;
    let pool = sqlx::PgPool::connect(&fixture.url).await.unwrap();
    let (updates, stream) = futures::channel::mpsc::unbounded();
    let driver = tokio::spawn(async move {
        let (id, exchange_id, _) = wait_for_callback_order(&pool, uuid::Uuid::nil(), false).await;
        gateway.set_exchange_state(id, OrderStatus::Cancelled);
        updates
            .unbounded_send(ExchangeUpdate {
                client_order_id: id,
                exchange_order_id: exchange_id,
                status: OrderStatus::Cancelled,
            })
            .unwrap();
    });
    let (stop, receiver) = oneshot::channel();
    let (report, receipt) = tokio::time::timeout(
        Duration::from_secs(10),
        run_live_trading_with_checkpoint_on_stop(
            config,
            &reg,
            execution_startup(selection, stop, false),
            manager,
            stream,
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    driver.await.unwrap();
    assert!(report.trades.is_empty());
    assert!(!health.is_engaged());
    assert_eq!(
        (
            receipt.order_count,
            receipt.fill_count,
            receipt.fill_sequence
        ),
        (1, 0, 0)
    );
    assert!(receipt.terminal_sequence > 0);
    let mut store = fixture.store().await;
    assert!(receipt.capture_sha256.is_none());
    store.verify_coherent_boundary(&receipt).await.unwrap();
    let bytes = store
        .load_strategy_checkpoint(Some(&receipt.checkpoint))
        .await
        .unwrap()
        .unwrap();
    let mut restored = Probe::new(0);
    apexes_strategy::checkpoint::restore(
        &mut restored,
        Probe::new(0),
        &bytes,
        &identity(),
        receipt.checkpoint.cursor,
    )
    .unwrap();
    assert_eq!(restored.value, 42 + 1 + 10 + 1000);
    drop(store);
    fixture.cleanup().await;
}

struct FlatTransitionFixture {
    selection: apexes_live_trading::CoherentRecoverySelection,
    instruments: Vec<Instrument>,
    opening: apexes_live_trading::OpeningWalletEvidence,
    window: apexes_live_trading::RecoveryEvidenceWindow,
    closing: apexes_live_trading::WalletEvidence,
    positions: apexes_live_trading::ClosingPositionEvidence,
    allocation: apexes_live_trading::OpeningTrackedAssetEvidence,
    fees: apexes_live_trading::ThirdAssetFeeEvidence,
    marks: apexes_live_trading::ClosingFeeMarkEvidence,
    conversion: apexes_live_trading::ConversionScopeEvidence,
    observations: apexes_live_trading::ObservationBoundaryEvidence,
}

impl FlatTransitionFixture {
    fn evidence(&self) -> apexes_live_trading::TransitionEvidence<'_> {
        apexes_live_trading::TransitionEvidence {
            instruments: &self.instruments,
            opening: &self.opening,
            window: &self.window,
            closing: &self.closing,
            inspected_at: self.closing.window.finished_at,
            positions: &self.positions,
            allocation: &self.allocation,
            fees: &self.fees,
            marks: &self.marks,
            conversion: &self.conversion,
            observations: &self.observations,
            registered_risk: None,
            market_monitor: None,
            margin_tiers: None,
        }
    }
}

async fn flat_boundary(fixture: &Fixture, previous: RecoverySelection) -> FlatTransitionFixture {
    use apexes_backtest::observation_receipt::{
        CompletedObservation, ObservationClock, ObservationReceipt,
    };
    use apexes_live_trading as live;
    let start = previous.receipt().cursor.observed_at;
    let at = start + chrono::TimeDelta::seconds(10);
    let mut event = sample_events().pop().unwrap();
    if let MarketEvent::Trade(trade) = &mut event {
        trade.timestamp = at;
    } else {
        panic!("trade fixture expected");
    }
    let registry = registry_with(
        Arc::new(MockExecutionGateway::new()),
        Arc::new(MockMarketDataProvider {
            events: vec![event.clone()],
            keep_open: true,
        }),
    );
    let (manager, mut config, _) = fixture.manager(&registry).await;
    config.initial_balances.insert("BTC".into(), dec!(0));
    let instruments = config.instruments.clone();
    let (stop, receiver) = oneshot::channel();
    let driver = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stop.send(());
    });
    let (_, boundary, _) = tokio::time::timeout(
        Duration::from_secs(5),
        live::run_live_trading_with_observation_capture_on_stop(
            config,
            &registry,
            restore_startup(previous.clone(), Probe::new(0)),
            manager,
            futures::stream::empty(),
            receiver,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    driver.await.unwrap();
    let expected_cursor = StateCursor {
        ordinal: previous.receipt().cursor.ordinal + 1,
        observed_at: at,
    };
    assert_eq!(boundary.checkpoint.cursor, expected_cursor);
    assert_eq!((boundary.order_count, boundary.fill_count), (0, 0));
    let point = |at| live::EvidenceWindow {
        started_at: at,
        finished_at: at,
    };
    let wallet = |at| live::WalletEvidence {
        window: point(at),
        owner_fingerprint: OWNER_SHA256.into(),
        venue: Venue::Binance,
        asset_class: AssetClass::Spot,
        account_wide_complete: true,
        balances: HashMap::from([
            (
                "BTC".into(),
                live::spot_recovery::SpotWalletBalance {
                    free: dec!(0),
                    locked: dec!(0),
                },
            ),
            (
                "USDT".into(),
                live::spot_recovery::SpotWalletBalance {
                    free: dec!(100000),
                    locked: dec!(0),
                },
            ),
        ]),
    };
    let opening_at = start + chrono::TimeDelta::seconds(3);
    let closing_at = at + chrono::TimeDelta::seconds(2);
    let opening = live::OpeningWalletEvidence {
        predecessor: previous.receipt().clone(),
        wallet: wallet(opening_at),
    };
    let window = live::RecoveryEvidenceWindow {
        terminal_window: point(at),
        terminal_owner_fingerprint: OWNER_SHA256.into(),
        terminal_evidence_complete: true,
        terminal_orders: vec![],
        open_orders: live::OpenOrderCoverage {
            window: point(at + chrono::TimeDelta::seconds(1)),
            venue: Venue::Binance,
            asset_class: AssetClass::Spot,
            owner_fingerprint: OWNER_SHA256.into(),
            account_wide_complete: true,
            open_order_count: 0,
            foreign_order_count: 0,
        },
    };
    let scope = vec![(Venue::Binance, "BTCUSDT".into())];
    let history = ObservationReceipt::build(
        previous.receipt().cursor,
        expected_cursor,
        ObservationClock::SourceAvailability,
        &scope,
        &[CompletedObservation {
            cursor: expected_cursor,
            event,
        }],
    )
    .unwrap();
    FlatTransitionFixture {
        selection: live::CoherentRecoverySelection::new(previous.clone(), boundary.clone())
            .unwrap(),
        instruments,
        opening,
        window,
        closing: wallet(closing_at),
        positions: live::ClosingPositionEvidence {
            version: 1,
            owner_fingerprint: OWNER_SHA256.into(),
            boundary: boundary.clone(),
            captured_at: closing_at,
            symbols: vec!["BTCUSDT".into()],
            positions: vec![],
        },
        allocation: live::OpeningTrackedAssetEvidence {
            version: 1,
            predecessor: previous.receipt().clone(),
            owner_fingerprint: OWNER_SHA256.into(),
            captured_at: opening_at,
            balances: vec![("BTC".into(), dec!(0)), ("USDT".into(), dec!(100000))],
        },
        fees: live::ThirdAssetFeeEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: OWNER_SHA256.into(),
            rates: vec![],
        },
        marks: live::ClosingFeeMarkEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: OWNER_SHA256.into(),
            rates: vec![],
            symbols: vec![],
        },
        conversion: live::ConversionScopeEvidence {
            version: 1,
            boundary: boundary.clone(),
            owner_fingerprint: OWNER_SHA256.into(),
            instruments: vec![live::EvidenceInstrumentScope {
                venue: Venue::Binance,
                symbol: "BTCUSDT".into(),
                base: "BTC".into(),
                quote: "USDT".into(),
                role: live::EvidenceInstrumentRole::StrategyInventory,
            }],
        },
        observations: live::ObservationBoundaryEvidence::new(
            OWNER_SHA256.into(),
            previous.receipt().clone(),
            boundary,
            history,
        )
        .unwrap(),
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn independently_preserved_flat_successor_boundaries_requalify_without_claim_mutation() {
    use apexes_live_trading as live;
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let a = flat_boundary(&first, first.seed().await).await;
    let mut first_store = first.store().await;
    let boundary = a.selection.receipt();
    let bytes = first_store
        .load_strategy_checkpoint(Some(&boundary.checkpoint))
        .await
        .unwrap()
        .unwrap();
    // Seed a separate disposable test store, not a second claim in the first.
    let mut second_store = second.store().await;
    let receipt = second_store
        .publish_strategy_checkpoint(
            None,
            &boundary.checkpoint.identity,
            boundary.checkpoint.cursor,
            &bytes,
        )
        .await
        .unwrap();
    assert_eq!(receipt, boundary.checkpoint);
    drop(second_store);
    let b = flat_boundary(
        &second,
        RecoverySelection::new(OWNER_SHA256.into(), receipt, HashMap::new()).unwrap(),
    )
    .await;
    let mut second_store = second.store().await;
    let ae = a.evidence();
    let be = b.evidence();
    let aw = a
        .selection
        .inspect_offline_transition(
            &mut first_store,
            &first.url,
            (Probe::new(0), Probe::new(0)),
            &ae,
        )
        .await
        .unwrap();
    let bw = b
        .selection
        .inspect_offline_transition(
            &mut second_store,
            &second.url,
            (Probe::new(0), Probe::new(0)),
            &be,
        )
        .await
        .unwrap();
    live::inspect_offline_successor(
        live::SuccessorRun {
            selection: &a.selection,
            store: &mut first_store,
            ems_postgres_url: &first.url,
            strategies: (Probe::new(0), Probe::new(0)),
            evidence: &ae,
            witness: &aw,
        },
        live::SuccessorRun {
            selection: &b.selection,
            store: &mut second_store,
            ems_postgres_url: &second.url,
            strategies: (Probe::new(0), Probe::new(0)),
            evidence: &be,
            witness: &bw,
        },
    )
    .await
    .unwrap();
    // Even an otherwise valid witness from the other run cannot substitute.
    assert!(
        live::inspect_offline_successor(
            live::SuccessorRun {
                selection: &a.selection,
                store: &mut first_store,
                ems_postgres_url: &first.url,
                strategies: (Probe::new(0), Probe::new(0)),
                evidence: &ae,
                witness: &aw,
            },
            live::SuccessorRun {
                selection: &b.selection,
                store: &mut second_store,
                ems_postgres_url: &second.url,
                strategies: (Probe::new(0), Probe::new(0)),
                evidence: &be,
                witness: &aw,
            },
        )
        .await
        .is_err()
    );
    assert_eq!(first.claim_count().await, 1);
    assert_eq!(second.claim_count().await, 1);
    first_store
        .verify_coherent_boundary(a.selection.receipt())
        .await
        .unwrap();
    second_store
        .verify_coherent_boundary(b.selection.receipt())
        .await
        .unwrap();
    drop(first_store);
    drop(second_store);
    first.cleanup().await;
    second.cleanup().await;
}

struct CollectionSource {
    terminal: Option<apexes_oms::TerminalOrderEvidence>,
    owner: String,
    open: usize,
    wallet: Option<HashMap<String, apexes_oms::SpotEvidenceBalance>>,
    calls: std::sync::Mutex<Vec<&'static str>>,
}

#[async_trait::async_trait]
impl apexes_oms::SpotEvidenceSource for CollectionSource {
    fn owner_fingerprint(&self, _: &serde_json::Value, _: &[Instrument]) -> Result<String, String> {
        Ok(self.owner.clone())
    }
    async fn terminal_order(&self, _: &Order) -> Result<apexes_oms::TerminalOrderEvidence, String> {
        self.calls.lock().unwrap().push("terminal");
        self.terminal
            .clone()
            .ok_or("unexpected terminal lookup".into())
    }
    async fn account_open_order_count(&self) -> Result<usize, String> {
        self.calls.lock().unwrap().push("open");
        Ok(self.open)
    }
    async fn account_wallet(
        &self,
    ) -> Result<HashMap<String, apexes_oms::SpotEvidenceBalance>, String> {
        self.calls.lock().unwrap().push("wallet");
        self.wallet.clone().ok_or("wallet unavailable".into())
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn read_only_collection_refuses_owner_mismatch_open_orders_and_incomplete_wallet() {
    use apexes_live_trading as live;
    use apexes_oms::SpotEvidenceBalance;
    let fixture = Fixture::new().await;
    let a = flat_boundary(&fixture, fixture.seed().await).await;
    let mut store = fixture.store().await;
    let strategy = serde_json::json!({"RustNative":{"name":"fixture"}});
    let balances = HashMap::from([
        (
            "BTC".into(),
            SpotEvidenceBalance {
                free: dec!(0),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            SpotEvidenceBalance {
                free: dec!(100000),
                locked: dec!(0),
            },
        ),
    ]);
    let mut source = CollectionSource {
        terminal: None,
        owner: "0".repeat(64),
        open: 0,
        wallet: Some(balances.clone()),
        calls: Default::default(),
    };
    assert!(matches!(
        a.selection
            .collect_spot_evidence(&mut store, &fixture.url, &source, &strategy, &a.instruments)
            .await,
        Err(live::SpotCollectionError::Owner)
    ));
    assert!(source.calls.lock().unwrap().is_empty());
    source.owner = OWNER_SHA256.into();
    source.open = 1;
    assert!(matches!(
        a.selection
            .collect_spot_evidence(&mut store, &fixture.url, &source, &strategy, &a.instruments)
            .await,
        Err(live::SpotCollectionError::Inspection(
            live::CoherentInspectionError::EvidenceWindow(live::EvidenceWindowError::OpenOrders)
        ))
    ));
    assert_eq!(*source.calls.lock().unwrap(), vec!["open"]);
    source.calls.lock().unwrap().clear();
    source.open = 0;
    source.wallet = None;
    assert!(matches!(
        a.selection
            .collect_spot_evidence(&mut store, &fixture.url, &source, &strategy, &a.instruments)
            .await,
        Err(live::SpotCollectionError::Source)
    ));
    source.wallet = Some(HashMap::from([("USDT".into(), balances["USDT"])]));
    assert!(
        a.selection
            .collect_spot_evidence(&mut store, &fixture.url, &source, &strategy, &a.instruments)
            .await
            .is_err()
    );
    source.wallet = Some(balances);
    let result = a
        .selection
        .collect_spot_evidence(&mut store, &fixture.url, &source, &strategy, &a.instruments)
        .await
        .unwrap();
    let receipt = a
        .selection
        .collect_spot_evidence_receipt(&mut store, &fixture.url, &source, &strategy, &a.instruments)
        .await
        .unwrap();
    let digest = receipt.sha256().to_owned();
    let dir = std::env::temp_dir().join(format!("apexes-spot-collection-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("collection.json");
    receipt.write_new(&path).unwrap();
    assert!(receipt.write_new(&path).is_err());
    assert!(live::SpotCollectionReceipt::read(&path, &"0".repeat(64)).is_err());
    let reopened = live::SpotCollectionReceipt::read(&path, &digest).unwrap();
    let historical = a
        .selection
        .inspect_spot_collection_receipt(
            &mut store,
            &fixture.url,
            &a.instruments,
            &reopened,
            &digest,
        )
        .await
        .unwrap();
    assert_eq!(historical.inspected_at, receipt.collected_at());
    assert_eq!(historical.wallet.balances, result.wallet.balances);
    assert!(
        a.selection
            .inspect_spot_collection_receipt(&mut store, &fixture.url, &[], &reopened, &digest,)
            .await
            .is_err()
    );
    assert!(
        a.selection
            .inspect_spot_collection_receipt(
                &mut store,
                &fixture.url,
                &a.instruments,
                &reopened,
                &"0".repeat(64),
            )
            .await
            .is_err()
    );
    let bytes = std::fs::read(&path).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    changed["body"]["wallet"]["balances"][0][1]["free"] = serde_json::json!("123");
    let altered = dir.join("altered.json");
    std::fs::write(&altered, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert!(live::SpotCollectionReceipt::read(&altered, &digest).is_err());
    let noncanonical = dir.join("noncanonical.json");
    let mut padded = bytes;
    padded.push(b' ');
    std::fs::write(&noncanonical, padded).unwrap();
    assert!(live::SpotCollectionReceipt::read(&noncanonical, &digest).is_err());
    std::fs::remove_dir_all(dir).unwrap();
    assert!(result.window.terminal_orders.is_empty());
    assert_eq!(result.wallet.balances["BTC"].free, dec!(0));
    assert!(result.window.open_orders.window.finished_at <= result.wallet.window.started_at);
    assert!(result.wallet.window.finished_at <= result.inspected_at);
    assert_eq!(fixture.claim_count().await, 1);
    store
        .verify_coherent_boundary(a.selection.receipt())
        .await
        .unwrap();
    drop(store);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL"]
async fn opening_collection_requires_unclaimed_checkpoint_and_independent_allocation() {
    use apexes_live_trading as live;
    use apexes_oms::SpotEvidenceBalance;
    let fixture = Fixture::new().await;
    let selection = fixture.seed().await;
    let mut store = fixture.store().await;
    let instruments = base_config().instruments;
    let strategy = serde_json::json!({"RustNative":{"name":"fixture"}});
    let balances = HashMap::from([
        (
            "BTC".into(),
            SpotEvidenceBalance {
                free: dec!(0),
                locked: dec!(0),
            },
        ),
        (
            "USDT".into(),
            SpotEvidenceBalance {
                free: dec!(100000),
                locked: dec!(0),
            },
        ),
        (
            "BNB".into(),
            SpotEvidenceBalance {
                free: dec!(3),
                locked: dec!(0),
            },
        ),
    ]);
    let tracked = vec![("BTC".into(), dec!(0)), ("USDT".into(), dec!(100000))];
    let mut source = CollectionSource {
        terminal: None,
        owner: "0".repeat(64),
        open: 0,
        wallet: Some(balances.clone()),
        calls: Default::default(),
    };
    assert!(matches!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await,
        Err(live::OpeningCollectionError::Owner)
    ));
    assert!(source.calls.lock().unwrap().is_empty());
    source.owner = OWNER_SHA256.into();
    assert!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                "wrong-db",
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await
            .is_err()
    );
    assert!(source.calls.lock().unwrap().is_empty());
    source.open = 1;
    assert!(matches!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await,
        Err(live::OpeningCollectionError::OpenOrders)
    ));
    assert_eq!(*source.calls.lock().unwrap(), vec!["open"]);
    source.open = 0;
    source.calls.lock().unwrap().clear();
    let wrong = vec![("BTC".into(), dec!(0)), ("USDT".into(), dec!(99999))];
    assert!(matches!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &wrong,
            )
            .await,
        Err(live::OpeningCollectionError::Allocation(_))
    ));
    source.wallet.as_mut().unwrap().remove("BTC");
    assert!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await
            .is_err()
    );
    source.wallet = Some(balances.clone());
    source
        .wallet
        .as_mut()
        .unwrap()
        .get_mut("BNB")
        .unwrap()
        .locked = dec!(1);
    assert!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await
            .is_err()
    );
    source.wallet = Some(balances);
    source.calls.lock().unwrap().clear();
    let result = selection
        .collect_opening_spot_evidence(
            &mut store,
            &fixture.url,
            &source,
            &strategy,
            &instruments,
            &tracked,
        )
        .await
        .unwrap();
    assert_eq!(
        *source.calls.lock().unwrap(),
        vec!["open", "wallet", "open"]
    );
    assert_eq!(result.opening.predecessor, *selection.receipt());
    assert_eq!(result.allocation.balances, tracked);
    assert_eq!(result.opening.wallet.balances["BNB"].free, dec!(3));
    assert!(result.open_before.finished_at <= result.opening.wallet.window.started_at);
    assert!(result.opening.wallet.window.finished_at <= result.open_after.started_at);
    assert!(result.open_after.finished_at <= result.inspected_at);
    assert_eq!(fixture.claim_count().await, 0);
    store
        .inspect_opening_checkpoint(selection.receipt(), OWNER_SHA256, &fixture.url)
        .await
        .unwrap();
    drop(store);
    let a = flat_boundary(&fixture, selection.clone()).await;
    let mut store = fixture.store().await;
    source.calls.lock().unwrap().clear();
    assert!(
        selection
            .collect_opening_spot_evidence(
                &mut store,
                &fixture.url,
                &source,
                &strategy,
                &instruments,
                &tracked,
            )
            .await
            .is_err()
    );
    assert!(source.calls.lock().unwrap().is_empty());
    assert_eq!(fixture.claim_count().await, 1);
    store
        .verify_coherent_boundary(a.selection.receipt())
        .await
        .unwrap();
    drop(store);
    fixture.cleanup().await;
}
