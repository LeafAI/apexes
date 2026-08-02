//! Verifies `IbkrExecutionGateway` genuinely establishes a real TCP
//! connection and sends correctly wire-protocol-encoded messages, by
//! running a hand-written mock TCP server that speaks the same
//! length-prefixed/null-separated protocol -- no real TWS/IB Gateway
//! process required. This is the IBKR-protocol equivalent of
//! `apexes-gateway-binance`'s `wiremock`-based tests, but hand-rolled
//! at the TCP level since IBKR's protocol is not HTTP.

use apexes_core::{AssetClass, Order, OrderId, OrderStatus, OrderType, Side, Venue};
use apexes_gateway_ibkr::{IbkrExecutionGateway, IbkrGatewayConfig};
use apexes_oms::ExecutionGateway;
use chrono::Utc;
use rust_decimal_macros::dec;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use uuid::Uuid;

fn sample_order() -> Order {
    Order {
        id: OrderId(Uuid::new_v4()),
        exchange_order_id: None,
        symbol: "AAPL".to_string(),
        venue: Venue::Nasdaq,
        asset_class: AssetClass::Equity,
        side: Side::Buy,
        order_type: OrderType::Limit,
        quantity: dec!(100),
        price: Some(dec!(150.25)),
        stop_price: None,
        leverage: None,
        margin_mode: None,
        filled_quantity: dec!(0),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Decodes exactly one IBKR wire-protocol message from a raw byte
/// buffer, panicking with a descriptive message if the buffer does
/// not contain a complete message -- used only in this test file to
/// assert on what the mock server actually received.
fn decode_one_message(buf: &[u8]) -> Vec<String> {
    apexes_gateway_ibkr::protocol::decode_message(buf)
        .expect("mock server should have received one complete IBKR wire-protocol message")
        .0
}

#[tokio::test]
async fn connect_establishes_a_real_tcp_connection_to_a_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (_socket, _peer_addr) = listener.accept().await.unwrap();
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    let result = gateway.connect().await;
    assert!(result.is_ok());

    server_task.await.unwrap();
}

#[tokio::test]
async fn submit_order_sends_a_correctly_encoded_place_order_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let order = sample_order();
    let expected_order_id = order.id.0.to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _peer_addr) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        decode_one_message(&buf[..n])
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    gateway.connect().await.unwrap();
    let outcome = gateway.submit_order(&order).await;

    let received_fields = server_task.await.unwrap();
    assert_eq!(received_fields[0], "PLACE_ORDER");
    assert_eq!(received_fields[1], expected_order_id);
    assert_eq!(received_fields[2], "AAPL");
    assert_eq!(received_fields[3], "BUY");
    assert_eq!(received_fields[4], "LMT");
    assert_eq!(received_fields[5], "100");

    // The gateway sent the message successfully but never received a
    // parsed response back (the mock server above only reads, it does
    // not reply) -- so the outcome must be Uncertain, never a
    // fabricated Accepted.
    assert!(matches!(
        outcome,
        apexes_oms::GatewayOutcome::Uncertain { .. }
    ));
}

#[tokio::test]
async fn cancel_order_sends_a_correctly_encoded_cancel_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let order = sample_order();
    let expected_order_id = order.id.0.to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _peer_addr) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        decode_one_message(&buf[..n])
    });

    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        client_id: 1,
    });
    gateway.connect().await.unwrap();
    gateway.cancel_order(&order).await;

    let received_fields = server_task.await.unwrap();
    assert_eq!(received_fields[0], "CANCEL_ORDER");
    assert_eq!(received_fields[1], expected_order_id);
}

#[tokio::test]
async fn submit_order_before_connect_returns_uncertain_not_connected() {
    let gateway = IbkrExecutionGateway::new(IbkrGatewayConfig {
        host: "127.0.0.1".to_string(),
        port: 65535,
        client_id: 1,
    });
    let outcome = gateway.submit_order(&sample_order()).await;
    match outcome {
        apexes_oms::GatewayOutcome::Uncertain { reason } => {
            assert!(reason.contains("not connected"));
        }
        other => panic!("expected Uncertain with a not-connected reason, got {other:?}"),
    }
}
