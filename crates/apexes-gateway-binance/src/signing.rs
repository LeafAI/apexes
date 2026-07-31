//! Binance's required request-signing scheme: HMAC-SHA256 over a
//! request's full query string, using the account's API secret as the
//! HMAC key, hex-encoded. Pure function -- deliberately has no HTTP
//! dependency so it's covered by fast, network-free unit tests
//! independent of the `wiremock`-based tests (Task 15) that verify the
//! resulting HTTP request shape end-to-end.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Computes Binance's required HMAC-SHA256 signature over `query`
/// (a request's query string, WITHOUT the `signature` parameter itself
/// -- that parameter is appended by the caller using this function's
/// return value), using `secret` as the HMAC key. Returns the
/// lowercase-hex-encoded signature, matching Binance's documented
/// format.
pub fn sign_query_string(secret: &str, query: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts a key of any length");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer test using the exact example from Binance's public
    /// API documentation (`developers.binance.com`'s signed-endpoint
    /// examples page), so this test's expected value is independently
    /// verifiable against the vendor's own published example rather
    /// than only self-consistent with this implementation.
    #[test]
    fn matches_binance_documented_example() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let signature = sign_query_string(secret, query);
        assert_eq!(
            signature,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn different_queries_produce_different_signatures() {
        let secret = "test-secret";
        let sig1 = sign_query_string(secret, "symbol=BTCUSDT&quantity=1");
        let sig2 = sign_query_string(secret, "symbol=BTCUSDT&quantity=2");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn different_secrets_produce_different_signatures_for_the_same_query() {
        let query = "symbol=BTCUSDT&quantity=1";
        let sig1 = sign_query_string("secret-one", query);
        let sig2 = sign_query_string("secret-two", query);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn signature_is_deterministic() {
        let secret = "test-secret";
        let query = "symbol=BTCUSDT&quantity=1";
        assert_eq!(
            sign_query_string(secret, query),
            sign_query_string(secret, query)
        );
    }
}
