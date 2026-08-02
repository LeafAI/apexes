//! XTP protocol request-shape types. Field names/structure derived
//! from the publicly-maintained `ztsec/xtp_api_java` GitHub repository
//! (the only publicly accessible XTP protocol reference found without
//! a real account -- see
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! §一 for the full research trail). These types may need correction
//! once real account access is available.

/// Which Shanghai/Shenzhen A-share market a symbol trades on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpMarket {
    ShanghaiA,
    ShenzhenA,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XtpPriceType {
    Limit,
    Market,
}

/// The XTP order-insert request shape (field names per
/// `ztsec/xtp_api_java`'s public `doc`/`xtp_api_demo` directories).
#[derive(Debug, Clone, PartialEq)]
pub struct XtpOrderInsertRequest {
    pub ticker: String,
    pub market: XtpMarket,
    pub side: XtpSide,
    pub price_type: XtpPriceType,
    pub price: f64,
    pub quantity: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xtp_market_variants_are_distinct() {
        assert_ne!(XtpMarket::ShanghaiA, XtpMarket::ShenzhenA);
    }

    #[test]
    fn xtp_side_variants_are_distinct() {
        assert_ne!(XtpSide::Buy, XtpSide::Sell);
    }

    #[test]
    fn xtp_price_type_variants_are_distinct() {
        assert_ne!(XtpPriceType::Limit, XtpPriceType::Market);
    }

    #[test]
    fn xtp_order_insert_request_is_constructible_and_comparable() {
        let a = XtpOrderInsertRequest {
            ticker: "600000".to_string(),
            market: XtpMarket::ShanghaiA,
            side: XtpSide::Buy,
            price_type: XtpPriceType::Limit,
            price: 10.5,
            quantity: 100,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
