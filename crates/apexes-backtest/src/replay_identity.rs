//! F03.04: opt-in run-local simulated matching IDs and report comparison.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayIdentityPolicy {
    pub namespace: Uuid,
}

#[derive(Default)]
pub(crate) struct MatchingIds {
    policy: Option<ReplayIdentityPolicy>,
    next: u128,
}

impl MatchingIds {
    pub(crate) fn new(policy: Option<ReplayIdentityPolicy>) -> Self {
        Self { policy, next: 0 }
    }

    fn allocate(&mut self, domain: &[u8]) -> Uuid {
        let Some(policy) = self.policy else {
            return Uuid::new_v4();
        };
        let mut digest = Sha256::new();
        digest.update(b"apexes-simulated-matching-id-v1\0");
        digest.update(domain);
        digest.update(policy.namespace.as_bytes());
        digest.update(self.next.to_be_bytes());
        // A run cannot execute 2^128 allocations. Counter is run-local.
        self.next = self.next.wrapping_add(1);
        let digest = digest.finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    pub(crate) fn order(&mut self) -> apexes_core::OrderId {
        apexes_core::OrderId(self.allocate(b"order\0"))
    }

    pub(crate) fn fill(&mut self) -> apexes_core::FillId {
        apexes_core::FillId(self.allocate(b"fill\0"))
    }
}

/// A sealed input must also be ordered by causal observation time.
/// A bar is available at its close, not its open. Legacy replay uses
/// the open timestamp without event-time latency, so refuse that
/// combination for the opt-in identity policy.
pub(crate) fn validate_observation_order(
    events: &[apexes_core::MarketEvent],
    event_time: bool,
) -> Result<(), String> {
    let mut previous = None;
    for (index, event) in events.iter().enumerate() {
        let at = match event {
            apexes_core::MarketEvent::Bar(bar) => {
                if !event_time {
                    return Err(
                        "deterministic replay with bars requires explicit event-time latency"
                            .into(),
                    );
                }
                if bar.close_time <= bar.open_time {
                    return Err(format!("invalid bar interval at event {index}"));
                }
                bar.close_time
            }
            _ => crate::market_data_source::event_timestamp(event),
        };
        if previous.is_some_and(|last| at < last) {
            return Err(format!(
                "causal observation time regressed at event {index}; sealed input order cannot be changed"
            ));
        }
        previous = Some(at);
    }
    Ok(())
}

/// Canonical comparison of a serialized report, not proof that all
/// inputs or externally supplied strategy decisions were identical.
pub fn canonical_report_sha256(
    report: &crate::BacktestReport,
) -> Result<String, serde_json::Error> {
    let mut value = serde_json::to_value(report)?;
    value.sort_all_objects();
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocator_is_run_local_domain_separated_and_opt_in() {
        let policy = ReplayIdentityPolicy {
            namespace: Uuid::from_u128(1),
        };
        let mut a = MatchingIds::new(Some(policy));
        let mut b = MatchingIds::new(Some(policy));
        let order = a.order();
        assert_eq!(order, b.order());
        let fill = a.fill();
        assert_eq!(fill, b.fill());
        assert_ne!(order.0, fill.0);
        assert_ne!(order, a.order());
        assert_ne!(
            order,
            MatchingIds::new(Some(ReplayIdentityPolicy {
                namespace: Uuid::from_u128(2)
            }))
            .order()
        );
        assert_ne!(
            MatchingIds::default().order(),
            MatchingIds::default().order()
        );
        assert_eq!(order.0.get_version_num(), 8);
    }
}
