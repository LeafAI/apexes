//! F04.05: bounded, versioned checkpoint contract; not a durable store or
//! proof of atomic strategy/OMS recovery.
use crate::Strategy;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_ENVELOPE_BYTES: usize = 7 * MAX_PAYLOAD_BYTES;
const VERSION: u32 = 1;
const MAX_ID_BYTES: usize = 256;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CheckpointError {
    #[error("strategy checkpointing is unsupported")]
    Unsupported,
    #[error("checkpoint exceeds size limit")]
    TooLarge,
    #[error("invalid checkpoint encoding or metadata")]
    Invalid,
    #[error("unsupported checkpoint version")]
    Version,
    #[error("checkpoint integrity mismatch")]
    Integrity,
    #[error("checkpoint identity or payload schema mismatch")]
    Identity,
    #[error("checkpoint observation cursor mismatch")]
    Cursor,
    #[error("invalid strategy checkpoint payload")]
    Payload,
}

/// Caller-owned identity; scope binds the account/session and instrument
/// universe. Configuration fingerprint is a lowercase SHA-256 hex digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateIdentity {
    pub strategy: String,
    pub revision: String,
    pub config_sha256: String,
    pub scope: String,
    pub payload_version: u32,
}

impl StateIdentity {
    fn valid(&self) -> bool {
        [&self.strategy, &self.revision, &self.scope]
            .into_iter()
            .all(|s| {
                !s.trim().is_empty() && s.len() <= MAX_ID_BYTES && !s.chars().any(char::is_control)
            })
            && self.payload_version != 0
            && self.config_sha256.len() == 64
            && self
                .config_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

/// Independently selected committed observation boundary, not a wall clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateCursor {
    pub ordinal: u64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u32,
    identity: StateIdentity,
    cursor: StateCursor,
    payload: String,
}

impl Body {
    fn validate(&self) -> Result<(), CheckpointError> {
        if self.version != VERSION {
            return Err(CheckpointError::Version);
        }
        if self.payload.len() > MAX_PAYLOAD_BYTES {
            return Err(CheckpointError::TooLarge);
        }
        if !self.identity.valid() || self.cursor.ordinal == 0 {
            return Err(CheckpointError::Invalid);
        }
        Ok(())
    }

    fn digest(&self) -> Result<String, CheckpointError> {
        let bytes = serde_json::to_vec(self).map_err(|_| CheckpointError::Invalid)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    body: Body,
    sha256: String,
}

/// Only constructed by successful envelope verification; not a claim that
/// the strategy-specific payload has been interpreted or is semantically valid.
pub struct ValidatedState<'a> {
    body: &'a Body,
}

impl ValidatedState<'_> {
    pub fn payload(&self) -> &str {
        &self.body.payload
    }
    pub fn identity(&self) -> &StateIdentity {
        &self.body.identity
    }
    pub fn cursor(&self) -> StateCursor {
        self.body.cursor
    }
}

fn verify_schema(strategy: &dyn Strategy, identity: &StateIdentity) -> Result<(), CheckpointError> {
    match strategy.checkpoint_schema_version() {
        None => Err(CheckpointError::Unsupported),
        Some(version) if version != 0 && version == identity.payload_version => Ok(()),
        Some(_) => Err(CheckpointError::Identity),
    }
}

/// Capture the caller-selected observation boundary. Persistence/publication
/// and coordination with execution journals are the caller's responsibility.
pub fn capture(
    strategy: &dyn Strategy,
    identity: StateIdentity,
    cursor: StateCursor,
) -> Result<Vec<u8>, CheckpointError> {
    verify_schema(strategy, &identity)?;
    let body = Body {
        version: VERSION,
        identity,
        cursor,
        payload: strategy.checkpoint_payload()?,
    };
    body.validate()?;
    let sha256 = body.digest()?;
    let bytes =
        serde_json::to_vec(&Envelope { body, sha256 }).map_err(|_| CheckpointError::Invalid)?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(CheckpointError::TooLarge);
    }
    Ok(bytes)
}

/// Verify before calling any strategy restore hook. Restore into a newly
/// constructed, isolated candidate, then replace the active strategy only on
/// success. The caller must ensure the candidate shares no mutable strategy
/// state or external side effects with the active instance. Expected identity
/// and cursor must come from trusted recovery metadata, not this checkpoint.
pub fn restore<S: Strategy>(
    strategy: &mut S,
    mut candidate: S,
    bytes: &[u8],
    expected_identity: &StateIdentity,
    expected_cursor: StateCursor,
) -> Result<(), CheckpointError> {
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(CheckpointError::TooLarge);
    }
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|_| CheckpointError::Invalid)?;
    // The wire representation itself is canonical; do not accept equivalent
    // but altered encodings that preserve the semantic body digest.
    if serde_json::to_vec(&envelope).map_err(|_| CheckpointError::Invalid)? != bytes {
        return Err(CheckpointError::Invalid);
    }
    let body = &envelope.body;
    body.validate()?;
    if body.digest()? != envelope.sha256 {
        return Err(CheckpointError::Integrity);
    }
    if &body.identity != expected_identity {
        return Err(CheckpointError::Identity);
    }
    if body.cursor != expected_cursor {
        return Err(CheckpointError::Cursor);
    }
    verify_schema(strategy, &body.identity)?;
    verify_schema(&candidate, &body.identity)?;
    candidate.restore_checkpoint(&ValidatedState { body })?;
    *strategy = candidate;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrderIntent, StrategyContext};
    use apexes_core::MarketEvent;

    #[derive(Default)]
    struct Counter {
        value: u64,
        calls: u64,
    }
    impl Strategy for Counter {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            Vec::new()
        }
        fn checkpoint_schema_version(&self) -> Option<u32> {
            Some(1)
        }
        fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
            Ok(self.value.to_string())
        }
        fn restore_checkpoint(
            &mut self,
            state: &ValidatedState<'_>,
        ) -> Result<(), CheckpointError> {
            let value = state
                .payload()
                .parse()
                .map_err(|_| CheckpointError::Payload)?;
            self.value = value;
            self.calls += 1;
            Ok(())
        }
    }

    fn identity() -> StateIdentity {
        StateIdentity {
            strategy: "counter".into(),
            revision: "v1".into(),
            config_sha256: "a".repeat(64),
            scope: "paper:account-a:BTCUSDT".into(),
            payload_version: 1,
        }
    }
    fn cursor() -> StateCursor {
        StateCursor {
            ordinal: 42,
            observed_at: DateTime::from_timestamp(1_700_000_000, 123).unwrap(),
        }
    }
    fn sample() -> Vec<u8> {
        capture(
            &Counter {
                value: 19,
                calls: 0,
            },
            identity(),
            cursor(),
        )
        .unwrap()
    }

    #[test]
    fn boxed_round_trip_is_deterministic() {
        let mut target: Box<dyn Strategy> = Box::new(Counter::default());
        let bytes = sample();
        assert_eq!(sample(), bytes);
        restore(
            &mut target,
            Box::new(Counter::default()),
            &bytes,
            &identity(),
            cursor(),
        )
        .unwrap();
        assert_eq!(capture(&target, identity(), cursor()).unwrap(), bytes);
    }

    #[test]
    fn mismatch_and_corruption_never_invoke_restore() {
        let mut target = Counter {
            value: 77,
            calls: 0,
        };
        let bytes = sample();
        let mut other = identity();
        other.scope = "live:account-b:BTCUSDT".into();
        assert_eq!(
            restore(&mut target, Counter::default(), &bytes, &other, cursor()),
            Err(CheckpointError::Identity)
        );
        let mut other = identity();
        other.payload_version = 2;
        assert_eq!(
            restore(&mut target, Counter::default(), &bytes, &other, cursor()),
            Err(CheckpointError::Identity)
        );
        let mut other_cursor = cursor();
        other_cursor.ordinal += 1;
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &bytes,
                &identity(),
                other_cursor
            ),
            Err(CheckpointError::Cursor)
        );
        let mut envelope: Envelope = serde_json::from_slice(&bytes).unwrap();
        envelope.body.payload = "20".into();
        let changed = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &changed,
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Integrity)
        );
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &bytes[..bytes.len() - 1],
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Invalid)
        );
        assert_eq!((target.value, target.calls), (77, 0));
    }

    #[test]
    fn strict_version_size_and_metadata() {
        let mut target = Counter::default();
        let mut envelope: Envelope = serde_json::from_slice(&sample()).unwrap();
        envelope.body.version = 2;
        let bytes = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &bytes,
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Version)
        );
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &vec![0; MAX_ENVELOPE_BYTES + 1],
                &identity(),
                cursor()
            ),
            Err(CheckpointError::TooLarge)
        );
        let mut id = identity();
        id.config_sha256 = "bad".into();
        assert_eq!(
            capture(&target, id, cursor()),
            Err(CheckpointError::Invalid)
        );
        let mut c = cursor();
        c.ordinal = 0;
        assert_eq!(
            capture(&target, identity(), c),
            Err(CheckpointError::Invalid)
        );
        let mut envelope: serde_json::Value = serde_json::from_slice(&sample()).unwrap();
        envelope["unexpected"] = true.into();
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &serde_json::to_vec(&envelope).unwrap(),
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Invalid)
        );
        assert_eq!(target.calls, 0);
    }

    #[test]
    fn semantic_payload_failure_keeps_state() {
        let mut envelope: Envelope = serde_json::from_slice(&sample()).unwrap();
        envelope.body.payload = "not a counter".into();
        envelope.sha256 = envelope.body.digest().unwrap();
        let mut target = Counter { value: 9, calls: 0 };
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &serde_json::to_vec(&envelope).unwrap(),
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Payload)
        );
        assert_eq!((target.value, target.calls), (9, 0));
    }

    #[test]
    fn independently_declared_schema_and_timestamp_are_checked() {
        let mut target = Counter { value: 7, calls: 0 };
        let mut id = identity();
        id.payload_version = 2;
        assert_eq!(
            capture(&target, id.clone(), cursor()),
            Err(CheckpointError::Identity)
        );
        let mut envelope: Envelope = serde_json::from_slice(&sample()).unwrap();
        envelope.body.identity = id.clone();
        envelope.sha256 = envelope.body.digest().unwrap();
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &serde_json::to_vec(&envelope).unwrap(),
                &id,
                cursor()
            ),
            Err(CheckpointError::Identity)
        );
        let mut c = cursor();
        c.observed_at += chrono::Duration::nanoseconds(1);
        assert_eq!(
            restore(&mut target, Counter::default(), &sample(), &identity(), c),
            Err(CheckpointError::Cursor)
        );
        assert_eq!((target.value, target.calls), (7, 0));
    }

    #[test]
    fn payload_limit_is_checked_even_within_envelope_limit() {
        let mut envelope: Envelope = serde_json::from_slice(&sample()).unwrap();
        envelope.body.payload = "x".repeat(MAX_PAYLOAD_BYTES + 1);
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let mut target = Counter::default();
        assert_eq!(
            restore(
                &mut target,
                Counter::default(),
                &bytes,
                &identity(),
                cursor()
            ),
            Err(CheckpointError::TooLarge)
        );
        assert_eq!(target.calls, 0);
    }

    #[derive(Default)]
    struct Faulty(u64);
    impl Strategy for Faulty {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            Vec::new()
        }
        fn checkpoint_schema_version(&self) -> Option<u32> {
            Some(1)
        }
        fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
            Ok(self.0.to_string())
        }
        fn restore_checkpoint(&mut self, _: &ValidatedState<'_>) -> Result<(), CheckpointError> {
            self.0 = 100;
            Err(CheckpointError::Payload)
        }
    }

    #[test]
    fn failed_candidate_is_never_published() {
        let mut active = Faulty(7);
        assert_eq!(
            restore(
                &mut active,
                Faulty::default(),
                &sample(),
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Payload)
        );
        assert_eq!(active.0, 7);
    }

    #[test]
    fn equivalent_noncanonical_encoding_is_rejected() {
        let mut bytes = sample();
        bytes.push(b' ');
        let mut active = Counter::default();
        assert_eq!(
            restore(
                &mut active,
                Counter::default(),
                &bytes,
                &identity(),
                cursor()
            ),
            Err(CheckpointError::Invalid)
        );
        assert_eq!(active.calls, 0);
    }

    struct TextPayload(String);
    impl Strategy for TextPayload {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            Vec::new()
        }
        fn checkpoint_schema_version(&self) -> Option<u32> {
            Some(1)
        }
        fn checkpoint_payload(&self) -> Result<String, CheckpointError> {
            Ok(self.0.clone())
        }
        fn restore_checkpoint(
            &mut self,
            state: &ValidatedState<'_>,
        ) -> Result<(), CheckpointError> {
            self.0 = state.payload().to_owned();
            Ok(())
        }
    }

    #[test]
    fn capture_payload_bounds_include_worst_case_json_escaping() {
        let input = TextPayload("\0".repeat(MAX_PAYLOAD_BYTES));
        let bytes = capture(&input, identity(), cursor()).unwrap();
        assert!(bytes.len() <= MAX_ENVELOPE_BYTES);
        let mut active = TextPayload(String::new());
        restore(
            &mut active,
            TextPayload(String::new()),
            &bytes,
            &identity(),
            cursor(),
        )
        .unwrap();
        assert_eq!(active.0, input.0);
        let oversized = TextPayload("x".repeat(MAX_PAYLOAD_BYTES + 1));
        assert_eq!(
            capture(&oversized, identity(), cursor()),
            Err(CheckpointError::TooLarge)
        );
    }

    struct Legacy;
    impl Strategy for Legacy {
        fn on_market_event(&mut self, _: &MarketEvent, _: &StrategyContext) -> Vec<OrderIntent> {
            Vec::new()
        }
    }
    #[test]
    fn legacy_is_explicitly_unsupported() {
        assert_eq!(
            capture(&Legacy, identity(), cursor()),
            Err(CheckpointError::Unsupported)
        );
        assert_eq!(
            restore(&mut Legacy, Legacy, &sample(), &identity(), cursor()),
            Err(CheckpointError::Unsupported)
        );
    }
}
