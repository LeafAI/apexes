//! F03.04: domain-separated replay parent identity, independent of child IDs.
use apexes_strategy::ParentOrderId;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(super) struct ReplayParentIds {
    namespace: Uuid,
    next: u128,
}

impl ReplayParentIds {
    pub(super) fn new(namespace: Uuid) -> Self {
        Self { namespace, next: 0 }
    }

    pub(super) fn allocate(&mut self) -> ParentOrderId {
        let mut digest = Sha256::new();
        digest.update(b"apexes-simulated-ems-parent-id-v1\0");
        digest.update(self.namespace.as_bytes());
        digest.update(self.next.to_be_bytes());
        self.next = self.next.wrapping_add(1);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest.finalize()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        ParentOrderId(Uuid::from_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parent_identity_repeats_and_separates_namespaces() {
        let mut a = ReplayParentIds::new(Uuid::from_u128(1));
        let mut b = ReplayParentIds::new(Uuid::from_u128(1));
        let mut c = ReplayParentIds::new(Uuid::from_u128(2));
        let id = a.allocate();
        assert_eq!(id, b.allocate());
        assert_ne!(id, c.allocate());
        assert_eq!(id.0.get_version_num(), 8);
        assert_ne!(id, a.allocate());
    }
}
