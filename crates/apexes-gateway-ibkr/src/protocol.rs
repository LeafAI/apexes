//! IBKR TWS API wire protocol: a length-prefixed (4-byte big-endian),
//! null-byte-separated-field text protocol. This format is publicly
//! documented (unlike XTP's closed-source binary SDK) -- see
//! `docs/superpowers/specs/2026-08-02-apexes-gateway-ashare-hshare-usequity-design.md`
//! §4.2 for the design rationale.

/// Encodes a list of fields into one IBKR wire-protocol message: each
/// field is written verbatim followed by a single null byte, and the
/// whole resulting body is prefixed with its length as a 4-byte
/// big-endian unsigned integer.
pub fn encode_message(fields: &[String]) -> Vec<u8> {
    let mut body = Vec::new();
    for field in fields {
        body.extend_from_slice(field.as_bytes());
        body.push(0);
    }
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Attempts to decode the next complete message from the front of
/// `buf`. Returns `None` if `buf` does not yet contain a complete
/// message (either the 4-byte length prefix itself is incomplete, or
/// the prefix is present but the declared body length exceeds what's
/// available) -- callers are responsible for buffering additional
/// bytes and retrying. On success, returns the decoded fields (empty
/// trailing fields from the null-byte splitting are filtered out) and
/// the total number of bytes consumed from the front of `buf`
/// (4 + body length), so the caller can advance past exactly this
/// message and leave any trailing bytes (a subsequent message, or a
/// partial one) untouched.
pub fn decode_message(buf: &[u8]) -> Option<(Vec<String>, usize)> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    let body = &buf[4..4 + len];
    let fields: Vec<String> = body
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string())
        .collect();
    Some((fields, 4 + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_message_produces_length_prefixed_null_separated_fields() {
        let fields = vec!["PLACE_ORDER".to_string(), "42".to_string()];
        let encoded = encode_message(&fields);

        // 4-byte length prefix.
        let expected_body = b"PLACE_ORDER\x0042\0";
        let expected_len = expected_body.len() as u32;
        assert_eq!(&encoded[0..4], &expected_len.to_be_bytes());
        assert_eq!(&encoded[4..], expected_body);
    }

    #[test]
    fn decode_message_round_trips_encode_message() {
        let fields = vec![
            "PLACE_ORDER".to_string(),
            "600000".to_string(),
            "BUY".to_string(),
        ];
        let encoded = encode_message(&fields);

        let (decoded, consumed) = decode_message(&encoded).unwrap();
        assert_eq!(decoded, fields);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn decode_message_returns_none_for_incomplete_length_prefix() {
        let partial = vec![0u8, 0u8];
        assert!(decode_message(&partial).is_none());
    }

    #[test]
    fn decode_message_returns_none_for_incomplete_body() {
        let fields = vec!["PLACE_ORDER".to_string()];
        let encoded = encode_message(&fields);
        let truncated = &encoded[..encoded.len() - 2];
        assert!(decode_message(truncated).is_none());
    }

    #[test]
    fn decode_message_reports_correct_consumed_length_with_trailing_bytes() {
        let fields = vec!["A".to_string()];
        let encoded = encode_message(&fields);
        let mut buffer = encoded.clone();
        buffer.extend_from_slice(b"extra-trailing-bytes");

        let (decoded, consumed) = decode_message(&buffer).unwrap();
        assert_eq!(decoded, fields);
        assert_eq!(consumed, encoded.len());
    }
}
