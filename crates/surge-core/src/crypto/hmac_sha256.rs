use sha2::{Digest, Sha256};

const SHA256_BLOCK_LEN: usize = 64;
const SHA256_OUTPUT_LEN: usize = 32;

/// Compute HMAC-SHA256, returning raw 32-byte MAC.
#[must_use]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let normalized_key = normalized_hmac_key(key);
    let mut inner_pad = [0x36_u8; SHA256_BLOCK_LEN];
    let mut outer_pad = [0x5c_u8; SHA256_BLOCK_LEN];

    for ((inner_byte, outer_byte), key_byte) in inner_pad.iter_mut().zip(outer_pad.iter_mut()).zip(normalized_key) {
        *inner_byte ^= key_byte;
        *outer_byte ^= key_byte;
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(data);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    outer.finalize().to_vec()
}

/// Compute HMAC-SHA256, returning lowercase hex string.
#[must_use]
pub fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

fn normalized_hmac_key(key: &[u8]) -> [u8; SHA256_BLOCK_LEN] {
    let mut normalized = [0_u8; SHA256_BLOCK_LEN];
    if key.len() > SHA256_BLOCK_LEN {
        let digest = Sha256::digest(key);
        normalized[..SHA256_OUTPUT_LEN].copy_from_slice(&digest);
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sha256_rfc4231_vector1() {
        // RFC 4231 Test Case 1
        let key = vec![0x0b; 20];
        let data = b"Hi There";
        assert_eq!(
            hmac_sha256_hex(&key, data),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn test_hmac_sha256_output_length() {
        let result = hmac_sha256(b"key", b"data");
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_hmac_sha256_rfc4231_vector6() {
        let key = vec![0xaa; 131];
        let data = b"Test Using Larger Than Block-Size Key - Hash Key First";
        assert_eq!(
            hmac_sha256_hex(&key, data),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }
}
