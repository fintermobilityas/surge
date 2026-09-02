//! Wire format for chunked bsdiff patches (`CSDF`).
//!
//! Version 1: header + per-chunk length-prefixed bsdiff payloads.
//! Version 2 adds an identity bitset: unchanged chunks carry no payload,
//! and bspatch copies the old chunk straight through.
//! Version 3 adds per-chunk target SHA-256 digests for the changed
//! chunks: the in-place applier verifies each rewritten chunk in memory
//! and skips the full-file target re-read. The digests are a superset of
//! version 2 (same sizes, boundaries, and payloads), so the installed
//! bytes are unchanged.
//!
//! Patch format:
//!   MAGIC (4 bytes) "CSDF"
//!   VERSION (1 byte)
//!   chunk_size (8 bytes LE)
//!   old_size (8 bytes LE)
//!   new_size (8 bytes LE)
//!   num_chunks (4 bytes LE)
//!   Version 2+ only: identity bitset (ceil(num_chunks / 8) bytes, LSB-first)
//!   For each chunk:
//!     patch_len (8 bytes LE)
//!     patch_data (patch_len bytes; empty for identity chunks)
//!     Version 3 only, changed chunks: target_sha256 (32 bytes)
use super::ChunkedPatchFormat;
use crate::error::{Result, SurgeError};

/// Magic bytes identifying the chunked patch format.
const MAGIC: &[u8; 4] = b"CSDF";

/// Legacy format version (no identity-chunk bitset).
const LEGACY_VERSION: u8 = 1;

/// Identity-chunk bitset version.
const IDENTITY_VERSION: u8 = 2;

/// Identity-chunk bitset plus per-chunk target SHA-256 digests.
const TARGET_HASHES_VERSION: u8 = 3;

/// SHA-256 digest length for the per-chunk target digests.
const DIGEST_LEN: usize = 32;

#[must_use]
pub fn has_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

fn header_size() -> usize {
    4 + 1 + 8 + 8 + 8 + 4
}

fn identity_bitset_len(num_chunks: usize) -> usize {
    num_chunks.div_ceil(8)
}

fn set_identity_bit(bitset: &mut [u8], idx: usize) {
    bitset[idx / 8] |= 1u8 << (idx % 8);
}

fn identity_bit_is_set(bitset: &[u8], idx: usize) -> bool {
    bitset[idx / 8] & (1u8 << (idx % 8)) != 0
}

/// Patch format:
///   MAGIC (4 bytes) "CSDF"
///   VERSION (1 byte)
///   chunk_size (8 bytes LE)
///   old_size (8 bytes LE)
///   new_size (8 bytes LE)
///   num_chunks (4 bytes LE)
///   Version 2 only: identity bitset (ceil(num_chunks / 8) bytes, LSB-first)
///   For each chunk:
///     patch_len (8 bytes LE)
///     patch_data (patch_len bytes; empty for identity chunks)
/// One chunk of a patch being serialized: its bsdiff payload, identity
/// marker, and (version 3) the SHA-256 of the chunk's target content.
#[derive(Debug, Clone)]
pub(super) struct SerializedChunk {
    pub(super) idx: usize,
    pub(super) patch: Vec<u8>,
    pub(super) identity: bool,
    pub(super) target_hash: Option<[u8; DIGEST_LEN]>,
}

pub(super) fn serialize_patch(
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[SerializedChunk],
    format: ChunkedPatchFormat,
) -> Result<Vec<u8>> {
    let num_chunks = chunks.len();
    let (version, mut bitset) = match format {
        ChunkedPatchFormat::Legacy => (LEGACY_VERSION, Vec::new()),
        ChunkedPatchFormat::IdentityChunks => (IDENTITY_VERSION, vec![0u8; identity_bitset_len(num_chunks)]),
        ChunkedPatchFormat::IdentityChunksWithTargetHashes => {
            (TARGET_HASHES_VERSION, vec![0u8; identity_bitset_len(num_chunks)])
        }
    };
    for chunk in chunks {
        if chunk.identity {
            if format == ChunkedPatchFormat::Legacy {
                return Err(SurgeError::Diff(
                    "identity chunk cannot be encoded in the legacy chunked patch format".into(),
                ));
            }
            if format == ChunkedPatchFormat::IdentityChunksWithTargetHashes && chunk.target_hash.is_some() {
                return Err(SurgeError::Diff("identity chunk must not carry a target digest".into()));
            }
            set_identity_bit(&mut bitset, chunk.idx);
        } else if format == ChunkedPatchFormat::IdentityChunksWithTargetHashes && chunk.target_hash.is_none() {
            return Err(SurgeError::Diff(
                "changed chunk is missing its target digest in the v3 chunked patch format".into(),
            ));
        }
    }

    let header_size = header_size();
    let with_hashes = format == ChunkedPatchFormat::IdentityChunksWithTargetHashes;
    let data_size: usize = chunks
        .iter()
        .map(|c| 8 + c.patch.len() + if with_hashes && !c.identity { DIGEST_LEN } else { 0 })
        .sum();
    let mut buf = Vec::with_capacity(header_size + bitset.len() + data_size);

    buf.extend_from_slice(MAGIC);
    buf.push(version);
    buf.extend_from_slice(
        &u64::try_from(chunk_size)
            .map_err(|_| SurgeError::Diff("chunk size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u64::try_from(old_size)
            .map_err(|_| SurgeError::Diff("old size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u64::try_from(new_size)
            .map_err(|_| SurgeError::Diff("new size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u32::try_from(chunks.len())
            .map_err(|_| SurgeError::Diff("chunk count exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&bitset);

    for chunk in chunks {
        buf.extend_from_slice(
            &u64::try_from(chunk.patch.len())
                .map_err(|_| SurgeError::Diff("patch chunk exceeds supported patch format".into()))?
                .to_le_bytes(),
        );
        buf.extend_from_slice(&chunk.patch);
        if with_hashes && !chunk.identity {
            let target_hash = chunk
                .target_hash
                .ok_or_else(|| SurgeError::Diff("changed chunk is missing its target digest".into()))?;
            buf.extend_from_slice(&target_hash);
        }
    }

    Ok(buf)
}

fn read_u64_le(data: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = data[offset..offset + 8]
        .try_into()
        .map_err(|_| SurgeError::Diff("patch truncated reading u64".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32_le(data: &[u8], offset: usize) -> Result<u32> {
    let bytes: [u8; 4] = data[offset..offset + 4]
        .try_into()
        .map_err(|_| SurgeError::Diff("patch truncated reading u32".into()))?;
    Ok(u32::from_le_bytes(bytes))
}

/// Decoded chunked patch: sizes, per-chunk payloads, the identity flags
/// (version 2+; always false for legacy version 1 patches), and the
/// per-chunk target digests (version 3; always `None` for v1/v2).
pub(super) struct ChunkedPatchData<'a> {
    pub(super) old_size: usize,
    pub(super) new_size: usize,
    pub(super) chunk_size: usize,
    pub(super) chunks: Vec<&'a [u8]>,
    pub(super) identity: Vec<bool>,
    pub(super) chunk_hashes: Vec<Option<[u8; DIGEST_LEN]>>,
}

pub(super) fn deserialize_patch(data: &[u8]) -> Result<ChunkedPatchData<'_>> {
    let header_size = header_size();
    if data.len() < header_size {
        return Err(SurgeError::Diff("patch too short for header".into()));
    }

    if &data[0..4] != MAGIC {
        return Err(SurgeError::Diff("invalid chunked patch magic".into()));
    }
    let version = data[4];
    let is_legacy = version == LEGACY_VERSION;
    if !is_legacy && version != IDENTITY_VERSION && version != TARGET_HASHES_VERSION {
        return Err(SurgeError::Diff(format!(
            "unsupported chunked patch version: {version}"
        )));
    }
    let with_hashes = version == TARGET_HASHES_VERSION;

    let chunk_size = usize::try_from(read_u64_le(data, 5)?)
        .map_err(|_| SurgeError::Diff("chunk size exceeds platform limits".into()))?;
    let old_size = usize::try_from(read_u64_le(data, 13)?)
        .map_err(|_| SurgeError::Diff("old size exceeds platform limits".into()))?;
    let new_size = usize::try_from(read_u64_le(data, 21)?)
        .map_err(|_| SurgeError::Diff("new size exceeds platform limits".into()))?;
    let num_chunks = usize::try_from(read_u32_le(data, 29)?)
        .map_err(|_| SurgeError::Diff("chunk count exceeds platform limits".into()))?;

    let bitset_len = if is_legacy { 0 } else { identity_bitset_len(num_chunks) };
    if !is_legacy && data.len() < header_size + bitset_len {
        return Err(SurgeError::Diff("patch truncated reading identity bitset".into()));
    }
    let bitset = &data[header_size..header_size + bitset_len];
    let mut offset = header_size + bitset_len;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut identity = vec![false; num_chunks];
    let mut chunk_hashes = vec![None; num_chunks];

    for (idx, _) in (0..num_chunks).enumerate() {
        if offset + 8 > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk length".into()));
        }
        let patch_len = usize::try_from(read_u64_le(data, offset)?)
            .map_err(|_| SurgeError::Diff("patch chunk length exceeds platform limits".into()))?;
        offset += 8;

        if offset + patch_len > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk data".into()));
        }
        if !is_legacy && identity_bit_is_set(bitset, idx) {
            if patch_len != 0 {
                return Err(SurgeError::Diff(
                    "identity chunk must carry an empty patch payload".into(),
                ));
            }
            identity[idx] = true;
        }
        chunks.push(&data[offset..offset + patch_len]);
        offset += patch_len;

        if with_hashes && !identity[idx] {
            if offset + DIGEST_LEN > data.len() {
                return Err(SurgeError::Diff("patch truncated at chunk target digest".into()));
            }
            let digest: [u8; DIGEST_LEN] = data[offset..offset + DIGEST_LEN]
                .try_into()
                .map_err(|_| SurgeError::Diff("chunk target digest has an invalid length".into()))?;
            chunk_hashes[idx] = Some(digest);
            offset += DIGEST_LEN;
        }
    }

    Ok(ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks,
        identity,
        chunk_hashes,
    })
}

#[cfg(test)]
mod tests {
    use super::super::wrapper;
    use super::super::{ChunkedDiffOptions, chunked_bsdiff, chunked_bspatch};
    use super::*;

    #[test]
    fn test_chunked_v2_identity_chunks_carry_no_payload() {
        // 4 chunks, middle two identical to the old file; identity chunks are opt-in.
        let chunk = 256usize;
        let old: Vec<u8> = (0..(4 * chunk)).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[10] = 0xFF; // chunk 0 changes
        new[4 * chunk - 1] = 0xAB; // chunk 3 changes
        // chunks 1 and 2 remain identical

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 2,
            format: ChunkedPatchFormat::IdentityChunks,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");

        // Version byte bumped; identity bitset present (4 chunks -> 1 byte).
        assert_eq!(patch[4], IDENTITY_VERSION);
        let header = 4 + 1 + 8 + 8 + 8 + 4;
        assert_eq!(
            patch[header], 0b0000_0110,
            "chunks 1 and 2 must be identity, 0 and 3 not"
        );
        // Chunks 1 and 2: empty payloads.
        let mut off = header + 1;
        let mut lens = Vec::new();
        for _ in 0..4 {
            let len = u64::from_le_bytes(patch[off..off + 8].try_into().unwrap()) as usize;
            lens.push(len);
            off += 8 + len;
        }
        assert_eq!(off, patch.len(), "no trailing bytes");
        assert_eq!(lens[1], 0, "identity chunk 1 payload must be empty");
        assert_eq!(lens[2], 0, "identity chunk 2 payload must be empty");
        assert!(lens[0] > 0 && lens[3] > 0);

        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_v1_patch_still_applies() {
        // Hand-build a version-1 patch (no bitset) and apply it with the v2
        // reader: unchanged chunks carry a real identity bsdiff payload.
        let chunk = 256usize;
        let old: Vec<u8> = vec![7u8; 3 * chunk];
        let mut new = old.clone();
        new[3 * chunk - 1] = 99;

        let c0 = wrapper::bsdiff_buffers(&old[0..chunk], &new[0..chunk]).unwrap();
        let c1 = wrapper::bsdiff_buffers(&old[chunk..2 * chunk], &new[chunk..2 * chunk]).unwrap();
        let c2 = wrapper::bsdiff_buffers(&old[2 * chunk..3 * chunk], &new[2 * chunk..3 * chunk]).unwrap();

        let mut patch = Vec::new();
        patch.extend_from_slice(MAGIC);
        patch.push(LEGACY_VERSION);
        patch.extend_from_slice(&(chunk as u64).to_le_bytes());
        patch.extend_from_slice(&(old.len() as u64).to_le_bytes());
        patch.extend_from_slice(&(new.len() as u64).to_le_bytes());
        patch.extend_from_slice(&3u32.to_le_bytes());
        for c in [c0, c1, c2] {
            patch.extend_from_slice(&(c.len() as u64).to_le_bytes());
            patch.extend_from_slice(&c);
        }

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
            format: ChunkedPatchFormat::Legacy,
        };
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch v1");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_default_format_is_legacy_and_carries_identity_payloads() {
        // Default options must produce a patch every fielded reader accepts: version byte 1,
        // no identity bitset, and a real bsdiff payload for the unchanged chunks.
        let chunk = 256usize;
        let old: Vec<u8> = (0..(4 * chunk))
            .map(|i| u8::try_from(i % 251).unwrap_or_default())
            .collect();
        let mut new = old.clone();
        new[10] ^= 0xff;
        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
            ..ChunkedDiffOptions::default()
        };
        let patch = chunked_bsdiff(&old, &new, &opts).unwrap();

        assert_eq!(patch[4], LEGACY_VERSION);
        let decoded = deserialize_patch(&patch).unwrap();
        assert_eq!(decoded.chunks.len(), 4);
        assert!(
            decoded.identity.iter().all(|flag| !flag),
            "legacy patches never carry identity flags"
        );
        assert!(
            decoded.chunks.iter().all(|c| !c.is_empty()),
            "every legacy chunk carries a payload"
        );
        assert_eq!(chunked_bspatch(&old, &patch, &opts).unwrap(), new);
    }

    #[test]
    fn test_chunked_legacy_format_rejects_identity_flags() {
        let chunks = vec![SerializedChunk {
            idx: 0,
            patch: vec![1u8, 2, 3],
            identity: true,
            target_hash: None,
        }];
        let err = serialize_patch(3, 3, 3, &chunks, ChunkedPatchFormat::Legacy).unwrap_err();
        assert!(err.to_string().contains("legacy chunked patch format"), "{err}");
    }

    #[test]
    fn test_chunked_v3_round_trip_carries_target_digests() {
        // 4 chunks of 256 bytes; chunks 1 and 2 are identity, chunks 0
        // and 3 change. v3 records a target digest for each changed chunk
        // and bspatch verifies them while reconstructing.
        let chunk = 256usize;
        let old: Vec<u8> = (0..(4 * chunk)).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[10] = 0xFF;
        new[4 * chunk - 1] = 0xAB;

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 2,
            format: ChunkedPatchFormat::IdentityChunksWithTargetHashes,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");

        assert_eq!(patch[4], TARGET_HASHES_VERSION);
        let decoded = deserialize_patch(&patch).expect("decode");
        assert_eq!(decoded.chunk_hashes[0].as_ref().map(|d| d.len()), Some(DIGEST_LEN));
        assert_eq!(decoded.chunk_hashes[1], None, "identity chunk carries no digest");
        assert_eq!(decoded.chunk_hashes[2], None, "identity chunk carries no digest");
        assert_eq!(decoded.chunk_hashes[3].as_ref().map(|d| d.len()), Some(DIGEST_LEN));

        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);

        // The recorded digests must match the actual target chunks.
        let expected0 = crate::crypto::sha256::sha256_raw(&new[0..chunk]);
        assert_eq!(decoded.chunk_hashes[0], Some(expected0.try_into().expect("digest")));
    }

    #[test]
    fn test_chunked_v3_tampered_target_digest_rejected() {
        let chunk = 256usize;
        let old: Vec<u8> = (0..(2 * chunk)).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[5] ^= 0xFF;

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
            format: ChunkedPatchFormat::IdentityChunksWithTargetHashes,
        };
        let mut patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");

        // Corrupt the first changed-chunk digest (first 32-byte digest
        // follows chunk 0's patch payload).
        let header = 4 + 1 + 8 + 8 + 8 + 4;
        let bitset = 2usize.div_ceil(8);
        let chunk0_len = u64::from_le_bytes(patch[header + bitset..header + bitset + 8].try_into().unwrap()) as usize;
        patch[header + bitset + 8 + chunk0_len] ^= 0xFF;

        let err = chunked_bspatch(&old, &patch, &opts).unwrap_err();
        assert!(err.to_string().contains("target digest mismatch"), "{err}");
    }

    #[test]
    fn test_chunked_v3_serialization_rules() {
        // A changed chunk without a digest cannot be written as v3.
        let no_hash = vec![SerializedChunk {
            idx: 0,
            patch: vec![1],
            identity: false,
            target_hash: None,
        }];
        let err = serialize_patch(1, 1, 1, &no_hash, ChunkedPatchFormat::IdentityChunksWithTargetHashes).unwrap_err();
        assert!(err.to_string().contains("missing its target digest"), "{err}");

        // An identity chunk with a digest cannot be written as v3.
        let with_hash = vec![SerializedChunk {
            idx: 0,
            patch: Vec::new(),
            identity: true,
            target_hash: Some([7u8; DIGEST_LEN]),
        }];
        let err = serialize_patch(1, 1, 1, &with_hash, ChunkedPatchFormat::IdentityChunksWithTargetHashes).unwrap_err();
        assert!(err.to_string().contains("must not carry a target digest"), "{err}");

        // v2 ignores digests entirely (they are not written).
        let v2 = vec![SerializedChunk {
            idx: 0,
            patch: vec![1],
            identity: false,
            target_hash: Some([7u8; DIGEST_LEN]),
        }];
        let bytes = serialize_patch(1, 1, 1, &v2, ChunkedPatchFormat::IdentityChunks).expect("serialize");
        assert_eq!(bytes[4], IDENTITY_VERSION);
        let decoded = deserialize_patch(&bytes).expect("decode");
        assert_eq!(decoded.chunk_hashes[0], None, "v2 patches never carry digests");
    }

    #[test]
    fn test_chunked_unknown_version_rejected() {
        let chunk = 256usize;
        let old: Vec<u8> = vec![1u8; chunk];
        let new = vec![2u8; chunk];
        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
            format: ChunkedPatchFormat::Legacy,
        };
        let mut patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        patch[4] = 4;
        let err = chunked_bspatch(&old, &patch, &opts).unwrap_err();
        assert!(err.to_string().contains("unsupported chunked patch version"), "{err}");
    }
}
