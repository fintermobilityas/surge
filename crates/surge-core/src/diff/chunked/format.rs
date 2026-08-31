//! Wire format for chunked bsdiff patches (`CSDF`).
//!
//! Version 1: header + per-chunk length-prefixed bsdiff payloads.
//! Version 2 adds an identity bitset: unchanged chunks carry no payload,
//! and bspatch copies the old chunk straight through. Old readers reject
//! version 2 via the version check.
//!
//! Patch format:
//!   MAGIC (4 bytes) "CSDF"
//!   VERSION (1 byte)
//!   chunk_size (8 bytes LE)
//!   old_size (8 bytes LE)
//!   new_size (8 bytes LE)
//!   num_chunks (4 bytes LE)
//!   Version 2 only: identity bitset (ceil(num_chunks / 8) bytes, LSB-first)
//!   For each chunk:
//!     patch_len (8 bytes LE)
//!     patch_data (patch_len bytes; empty for identity chunks)
use crate::error::{Result, SurgeError};

/// Magic bytes identifying the chunked patch format.
const MAGIC: &[u8; 4] = b"CSDF";

/// Format version.
///
/// Version 2 adds an identity-chunk bitset: chunks whose old and new content
/// are identical carry no bsdiff payload, and bspatch copies the old chunk
/// straight through. Old readers reject version 2 via the version check.
const VERSION: u8 = 2;

/// Legacy format version (no identity-chunk bitset).
const LEGACY_VERSION: u8 = 1;

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
pub(super) fn serialize_patch(
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[(usize, Vec<u8>, bool)],
) -> Result<Vec<u8>> {
    let num_chunks = chunks.len();
    let mut bitset = vec![0u8; identity_bitset_len(num_chunks)];
    for (idx, _, identity) in chunks {
        if *identity {
            set_identity_bit(&mut bitset, *idx);
        }
    }

    let header_size = header_size();
    let data_size: usize = chunks.iter().map(|(_, p, _)| 8 + p.len()).sum();
    let mut buf = Vec::with_capacity(header_size + bitset.len() + data_size);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
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

    for (_, patch, _) in chunks {
        buf.extend_from_slice(
            &u64::try_from(patch.len())
                .map_err(|_| SurgeError::Diff("patch chunk exceeds supported patch format".into()))?
                .to_le_bytes(),
        );
        buf.extend_from_slice(patch);
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

/// Decoded chunked patch: sizes, per-chunk payloads, and the identity flags
/// (version 2; always empty flags for legacy version 1 patches).
pub(super) struct ChunkedPatchData<'a> {
    pub(super) old_size: usize,
    pub(super) new_size: usize,
    pub(super) chunk_size: usize,
    pub(super) chunks: Vec<&'a [u8]>,
    pub(super) identity: Vec<bool>,
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
    if !is_legacy && version != VERSION {
        return Err(SurgeError::Diff(format!(
            "unsupported chunked patch version: {version}"
        )));
    }

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
    }

    Ok(ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks,
        identity,
    })
}

#[cfg(test)]
mod tests {
    use super::super::wrapper;
    use super::super::{ChunkedDiffOptions, chunked_bsdiff, chunked_bspatch};
    use super::*;

    #[test]
    fn test_chunked_v2_identity_chunks_carry_no_payload() {
        // 4 chunks, middle two identical to the old file.
        let chunk = 256usize;
        let old: Vec<u8> = (0..(4 * chunk)).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[10] = 0xFF; // chunk 0 changes
        new[4 * chunk - 1] = 0xAB; // chunk 3 changes
        // chunks 1 and 2 remain identical

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 2,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");

        // Version byte bumped; identity bitset present (4 chunks -> 1 byte).
        assert_eq!(patch[4], VERSION);
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
        };
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch v1");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_unknown_version_rejected() {
        let chunk = 256usize;
        let old: Vec<u8> = vec![1u8; chunk];
        let new = vec![2u8; chunk];
        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
        };
        let mut patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        patch[4] = 3;
        let err = chunked_bspatch(&old, &patch, &opts).unwrap_err();
        assert!(err.to_string().contains("unsupported chunked patch version"), "{err}");
    }
}
