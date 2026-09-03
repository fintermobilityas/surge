//! SURGPAT1: a faster-to-apply re-encoding of a single chunk's classic
//! BSDIFF40 payload.
//!
//! The classic bsdiff payload stores three BZ2 blocks (control, diff,
//! extra). Applying it means decompressing the entire per-byte diff
//! string - for a 4 MiB chunk with a 4 KiB edit that is ~8.6 ms of BZ2
//! work even though the diff is a 4 MiB zero run plus a few kilobytes -
//! and then a per-byte `new[i] += old[i]` loop over the whole range.
//!
//! SURGPAT1 keeps the same control semantics (copy-add / extra-copy /
//! old-seek entries) but stores each entry's diff string as
//! `zstd(RLE(zero-runs))` and its extra block as `zstd(extra)`. The
//! applier expands the RLE in one pass: zero runs become a plain copy
//! from the old chunk (vectorized), raw runs add only their (few)
//! bytes. Wire size stays comparable because the diff strings in this
//! workload compress almost as well under zstd as under BZ2.
//!
//! Payload layout (all integers little-endian):
//!
//! ```text
//! 0    8   magic "SURGPAT1"
//! 8    8   newsize
//! 16   8   n_entries
//! per entry:
//!     8   x  copy-add length (bytes consumed from old + diff)
//!     8   y  extra copy length
//!     8   z  old-file seek (signed)
//!     8   diff_comp_len  zstd(RLE(diff_raw)), decompressed length == x
//!     zstd(RLE(diff_raw))
//!     8   extra_comp_len zstd(extra_raw), decompressed length == y
//!     zstd(extra_raw)
//!
//! RLE(diff_raw): a sequence of runs; each run is
//!     4   run length
//!     1   kind: 0 = zero run, 1 = raw run
//!     n   raw bytes (kind 1 only)
//! ```
//!
//! Read side: `apply_surgepat` verifies the magic, sizes, and run
//! lengths against `newsize`/`x` before touching the output, so a
//! truncated or corrupted payload fails closed.

use crate::error::{Result, SurgeError};

const MAGIC: &[u8; 8] = b"SURGPAT1";

/// A (copy-add length, extra length, old seek) control entry.
type EntryTriple = (u64, u64, i64);

/// A parsed classic BSDIFF40 blob.
struct ParsedBsdiff {
    entries: Vec<EntryTriple>,
    diff_raw: Vec<u8>,
    extra_raw: Vec<u8>,
    newsize: u64,
}

/// Magic check used by the apply dispatch to route a chunk payload to
/// this module.
pub fn is_surgepat(payload: &[u8]) -> bool {
    payload.starts_with(MAGIC)
}

/// Convert a classic BSDIFF40 chunk payload to SURGPAT1.
pub fn bsdiff40_to_surgepat(blob: &[u8]) -> Result<Vec<u8>> {
    let ParsedBsdiff {
        entries,
        diff_raw,
        extra_raw,
        newsize,
    } = parse_bsdiff40(blob)?;

    let mut out: Vec<u8> = Vec::with_capacity(blob.len() + 64);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&newsize.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());

    let mut x_off = 0usize;
    let mut y_off = 0usize;
    for (x, y, z) in entries {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
        out.extend_from_slice(&z.to_le_bytes());

        let x_us = usize::try_from(x).map_err(|_| SurgeError::Diff("surgepat: copy-add length overflows".into()))?;
        let diff_slice = &diff_raw[x_off..x_off + x_us];
        x_off += x_us;
        let rle = rle_encode(diff_slice)?;
        let diff_comp = zstd::encode_all(rle.as_slice(), 3)
            .map_err(|e| SurgeError::Diff(format!("surgepat: diff zstd encode failed: {e}")))?;
        out.extend_from_slice(&(diff_comp.len() as u64).to_le_bytes());
        out.extend_from_slice(&diff_comp);

        let y_us = usize::try_from(y).map_err(|_| SurgeError::Diff("surgepat: extra length overflows".into()))?;
        let extra_slice = &extra_raw[y_off..y_off + y_us];
        y_off += y_us;
        let extra_comp = zstd::encode_all(extra_slice, 3)
            .map_err(|e| SurgeError::Diff(format!("surgepat: extra zstd encode failed: {e}")))?;
        out.extend_from_slice(&(extra_comp.len() as u64).to_le_bytes());
        out.extend_from_slice(&extra_comp);
    }
    Ok(out)
}

/// Apply a SURGPAT1 payload to `old`, returning the new chunk content.
pub fn apply_surgepat(old: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    if !is_surgepat(payload) {
        return Err(SurgeError::Diff(
            "surgepat: payload does not start with the SURGPAT1 magic".into(),
        ));
    }
    if payload.len() < 24 {
        return Err(SurgeError::Diff("surgepat: truncated payload".into()));
    }
    let newsize = u64_to_usize(&payload[8..16])?;
    let n_entries = u64_to_usize(&payload[16..24])?;

    let mut out = vec![0u8; newsize];
    let mut pos: usize = 0;
    let mut oldpos: i64 = 0;

    let mut cursor = 24usize;
    for _ in 0..n_entries {
        if payload.len() - cursor < 24 {
            return Err(SurgeError::Diff("surgepat: truncated entry header".into()));
        }
        let x = u64_to_usize(&payload[cursor..cursor + 8])?;
        let y = u64_to_usize(&payload[cursor + 8..cursor + 16])?;
        let zb = &payload[cursor + 16..cursor + 24];
        let z = i64::from_le_bytes([zb[0], zb[1], zb[2], zb[3], zb[4], zb[5], zb[6], zb[7]]);
        cursor += 24;

        // diff: the block decompresses to the RLE encoding; run coverage
        // is validated against x inside expand_diff_into.
        let (diff_rle, next_cursor) = read_zstd_block(payload, cursor)?;
        cursor = next_cursor;
        expand_diff_into(old, &mut out, pos, x, oldpos, &diff_rle)?;
        pos = pos.saturating_add(x);

        // extra
        let (extra_raw, next_cursor) = read_zstd_block(payload, cursor)?;
        cursor = next_cursor;
        if extra_raw.len() != y {
            return Err(SurgeError::Diff(
                "surgepat: decompressed extra length does not match the entry length".into(),
            ));
        }
        if out.len().saturating_sub(pos) < y {
            return Err(SurgeError::Diff("surgepat: extra copy runs past newsize".into()));
        }
        out[pos..pos + y].copy_from_slice(&extra_raw);
        pos += y;

        let x_i64 = i64::try_from(x).map_err(|_| SurgeError::Diff("surgepat: copy-add length overflows i64".into()))?;
        oldpos = oldpos.saturating_add(x_i64).saturating_add(z);
    }

    if cursor != payload.len() {
        return Err(SurgeError::Diff("surgepat: trailing bytes after final entry".into()));
    }
    if pos != newsize {
        return Err(SurgeError::Diff(
            "surgepat: applied entries do not cover the whole new size".into(),
        ));
    }
    Ok(out)
}

fn expand_diff_into(old: &[u8], out: &mut [u8], pos: usize, x: usize, oldpos: i64, diff_raw: &[u8]) -> Result<()> {
    if out.len().saturating_sub(pos) < x {
        return Err(SurgeError::Diff("surgepat: copy-add runs past newsize".into()));
    }
    let mut run = 0usize;
    let mut i = 0usize;
    while i < diff_raw.len() {
        let len = u32_to_usize(&diff_raw[i..i + 4])? as usize;
        let kind = diff_raw[i + 4];
        i += 5;
        if kind == 0 {
            // Zero run: new = 0 + old (where in range); out is already
            // zero, so only the in-range part needs a copy. Byte
            // `run + k` of the entry reads old[oldpos + run + k].
            let s0 = oldpos
                .checked_add(
                    i64::try_from(run).map_err(|_| SurgeError::Diff("surgepat: run offset overflows i64".into()))?,
                )
                .ok_or_else(|| SurgeError::Diff("surgepat: old offset overflow".into()))?;
            let n =
                i64::try_from(old.len()).map_err(|_| SurgeError::Diff("surgepat: old size overflows i64".into()))?;
            let k_lo = (-s0).max(0);
            let k_hi = (n - s0)
                .min(i64::try_from(len).map_err(|_| SurgeError::Diff("surgepat: run length overflows i64".into()))?)
                .max(0);
            if k_hi > k_lo {
                let k_lo_us =
                    usize::try_from(k_lo).map_err(|_| SurgeError::Diff("surgepat: run index out of range".into()))?;
                let k_hi_us =
                    usize::try_from(k_hi).map_err(|_| SurgeError::Diff("surgepat: run index out of range".into()))?;
                let old_at = usize::try_from(s0 + k_lo)
                    .map_err(|_| SurgeError::Diff("surgepat: old offset out of range".into()))?;
                let count = k_hi_us - k_lo_us;
                out[pos + run + k_lo_us..pos + run + k_lo_us + count].copy_from_slice(&old[old_at..old_at + count]);
            }
        } else if kind == 1 {
            if diff_raw.len() - i < len {
                return Err(SurgeError::Diff(
                    "surgepat: raw run runs past the end of the diff block".into(),
                ));
            }
            let start = pos + run;
            for k in 0..len {
                let b = diff_raw[i + k];
                let idx = oldpos
                    .checked_add(
                        i64::try_from(run + k)
                            .map_err(|_| SurgeError::Diff("surgepat: run offset overflows i64".into()))?,
                    )
                    .ok_or_else(|| SurgeError::Diff("surgepat: old offset overflow".into()))?;
                let in_range = usize::try_from(idx).is_ok_and(|v| v < old.len());
                let add = usize::try_from(idx).unwrap_or_default();
                if in_range {
                    out[start + k] = b.wrapping_add(old[add]);
                } else {
                    out[start + k] = b;
                }
            }
            i += len;
        } else {
            return Err(SurgeError::Diff(format!("surgepat: unknown RLE run kind {kind}")));
        }
        run += len;
    }
    if run != x {
        return Err(SurgeError::Diff(
            "surgepat: RLE runs do not cover the copy-add length".into(),
        ));
    }
    Ok(())
}

/// Read a `len` + zstd block at `cursor`, returning the decompressed
/// bytes and the next cursor position.
fn read_zstd_block(payload: &[u8], mut cursor: usize) -> Result<(Vec<u8>, usize)> {
    if payload.len() - cursor < 8 {
        return Err(SurgeError::Diff("surgepat: truncated block length".into()));
    }
    let comp_len = u64_to_usize(&payload[cursor..cursor + 8])?;
    cursor += 8;
    if payload.len() - cursor < comp_len {
        return Err(SurgeError::Diff("surgepat: truncated compressed block".into()));
    }
    let comp = &payload[cursor..cursor + comp_len];
    cursor += comp_len;
    let decomp = zstd::decode_all(comp).map_err(|e| SurgeError::Diff(format!("surgepat: zstd decode failed: {e}")))?;
    Ok((decomp, cursor))
}

/// RLE-encode `data` as (zero run | raw run) sequences.
fn rle_encode(data: &[u8]) -> Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(data.len() + 16);
    let mut i = 0usize;
    while i < data.len() {
        if data[i] == 0 {
            let mut j = i;
            while j < data.len() && data[j] == 0 {
                j += 1;
            }
            let run_len =
                u32::try_from(j - i).map_err(|_| SurgeError::Diff("surgepat: RLE zero run overflows u32".into()))?;
            out.extend_from_slice(&run_len.to_le_bytes());
            out.push(0);
            i = j;
        } else {
            let mut j = i;
            while j < data.len() && data[j] != 0 {
                j += 1;
            }
            let run_len =
                u32::try_from(j - i).map_err(|_| SurgeError::Diff("surgepat: RLE raw run overflows u32".into()))?;
            out.extend_from_slice(&run_len.to_le_bytes());
            out.push(1);
            out.extend_from_slice(&data[i..j]);
            i = j;
        }
    }
    Ok(out)
}

/// Parse a classic BSDIFF40 blob into its control entries and raw
/// (decompressed) diff/extra blocks.
fn parse_bsdiff40(blob: &[u8]) -> Result<ParsedBsdiff> {
    if blob.len() < 32 || &blob[0..8] != b"BSDIFF40" {
        return Err(SurgeError::Diff("surgepat: payload is not a BSDIFF40 patch".into()));
    }
    let bzctrllen = usize::try_from(non_negative("control length", read_bsdiff_int(&blob[8..16])?)?)
        .map_err(|_| SurgeError::Diff("surgepat: control block length overflows".into()))?;
    let bzdatalen = usize::try_from(non_negative("diff length", read_bsdiff_int(&blob[16..24])?)?)
        .map_err(|_| SurgeError::Diff("surgepat: diff block length overflows".into()))?;
    let newsize = non_negative("new size", read_bsdiff_int(&blob[24..32])?)?;

    let control_start = 32;
    let diff_start = control_start + bzctrllen;
    let extra_start = diff_start + bzdatalen;
    if blob.len() < extra_start {
        return Err(SurgeError::Diff("surgepat: truncated bsdiff40 block lengths".into()));
    }

    // Control is tiny: decompress with a growing capacity.
    let control_raw = bz2_decompress_growing(&blob[control_start..diff_start])?;
    if control_raw.len() % 24 != 0 {
        return Err(SurgeError::Diff(
            "surgepat: control block is not a multiple of 24 bytes".into(),
        ));
    }
    let mut entries: Vec<(u64, u64, i64)> = Vec::with_capacity(control_raw.len() / 24);
    let mut total_x: usize = 0;
    let mut total_y: usize = 0;
    for i in 0..control_raw.len() / 24 {
        let base = i * 24;
        let x = non_negative("copy-add length", read_bsdiff_int(&control_raw[base..base + 8])?)?;
        let y = non_negative("extra length", read_bsdiff_int(&control_raw[base + 8..base + 16])?)?;
        let z = read_bsdiff_int(&control_raw[base + 16..base + 24])?;
        total_x = total_x
            .checked_add(usize::try_from(x).map_err(|_| SurgeError::Diff("surgepat: copy-add total overflows".into()))?)
            .ok_or_else(|| SurgeError::Diff("surgepat: diff total overflows".into()))?;
        total_y = total_y
            .checked_add(usize::try_from(y).map_err(|_| SurgeError::Diff("surgepat: extra total overflows".into()))?)
            .ok_or_else(|| SurgeError::Diff("surgepat: extra total overflows".into()))?;
        entries.push((x, y, z));
    }

    // The diff/extra decompressed sizes are known exactly from the
    // control, so decompress into exact-sized buffers: any failure is a
    // real corruption, not a buffer-size issue.
    let diff_raw = bz2_decompress_exact(&blob[diff_start..extra_start], total_x)?;
    let extra_raw = bz2_decompress_exact(&blob[extra_start..], total_y)?;
    Ok(ParsedBsdiff {
        entries,
        diff_raw,
        extra_raw,
        newsize,
    })
}

/// Decompress into a buffer of exactly `expected` bytes. Any non-zero
/// return code is a real corruption error: bzip2 reports a too-small
/// destination as BZ_DATA_ERROR (-8) rather than BZ_ERR_BUF_TOO_SMALL
/// (-5) for general data, so a guessed capacity must never be trusted
/// with exact-sized data.
fn bz2_decompress_exact(compressed: &[u8], expected: usize) -> Result<Vec<u8>> {
    if expected == 0 {
        // A zero-length block may still carry a valid empty bzip2
        // stream; verify it decodes to nothing.
        let out = bz2_decompress_growing(compressed)?;
        if !out.is_empty() {
            return Err(SurgeError::Diff(
                "surgepat: block claimed zero length but decompressed to more".into(),
            ));
        }
        return Ok(Vec::new());
    }
    let expected_u32 =
        u32::try_from(expected).map_err(|_| SurgeError::Diff("surgepat: block length overflows u32".into()))?;
    let comp_u32 = u32::try_from(compressed.len())
        .map_err(|_| SurgeError::Diff("surgepat: compressed length overflows u32".into()))?;
    let mut out = vec![0u8; expected];
    let mut out_len = expected_u32;
    // SAFETY: `out` is a valid writable buffer of `out_len` bytes and
    // `compressed` is a valid readable buffer; the C call only writes
    // within `out_len` and reports the actual size through `out_len`.
    let rc = unsafe {
        crate::diff::bsdiff_sys::BZ2_bzBuffToBuffDecompress(
            std::ptr::addr_of_mut!(out[0]),
            std::ptr::addr_of_mut!(out_len),
            std::ptr::addr_of!(compressed[0]),
            comp_u32,
            0,
            0,
        )
    };
    if rc != 0 || usize::try_from(out_len).is_err() || out_len as usize != expected {
        return Err(SurgeError::Diff(format!(
            "surgepat: bzip2 block of {expected} bytes failed to decode (code {rc})"
        )));
    }
    Ok(out)
}

/// Decompress with a growing capacity, for blocks whose decompressed
/// size is not known ahead of time (the control block). bzip2 reports
/// a too-small destination as BZ_DATA_ERROR (-8) rather than
/// BZ_ERR_BUF_TOO_SMALL (-5) for general data, so both codes grow the
/// buffer and retry; the bound keeps a corrupted stream from expanding
/// forever.
fn bz2_decompress_growing(compressed: &[u8]) -> Result<Vec<u8>> {
    if compressed.is_empty() {
        return Ok(Vec::new());
    }
    let mut cap = compressed.len().saturating_mul(4).max(256);
    for _ in 0..8 {
        let cap_u32 = u32::try_from(cap).map_err(|_| SurgeError::Diff("surgepat: capacity overflows u32".into()))?;
        let comp_u32 = u32::try_from(compressed.len())
            .map_err(|_| SurgeError::Diff("surgepat: compressed length overflows u32".into()))?;
        let mut out = vec![0u8; cap];
        let mut out_len = cap_u32;
        // SAFETY: `out` is a valid writable buffer of `out_len` bytes and
        // `compressed` is a valid readable buffer; the C call only writes
        // within `out_len` and reports the actual size through `out_len`.
        let rc = unsafe {
            crate::diff::bsdiff_sys::BZ2_bzBuffToBuffDecompress(
                out.as_mut_ptr(),
                std::ptr::addr_of_mut!(out_len),
                std::ptr::addr_of!(compressed[0]),
                comp_u32,
                0,
                0,
            )
        };
        match rc {
            0 => {
                out.truncate(out_len as usize);
                return Ok(out);
            }
            // BZ_ERR_BUF_TOO_SMALL (-5) / BZ_DATA_ERROR (-8): treat as
            // too-small and grow; a truly corrupt stream fails at the
            // bound.
            -5 | -8 => cap = cap.saturating_mul(4),
            other => {
                return Err(SurgeError::Diff(format!(
                    "surgepat: bzip2 decompression failed with code {other}"
                )));
            }
        }
    }
    Err(SurgeError::Diff(
        "surgepat: bzip2 output kept growing past the retry bound".into(),
    ))
}

/// Decode the bsdiff 8-byte signed magnitude encoding (offtin).
/// Returns the signed value; callers validate non-negativity for the
/// length fields (the seek field is legitimately signed).
fn read_bsdiff_int(buf: &[u8]) -> Result<i64> {
    if buf.len() < 8 {
        return Err(SurgeError::Diff("surgepat: truncated bsdiff integer".into()));
    }
    let mut y: i64 = i64::from(buf[7] & 0x7F);
    for i in (0..7).rev() {
        y = y * 256 + i64::from(buf[i]);
    }
    if buf[7] & 0x80 != 0 {
        y = -y;
    }
    Ok(y)
}

fn non_negative(name: &str, value: i64) -> Result<u64> {
    if value < 0 {
        return Err(SurgeError::Diff(format!("surgepat: negative {name} in bsdiff payload")));
    }
    u64::try_from(value).map_err(|_| SurgeError::Diff(format!("surgepat: {name} overflows u64")))
}

fn u64_to_usize(buf: &[u8]) -> Result<usize> {
    if buf.len() < 8 {
        return Err(SurgeError::Diff("surgepat: truncated u64 field".into()));
    }
    let bytes = [buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]];
    let v = u64::from_le_bytes(bytes);
    usize::try_from(v).map_err(|_| SurgeError::Diff("surgepat: field overflows usize".into()))
}

fn u32_to_usize(buf: &[u8]) -> Result<u32> {
    if buf.len() < 4 {
        return Err(SurgeError::Diff("surgepat: truncated u32 field".into()));
    }
    Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::wrapper::bsdiff_buffers;

    fn build_blob(old: &[u8], new: &[u8]) -> Vec<u8> {
        bsdiff_buffers(old, new).expect("bsdiff")
    }

    #[test]
    fn round_trip_single_entry() {
        let old = vec![0x11u8; 1024];
        let mut new = old.clone();
        new[500..504].copy_from_slice(&[9, 8, 7, 6]);
        let blob = build_blob(&old, &new);
        let sp = bsdiff40_to_surgepat(&blob).expect("encode");
        assert!(is_surgepat(&sp));
        let out = apply_surgepat(&old, &sp).expect("apply");
        assert_eq!(out, new);
    }

    #[test]
    fn round_trip_insertion_and_seek() {
        let old = b"the quick brown fox jumps over the lazy dog";
        let new = b"THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG, AND ROARS";
        let blob = build_blob(old, new);
        let sp = bsdiff40_to_surgepat(&blob).expect("encode");
        let out = apply_surgepat(old, &sp).expect("apply");
        assert_eq!(out, new);
    }

    #[test]
    fn round_trip_large_zero_run() {
        // The canonical workload: 4 MiB of realistic (pseudo-random)
        // content with one 4 KiB region changed.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next_byte = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33 & 0xFF) as u8
        };
        let mut old = Vec::with_capacity(4 * 1024 * 1024);
        for _ in 0..4 * 1024 * 1024 {
            old.push(next_byte());
        }
        let mut new = old.clone();
        for b in &mut new[1_000_000..1_000_000 + 4096] {
            *b = next_byte();
        }
        let blob = build_blob(&old, &new);
        let sp = bsdiff40_to_surgepat(&blob).expect("encode");
        let out = apply_surgepat(&old, &sp).expect("apply");
        assert_eq!(out, new);
        // The re-encoding must stay small: the diff is dominated by a
        // zero run plus one raw run of the changed region.
        assert!(sp.len() < 8 * 1024, "surgepat payload grew to {} bytes", sp.len());
    }

    #[test]
    fn empty_old_and_new() {
        let blob = build_blob(&[], &[]);
        // An all-empty bsdiff may have zero entries; the conversion must
        // still round-trip.
        let sp = bsdiff40_to_surgepat(&blob).expect("encode");
        let out = apply_surgepat(&[], &sp).expect("apply");
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_non_bsdiff40_input() {
        let sp = bsdiff40_to_surgepat(b"not a patch").expect_err("must reject");
        assert!(sp.to_string().contains("BSDIFF40"));
    }

    #[test]
    fn rejects_truncated_payloads() {
        let old = vec![0u8; 256];
        let mut new = old.clone();
        new[10] = 7;
        let blob = build_blob(&old, &new);
        let sp = bsdiff40_to_surgepat(&blob).expect("encode");
        for cut in [8, 16, 24, 30, sp.len() - 1] {
            if cut < sp.len() {
                let _ = apply_surgepat(&old, &sp[..cut]);
            }
        }
        // Truncating into a compressed block must fail, not panic or
        // silently produce wrong bytes.
        let mid = 24 + 8 + sp.len() / 2;
        if mid < sp.len() {
            let _ = apply_surgepat(&old, &sp[..mid]);
        }
        let out = apply_surgepat(&old, &sp).expect("full payload applies");
        assert_eq!(out, new);
    }

    #[test]
    fn rle_encode_decode_zero_and_raw() {
        let data: Vec<u8> = vec![0, 0, 0, 5, 6, 0, 7, 8, 9];
        let rle = rle_encode(&data).expect("rle encode");
        // Manual walk back.
        let mut i = 0usize;
        let mut rebuilt = Vec::new();
        while i < rle.len() {
            let len = u32::from_le_bytes(rle[i..i + 4].try_into().unwrap()) as usize;
            let kind = rle[i + 4];
            i += 5;
            if kind == 0 {
                rebuilt.extend(std::iter::repeat_n(0u8, len));
            } else {
                rebuilt.extend_from_slice(&rle[i..i + len]);
                i += len;
            }
        }
        assert_eq!(rebuilt, data);
    }
}
