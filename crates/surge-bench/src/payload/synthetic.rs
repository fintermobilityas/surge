use std::fs;
use std::io::{self, Write};
use std::path::Path;

use super::rng::{Xorshift64, mix_seed};
use super::specs::{FilePattern, FileSpec};

fn build_templates(rng: &mut Xorshift64, pattern: FilePattern, name: &str) -> Vec<Vec<u8>> {
    let template_count = match pattern {
        FilePattern::NativeSdk => 4,
        FilePattern::Model => 8,
        FilePattern::Binary => 6,
        FilePattern::Text => 3,
    };
    (0..template_count)
        .map(|idx| build_template(rng, pattern, name, idx))
        .collect()
}

fn build_template(rng: &mut Xorshift64, pattern: FilePattern, name: &str, template_idx: usize) -> Vec<u8> {
    const PAGE: usize = 4096;
    let mut template = vec![0u8; PAGE];

    match pattern {
        FilePattern::Text => {
            let line = format!(
                "{{\"file\":\"{name}\",\"section\":{template_idx},\"value\":\"{:016x}\",\"mode\":\"release\"}}\n",
                rng.next_u64()
            );
            let bytes = line.as_bytes();
            for chunk in template.chunks_mut(bytes.len().max(1)) {
                let len = chunk.len().min(bytes.len());
                chunk[..len].copy_from_slice(&bytes[..len]);
            }
        }
        FilePattern::NativeSdk | FilePattern::Model | FilePattern::Binary => {
            let marker = match pattern {
                FilePattern::NativeSdk => "ELF-NATIVE-SDK",
                FilePattern::Model => "MODEL-WEIGHTS",
                FilePattern::Binary => "MANAGED-ASSEMBLY",
                FilePattern::Text => unreachable!(),
            };
            let segment_size = match pattern {
                FilePattern::NativeSdk => 512,
                FilePattern::Model => 768,
                FilePattern::Binary => 256,
                FilePattern::Text => unreachable!(),
            };
            let active_size = match pattern {
                FilePattern::NativeSdk => 80,
                FilePattern::Model => 224,
                FilePattern::Binary => 112,
                FilePattern::Text => unreachable!(),
            };
            let name_bytes = name.as_bytes();

            for (segment_idx, chunk) in template.chunks_mut(segment_size).enumerate() {
                let chunk_len = chunk.len();
                let header = format!("{marker}:{name}:{template_idx}:{segment_idx:04x}|");
                let header_bytes = header.as_bytes();
                let header_len = header_bytes.len().min(chunk_len).min(active_size);
                chunk[..header_len].copy_from_slice(&header_bytes[..header_len]);

                for (offset, byte) in chunk
                    .iter_mut()
                    .enumerate()
                    .take(active_size.min(chunk_len))
                    .skip(header_len)
                {
                    let source = name_bytes[(offset + segment_idx) % name_bytes.len()];
                    *byte = source.wrapping_add((rng.next_u64() & 0x0f) as u8);
                }
            }
        }
    }

    template
}

fn fill_chunk(buf: &mut [u8], templates: &[Vec<u8>], pattern: FilePattern, rng: &mut Xorshift64, chunk_idx: u64) {
    const PAGE: usize = 4096;

    for (page_idx, page) in buf.chunks_mut(PAGE).enumerate() {
        let template = &templates[(chunk_idx as usize + page_idx) % templates.len()];
        page.copy_from_slice(&template[..page.len()]);

        let unique_prefix = match pattern {
            FilePattern::NativeSdk => 512,
            FilePattern::Binary => 256,
            FilePattern::Model => 768,
            FilePattern::Text => 32,
        }
        .min(page.len());
        let mut page_rng = Xorshift64::new(
            rng.next_u64() ^ chunk_idx.wrapping_mul(0x9e37_79b9) ^ (page_idx as u64).wrapping_mul(0xbf58_476d),
        );
        page_rng.fill_bytes(&mut page[..unique_prefix]);

        let mutation_count = match pattern {
            FilePattern::NativeSdk => 2,
            FilePattern::Binary => 6,
            FilePattern::Model => 18,
            FilePattern::Text => 1,
        };
        for mutation in 0..mutation_count {
            let offset = (rng.next_u64() as usize) % page.len();
            page[offset] ^= (rng.next_u64() as u8 & 0x0f).wrapping_add(mutation as u8);
        }

        let counter = (chunk_idx * 17 + page_idx as u64).to_le_bytes();
        let len = counter.len().min(page.len());
        page[..len].copy_from_slice(&counter[..len]);
    }
}

pub(super) fn write_synthetic_file(seed: u64, spec: &FileSpec, path: &Path) -> io::Result<()> {
    const CHUNK: usize = 64 * 1024;
    let mut writer = io::BufWriter::new(fs::File::create(path)?);
    let mut remaining = spec.size as usize;
    let mut buffer = vec![0u8; CHUNK];
    let mut rng = Xorshift64::new(mix_seed(seed, &spec.name));
    let templates = build_templates(&mut rng, spec.pattern, &spec.name);
    let mut chunk_idx = 0u64;

    while remaining > 0 {
        let len = remaining.min(CHUNK);
        fill_chunk(&mut buffer[..len], &templates, spec.pattern, &mut rng, chunk_idx);
        writer.write_all(&buffer[..len])?;
        remaining -= len;
        chunk_idx += 1;
    }

    writer.flush()
}
