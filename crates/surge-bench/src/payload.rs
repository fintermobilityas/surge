use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i < buf.len() {
            let val = self.next_u64().to_le_bytes();
            let remaining = buf.len() - i;
            let to_copy = remaining.min(8);
            buf[i..i + to_copy].copy_from_slice(&val[..to_copy]);
            i += to_copy;
        }
    }
}

pub struct GeneratedPayload {
    pub v1_dir: PathBuf,
    pub v2_dir: PathBuf,
    pub total_files: usize,
    pub total_size_v1: u64,
    pub total_size_v2: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioProfile {
    FullRelease,
    SdkOnly,
}

impl ScenarioProfile {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullRelease => "full_release",
            Self::SdkOnly => "sdk_only",
        }
    }
}

#[derive(Clone, Copy)]
enum FilePattern {
    NativeSdk,
    Model,
    Binary,
    Text,
}

struct FileSpec {
    name: String,
    size: u64,
    pattern: FilePattern,
}

pub struct PayloadTemplate {
    scale: f64,
    specs: Vec<FileSpec>,
    pub total_files: usize,
}

const LARGE_APP_TOTAL_FILES: usize = 319;
const LARGE_APP_TOTAL_BYTES: u64 = 1_244_933_949;

const LARGE_APP_CORE_FILES: &[(&str, u64, FilePattern)] = &[
    ("nativesdk.so", 1_063_055_160, FilePattern::NativeSdk),
    ("model-country-classifier.onnx", 21_974_034, FilePattern::Model),
    ("System.Private.CoreLib.dll", 15_562_032, FilePattern::Binary),
    ("model-vehicle-detector.onnx", 10_587_996, FilePattern::Model),
    ("model-anpr-detector.onnx", 10_563_807, FilePattern::Model),
    ("libSkiaSharp.so", 9_244_960, FilePattern::Binary),
    ("System.Private.Xml.dll", 7_900_936, FilePattern::Binary),
    ("libcoreclr.so", 7_101_928, FilePattern::Binary),
    ("libsurge.so", 5_951_944, FilePattern::Binary),
    ("PhoneNumbers.dll", 5_025_280, FilePattern::Binary),
    ("libclrjit.so", 4_785_584, FilePattern::Binary),
    ("System.Linq.Expressions.dll", 3_724_600, FilePattern::Binary),
    ("app.main.dll", 3_469_312, FilePattern::Binary),
    ("app-supervisor", 2_814_856, FilePattern::Binary),
    ("System.Data.Common.dll", 2_804_536, FilePattern::Binary),
    ("System.Security.Cryptography.dll", 2_638_640, FilePattern::Binary),
    ("libHarfBuzzSharp.so", 2_471_408, FilePattern::Binary),
    ("libmscordaccore.so", 2_395_328, FilePattern::Binary),
    ("Avalonia.Base.dll", 2_127_360, FilePattern::Binary),
    (
        "System.Private.DataContractSerialization.dll",
        2_071_816,
        FilePattern::Binary,
    ),
    ("System.Text.Json.dll", 1_883_400, FilePattern::Binary),
    ("System.Net.Http.dll", 1_725_704, FilePattern::Binary),
    ("libmscordbi.so", 1_631_336, FilePattern::Binary),
    ("Azure.Storage.Blobs.dll", 1_363_016, FilePattern::Binary),
    ("System.Reactive.dll", 1_349_584, FilePattern::Binary),
    ("System.Linq.AsyncEnumerable.dll", 1_329_464, FilePattern::Binary),
    ("System.Linq.Async.dll", 1_185_744, FilePattern::Binary),
    ("Microsoft.VisualBasic.Core.dll", 1_166_648, FilePattern::Binary),
    ("System.Reflection.Metadata.dll", 1_142_576, FilePattern::Binary),
    ("Avalonia.Controls.dll", 1_083_392, FilePattern::Binary),
    ("System.Text.RegularExpressions.dll", 1_036_080, FilePattern::Binary),
    ("libSystem.IO.Compression.Native.so", 1_025_728, FilePattern::Binary),
    ("libclrgcexp.so", 1_022_096, FilePattern::Binary),
    ("app.infrastructure.dll", 993_792, FilePattern::Binary),
    ("DynamicData.dll", 970_240, FilePattern::Binary),
    ("libclrgc.so", 937_624, FilePattern::Binary),
    ("NLog.dll", 924_672, FilePattern::Binary),
    ("libcoreclrtraceptprovider.so", 890_904, FilePattern::Binary),
    ("System.Collections.Immutable.dll", 853_264, FilePattern::Binary),
    ("System.Text.Encoding.CodePages.dll", 852_752, FilePattern::Binary),
];

const DLL_NAMES: &[&str] = &[
    "Microsoft.Extensions.DependencyInjection",
    "Microsoft.Extensions.Logging",
    "Microsoft.Extensions.Configuration",
    "Microsoft.Extensions.Options",
    "Microsoft.Extensions.Hosting",
    "Microsoft.Extensions.Http",
    "Microsoft.Extensions.Caching.Memory",
    "Microsoft.Extensions.FileProviders",
    "Microsoft.Extensions.Primitives",
    "Microsoft.Extensions.Diagnostics",
    "System.Runtime",
    "System.Collections",
    "System.Linq",
    "System.IO",
    "System.Threading",
    "System.Net.Http.Json",
    "System.Text.Json.Nodes",
    "System.Text.Encoding.Extensions",
    "System.Xml.Linq",
    "System.Security.Cryptography.Pkcs",
    "System.ComponentModel.Annotations",
    "System.Reflection.Emit",
    "System.Diagnostics.Tracing",
    "System.IO.Hashing",
    "System.IO.MemoryMappedFiles",
    "System.Memory.Data",
    "System.Buffers.Text",
    "System.Numerics.Tensors",
    "System.Runtime.Loader",
    "System.Console.Abstractions",
    "Newtonsoft.Json",
    "AutoMapper",
    "FluentValidation",
    "MediatR",
    "Polly",
    "Serilog",
    "Serilog.Sinks.Console",
    "Serilog.Sinks.File",
    "StackExchange.Redis",
    "Dapper",
    "Npgsql",
    "Microsoft.EntityFrameworkCore",
    "Microsoft.EntityFrameworkCore.Relational",
    "Microsoft.EntityFrameworkCore.SqlServer",
    "Grpc.Core",
    "Grpc.Net.Client",
    "Google.Protobuf",
    "Azure.Core",
    "Azure.Identity",
    "Azure.Storage.Blobs",
    "Microsoft.Identity.Client",
    "Microsoft.AspNetCore.Mvc",
    "Microsoft.AspNetCore.Routing",
    "Microsoft.AspNetCore.Authentication",
    "Microsoft.AspNetCore.Authorization",
    "Microsoft.AspNetCore.Cors",
    "Microsoft.AspNetCore.SignalR",
    "Microsoft.AspNetCore.Http",
    "Microsoft.AspNetCore.Hosting",
    "Microsoft.AspNetCore.Server.Kestrel",
    "Microsoft.AspNetCore.StaticFiles",
];

const NATIVE_LIB_NAMES: &[&str] = &[
    "libgrpc_csharp_ext",
    "libe_sqlite3",
    "libSystem.Native",
    "libSystem.Security.Cryptography.Native.OpenSsl",
    "libSystem.IO.Compression.Native",
    "libSystem.Net.Security.Native",
    "libSystem.Globalization.Native",
    "libclrjit",
    "libcoreclr",
    "libhostpolicy",
    "libhostfxr",
    "libmscordaccore",
    "libmscordbi",
    "libdbgshim",
    "libcoreclrtraceptprovider",
    "libSystem.Native.so",
    "libSkiaSharp",
    "libHarfBuzzSharp",
    "libonnxruntime",
];

fn build_file_specs(rng: &mut Xorshift64, scale: f64) -> Vec<FileSpec> {
    let mut specs: Vec<FileSpec> = LARGE_APP_CORE_FILES
        .iter()
        .map(|(name, size, pattern)| FileSpec {
            name: (*name).to_string(),
            size: scale_size(*size, scale),
            pattern: *pattern,
        })
        .collect();

    append_filler_specs(rng, &mut specs, scale);
    specs
}

impl PayloadTemplate {
    #[must_use]
    pub fn new(scale: f64, seed: u64) -> Self {
        let mut rng = Xorshift64::new(seed);
        let specs = build_file_specs(&mut rng, scale);
        let total_files = specs.len();
        Self {
            scale,
            specs,
            total_files,
        }
    }

    pub fn write_base(&self, dir: &Path, seed: u64) -> io::Result<u64> {
        reset_directory(dir)?;
        for spec in &self.specs {
            write_synthetic_file(seed, spec, &dir.join(&spec.name))?;
        }
        dir_size(dir)
    }

    pub fn mutate_version(
        &self,
        dir: &Path,
        seed: u64,
        version_index: usize,
        scenario: ScenarioProfile,
    ) -> io::Result<u64> {
        match scenario {
            ScenarioProfile::FullRelease => {
                rewrite_large_release_files(&self.specs, dir, seed, version_index)?;
                mutate_nativesdk(dir, seed, version_index)?;
                write_feature_files(dir, self.scale, seed, version_index)?;
                remove_rotating_config(dir, version_index)?;
            }
            ScenarioProfile::SdkOnly => {
                mutate_nativesdk(dir, seed, version_index)?;
            }
        }

        dir_size(dir)
    }
}

fn append_filler_specs(rng: &mut Xorshift64, specs: &mut Vec<FileSpec>, scale: f64) {
    let target_files = LARGE_APP_TOTAL_FILES;
    let target_bytes = scale_size(LARGE_APP_TOTAL_BYTES, scale);
    let mut remaining_files = target_files.saturating_sub(specs.len());
    let mut remaining_bytes = target_bytes.saturating_sub(specs.iter().map(|spec| spec.size).sum());
    let mut dll_idx = 0usize;
    let mut native_idx = 0usize;

    while remaining_files > 0 {
        let files_left_after_this = remaining_files - 1;
        let min_tail_bytes = files_left_after_this as u64;
        let max_for_this = remaining_bytes.saturating_sub(min_tail_bytes).max(1);

        let preferred = if remaining_files % 29 == 0 {
            scale_size(320_000, scale)
        } else if remaining_files % 11 == 0 {
            scale_size(96_000, scale)
        } else {
            scale_size(48_000, scale)
        };
        let jitter_cap = scale_size(64_000, scale).max(1);
        let min_size = scale_size(4_096, scale).max(1);
        let mut size = preferred
            .saturating_add(rng.next_u64() % jitter_cap)
            .clamp(min_size, max_for_this);
        if remaining_files == 1 {
            size = remaining_bytes;
        }

        let (name, pattern) = if remaining_files % 29 == 0 {
            (format!("generated-data-{remaining_files:03}.bin"), FilePattern::Binary)
        } else if remaining_files % 11 == 0 {
            (next_native_name(&mut native_idx), FilePattern::Binary)
        } else if remaining_files % 7 == 0 {
            (format!("generated-config-{remaining_files:03}.json"), FilePattern::Text)
        } else {
            (next_dll_name(&mut dll_idx), FilePattern::Binary)
        };

        specs.push(FileSpec { name, size, pattern });
        remaining_files -= 1;
        remaining_bytes = remaining_bytes.saturating_sub(size);
    }
}

fn next_dll_name(idx: &mut usize) -> String {
    let name = if *idx < DLL_NAMES.len() {
        format!("{}.dll", DLL_NAMES[*idx])
    } else {
        format!("Assembly.Generated.{}.dll", *idx)
    };
    *idx += 1;
    name
}

fn next_native_name(idx: &mut usize) -> String {
    let name = if *idx < NATIVE_LIB_NAMES.len() {
        format!("{}.so", NATIVE_LIB_NAMES[*idx])
    } else {
        format!("libnative.generated.{}.so", *idx)
    };
    *idx += 1;
    name
}

fn scale_size(base: u64, scale: f64) -> u64 {
    #[allow(clippy::cast_lossless)]
    let scaled = (base as f64 * scale) as u64;
    scaled.max(1)
}

fn mix_seed(seed: u64, name: &str) -> u64 {
    let mut mixed = seed ^ 0x9e37_79b9_7f4a_7c15;
    for byte in name.bytes() {
        mixed = mixed.rotate_left(9) ^ u64::from(byte);
        mixed = mixed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    }
    mixed
}

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

fn write_synthetic_file(seed: u64, spec: &FileSpec, path: &Path) -> io::Result<()> {
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

fn reset_directory(dir: &Path) -> io::Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)
}

fn copy_flat_directory(from: &Path, to: &Path) -> io::Result<()> {
    reset_directory(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        fs::copy(entry.path(), to.join(entry.file_name()))?;
    }
    Ok(())
}

fn rewrite_large_release_files(specs: &[FileSpec], dir: &Path, seed: u64, version_index: usize) -> io::Result<()> {
    let rewrite_count = (specs.len() / 20).max(1);
    let offset = version_index.saturating_sub(2) % specs.len().max(1);
    let version_seed = seed.wrapping_add((version_index as u64).wrapping_mul(1_000));

    for step in 0..rewrite_count {
        let spec = &specs[(offset + step) % specs.len()];
        write_synthetic_file(version_seed, spec, &dir.join(&spec.name))?;
    }

    Ok(())
}

fn mutate_nativesdk(dir: &Path, seed: u64, version_index: usize) -> io::Result<()> {
    let nativesdk_path = dir.join("nativesdk.so");
    if !nativesdk_path.exists() {
        return Ok(());
    }

    let mut data = fs::read(&nativesdk_path)?;
    if data.is_empty() {
        return Ok(());
    }

    let page_span = 4096usize;
    let page_index = version_index.saturating_sub(1) % 64;
    let offset = (page_index * page_span).min(data.len().saturating_sub(1));
    let patch_end = (offset + page_span).min(data.len());
    let mut patch_rng = Xorshift64::new(seed.wrapping_add((version_index as u64).wrapping_mul(2_000)));
    patch_rng.fill_bytes(&mut data[offset..patch_end]);
    fs::write(&nativesdk_path, &data)
}

fn write_feature_files(dir: &Path, scale: f64, seed: u64, version_index: usize) -> io::Result<()> {
    let feature_binary = FileSpec {
        name: "app.feature.dll".to_string(),
        size: scale_size(411_136, scale),
        pattern: FilePattern::Binary,
    };
    let feature_config = FileSpec {
        name: "app.feature.config.json".to_string(),
        size: scale_size(12_000, scale),
        pattern: FilePattern::Text,
    };

    write_synthetic_file(
        seed.wrapping_add((version_index as u64).wrapping_mul(3_000)),
        &feature_binary,
        &dir.join(&feature_binary.name),
    )?;
    write_synthetic_file(
        seed.wrapping_add((version_index as u64).wrapping_mul(4_000)),
        &feature_config,
        &dir.join(&feature_config.name),
    )
}

fn remove_rotating_config(dir: &Path, version_index: usize) -> io::Result<()> {
    const CANDIDATES: &[&str] = &[
        "generated-config-007.json",
        "generated-config-014.json",
        "generated-config-021.json",
        "generated-config-028.json",
    ];
    let candidate = CANDIDATES[version_index.saturating_sub(2) % CANDIDATES.len()];
    match fs::remove_file(dir.join(candidate)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn dir_size(dir: &Path) -> io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() {
            total += meta.len();
        }
    }
    Ok(total)
}

pub fn generate(work_dir: &Path, scale: f64, seed: u64, scenario: ScenarioProfile) -> io::Result<GeneratedPayload> {
    let v1_dir = work_dir.join("v1");
    let v2_dir = work_dir.join("v2");
    let template = PayloadTemplate::new(scale, seed);
    let total_files = template.total_files;
    let total_size_v1 = template.write_base(&v1_dir, seed)?;
    copy_flat_directory(&v1_dir, &v2_dir)?;
    let total_size_v2 = template.mutate_version(&v2_dir, seed, 2, scenario)?;

    Ok(GeneratedPayload {
        v1_dir,
        v2_dir,
        total_files,
        total_size_v1,
        total_size_v2,
    })
}
