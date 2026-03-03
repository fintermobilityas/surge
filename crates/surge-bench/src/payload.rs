use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Simple xorshift64 PRNG for deterministic data generation without external deps.
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

struct FileSpec {
    name: String,
    size: u64,
}

/// Realistic .NET-style DLL names for generated files.
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
    "System.Net.Http",
    "System.Text.Json",
    "System.Text.Encoding",
    "System.Xml.Linq",
    "System.Security.Cryptography",
    "System.ComponentModel",
    "System.Reflection",
    "System.Diagnostics.Process",
    "System.IO.Compression",
    "System.IO.Pipelines",
    "System.Memory",
    "System.Buffers",
    "System.Numerics.Vectors",
    "System.Runtime.InteropServices",
    "System.Console",
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
    let mut specs = Vec::new();
    let mut dll_idx = 0;
    let mut native_idx = 0;

    let next_dll_name = |idx: &mut usize| -> String {
        let name = if *idx < DLL_NAMES.len() {
            format!("{}.dll", DLL_NAMES[*idx])
        } else {
            format!("Assembly.Generated.{idx}.dll")
        };
        *idx += 1;
        name
    };

    let next_native_name = |idx: &mut usize| -> String {
        let name = if *idx < NATIVE_LIB_NAMES.len() {
            format!("{}.so", NATIVE_LIB_NAMES[*idx])
        } else {
            format!("libnative.generated.{idx}.so")
        };
        *idx += 1;
        name
    };

    // 184 small DLLs: 1–100 KB
    for _ in 0..184 {
        let size = scale_size(rng.next_u64() % 99_000 + 1_000, scale);
        specs.push(FileSpec {
            name: next_dll_name(&mut dll_idx),
            size,
        });
    }

    // 85 medium DLLs: 100 KB–1 MB
    for _ in 0..85 {
        let size = scale_size(rng.next_u64() % 924_000 + 100_000, scale);
        specs.push(FileSpec {
            name: next_dll_name(&mut dll_idx),
            size,
        });
    }

    // 19 native .so libs: 100 KB–1 MB
    for _ in 0..19 {
        let size = scale_size(rng.next_u64() % 924_000 + 100_000, scale);
        specs.push(FileSpec {
            name: next_native_name(&mut native_idx),
            size,
        });
    }

    // 15 large DLLs: 1–10 MB
    for _ in 0..15 {
        let size = scale_size(rng.next_u64() % 9_000_000 + 1_000_000, scale);
        specs.push(FileSpec {
            name: next_dll_name(&mut dll_idx),
            size,
        });
    }

    // 4 ONNX models: 5–21 MB
    let onnx_names = [
        "model_detection.onnx",
        "model_recognition.onnx",
        "model_segmentation.onnx",
        "model_classification.onnx",
    ];
    for name in &onnx_names {
        let size = scale_size(rng.next_u64() % 16_000_000 + 5_000_000, scale);
        specs.push(FileSpec {
            name: (*name).to_string(),
            size,
        });
    }

    // 4 more large DLLs: 1–10 MB
    for _ in 0..4 {
        let size = scale_size(rng.next_u64() % 9_000_000 + 1_000_000, scale);
        specs.push(FileSpec {
            name: next_dll_name(&mut dll_idx),
            size,
        });
    }

    // 3 PDB files: 100–500 KB
    let pdb_names = ["App.pdb", "App.Core.pdb", "App.Services.pdb"];
    for name in &pdb_names {
        let size = scale_size(rng.next_u64() % 400_000 + 100_000, scale);
        specs.push(FileSpec {
            name: (*name).to_string(),
            size,
        });
    }

    // 1 dominant native SDK: ~1004 MB
    let size = scale_size(1_004_000_000, scale);
    specs.push(FileSpec {
        name: "nativesdk.so".to_string(),
        size,
    });

    // 2 JSON config files: 1–10 KB
    for name in &["appsettings.json", "config.json"] {
        let size = scale_size(rng.next_u64() % 9_000 + 1_000, scale);
        specs.push(FileSpec {
            name: (*name).to_string(),
            size,
        });
    }

    // 2 text files: 1–10 KB
    for name in &["LICENSE.txt", "NOTICE.txt"] {
        let size = scale_size(rng.next_u64() % 9_000 + 1_000, scale);
        specs.push(FileSpec {
            name: (*name).to_string(),
            size,
        });
    }

    // 1 SVG file: 1–10 KB
    let size = scale_size(rng.next_u64() % 9_000 + 1_000, scale);
    specs.push(FileSpec {
        name: "logo.svg".to_string(),
        size,
    });

    // 2 main app files: 500 KB–2 MB
    let size = scale_size(rng.next_u64() % 1_500_000 + 500_000, scale);
    specs.push(FileSpec {
        name: "MyApp".to_string(),
        size,
    });
    let size = scale_size(rng.next_u64() % 1_500_000 + 500_000, scale);
    specs.push(FileSpec {
        name: "MyApp.dll".to_string(),
        size,
    });

    specs
}

fn scale_size(base: u64, scale: f64) -> u64 {
    #[allow(clippy::cast_lossless)]
    let scaled = (base as f64 * scale) as u64;
    scaled.max(1)
}

fn write_random_file(rng: &mut Xorshift64, path: &Path, size: u64) -> io::Result<()> {
    // Write in 64 KB chunks to avoid large allocations
    const CHUNK: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK];
    let file = fs::File::create(path)?;
    let mut writer = io::BufWriter::new(file);
    let mut remaining = size as usize;
    while remaining > 0 {
        let n = remaining.min(CHUNK);
        rng.fill_bytes(&mut buf[..n]);
        io::Write::write_all(&mut writer, &buf[..n])?;
        remaining -= n;
    }
    Ok(())
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

pub fn generate(work_dir: &Path, scale: f64, seed: u64) -> io::Result<GeneratedPayload> {
    let v1_dir = work_dir.join("v1");
    let v2_dir = work_dir.join("v2");
    fs::create_dir_all(&v1_dir)?;
    fs::create_dir_all(&v2_dir)?;

    let mut rng = Xorshift64::new(seed);
    let specs = build_file_specs(&mut rng, scale);
    let total_files = specs.len();

    // Generate v1
    for spec in &specs {
        let path = v1_dir.join(&spec.name);
        write_random_file(&mut rng, &path, spec.size)?;
    }

    let total_size_v1 = dir_size(&v1_dir)?;

    // Generate v2: start by copying all v1 files
    for entry in fs::read_dir(&v1_dir)? {
        let entry = entry?;
        fs::copy(entry.path(), v2_dir.join(entry.file_name()))?;
    }

    // Regenerate ~5-10% of small/medium files (simulates recompiled DLLs)
    let num_to_regen = total_files / 15; // ~6-7%
    let mut regen_rng = Xorshift64::new(seed.wrapping_add(1000));
    for spec in specs.iter().take(num_to_regen) {
        let path = v2_dir.join(&spec.name);
        write_random_file(&mut regen_rng, &path, spec.size)?;
    }

    // Patch 4 KB at a fixed offset in nativesdk.so
    let nativesdk_path = v2_dir.join("nativesdk.so");
    if nativesdk_path.exists() {
        let mut data = fs::read(&nativesdk_path)?;
        let offset = 4096.min(data.len());
        let patch_end = (offset + 4096).min(data.len());
        let mut patch_rng = Xorshift64::new(seed.wrapping_add(2000));
        patch_rng.fill_bytes(&mut data[offset..patch_end]);
        fs::write(&nativesdk_path, &data)?;
    }

    // Add 2 new small files
    let new_file_size = scale_size(5_000, scale);
    write_random_file(&mut regen_rng, &v2_dir.join("NewFeature.dll"), new_file_size)?;
    write_random_file(
        &mut regen_rng,
        &v2_dir.join("NewFeature.Config.json"),
        scale_size(1_000, scale),
    )?;

    // Remove 1 file
    let _ = fs::remove_file(v2_dir.join("NOTICE.txt"));

    let total_size_v2 = dir_size(&v2_dir)?;

    Ok(GeneratedPayload {
        v1_dir,
        v2_dir,
        total_files,
        total_size_v1,
        total_size_v2,
    })
}
