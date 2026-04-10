use super::rng::Xorshift64;

#[derive(Clone, Copy)]
pub(super) enum FilePattern {
    NativeSdk,
    Model,
    Binary,
    Text,
}

pub(super) struct FileSpec {
    pub(super) name: String,
    pub(super) size: u64,
    pub(super) pattern: FilePattern,
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

pub(super) fn build_file_specs(rng: &mut Xorshift64, scale: f64) -> Vec<FileSpec> {
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

        let preferred = if remaining_files.is_multiple_of(29) {
            scale_size(320_000, scale)
        } else if remaining_files.is_multiple_of(11) {
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

        let (name, pattern) = if remaining_files.is_multiple_of(29) {
            (format!("generated-data-{remaining_files:03}.bin"), FilePattern::Binary)
        } else if remaining_files.is_multiple_of(11) {
            (next_native_name(&mut native_idx), FilePattern::Binary)
        } else if remaining_files.is_multiple_of(7) {
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

pub(super) fn scale_size(base: u64, scale: f64) -> u64 {
    #[allow(clippy::cast_lossless)]
    let scaled = (base as f64 * scale) as u64;
    scaled.max(1)
}
