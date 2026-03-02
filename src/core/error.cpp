#include "core/error.hpp"

namespace surge {

std::string_view error_to_string(ErrorCode code) noexcept {
    switch (code) {
    case ErrorCode::Ok:
        return "Success";
    case ErrorCode::Unknown:
        return "Unknown error";
    case ErrorCode::Cancelled:
        return "Operation cancelled";
    case ErrorCode::NotFound:
        return "Not found";
    case ErrorCode::InvalidArgument:
        return "Invalid argument";
    case ErrorCode::InvalidManifest:
        return "Invalid manifest";
    case ErrorCode::IoError:
        return "I/O error";
    case ErrorCode::NetworkError:
        return "Network error";
    case ErrorCode::Timeout:
        return "Operation timed out";
    case ErrorCode::AuthError:
        return "Authentication error";
    case ErrorCode::StorageError:
        return "Storage error";
    case ErrorCode::UploadFailed:
        return "Upload failed";
    case ErrorCode::DownloadFailed:
        return "Download failed";
    case ErrorCode::ChecksumMismatch:
        return "Checksum mismatch";
    case ErrorCode::DecompressionErr:
        return "Decompression error";
    case ErrorCode::DiffError:
        return "Binary diff error";
    case ErrorCode::PatchError:
        return "Binary patch error";
    case ErrorCode::LockFailed:
        return "Lock acquisition failed";
    case ErrorCode::LockConflict:
        return "Lock conflict";
    case ErrorCode::SupervisorError:
        return "Supervisor error";
    case ErrorCode::VersionError:
        return "Version error";
    case ErrorCode::AlreadyUpToDate:
        return "Already up to date";
    }
    return "Unknown error code";
}

ErrorCode from_surge_result(int32_t result) noexcept {
    switch (result) {
    case 0:
        return ErrorCode::Ok;
    case -1:
        return ErrorCode::Unknown;
    case -2:
        return ErrorCode::Cancelled;
    case -3:
        return ErrorCode::NotFound;
    default:
        return static_cast<ErrorCode>(result);
    }
}

int32_t to_surge_result(ErrorCode code) noexcept {
    return static_cast<int32_t>(code);
}

// --- std::error_code integration ---

const char* SurgeErrorCategory::name() const noexcept {
    return "surge";
}

std::string SurgeErrorCategory::message(int ev) const {
    return std::string(error_to_string(static_cast<ErrorCode>(ev)));
}

const std::error_category& surge_error_category() noexcept {
    static const SurgeErrorCategory instance;
    return instance;
}

std::error_code make_error_code(ErrorCode code) noexcept {
    return {static_cast<int>(code), surge_error_category()};
}

}  // namespace surge
