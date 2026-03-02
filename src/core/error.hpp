/**
 * @file error.hpp
 * @brief Error codes and std::error_code integration for Surge.
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

namespace surge {

/** Strongly-typed error codes used throughout the library. */
enum class ErrorCode : int32_t {
    Ok               =  0,
    Unknown          = -1,
    Cancelled        = -2,
    NotFound         = -3,
    InvalidArgument  = -10,
    InvalidManifest  = -11,
    IoError          = -20,
    NetworkError     = -21,
    Timeout          = -22,
    AuthError        = -23,
    StorageError     = -30,
    UploadFailed     = -31,
    DownloadFailed   = -32,
    ChecksumMismatch = -40,
    DecompressionErr = -41,
    DiffError        = -50,
    PatchError       = -51,
    LockFailed       = -60,
    LockConflict     = -61,
    SupervisorError  = -70,
    VersionError     = -80,
    AlreadyUpToDate  = -81,
};

/** Convert an ErrorCode to a human-readable string. */
std::string_view error_to_string(ErrorCode code) noexcept;

/** Map a C API surge_result to an ErrorCode. */
ErrorCode from_surge_result(int32_t result) noexcept;

/** Map an ErrorCode to a C API surge_result. */
int32_t to_surge_result(ErrorCode code) noexcept;

/* ----- std::error_code integration ----- */

class SurgeErrorCategory final : public std::error_category {
public:
    const char* name() const noexcept override;
    std::string message(int ev) const override;
};

/** Singleton instance of the Surge error category. */
const std::error_category& surge_error_category() noexcept;

/** Create a std::error_code from an ErrorCode. */
std::error_code make_error_code(ErrorCode code) noexcept;

} // namespace surge

/** Register ErrorCode as an error-code enum so implicit conversion works. */
template <>
struct std::is_error_code_enum<surge::ErrorCode> : std::true_type {};
