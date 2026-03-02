/**
 * @file bsdiff_wrapper.hpp
 * @brief C++ wrapper around bsdiff / bspatch for binary delta creation and application.
 */

#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace surge::diff {

/** Result of a binary diff operation. */
struct DiffResult {
    std::vector<uint8_t> patch_data;
};

/** Result of a binary patch operation. */
struct PatchResult {
    std::vector<uint8_t> new_data;
};

/**
 * Create a binary diff between two buffers using bsdiff.
 *
 * @param old_data The original (old) file contents.
 * @param new_data The updated (new) file contents.
 * @return DiffResult containing the patch bytes.
 * @throws std::runtime_error if the diff operation fails.
 */
DiffResult bsdiff(std::span<const uint8_t> old_data,
                  std::span<const uint8_t> new_data);

/**
 * Apply a binary patch to reconstruct the new file from the old file.
 *
 * @param old_data   The original (old) file contents.
 * @param patch_data The patch produced by bsdiff().
 * @return PatchResult containing the reconstructed new file contents.
 * @throws std::runtime_error if the patch operation fails.
 */
PatchResult bspatch(std::span<const uint8_t> old_data,
                    std::span<const uint8_t> patch_data);

} // namespace surge::diff
