/**
 * @file bsdiff_wrapper.cpp
 * @brief Modern C++ wrapper around bsdiff/bspatch.
 *
 * Cleanup follows the same order as the reference snapx implementation:
 *   1. Close the patch packer (which internally closes its underlying stream).
 *   2. Close remaining streams in reverse-open order.
 * The packer owns a reference to the patchfile stream and closes it during
 * its own close, so the patchfile must NOT be closed separately.
 */

#include "diff/bsdiff_wrapper.hpp"

extern "C" {
#include "bsdiff.h"
}

#include <cstring>
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace surge::diff {

DiffResult bsdiff(std::span<const uint8_t> old_data, std::span<const uint8_t> new_data) {
    DiffResult result;

    struct bsdiff_stream oldfile = {};
    struct bsdiff_stream newfile = {};
    struct bsdiff_stream patchfile = {};
    struct bsdiff_ctx ctx = {};
    struct bsdiff_patch_packer packer = {};

    int ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ, old_data.data(), old_data.size(), &oldfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(fmt::format("bsdiff: failed to open old stream: {}", ret));
    }

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ, new_data.data(), new_data.size(), &newfile);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bsdiff: failed to open new stream: {}", ret));
    }

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE, nullptr, 0, &patchfile);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bsdiff: failed to open patch stream: {}", ret));
    }

    ret = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_WRITE, &patchfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&patchfile);
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bsdiff: failed to open patch packer: {}", ret));
    }

    ret = ::bsdiff(&ctx, &oldfile, &newfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        // Cleanup order: packer (closes patchfile internally), then streams
        bsdiff_close_patch_packer(&packer);
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bsdiff: diff computation failed: {}", ret));
    }

    // Read the patch buffer BEFORE closing the packer. The bsdiff() C function
    // already calls packer->flush() internally, so all compressed data is in the
    // patchfile stream. Closing the packer would also close the underlying
    // patchfile stream and free its state, making get_buffer() invalid.
    const void* patch_buffer = nullptr;
    size_t patch_buffer_len = 0;
    patchfile.get_buffer(patchfile.state, &patch_buffer, &patch_buffer_len);

    result.patch_data.resize(patch_buffer_len);
    std::memcpy(result.patch_data.data(), patch_buffer, patch_buffer_len);

    // Cleanup: packer close also closes patchfile internally
    bsdiff_close_patch_packer(&packer);
    // Do NOT close patchfile -- packer already did that
    bsdiff_close_stream(&newfile);
    bsdiff_close_stream(&oldfile);

    spdlog::debug("bsdiff: created patch: old={} new={} patch={}", old_data.size(), new_data.size(), patch_buffer_len);
    return result;
}

PatchResult bspatch(std::span<const uint8_t> old_data, std::span<const uint8_t> patch_data) {
    PatchResult result;

    struct bsdiff_stream oldfile = {};
    struct bsdiff_stream newfile = {};
    struct bsdiff_stream patchfile = {};
    struct bsdiff_ctx ctx = {};
    struct bsdiff_patch_packer packer = {};

    int ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ, old_data.data(), old_data.size(), &oldfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(fmt::format("bspatch: failed to open old stream: {}", ret));
    }

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE, nullptr, 0, &newfile);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bspatch: failed to open new stream: {}", ret));
    }

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ, patch_data.data(), patch_data.size(), &patchfile);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bspatch: failed to open patch stream: {}", ret));
    }

    ret = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_READ, &patchfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        bsdiff_close_stream(&patchfile);
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bspatch: failed to open patch packer: {}", ret));
    }

    ret = ::bspatch(&ctx, &oldfile, &newfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        // Cleanup: packer close also closes patchfile internally
        bsdiff_close_patch_packer(&packer);
        bsdiff_close_stream(&newfile);
        bsdiff_close_stream(&oldfile);
        throw std::runtime_error(fmt::format("bspatch: patch application failed: {}", ret));
    }

    // Extract the output buffer from newfile (independent of packer/patchfile)
    const void* new_buffer = nullptr;
    size_t new_buffer_len = 0;
    newfile.get_buffer(newfile.state, &new_buffer, &new_buffer_len);

    result.new_data.resize(new_buffer_len);
    std::memcpy(result.new_data.data(), new_buffer, new_buffer_len);

    // Cleanup: packer close also closes patchfile internally
    bsdiff_close_patch_packer(&packer);
    // Do NOT close patchfile -- packer already did that
    bsdiff_close_stream(&newfile);
    bsdiff_close_stream(&oldfile);

    spdlog::debug("bspatch: applied patch: old={} patch={} new={}", old_data.size(), patch_data.size(), new_buffer_len);
    return result;
}

}  // namespace surge::diff
