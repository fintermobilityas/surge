/**
 * @file bsdiff_wrapper.cpp
 * @brief Modern C++ wrapper around bsdiff/bspatch.
 */

#include "diff/bsdiff_wrapper.hpp"

extern "C" {
#include "bsdiff.h"
}

#include <spdlog/spdlog.h>
#include <cstring>
#include <memory>
#include <stdexcept>

namespace surge::diff {

namespace {

// RAII wrapper for bsdiff streams and patch packers
struct StreamGuard {
    struct bsdiff_stream* streams[4] = {};
    struct bsdiff_patch_packer* packer = nullptr;
    int count = 0;

    void add(struct bsdiff_stream* s) {
        if (count < 4) streams[count++] = s;
    }

    ~StreamGuard() {
        if (packer) bsdiff_close_patch_packer(packer);
        for (int i = count - 1; i >= 0; --i) {
            if (streams[i]) bsdiff_close_stream(streams[i]);
        }
    }
};

} // anonymous namespace

DiffResult bsdiff(std::span<const uint8_t> old_data,
                  std::span<const uint8_t> new_data) {
    DiffResult result;

    struct bsdiff_stream oldfile = {nullptr};
    struct bsdiff_stream newfile = {nullptr};
    struct bsdiff_stream patchfile = {nullptr};
    struct bsdiff_ctx ctx = {nullptr};
    struct bsdiff_patch_packer packer = {nullptr};

    StreamGuard guard;

    int ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ,
        old_data.data(), old_data.size(), &oldfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bsdiff: failed to open old stream: {}", ret));
    }
    guard.add(&oldfile);

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ,
        new_data.data(), new_data.size(), &newfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bsdiff: failed to open new stream: {}", ret));
    }
    guard.add(&newfile);

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE,
        nullptr, 0, &patchfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bsdiff: failed to open patch stream: {}", ret));
    }
    guard.add(&patchfile);

    ret = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_WRITE, &patchfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bsdiff: failed to open patch packer: {}", ret));
    }
    guard.packer = &packer;

    ret = ::bsdiff(&ctx, &oldfile, &newfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bsdiff: diff computation failed: {}", ret));
    }

    // Close packer to flush output
    bsdiff_close_patch_packer(&packer);
    guard.packer = nullptr;

    // Extract the patch buffer
    const void* patch_buffer = nullptr;
    size_t patch_buffer_len = 0;
    patchfile.get_buffer(patchfile.state, &patch_buffer, &patch_buffer_len);

    result.patch_data.resize(patch_buffer_len);
    std::memcpy(result.patch_data.data(), patch_buffer, patch_buffer_len);

    spdlog::debug("bsdiff: created patch: old={} new={} patch={}",
                   old_data.size(), new_data.size(), patch_buffer_len);
    return result;
}

PatchResult bspatch(std::span<const uint8_t> old_data,
                    std::span<const uint8_t> patch_data) {
    PatchResult result;

    struct bsdiff_stream oldfile = {nullptr};
    struct bsdiff_stream newfile = {nullptr};
    struct bsdiff_stream patchfile = {nullptr};
    struct bsdiff_ctx ctx = {nullptr};
    struct bsdiff_patch_packer packer = {nullptr};

    StreamGuard guard;

    int ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ,
        old_data.data(), old_data.size(), &oldfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bspatch: failed to open old stream: {}", ret));
    }
    guard.add(&oldfile);

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE,
        nullptr, 0, &newfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bspatch: failed to open new stream: {}", ret));
    }
    guard.add(&newfile);

    ret = bsdiff_open_memory_stream(BSDIFF_MODE_READ,
        patch_data.data(), patch_data.size(), &patchfile);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bspatch: failed to open patch stream: {}", ret));
    }
    guard.add(&patchfile);

    ret = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_READ, &patchfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bspatch: failed to open patch packer: {}", ret));
    }
    guard.packer = &packer;

    ret = ::bspatch(&ctx, &oldfile, &newfile, &packer);
    if (ret != BSDIFF_SUCCESS) {
        throw std::runtime_error(
            fmt::format("bspatch: patch application failed: {}", ret));
    }

    // Extract the output buffer
    const void* new_buffer = nullptr;
    size_t new_buffer_len = 0;
    newfile.get_buffer(newfile.state, &new_buffer, &new_buffer_len);

    result.new_data.resize(new_buffer_len);
    std::memcpy(result.new_data.data(), new_buffer, new_buffer_len);

    spdlog::debug("bspatch: applied patch: old={} patch={} new={}",
                   old_data.size(), patch_data.size(), new_buffer_len);
    return result;
}

} // namespace surge::diff
