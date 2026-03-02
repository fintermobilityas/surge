#include "async/download_manager.hpp"
#include "core/context.hpp"
#include "crypto/sha256.hpp"
#include <curl/curl.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <fstream>
#include <mutex>
#include <chrono>

namespace surge::async {

namespace {

struct TransferData {
    std::ofstream file;
    int64_t bytes_downloaded = 0;
    bool cancelled = false;
    std::stop_token stop;
};

size_t write_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* td = static_cast<TransferData*>(userdata);
    if (td->cancelled || td->stop.stop_requested()) return 0;
    size_t total = size * nmemb;
    td->file.write(ptr, static_cast<std::streamsize>(total));
    if (td->file.fail()) return 0;
    td->bytes_downloaded += static_cast<int64_t>(total);
    return total;
}

int progress_cb(void* userdata, curl_off_t /*dltotal*/, curl_off_t /*dlnow*/,
                curl_off_t /*ultotal*/, curl_off_t /*ulnow*/) {
    auto* td = static_cast<TransferData*>(userdata);
    return (td->cancelled || td->stop.stop_requested()) ? 1 : 0;
}

} // anonymous namespace

struct DownloadManager::Impl {
    Context& ctx;
    int32_t max_concurrent = 4;
    int64_t max_speed_bps = 0;

    explicit Impl(Context& c) : ctx(c) {
        auto budget = ctx.resource_budget();
        if (budget.max_concurrent_downloads > 0) {
            max_concurrent = budget.max_concurrent_downloads;
        }
        if (budget.max_download_speed_bps > 0) {
            max_speed_bps = budget.max_download_speed_bps;
        }
    }

    std::vector<DownloadResult> do_download(
        std::span<const DownloadRequest> requests,
        std::stop_token stop,
        DownloadProgressCallback progress_cb) {

        std::vector<DownloadResult> results(requests.size());
        for (size_t i = 0; i < requests.size(); ++i) {
            results[i].index = static_cast<int32_t>(i);
        }

        if (requests.empty()) return results;

        CURLM* multi = curl_multi_init();
        if (!multi) {
            for (auto& r : results) {
                r.status = -1;
                r.error_message = "Failed to initialize curl multi handle";
            }
            return results;
        }

        // Track active transfers
        struct ActiveTransfer {
            size_t index;
            CURL* easy;
            TransferData data;
        };

        std::vector<std::unique_ptr<ActiveTransfer>> active;
        size_t next_to_start = 0;
        int32_t files_done = 0;
        int64_t total_bytes_done = 0;
        int64_t total_bytes_total = 0;

        // Compute total expected size
        for (const auto& req : requests) {
            if (req.expected_size > 0) {
                total_bytes_total += req.expected_size;
            }
        }

        auto report_progress = [&] {
            if (!progress_cb) return;
            DownloadProgress dp;
            dp.files_done = files_done;
            dp.files_total = static_cast<int32_t>(requests.size());
            dp.total_bytes_done = total_bytes_done;
            dp.total_bytes_total = total_bytes_total;
            progress_cb(dp);
        };

        auto start_transfer = [&](size_t idx) -> bool {
            const auto& req = requests[idx];

            auto transfer = std::make_unique<ActiveTransfer>();
            transfer->index = idx;
            transfer->data.stop = stop;

            // Create parent directory
            std::error_code ec;
            std::filesystem::create_directories(req.dest_path.parent_path(), ec);

            transfer->data.file.open(req.dest_path,
                                     std::ios::binary | std::ios::trunc);
            if (!transfer->data.file.is_open()) {
                results[idx].status = -1;
                results[idx].error_message = fmt::format(
                    "Failed to open '{}' for writing", req.dest_path.string());
                return false;
            }

            CURL* easy = curl_easy_init();
            if (!easy) {
                results[idx].status = -1;
                results[idx].error_message = "Failed to initialize curl";
                return false;
            }

            transfer->easy = easy;

            curl_easy_setopt(easy, CURLOPT_URL, req.url.c_str());
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, write_cb);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA, &transfer->data);
            curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION, progress_cb);
            curl_easy_setopt(easy, CURLOPT_XFERINFODATA, &transfer->data);
            curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(easy, CURLOPT_MAXREDIRS, 5L);
            curl_easy_setopt(easy, CURLOPT_FAILONERROR, 1L);
            curl_easy_setopt(easy, CURLOPT_PRIVATE,
                             reinterpret_cast<void*>(transfer.get()));

            if (max_speed_bps > 0) {
                auto per_transfer = max_speed_bps /
                    static_cast<int64_t>(max_concurrent);
                curl_easy_setopt(easy, CURLOPT_MAX_RECV_SPEED_LARGE,
                                 static_cast<curl_off_t>(per_transfer));
            }

            curl_multi_add_handle(multi, easy);
            active.push_back(std::move(transfer));
            return true;
        };

        // Start initial batch
        while (next_to_start < requests.size() &&
               static_cast<int32_t>(active.size()) < max_concurrent) {
            start_transfer(next_to_start++);
        }

        // Run loop
        while (!active.empty()) {
            int still_running = 0;
            curl_multi_perform(multi, &still_running);

            // Check completed
            CURLMsg* msg;
            int msgs_in_queue;
            while ((msg = curl_multi_info_read(multi, &msgs_in_queue))) {
                if (msg->msg != CURLMSG_DONE) continue;

                CURL* easy = msg->easy_handle;
                CURLcode curl_result = msg->data.result;

                // Find the transfer
                void* priv = nullptr;
                curl_easy_getinfo(easy, CURLINFO_PRIVATE, &priv);
                auto* transfer = static_cast<ActiveTransfer*>(priv);
                size_t idx = transfer->index;

                long http_code = 0;
                curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_code);

                transfer->data.file.close();
                curl_multi_remove_handle(multi, easy);
                curl_easy_cleanup(easy);

                results[idx].http_status = static_cast<int32_t>(http_code);
                results[idx].bytes_downloaded = transfer->data.bytes_downloaded;
                total_bytes_done += transfer->data.bytes_downloaded;

                if (curl_result == CURLE_OK && !transfer->data.cancelled &&
                    !stop.stop_requested()) {
                    results[idx].status = 0;

                    // Verify checksum if expected
                    if (!requests[idx].expected_sha256.empty()) {
                        try {
                            auto hash = crypto::sha256_hex_file(
                                requests[idx].dest_path);
                            results[idx].sha256 = hash;
                            if (hash != requests[idx].expected_sha256) {
                                results[idx].status = -1;
                                results[idx].error_message =
                                    "Checksum mismatch";
                                std::error_code ec;
                                std::filesystem::remove(
                                    requests[idx].dest_path, ec);
                            }
                        } catch (const std::exception& e) {
                            results[idx].status = -1;
                            results[idx].error_message = e.what();
                        }
                    }

                    spdlog::debug("Download #{} completed: {} bytes",
                                  idx, results[idx].bytes_downloaded);
                } else {
                    results[idx].status = -1;
                    if (stop.stop_requested() || transfer->data.cancelled) {
                        results[idx].error_message = "Cancelled";
                    } else {
                        results[idx].error_message =
                            curl_easy_strerror(curl_result);
                    }
                    std::error_code ec;
                    std::filesystem::remove(requests[idx].dest_path, ec);
                    spdlog::warn("Download #{} failed: {}",
                                 idx, results[idx].error_message);
                }

                files_done++;
                report_progress();

                // Remove from active list
                std::erase_if(active, [transfer](const auto& t) {
                    return t.get() == transfer;
                });

                // Start next if available
                if (next_to_start < requests.size() &&
                    !stop.stop_requested()) {
                    start_transfer(next_to_start++);
                }
            }

            if (stop.stop_requested()) {
                // Cancel all active transfers
                for (auto& t : active) {
                    t->data.cancelled = true;
                }
            }

            if (!active.empty() && still_running > 0) {
                int numfds = 0;
                curl_multi_poll(multi, nullptr, 0, 100, &numfds);
            }
        }

        curl_multi_cleanup(multi);
        return results;
    }
};

DownloadManager::DownloadManager(Context& ctx)
    : impl_(std::make_unique<Impl>(ctx)) {}

DownloadManager::~DownloadManager() = default;

std::vector<DownloadResult> DownloadManager::download(
    std::span<const DownloadRequest> requests,
    DownloadProgressCallback progress) {
    return impl_->do_download(requests, impl_->ctx.stop_token(), std::move(progress));
}

std::vector<DownloadResult> DownloadManager::download(
    std::span<const DownloadRequest> requests,
    std::stop_token stop_token,
    DownloadProgressCallback progress) {
    return impl_->do_download(requests, stop_token, std::move(progress));
}

void DownloadManager::set_max_concurrent(int32_t max_concurrent) {
    if (max_concurrent > 0) {
        impl_->max_concurrent = max_concurrent;
    }
}

void DownloadManager::set_max_speed(int64_t bytes_per_sec) {
    impl_->max_speed_bps = bytes_per_sec;
}

} // namespace surge::async
