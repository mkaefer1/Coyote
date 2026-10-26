/**
 * This file is part of the Coyote <https://github.com/fpgasystems/Coyote>
 *
 * MIT Licence
 * Copyright (c) 2025, Systems Group, ETH Zurich
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _PERF_RDMA_STALL_WATCHDOG_HPP_
#define _PERF_RDMA_STALL_WATCHDOG_HPP_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <coyote/cThread.hpp>

// Debug aid for stalls with several concurrent connections: the benchmark loop records which phase it is in,
// and a background thread prints that phase together with the completion counters once nothing has moved for a while.
// The report also lists the cumulative counts of every watched Coyote thread in this process, to tell a dropped
// request (one completion missing) from a misrouted one (one completion too many on another thread).
// The raw completion counters must not be cleared during the benchmark for the cumulative comparison to hold.

// Seconds without any progress before a stall is reported
#define STALL_TIMEOUT_S 3

// Length of the marker RDMA WRITE a watchdog with send_marker posts on a stall; the debug ILAs
// (ila_dbg_cmd_queue, ila_dbg_rdma_wr_path) trigger on it. Must match DBG_MARKER_LEN in the RTL.
#define STALL_MARKER_LEN 4044

enum class BenchPhase : uint32_t { SETUP, SYNC, INVOKE, WAIT_LOCAL_WRITE, DONE };

inline const char* phaseName(BenchPhase phase) {
    switch (phase) {
        case BenchPhase::SETUP:            return "SETUP";
        case BenchPhase::SYNC:             return "SYNC (blocked in connSync, waiting for the peer)";
        case BenchPhase::INVOKE:           return "INVOKE (posting RDMA operations)";
        case BenchPhase::WAIT_LOCAL_WRITE: return "WAIT_LOCAL_WRITE (waiting for incoming data)";
        case BenchPhase::DONE:             return "DONE";
    }
    return "UNKNOWN";
}

// Written by the benchmark thread, read by the watchdogs
struct BenchProgress {
    std::atomic<BenchPhase> phase{BenchPhase::SETUP};
    std::atomic<uint32_t> size{0};
    std::atomic<uint32_t> run{0};
    std::atomic<uint32_t> n_runs{0};
    std::atomic<uint32_t> transfers{0};
    std::atomic<uint32_t> invoked{0};
    std::atomic<uint32_t> base_local_wr{0};
    std::atomic<uint32_t> base_remote_wr{0};

    // Since the start of the benchmark
    std::atomic<uint64_t> total_invoked{0};
    std::atomic<uint64_t> expected_local_wr{0};

    // Called at the start of every run, before connSync; expected_incoming is the number of LOCAL_WRITEs this run should produce
    void startRun(coyote::cThread &coyote_thread, uint32_t run_idx, uint32_t expected_incoming) {
        run = run_idx;
        invoked = 0;
        base_local_wr = coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE);
        base_remote_wr = coyote_thread.checkCompleted(coyote::CoyoteOper::REMOTE_RDMA_WRITE);
        expected_local_wr += expected_incoming;
    }

    // Called after every invoke()
    void addInvoked() {
        invoked++;
        total_invoked++;
    }
};

class StallWatchdog {
public:
    StallWatchdog(coyote::cThread &coyote_thread, BenchProgress &progress, std::string label, bool send_marker = false)
        : coyote_thread(coyote_thread), progress(progress), label(std::move(label)), send_marker(send_marker) {
        {
            std::lock_guard<std::mutex> guard(registryLock());
            registry().push_back(this);
        }
        worker = std::thread([this] { watch(); });
    }

    ~StallWatchdog() {
        stop = true;
        worker.join();
        std::lock_guard<std::mutex> guard(registryLock());
        auto &reg = registry();
        reg.erase(std::remove(reg.begin(), reg.end(), this), reg.end());
    }

private:
    coyote::cThread &coyote_thread;
    BenchProgress &progress;
    std::string label;
    bool send_marker;
    std::atomic<bool> stop{false};
    std::thread worker;

    // All watchdogs of this process, so a report can show every Coyote thread
    static std::mutex& registryLock() { static std::mutex lock; return lock; }
    static std::vector<StallWatchdog*>& registry() { static std::vector<StallWatchdog*> reg; return reg; }

    // Everything that changes while the benchmark makes progress
    struct Snapshot {
        BenchPhase phase;
        uint32_t run, invoked, local_wr, remote_wr;
        bool operator==(const Snapshot &o) const {
            return phase == o.phase && run == o.run && invoked == o.invoked && local_wr == o.local_wr && remote_wr == o.remote_wr;
        }
    };

    Snapshot snapshot() const {
        return {
            progress.phase.load(), progress.run.load(), progress.invoked.load(),
            coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE),
            coyote_thread.checkCompleted(coyote::CoyoteOper::REMOTE_RDMA_WRITE)
        };
    }

    void watch() {
        Snapshot last = snapshot();
        auto last_change = std::chrono::steady_clock::now();
        bool reported = false;

        while (!stop) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            Snapshot now = snapshot();
            if (!(now == last)) {
                if (reported) { log("progress resumed"); }
                last = now;
                last_change = std::chrono::steady_clock::now();
                reported = false;
                continue;
            }

            auto idle = std::chrono::steady_clock::now() - last_change;
            if (!reported && now.phase != BenchPhase::DONE && idle >= std::chrono::seconds(STALL_TIMEOUT_S)) {
                report(now, std::chrono::duration_cast<std::chrono::seconds>(idle).count());
                reported = true;

                // Trigger the debug ILAs: their pre-trigger buffers then hold the last requests of this thread before the stall
                // The marker is not counted in total_invoked, so this thread's REMOTE_WRITE diff can go up by one afterwards
                if (send_marker) {
                    coyote::rdmaSg sg = { .len = STALL_MARKER_LEN };
                    coyote_thread.invoke(coyote::CoyoteOper::REMOTE_RDMA_WRITE, sg);
                    log("posted ILA marker write (" + std::to_string(STALL_MARKER_LEN) + " B)");
                }
            }
        }
    }

    static std::mutex& printLock() { static std::mutex lock; return lock; }

    void log(const std::string &msg) const {
        std::lock_guard<std::mutex> guard(printLock());
        std::cout << "[WATCHDOG " << label << "] " << msg << std::endl;
    }

    // Signed difference of a 32-bit hardware counter and a 64-bit software count, robust to counter wrap-around
    static int32_t counterDiff(uint32_t raw, uint64_t expected) {
        return (int32_t) (raw - (uint32_t) expected);
    }

    void printTotals() const {
        std::cout << "Cumulative counts of all Coyote threads in this process:" << std::endl;
        std::cout << "  (REMOTE_WRITE diff < 0: posted writes not completed yet or lost; > 0: completions this thread never posted)" << std::endl;
        std::cout << "  (LOCAL_WRITE diff < 0: incoming writes not arrived yet or lost; > 0: incoming writes nobody sent to this thread)" << std::endl;

        std::lock_guard<std::mutex> guard(registryLock());
        for (const StallWatchdog *w : registry()) {
            const BenchProgress &p = w->progress;
            uint32_t remote_wr = w->coyote_thread.checkCompleted(coyote::CoyoteOper::REMOTE_RDMA_WRITE);
            uint32_t local_wr = w->coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE);
            uint64_t total_invoked = p.total_invoked;
            uint64_t expected_local_wr = p.expected_local_wr;

            std::cout << "  [" << w->label << "] ctid " << w->coyote_thread.getCtid()
                      << ", size " << p.size << " B, run " << p.run + 1 << "/" << p.n_runs << ", phase " << phaseName(p.phase) << std::endl;
            std::cout << "      invoked total " << std::setw(10) << total_invoked
                      << "   REMOTE_WRITE raw " << std::setw(10) << remote_wr
                      << "   diff " << std::showpos << counterDiff(remote_wr, total_invoked) << std::noshowpos << std::endl;
            std::cout << "      expected in   " << std::setw(10) << expected_local_wr
                      << "   LOCAL_WRITE raw  " << std::setw(10) << local_wr
                      << "   diff " << std::showpos << counterDiff(local_wr, expected_local_wr) << std::noshowpos << std::endl;
        }
    }

    void report(const Snapshot &s, long idle_s) const {
        std::lock_guard<std::mutex> guard(printLock());

        uint32_t transfers = progress.transfers;
        std::cout << std::endl << "===== [WATCHDOG " << label << "] STALL: no progress for " << idle_s << " s =====" << std::endl;
        std::cout << "ctid:                     " << coyote_thread.getCtid() << std::endl;
        std::cout << "transfer size:            " << progress.size << " B" << std::endl;
        std::cout << "run:                      " << s.run + 1 << " / " << progress.n_runs << std::endl;
        std::cout << "phase:                    " << phaseName(s.phase) << std::endl;
        std::cout << "invoked this run:         " << s.invoked << " / " << transfers << std::endl;
        std::cout << "LOCAL_WRITE this run:     " << (uint32_t) (s.local_wr - progress.base_local_wr) << " / " << transfers
                  << "   (raw " << s.local_wr << ")" << std::endl;
        std::cout << "REMOTE_WRITE this run:    " << (uint32_t) (s.remote_wr - progress.base_remote_wr)
                  << "   (raw " << s.remote_wr << ")" << std::endl;
        std::cout << std::endl;

        printTotals();
        std::cout << std::endl;

        // vFPGA-wide request statistics (shared by all cThreads on this vFPGA)
        coyote_thread.printDebug();

        // Network counters of this FPGA
        std::ifstream nstats("/sys/kernel/coyote_sysfs_0/cyt_attr_nstats");
        std::string line;
        while (std::getline(nstats, line)) {
            if (line.find("IBV") != std::string::npos || line.find("ROCE") != std::string::npos ||
                line.find("PSN drop") != std::string::npos || line.find("Retrans") != std::string::npos) {
                std::cout << line << std::endl;
            }
        }
        std::cout << "===== end of report =====" << std::endl << std::endl;
    }
};

#endif
