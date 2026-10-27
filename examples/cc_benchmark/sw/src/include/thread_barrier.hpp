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

#ifndef _CC_BENCHMARK_THREAD_BARRIER_HPP_
#define _CC_BENCHMARK_THREAD_BARRIER_HPP_

#include <cstdint>
#include <mutex>
#include <condition_variable>

/* Reusable barrier for the server's worker threads (std::barrier needs C++20, Coyote builds with C++17)
 *
 * Each worker calls arriveAndWait() before syncing with its client, so a client only starts once all
 * workers (and therefore all clients) have reached the same point. A worker that fails calls arriveAndDrop(),
 * so the remaining workers are not blocked forever.
 */
class ThreadBarrier {
public:
    explicit ThreadBarrier(unsigned int n_threads) : n_threads(n_threads) {}

    void arriveAndWait() {
        std::unique_lock<std::mutex> lock(mtx);
        uint64_t gen = generation;
        if (++arrived >= n_threads) {
            release();
        } else {
            cv.wait(lock, [&] { return gen != generation; });
        }
    }

    // Leave the barrier for good; the waiting threads are released if they were only waiting for this one
    void arriveAndDrop() {
        std::lock_guard<std::mutex> lock(mtx);
        n_threads--;
        if (arrived > 0 && arrived >= n_threads) {
            release();
        }
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    unsigned int n_threads;
    unsigned int arrived = 0;
    uint64_t generation = 0;

    void release() {
        arrived = 0;
        generation++;
        cv.notify_all();
    }
};

#endif
