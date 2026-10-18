/**
 * This file is part of the Coyote <https://github.com/fpgasystems/Coyote>
 *
 * MIT Licence
 * Copyright (c) 2021-2025, Systems Group, ETH Zurich
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

#include <iostream>
#include <cstdlib>
#include <thread>
#include <vector>

// External library for easier parsing of CLI arguments by the executable
#include <boost/program_options.hpp>

// Coyote-specific includes
#include <coyote/cThread.hpp>
#include <constants.hpp>
#include <port_dispatcher.hpp>

constexpr bool const IS_CLIENT = false;

// Note, how the Coyote thread is passed by reference; to avoid creating a copy of 
// the thread object which can lead to undefined behaviour and bugs. 
void run_bench(
    coyote::cThread &coyote_thread, coyote::rdmaSg &sg, 
    int *mem, uint transfers, uint n_runs, bool operation
) {
    // When writing, the server asserts the written payload is correct (which the client sets)
    // When reading, the client asserts the read payload is correct (which the server sets)
    for (int i = 0; i < sg.len / sizeof(int); i++) {
        mem[i] = operation ? 0 : i;        
    }

    for (int i = 0; i < n_runs; i++) {
        // Clear previous completion flags and sync with client
        coyote_thread.clearCompleted();
        coyote_thread.connSync(IS_CLIENT);

        // For writes, wait until client has written the targer number of messages; then write them back
        if (operation) {
            while (coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE) != transfers) {}

            for (int i = 0; i < transfers; i++) {
                coyote_thread.invoke(coyote::CoyoteOper::REMOTE_RDMA_WRITE, sg);
            }
        // For reads, the server is completely passive 
        } else { 

        }
    }
    
    // Functional correctness check
    if (operation) {
        for (int i = 0; i < sg.len / sizeof(int); i++) {
            assert(mem[i] == i);                        
        }
    }
}

// One worker per client: its own Coyote thread (and therefore its own QP) on its own port
void serve_client(
    unsigned int client_id, uint16_t port, unsigned int min_size, unsigned int max_size,
    unsigned int n_runs, bool operation
) {
    try {
        // Allocate Coyote thread and set-up RDMA connection, buffer etc.
        // initRDMA is explained in more detail in client/main.cpp
        coyote::cThread coyote_thread(DEFAULT_VFPGA_ID, getpid());
        int *mem = (int *) coyote_thread.initRDMA(max_size, port);
        if (!mem) { throw std::runtime_error("Could not allocate memory; exiting..."); }
        std::cout << "Client " << client_id << " connected on port " << port << std::endl;

        // Benchmark sweep; exactly like done in the client code
        unsigned int curr_size = min_size;
        while(curr_size <= max_size) {
            coyote::rdmaSg sg = { .len = curr_size };
            run_bench(coyote_thread, sg, mem, N_THROUGHPUT_REPS, n_runs, operation);
            run_bench(coyote_thread, sg, mem, N_LATENCY_REPS, n_runs, operation);
            curr_size *= 2;
        }

        // Final sync
        coyote_thread.connSync(IS_CLIENT);
        std::cout << "Client " << client_id << " finished" << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Client " << client_id << ": " << e.what() << std::endl;
    }
}

int main(int argc, char *argv[])  {
    // CLI arguments
    bool operation;
    unsigned int min_size, max_size, n_runs, n_clients;

    boost::program_options::options_description runtime_options("Coyote Perf RDMA Options");
    runtime_options.add_options()
        ("operation,o", boost::program_options::value<bool>(&operation)->default_value(false), "Benchmark operation: READ(0) or WRITE(1)")
        ("runs,r", boost::program_options::value<unsigned int>(&n_runs)->default_value(N_RUNS_DEFAULT), "Number of times to repeat the test")
        ("min_size,x", boost::program_options::value<unsigned int>(&min_size)->default_value(MIN_TRANSFER_SIZE_DEFAULT), "Starting (minimum) transfer size")
        ("max_size,X", boost::program_options::value<unsigned int>(&max_size)->default_value(MAX_TRANSFER_SIZE_DEFAULT), "Ending (maximum) transfer size")
        ("clients,n", boost::program_options::value<unsigned int>(&n_clients)->default_value(1), "Number of clients served in parallel");
    boost::program_options::variables_map command_line_arguments;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, runtime_options), command_line_arguments);
    boost::program_options::notify(command_line_arguments);

    if (n_clients < 1 || n_clients >= coyote::MAX_NUM_CLIENTS) {
        throw std::runtime_error("Number of clients must be between 1 and " + std::to_string(coyote::MAX_NUM_CLIENTS - 1));
    }

    HEADER("CLI PARAMETERS:");
    std::cout << "Benchmark operation: " << (operation ? "WRITE" : "READ") << std::endl;
    std::cout << "Number of test runs: " << n_runs << std::endl;
    std::cout << "Starting transfer size: " << min_size << std::endl;
    std::cout << "Ending transfer size: " << max_size << std::endl;
    std::cout << "Number of clients: " << n_clients << std::endl << std::endl;

    HEADER("RDMA BENCHMARK: SERVER");

    // Start one worker per client; each waits in initRDMA(...) for its client
    std::vector<std::thread> workers;
    for (unsigned int c = 0; c < n_clients; c++) {
        workers.emplace_back(serve_client, c, (uint16_t) (DISPATCH_PORT + 1 + c), min_size, max_size, n_runs, operation);
    }

    // Tell each incoming client which worker port to use, then wait for all benchmarks to finish
    dispatchPorts(n_clients);
    for (auto &worker : workers) {
        worker.join();
    }

    return EXIT_SUCCESS;
}
