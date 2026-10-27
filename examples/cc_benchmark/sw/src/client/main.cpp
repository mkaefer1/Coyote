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
#include <chrono>
#include <thread>

// External library for easier parsing of CLI arguments by the executable
#include <boost/program_options.hpp>

// Coyote-specific includes
#include <coyote/cBench.hpp>
#include <coyote/cThread.hpp>
#include <constants.hpp>
#include <port_dispatcher.hpp>

constexpr bool const IS_CLIENT = true;

// Note, how the Coyote thread is passed by reference; to avoid creating a copy of
// the thread object which can lead to undefined behaviour and bugs.
double run_bench(
    coyote::cThread &coyote_thread, coyote::rdmaSg &sg,
    int *mem, uint transfers, uint n_runs, bool operation
) {
    // When writing, the server asserts the written payload is correct (which the client sets)
    // When reading, the client asserts the read payload is correct (which the server sets)
    for (int i = 0; i < sg.len / sizeof(int); i++) {
        mem[i] = operation ? i : 0;
    }

    // Before every benchmark, record the completion counter and sync with server
    // Sync is in a way equivalent to MPI_Barrier()
    uint32_t base = 0;
    auto prep_fn = [&]() {
        base = coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE);
        coyote_thread.connSync(IS_CLIENT);
    };
    
    /* Benchmark function; as eplained in the README
     * For RDMA_WRITEs, the client writes multiple times to the server and then the server writes the same content back
     * For RDMA READs, the client reads from the server multiple times
     * In boths cases, that means there will be n_transfers completed writes to local memory (LOCAL_WRITE)
     */
    coyote::CoyoteOper coyote_operation = operation ? coyote::CoyoteOper::REMOTE_RDMA_WRITE : coyote::CoyoteOper::REMOTE_RDMA_READ;
    auto bench_fn = [&]() {
        for (int i = 0; i < transfers; i++) {
            coyote_thread.invoke(coyote_operation, sg);
        }

        while ((uint32_t) (coyote_thread.checkCompleted(coyote::CoyoteOper::LOCAL_WRITE) - base) != transfers) {}
    };

    // Execute benchmark
    coyote::cBench bench(n_runs, 0);
    bench.execute(bench_fn, prep_fn);

    // Functional correctness check
    if (!operation) {
        for (int i = 0; i < sg.len / sizeof(int); i++) {
            assert(mem[i] == i);
        }
    }
    
    // For writes, divide by 2, since that is sent two ways (from client to server and then from server to client)
    // Reads are one way, so no need to scale
    return bench.getAvg() / (1. + (double) operation);
}

int main(int argc, char *argv[])  {
    // CLI arguments
    bool operation;
    std::string server_ip;
    unsigned int min_size, max_size, n_runs;

    boost::program_options::options_description runtime_options("Coyote Perf RDMA Options");
    runtime_options.add_options()
        ("ip_address,i", boost::program_options::value<std::string>(&server_ip), "Server's IP address")
        ("operation,o", boost::program_options::value<bool>(&operation)->default_value(false), "Benchmark operation: READ(0) or WRITE(1)")
        ("runs,r", boost::program_options::value<unsigned int>(&n_runs)->default_value(N_RUNS_DEFAULT), "Number of times to repeat the test")
        ("min_size,x", boost::program_options::value<unsigned int>(&min_size)->default_value(MIN_TRANSFER_SIZE_DEFAULT), "Starting (minimum) transfer size")
        ("max_size,X", boost::program_options::value<unsigned int>(&max_size)->default_value(MAX_TRANSFER_SIZE_DEFAULT), "Ending (maximum) transfer size");
    boost::program_options::variables_map command_line_arguments;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, runtime_options), command_line_arguments);
    boost::program_options::notify(command_line_arguments);

    HEADER("CLI PARAMETERS:");
    std::cout << "Server's TCP address: " << server_ip << std::endl;
    std::cout << "Benchmark operation: " << (operation ? "WRITE" : "READ") << std::endl;
    std::cout << "Number of test runs: " << n_runs << std::endl;
    std::cout << "Starting transfer size: " << min_size << std::endl;
    std::cout << "Ending transfer size: " << max_size << std::endl << std::endl;

    /* Coyote completely abstracts the complexity behind exchanging QPs and setting up an RDMA connection
     * Instead, given a cThread, the target RDMA buffer size and the remote server's TCP address,
     * One can use the function initRDMA, which will allocate the buffer and 
     * Exchange the necessary information with the server; the server calls the equivalent function but without the IP address
     *
     * The server can serve several clients in parallel (one Coyote thread and port per client), so the client first
     * asks the server's port dispatcher which port to use. The server's worker for that port may not be listening yet
     * right after start-up, so connection attempts are retried for a short while.
     */
    uint16_t port = requestPort(server_ip);
    std::cout << "Assigned port: " << port << std::endl << std::endl;

    coyote::cThread coyote_thread(DEFAULT_VFPGA_ID, getpid(), 0);
    int *mem = nullptr;
    for (unsigned int attempt = 0; ; attempt++) {
        try {
            mem = (int *) coyote_thread.initRDMA(max_size, port, server_ip.c_str());
            break;
        } catch (const std::runtime_error &e) {
            bool not_listening_yet = std::string(e.what()).find("Could not connect") != std::string::npos;
            if (!not_listening_yet || attempt >= CONNECT_RETRIES) { throw; }
            std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_RETRY_DELAY_MS));
        }
    }
    if (!mem) { throw std::runtime_error("Could not allocate memory; exiting..."); }
    std::cout << "Connected (ctid " << coyote_thread.getCtid() << ")" << std::endl << std::endl;

    std::cout << "Waiting for all clients to connect..." << std::endl << std::endl;

    // Benchmark sweep of latency and throughput
    HEADER("RDMA BENCHMARK: CLIENT");
    unsigned int curr_size = min_size;
    while(curr_size <= max_size) {
        // Start of a size section: the server only answers once all of its clients have reached this point,
        // so all clients start the section together; within the section, the runs follow each other without further waiting
        coyote_thread.connSync(IS_CLIENT);
        auto section_start = std::chrono::steady_clock::now();

        std::cout << "Size: " << std::setw(8) << curr_size << "; ";
        
        coyote::rdmaSg sg = { .len = curr_size };
    
        double throughput_time = run_bench(coyote_thread, sg, mem, N_THROUGHPUT_REPS, n_runs, operation);
        auto throughput_end = std::chrono::steady_clock::now();
        coyote_thread.connSync(IS_CLIENT);  // Tells the server this client finished its throughput runs
        auto latency_start = std::chrono::steady_clock::now();
        double throughput = ((double) N_THROUGHPUT_REPS * (double) curr_size) / (1024.0 * 1024.0 * throughput_time * 1e-9);
        std::cout << "Average throughput: " << std::setw(8) << throughput << " MB/s; ";
        
        double latency_time = run_bench(coyote_thread, sg, mem, N_LATENCY_REPS, n_runs, operation);
        auto section_end = std::chrono::steady_clock::now();
        coyote_thread.connSync(IS_CLIENT);  // Tells the server this client finished its latency runs
        std::cout << "Average latency: " << std::setw(8) << latency_time / 1e3 << " us; ";

        // Wall-clock time of the whole section, including the syncs between runs
        std::chrono::duration<double, std::milli> throughput_section = throughput_end - section_start;
        std::chrono::duration<double, std::milli> latency_section = section_end - latency_start;
        std::cout << "Section time: throughput " << std::setw(8) << throughput_section.count() << " ms, latency "
                  << std::setw(8) << latency_section.count() << " ms" << std::endl;

        curr_size *= 2;
    }

    // Final sync and exit
    coyote_thread.connSync(IS_CLIENT);
    return EXIT_SUCCESS;
}
