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

#pragma once

#include <string>
#include <iostream>
#include <cstdint>
#include <stdexcept>

#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <coyote/cDefs.hpp>

/* Port dispatcher
 *
 * initRDMA(...) on the server accepts exactly one client per port and per cThread.
 * To serve several clients without them having to know which port to use, the server runs a small
 * dispatcher on the well-known port DISPATCH_PORT and one worker (cThread) per client on
 * DISPATCH_PORT + 1 ... DISPATCH_PORT + n_clients. Each client first asks the dispatcher for its port
 * and then calls initRDMA(...) with that port.
 */

// Well-known port of the dispatcher; the workers use the ports directly above it
constexpr uint16_t DISPATCH_PORT = coyote::DEF_PORT;

// Server side: hand out the ports DISPATCH_PORT + 1 ... DISPATCH_PORT + n_clients, one per incoming client
inline void dispatchPorts(unsigned int n_clients) {
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) { throw std::runtime_error("ERROR: Dispatcher could not create a socket"); }

    // Allow quick restarts, even if the previous socket is still in TIME_WAIT
    int optval = 1;
    ::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    struct sockaddr_in server = {};
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(DISPATCH_PORT);
    if (::bind(sockfd, (struct sockaddr *) &server, sizeof(server)) < 0) {
        ::close(sockfd);
        throw std::runtime_error("ERROR: Dispatcher could not bind port " + std::to_string(DISPATCH_PORT));
    }
    if (::listen(sockfd, n_clients) == -1) {
        ::close(sockfd);
        throw std::runtime_error("ERROR: Dispatcher listen failed");
    }

    for (unsigned int c = 0; c < n_clients; c++) {
        int connfd = ::accept(sockfd, NULL, 0);
        if (connfd == -1) {
            ::close(sockfd);
            throw std::runtime_error("ERROR: Dispatcher accept failed");
        }

        uint16_t port = htons(DISPATCH_PORT + 1 + c);
        if (::write(connfd, &port, sizeof(port)) != sizeof(port)) {
            ::close(connfd);
            ::close(sockfd);
            throw std::runtime_error("ERROR: Dispatcher could not send the port to a client");
        }
        std::cout << "Dispatcher: client " << c << " assigned port " << DISPATCH_PORT + 1 + c << std::endl;
        ::close(connfd);
    }

    ::close(sockfd);
}

// Client side: ask the dispatcher on the server for the port to use with initRDMA(...)
inline uint16_t requestPort(const std::string &server_address) {
    struct addrinfo hints = {}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string service = std::to_string(DISPATCH_PORT);
    if (::getaddrinfo(server_address.c_str(), service.c_str(), &hints, &res) != 0) {
        throw std::runtime_error("ERROR: getaddrinfo() failed for " + server_address);
    }

    int connfd = -1;
    for (struct addrinfo *t = res; t; t = t->ai_next) {
        connfd = ::socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (connfd >= 0) {
            if (!::connect(connfd, t->ai_addr, t->ai_addrlen)) {
                break;
            }
            ::close(connfd);
            connfd = -1;
        }
    }
    ::freeaddrinfo(res);
    if (connfd < 0) {
        throw std::runtime_error("ERROR: Could not reach the port dispatcher at " + server_address + ":" + service);
    }

    uint16_t port;
    if (::read(connfd, &port, sizeof(port)) != sizeof(port)) {
        ::close(connfd);
        throw std::runtime_error("ERROR: Could not read the assigned port from the dispatcher");
    }
    ::close(connfd);
    return ntohs(port);
}
