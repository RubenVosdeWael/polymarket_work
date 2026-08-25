#include "ws_client.h"
#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>

namespace {
    std::atomic<bool> g_keep_running{true};
    void handle_signal(int) { g_keep_running = false; }
}

int main()
{
    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    WSClient client;

    // Start the local IPC server first so Python can connect and send an
    // initial "subscribe" command whenever it's ready. Starting the
    // exchange connection doesn't depend on Python being connected yet —
    // it'll just subscribe to an empty token list until the first
    // "subscribe" command arrives, then reconnect with the real one.
    client.start_ipc_server(47654);
    client.start();

    std::cout << "[main] Running. Waiting for Python to connect on 127.0.0.1:47654 "
                 "and send a subscribe command. Ctrl+C to stop.\n";

    while (g_keep_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "[main] Shutting down...\n";
    client.stop();
    client.stop_ipc_server();
    return 0;
}