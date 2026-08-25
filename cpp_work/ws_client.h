#pragma once
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/streambuf.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <optional>
// add this line to the existing #include block near the top of ws_client.h:
#include <boost/asio/socket_base.hpp>


namespace beast     = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
using tcp           = boost::asio::ip::tcp;
using json          = nlohmann::json;

// Standalone Polymarket CLOB websocket ingestion + local IPC bridge to a
// Python process. Deliberately has no concept of "markets" — it just holds
// a flat set of token ids to subscribe to on the exchange feed, replaced
// wholesale whenever Python sends a "subscribe" command. Python already
// knows how to map token_id -> (market, up/down) from its own Gamma API
// discovery, so that mapping isn't duplicated here.
//
// Responsibilities:
//   - own the actual exchange websocket in a dedicated thread (fast enough
//     to never trigger the exchange's "slow consumer" disconnect)
//   - publish every price tick to Python over a local loopback socket
//   - check any Python-registered stop-loss threshold inline, the instant
//     a price update is parsed, and push a one-shot event immediately
class WSClient
{
public:
    WSClient() = default;
    ~WSClient();

    // Exchange connection lifecycle.
    void start();
    void stop();

    // Local IPC server for the Python process (loopback TCP, newline-
    // delimited JSON). Independent lifecycle from start()/stop() — Python
    // connecting/disconnecting never touches the exchange connection.
    void start_ipc_server(unsigned short port = 47654);
    void stop_ipc_server();

private:
    std::thread       ws_thread_;
    std::atomic<bool> running_{false};

    // ---- Subscribed token set, replaced wholesale via IPC "subscribe" ----
    std::vector<std::string> tokens_;
    std::mutex                tokens_mutex_;
    std::atomic<bool>         needs_reconnect_{false};

    void run();
    void connect_and_listen();

    void handle_message(const std::string& msg);
    void handle_single_event(const json& j);
    void handle_price_change(const json& j);
    void handle_book(const json& j);
    void handle_last_trade(const json& j);

    // ---- Stop-loss watch table, keyed by the exact token_id held ----
    struct WatchedPosition { double buy_price; double max_loss; };
    std::unordered_map<std::string, WatchedPosition> watched_;
    std::mutex                                        watched_mutex_;
    void check_stop_loss(const std::string& token_id, double ask);

    // ---- IPC to Python ----
    net::io_context               ipc_ioc_;
    std::optional<tcp::acceptor>  ipc_acceptor_;
    std::optional<tcp::socket>    ipc_socket_;
    std::mutex                    ipc_write_mutex_;
    std::thread                   ipc_accept_thread_;
    std::thread                   ipc_read_thread_;
    std::atomic<bool>             ipc_running_{false};
    std::atomic<bool>             ipc_client_connected_{false};

    void ipc_accept_loop(unsigned short port);
    void ipc_read_loop();
    void ipc_handle_command(const std::string& line);
    void ipc_publish(const json& msg);   // thread-safe, best-effort, never throws
};