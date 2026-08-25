#include "ws_client.h"
#include "config.h"
#include <iostream>
#include <chrono>
#include <istream>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/socket_base.hpp>

WSClient::~WSClient()
{
    stop();
    stop_ipc_server();
}

void WSClient::start()
{
    running_  = true;
    ws_thread_ = std::thread(&WSClient::run, this);
}

void WSClient::stop()
{
    running_ = false;
    if (ws_thread_.joinable()) ws_thread_.join();
}

void WSClient::run()
{
    while (running_) {
        try {
            connect_and_listen();
            // Clean return means token set changed or watchdog fired —
            // reconnect immediately, no backoff needed.
        } catch (const std::exception& e) {
            std::cerr << "[WS] Disconnected: " << e.what()
                      << " - reconnecting in 3s\n";
            if (running_) std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

void WSClient::connect_and_listen()
{
    net::io_context ioc;
    ssl::context    ssl_ctx{ssl::context::tlsv12_client};
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(ssl::verify_peer);

    websocket::stream<beast::ssl_stream<tcp::socket>> ws{ioc, ssl_ctx};

    tcp::resolver resolver{ioc};
    auto endpoints = resolver.resolve(Config::WS_HOST, "443");
    net::connect(ws.next_layer().next_layer(), endpoints);

    SSL_set_tlsext_host_name(ws.next_layer().native_handle(),
                             Config::WS_HOST.c_str());
    ws.next_layer().handshake(ssl::stream_base::client);

    ws.set_option(websocket::stream_base::decorator(
        [](websocket::request_type& req) {
            req.set(boost::beast::http::field::host, Config::WS_HOST);
            req.set(boost::beast::http::field::user_agent, "polymarket-cpp/1.0");
        }
    ));
    ws.handshake(Config::WS_HOST, Config::WS_PATH);

    std::vector<std::string> ids_snapshot;
    {
        std::lock_guard<std::mutex> lock(tokens_mutex_);
        ids_snapshot = tokens_;
    }
    json sub = {
        {"type",                   "market"},
        {"assets_ids",             ids_snapshot},
        {"custom_feature_enabled", true}
    };
    ws.write(net::buffer(sub.dump()));

    needs_reconnect_ = false;

    std::cout << "[WS] Connected and subscribed to "
              << ids_snapshot.size() << " tokens.\n";
    std::flush(std::cout);

    beast::flat_buffer buffer;
    time_t last_ping    = std::time(nullptr);
    time_t last_message = std::time(nullptr);

    ws.control_callback([](websocket::frame_type, beast::string_view) {});

    while (running_) {
        time_t now = std::time(nullptr);

        // Reconnect if Python sent a new token set (market rollover)
        if (needs_reconnect_) {
            std::cout << "[WS] Token set changed - reconnecting...\n";
            std::flush(std::cout);
            boost::beast::error_code ec;
            ws.close(websocket::close_code::normal, ec);
            return;
        }

        // Watchdog: reconnect if silent for 30s
        if (now - last_message > 30) {
            std::cout << "[WS] No message for 30s - reconnecting\n";
            std::flush(std::cout);
            boost::beast::error_code ec;
            ws.close(websocket::close_code::normal, ec);
            return;
        }

        // Send PING (Polymarket expects text "PING", not a WS ping frame)
        if (now - last_ping >= Config::PING_INTERVAL_SEC) {
            ws.write(net::buffer(std::string("PING")));
            last_ping = now;
        }

        // Read next message
        buffer.clear();
        boost::beast::error_code ec;
        ws.read(buffer, ec);

        if (ec) {
            if (ec == boost::beast::websocket::error::closed) {
                auto cr = ws.reason();
                std::cerr << "[WS] Server closed connection. code="
                          << static_cast<unsigned>(cr.code)
                          << " reason=" << cr.reason.c_str() << "\n";
                break;
            }
            if (ec == net::ssl::error::stream_truncated) {
                std::cout << "[WS] Connection closed without TLS shutdown "
                             "(idle timeout or connection recycle) - reconnecting\n";
                std::flush(std::cout);
                break;
            }
            throw boost::beast::system_error{ec};
        }

        last_message = std::time(nullptr);
        std::string msg = beast::buffers_to_string(buffer.data());

        if (msg == "PONG") continue;
        handle_message(msg);
    }
}

void WSClient::handle_message(const std::string& msg)
{
    try {
        json j = json::parse(msg);
        if (j.is_array()) {
            for (const auto& item : j) handle_single_event(item);
        } else {
            handle_single_event(j);
        }
    } catch (const std::exception& e) {
        std::cerr << "[WS] Parse error: " << e.what() << "\n";
    }
}

void WSClient::handle_single_event(const json& j)
{
    std::string event_type = j.value("event_type", "");
    if      (event_type == "price_change")     handle_price_change(j);
    else if (event_type == "book")             handle_book(j);
    else if (event_type == "last_trade_price") handle_last_trade(j);
}

void WSClient::handle_price_change(const json& j)
{
    for (const auto& pc : j.value("price_changes", json::array())) {
        std::string asset = pc.value("asset_id", "");
        double best_ask   = std::stod(pc.value("best_ask", "0"));
        double best_bid   = std::stod(pc.value("best_bid", "0"));

        // FIX: stop-loss should trigger on the BID, not the ask.
        // You are long and selling into the bid. The bid is always <= ask,
        // so checking the ask was letting you hold past your real loss.
        double stop_price = (best_bid > 0.0) ? best_bid : best_ask;
        check_stop_loss(asset, stop_price);

        // FIX: skip publishing ticks where both sides are zero
        // (means no live quote on this token right now).
        if (best_ask > 0.0 || best_bid > 0.0) {
            ipc_publish({{"type", "tick"}, {"token_id", asset},
                         {"ask", best_ask}, {"bid", best_bid}});
        }
    }
}

void WSClient::handle_book(const json& j)
{
    std::string asset = j.value("asset_id", "");

    auto asks = j.value("asks", json::array());
    auto bids = j.value("bids", json::array());
    if (asks.empty() || bids.empty()) return;

    // FIX: scan for the actual best (lowest) ask and (highest) bid
    // instead of assuming index 0 is best. If the exchange ever sends
    // an unsorted book, the old code read the worst price.
    double best_ask = 1.0;
    for (const auto& a : asks) {
        try {
            double p = std::stod(a["price"].get<std::string>());
            if (p < best_ask) best_ask = p;
        } catch (...) { continue; }
    }
    double best_bid = 0.0;
    for (const auto& b : bids) {
        try {
            double p = std::stod(b["price"].get<std::string>());
            if (p > best_bid) best_bid = p;
        } catch (...) { continue; }
    }

    check_stop_loss(asset, best_bid);
    ipc_publish({{"type", "tick"}, {"token_id", asset},
                 {"ask", best_ask}, {"bid", best_bid}});
}

void WSClient::handle_last_trade(const json& j) { (void)j; }

// ============================== stop-loss ===================================

void WSClient::check_stop_loss(const std::string& token_id, double price)
{
    std::lock_guard<std::mutex> lock(watched_mutex_);
    auto it = watched_.find(token_id);
    if (it == watched_.end()) return;

    double threshold = it->second.buy_price - it->second.max_loss;

    if (price <= threshold) {
        json evt = {{"type", "stop_loss"}, {"token_id", token_id}, {"ask", price}};

        // Log locally so there's a record even if the IPC write below fails.
        std::cout << "[STOP_LOSS] token=" << token_id
                  << " price=" << price
                  << " threshold=" << threshold
                  << " buy_price=" << it->second.buy_price << "\n";
        std::flush(std::cout);

        // One-shot: erase before publishing so the next tick can't re-fire.
        watched_.erase(it);
        ipc_publish(evt);
    }
}

// ============================== IPC to Python ===============================

void WSClient::ipc_publish(const json& msg)
{
    std::lock_guard<std::mutex> lock(ipc_write_mutex_);
    if (!ipc_client_connected_ || !ipc_socket_) {
        // FIX: log dropped stop-loss events specifically. A dropped tick is
        // recoverable (next tick arrives in ms). A dropped stop-loss means
        // your position has no protection and you won't know until you check.
        if (msg.value("type", "") == "stop_loss") {
            std::cerr << "[IPC] CRITICAL: stop_loss event DROPPED "
                         "(Python not connected): "
                      << msg.dump() << "\n";
        }
        return;
    }

    boost::system::error_code ec;
    std::string line = msg.dump() + "\n";
    net::write(*ipc_socket_, net::buffer(line), ec);
    if (ec) {
        std::cerr << "[IPC] Write failed: " << ec.message() << "\n";
        ipc_client_connected_ = false;
    }
}

void WSClient::ipc_handle_command(const std::string& line)
{
    try {
        json cmd = json::parse(line);
        std::string c = cmd.value("cmd", "");

        if (c == "subscribe") {
            std::vector<std::string> new_tokens =
                cmd.at("tokens").get<std::vector<std::string>>();
            {
                std::lock_guard<std::mutex> lock(tokens_mutex_);
                tokens_ = std::move(new_tokens);
            }
            needs_reconnect_ = true;

        } else if (c == "watch") {
            double bp = cmd.at("buy_price").get<double>();
            double ml = cmd.at("max_loss").get<double>();
            std::string tid = cmd.at("token_id").get<std::string>();
            {
                std::lock_guard<std::mutex> lock(watched_mutex_);
                watched_[tid] = {bp, ml};
            }
            std::cout << "[IPC] watch set: token=" << tid
                      << " buy=" << bp << " max_loss=" << ml << "\n";
            std::flush(std::cout);

        } else if (c == "unwatch") {
            std::string tid = cmd.at("token_id").get<std::string>();
            {
                std::lock_guard<std::mutex> lock(watched_mutex_);
                watched_.erase(tid);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[IPC] Bad command: " << e.what() << "\n";
    }
}

void WSClient::ipc_read_loop()
{
    boost::asio::streambuf buf;
    while (ipc_running_ && ipc_client_connected_) {
        boost::system::error_code ec;
        // TCP keepalive is set on the socket (see ipc_accept_loop), so if
        // Python dies without closing the connection, the kernel will
        // detect it within ~15s and this read will return with an error
        // instead of blocking forever.
        net::read_until(*ipc_socket_, buf, '\n', ec);
        if (ec) {
            std::cout << "[IPC] Read ended: " << ec.message() << "\n";
            std::flush(std::cout);
            ipc_client_connected_ = false;
            break;
        }

        std::istream is(&buf);
        std::string line;
        std::getline(is, line);
        if (!line.empty()) ipc_handle_command(line);
    }
}

void WSClient::ipc_accept_loop(unsigned short port)
{
    try {
        ipc_acceptor_.emplace(ipc_ioc_,
            tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
    } catch (const std::exception& e) {
        std::cerr << "[IPC] Failed to bind loopback port " << port << ": "
                  << e.what() << "\n";
        return;
    }

    std::cout << "[IPC] Listening on 127.0.0.1:" << port << " for Python client\n";
    std::flush(std::cout);

    while (ipc_running_) {
        tcp::socket sock(ipc_ioc_);
        boost::system::error_code ec;
        ipc_acceptor_->accept(sock, ec);
        if (ec) {
            // FIX: if we're shutting down, break instead of spinning.
            //     (old code did `continue` which looped forever after
            //      the acceptor was closed)
            if (!ipc_running_) break;
            continue;
        }

        // FIX: TCP keepalive so a dead Python connection is detected in
        // ~15 seconds instead of the Windows default of ~2 hours.
        //   idle=5s  → start probing after 5s of silence
        //   interval=2s → probe every 2s
        //   probes=5 → give up after 5 failed probes
        //   total detection time ≈ 5 + (5 × 2) = 15 seconds
        sock.set_option(net::socket_base::keep_alive(true));

        {
            std::lock_guard<std::mutex> lock(ipc_write_mutex_);
            ipc_socket_.emplace(std::move(sock));
            ipc_client_connected_ = true;
        }
        std::cout << "[IPC] Python client connected\n";
        std::flush(std::cout);

        // Tell Python we're live. It uses this to clear stale prices
        // and re-arm any stop-loss watches.
        ipc_publish({{"type", "hello"}});

        if (ipc_read_thread_.joinable()) ipc_read_thread_.join();
        ipc_read_thread_ = std::thread(&WSClient::ipc_read_loop, this);
        ipc_read_thread_.join();  // blocks until this client disconnects

        // FIX: explicitly reset the socket under the write lock so a
        // concurrent ipc_publish() can't be mid-write on a socket we're
        // about to destroy.
        {
            std::lock_guard<std::mutex> lock(ipc_write_mutex_);
            ipc_socket_.reset();
            ipc_client_connected_ = false;
        }
        std::cout << "[IPC] Python client disconnected - waiting for reconnect\n";
        std::flush(std::cout);
    }
}

void WSClient::start_ipc_server(unsigned short port)
{
    ipc_running_ = true;
    ipc_accept_thread_ = std::thread(&WSClient::ipc_accept_loop, this, port);
}

void WSClient::stop_ipc_server()
{
    ipc_running_          = false;
    ipc_client_connected_ = false;

    boost::system::error_code ec;
    if (ipc_acceptor_) ipc_acceptor_->close(ec);

    // FIX: close the socket under the write lock so a concurrent
    // ipc_publish() can't be mid-write when we destroy it.
    {
        std::lock_guard<std::mutex> lock(ipc_write_mutex_);
        if (ipc_socket_) ipc_socket_->close(ec);
    }

    if (ipc_accept_thread_.joinable()) ipc_accept_thread_.join();
    if (ipc_read_thread_.joinable())   ipc_read_thread_.join();
}
