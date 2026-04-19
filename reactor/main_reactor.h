#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "logic/chat_service.h"
#include "logic/session_manager.h"
#include "reactor/worker_reactor.h"

// MainReactor 负责：
// 1. 建立监听 socket
// 2. accept 新连接
// 3. 按 round-robin 把连接分给 worker reactor
// 4. 作为全局调度中心，负责跨 worker 发消息
class MainReactor {
public:
    MainReactor();
    ~MainReactor();

    bool init();
    void run();
    void shutdown();
    void request_stop();

private:
    static void handle_signal(int);

    static int set_nonblocking(int fd);
    static bool add_epoll_fd(int epoll_fd, int fd, uint32_t events);

    void accept_new_clients();

    void queue_message(int fd, const std::string& text);
    void broadcast_message(const std::string& message, int sender_fd, bool include_sender);

    void on_accept();
    void on_disconnect();
    void metrics_loop();

private:
    int server_fd_ = -1;
    int epoll_fd_ = -1;

    SessionManager session_manager_;
    ChatService chat_service_;

    std::vector<std::unique_ptr<WorkerReactor>> workers_;
    std::atomic<size_t> next_worker_{0};

    std::atomic<bool> stopping_{false};
    bool shutdown_done_ = false;

    // 简单指标统计
    std::atomic<uint64_t> accept_count_{0};
    std::atomic<uint64_t> current_connections_{0};
    std::atomic<uint64_t> peak_connections_{0};
    std::atomic<uint64_t> disconnect_count_{0};

    std::thread metrics_thread_;
    std::atomic<bool> metrics_stop_{false};

    static MainReactor* g_instance_;
};

