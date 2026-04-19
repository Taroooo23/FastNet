#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "logic/chat_service.h"
#include "logic/session_manager.h"
#include "network/connection.h"

// WorkerReactor 负责：
// 1. 管理自己名下的客户端连接
// 2. 监听连接的可读/可写事件
// 3. 读到完整一行后交给 ChatService 处理
class WorkerReactor {
public:
    using SendToFdFn = std::function<void(int fd, const std::string& text)>;
    using BroadcastFn = std::function<void(const std::string& message, int sender_fd, bool include_sender)>;
    using DisconnectNotifyFn = std::function<void()>;
    using ForceRemoveFdFn = std::function<void(int fd)>;

    WorkerReactor(int id,
                  SessionManager& session_manager,
                  ChatService& chat_service,
                  SendToFdFn send_to_fd,
                  BroadcastFn broadcast,
                  DisconnectNotifyFn on_disconnect,
                  ForceRemoveFdFn force_remove_fd);

    ~WorkerReactor() = default;

    bool init();
    void start();
    void stop();
    void join();

    int id() const;

    void add_client(int client_fd, const std::string& client_ip, int client_port);
    void enqueue_message(int fd, const std::string& text);
    void enqueue_broadcast_message(const std::vector<int>& fds, const std::string& text);

private:
    static int set_nonblocking(int fd);
    static bool add_epoll_fd(int epoll_fd, int fd, uint32_t events);
    static bool mod_epoll_fd(int epoll_fd, int fd, uint32_t events);
    static void del_epoll_fd(int epoll_fd, int fd);

    void update_client_events(int fd);
    bool flush_output(int fd);
    void queue_message_local(int fd, const std::string& text);

    void handle_client_readable(int client_fd);
    void handle_client_writable(int client_fd);
    void remove_client(int client_fd);

    void drain_tasks();
    void close_all_clients_for_shutdown();
    void run();

private:
    int id_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;

    SessionManager& session_manager_;
    ChatService& chat_service_;

    SendToFdFn send_to_fd_;
    BroadcastFn broadcast_;
    DisconnectNotifyFn on_disconnect_;
    ForceRemoveFdFn force_remove_fd_;

    std::thread thread_;
    std::atomic<bool> stopping_{false};

    std::unordered_map<int, std::shared_ptr<Connection>> clients_;

    std::mutex task_mutex_;
    std::queue<std::function<void()>> tasks_;
};

