#include "reactor/main_reactor.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstring>

#include "common/config.h"
#include "common/logger.h"

MainReactor* MainReactor::g_instance_ = nullptr;

MainReactor::MainReactor()
    : chat_service_(session_manager_) {}

MainReactor::~MainReactor() {
    shutdown();
}

bool MainReactor::init() {
    std::signal(SIGPIPE, SIG_IGN);
    g_instance_ = this;
    std::signal(SIGINT, MainReactor::handle_signal);
    std::signal(SIGTERM, MainReactor::handle_signal);

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        LOG_ERROR("socket creation failed");
        return false;
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        LOG_ERROR("setsockopt SO_REUSEADDR failed");
        return false;
    }

    if (set_nonblocking(server_fd_) < 0) {
        LOG_ERROR("set_nonblocking failed for server socket");
        return false;
    }

    sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(config::kPort);

    if (bind(server_fd_, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        LOG_ERROR("bind failed");
        return false;
    }

    if (listen(server_fd_, config::kListenBacklog) < 0) {
        LOG_ERROR("listen failed");
        return false;
    }

    // 构建 worker reactors
    for (int i = 0; i < config::kWorkerCount; ++i) {
        auto worker = std::make_unique<WorkerReactor>( // 创建智能指针worker指向WorkerReactor对象，初始化如下
            i,
            session_manager_,
            chat_service_,
            [this](int fd, const std::string& text) {
                this->queue_message(fd, text);
            },
            [this](const std::string& message, int sender_fd, bool include_sender) {
                this->broadcast_message(message, sender_fd, include_sender);
            },
            [this]() {
                this->on_disconnect();
            },
            [this](int fd) {
                session_manager_.force_remove_fd_mapping(fd);
            });

        if (!worker->init()) {
            return false;
        }

        workers_.push_back(std::move(worker));
    }

    for (auto& worker : workers_) {
        worker->start(); // 创建线程，开始运行
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        LOG_ERROR("main epoll_create1 failed");
        return false;
    }

    if (!add_epoll_fd(epoll_fd_, server_fd_, EPOLLIN)) {
        LOG_ERROR("main epoll add server fd failed");
        return false;
    }

    metrics_thread_ = std::thread(&MainReactor::metrics_loop, this);

    LOG_INFO("chat server started on port " + std::to_string(config::kPort));
    return true;
}

void MainReactor::run() {
    epoll_event events[config::kMaxEvents];

    while (!stopping_) {
        int nready = epoll_wait(epoll_fd_, events, config::kMaxEvents, config::kMainReactorWaitMs);
        if (nready < 0) {
            if (errno == EINTR) {
                continue;
            }

            LOG_ERROR("main epoll_wait failed");
            break;
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            if (fd == server_fd_) {
                accept_new_clients();
            }
        }
    }

    shutdown();
}

void MainReactor::shutdown() {
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true) && shutdown_done_) {
        return;
    }

    if (shutdown_done_) {
        return;
    }
    shutdown_done_ = true;

    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    for (auto& worker : workers_) {
        worker->stop();
    }

    for (auto& worker : workers_) {
        worker->join();
    }

    workers_.clear();

    metrics_stop_ = true;
    if (metrics_thread_.joinable()) {
        metrics_thread_.join();
    }

    LOG_INFO("server shutdown complete");
}

void MainReactor::request_stop() {
    stopping_ = true;
}

void MainReactor::handle_signal(int) {
    if (g_instance_ != nullptr) {
        g_instance_->request_stop();
    }
}

int MainReactor::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    return 0;
}

bool MainReactor::add_epoll_fd(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev {};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0;
}

void MainReactor::accept_new_clients() {
    while (true) {
        sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd_,
                               reinterpret_cast<sockaddr*>(&client_addr),
                               &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }

            LOG_ERROR("accept failed");
            break;
        }

        on_accept();

        if (set_nonblocking(client_fd) < 0) {
            LOG_ERROR("set_nonblocking failed for client");
            close(client_fd);
            on_disconnect();
            continue;
        }

        std::string client_ip = inet_ntoa(client_addr.sin_addr);
        int client_port = ntohs(client_addr.sin_port);

        int worker_id = static_cast<int>(next_worker_.fetch_add(1) % workers_.size());
        session_manager_.bind_fd_worker(client_fd, worker_id);

        LOG_INFO("[system] new connection from " + client_ip + ":" + std::to_string(client_port));
        workers_[worker_id]->add_client(client_fd, client_ip, client_port);
    }
}

void MainReactor::queue_message(int fd, const std::string& text) {
    int worker_id = -1;
    if (!session_manager_.get_worker_id_by_fd(fd, worker_id)) {
        return;
    }

    if (worker_id < 0 || worker_id >= static_cast<int>(workers_.size())) {
        return;
    }

    workers_[worker_id]->enqueue_message(fd, text);
}

void MainReactor::broadcast_message(const std::string& message, int sender_fd, bool include_sender) {
    auto worker_to_fds =
        session_manager_.collect_online_fds_grouped_by_worker(sender_fd, include_sender);

    for (auto& pair : worker_to_fds) {
        int worker_id = pair.first;
        if (worker_id < 0 || worker_id >= static_cast<int>(workers_.size())) {
            continue;
        }
        workers_[worker_id]->enqueue_broadcast_message(pair.second, message);
    }
}

void MainReactor::on_accept() {
    uint64_t now = current_connections_.fetch_add(1) + 1;
    accept_count_.fetch_add(1);

    uint64_t peak = peak_connections_.load();
    while (now > peak && !peak_connections_.compare_exchange_weak(peak, now)) {}
}

void MainReactor::on_disconnect() {
    disconnect_count_.fetch_add(1);
    current_connections_.fetch_sub(1);
}

void MainReactor::metrics_loop() {
    uint64_t last_accept = 0;
    uint64_t last_disconnect = 0;

    while (!metrics_stop_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        uint64_t accept = accept_count_.load();
        uint64_t conn = current_connections_.load();
        uint64_t peak_conn = peak_connections_.load();
        uint64_t disconnects = disconnect_count_.load();

        LOG_INFO(
            "[metrics] conn=" + std::to_string(conn) +
            " peak_conn=" + std::to_string(peak_conn) +
            " accept/s=" + std::to_string(accept - last_accept) +
            " disconnect/s=" + std::to_string(disconnects - last_disconnect));

        last_accept = accept;
        last_disconnect = disconnects;
    }
}

