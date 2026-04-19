#include "reactor/worker_reactor.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/config.h"
#include "common/logger.h"

WorkerReactor::WorkerReactor(int id,
                             SessionManager& session_manager,
                             ChatService& chat_service,
                             SendToFdFn send_to_fd,
                             BroadcastFn broadcast,
                             DisconnectNotifyFn on_disconnect,
                             ForceRemoveFdFn force_remove_fd)
    : id_(id),
      session_manager_(session_manager),
      chat_service_(chat_service),
      send_to_fd_(std::move(send_to_fd)),
      broadcast_(std::move(broadcast)),
      on_disconnect_(std::move(on_disconnect)),
      force_remove_fd_(std::move(force_remove_fd)) {}

bool WorkerReactor::init() {
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        LOG_ERROR("worker epoll_create1 failed");
        return false;
    }

    wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        LOG_ERROR("worker eventfd failed");
        return false;
    }

    if (!add_epoll_fd(epoll_fd_, wake_fd_, EPOLLIN)) {
        LOG_ERROR("worker add wake_fd failed");
        return false;
    }

    return true;
}

void WorkerReactor::start() {
    thread_ = std::thread(&WorkerReactor::run, this);
}

void WorkerReactor::stop() {
    stopping_ = true;

    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        tasks_.push([]() {});
    }

    uint64_t one = 1;
    ssize_t n = write(wake_fd_, &one, sizeof(one));
    (void)n;
}

void WorkerReactor::join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

int WorkerReactor::id() const {
    return id_;
}

void WorkerReactor::add_client(int client_fd, const std::string& client_ip, int client_port) {
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        tasks_.push([this, client_fd, client_ip, client_port]() {
            auto conn = std::make_shared<Connection>(client_fd, client_ip, client_port, id_);
            clients_[client_fd] = conn;

            if (!add_epoll_fd(epoll_fd_, client_fd, EPOLLIN | EPOLLRDHUP)) {
                LOG_ERROR("epoll add client failed");
                close(client_fd);
                clients_.erase(client_fd);
                force_remove_fd_(client_fd);
                return;
            }

            queue_message_local(client_fd, "[system] connected. please enter your username\n");
        });
    }

    uint64_t one = 1;
    ssize_t n = write(wake_fd_, &one, sizeof(one));
    (void)n;
}

void WorkerReactor::enqueue_message(int fd, const std::string& text) {
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        tasks_.push([this, fd, text]() {
            queue_message_local(fd, text);
        });
    }

    uint64_t one = 1;
    ssize_t n = write(wake_fd_, &one, sizeof(one));
    (void)n;
}

void WorkerReactor::enqueue_broadcast_message(const std::vector<int>& fds, const std::string& text) {
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        tasks_.push([this, fds, text]() {
            for (int fd : fds) {
                queue_message_local(fd, text);
            }
        });
    }

    uint64_t one = 1;
    ssize_t n = write(wake_fd_, &one, sizeof(one));
    (void)n;
}

int WorkerReactor::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    return 0;
}

bool WorkerReactor::add_epoll_fd(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev {};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0;
}

bool WorkerReactor::mod_epoll_fd(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev {};
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0;
}

void WorkerReactor::del_epoll_fd(int epoll_fd, int fd) {
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
}

void WorkerReactor::update_client_events(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) {
        return;
    }

    uint32_t events = EPOLLIN | EPOLLRDHUP;
    if (!it->second->out_buffer().empty()) {
        events |= EPOLLOUT;
    }

    mod_epoll_fd(epoll_fd_, fd, events);
}

bool WorkerReactor::flush_output(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) {
        return false;
    }

    std::string& out = it->second->out_buffer();

    while (!out.empty()) {
        int flags = 0;
#ifdef MSG_NOSIGNAL
        flags = MSG_NOSIGNAL;
#endif
        ssize_t n = send(fd, out.data(), out.size(), flags);

        if (n > 0) {
            out.erase(0, static_cast<size_t>(n));
            continue;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                update_client_events(fd); // 保留剩余数据,打开 EPOLLOUT 监听
                return true;
            }

            return false;
        }

        return false;
    }

    update_client_events(fd); // 关闭多余的 EPOLLOUT 监听
    return true;
}

void WorkerReactor::queue_message_local(int fd, const std::string& text) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) {
        return;
    }

    it->second->out_buffer() += text;

    if (!flush_output(fd)) {
        auto check = clients_.find(fd);
        if (check != clients_.end()) {
            check->second->set_closing(true);
        }
    }
}

void WorkerReactor::remove_client(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }

    auto conn = it->second;
    const bool logged_in = conn->logged_in();
    const std::string username = conn->username();
    const std::string ip = conn->ip();
    const int port = conn->port();

    if (logged_in) {
        session_manager_.remove_logged_in_user(username, client_fd);
    } else {
        session_manager_.remove_fd_mapping(client_fd);
    }

    del_epoll_fd(epoll_fd_, client_fd);
    close(client_fd);
    clients_.erase(it);

    on_disconnect_();

    if (logged_in) {
        LOG_INFO("[system] " + username + " left the chat");
        broadcast_("[system] " + username + " left the chat\n", client_fd, false);
    } else {
        LOG_INFO("[system] " + ip + ":" + std::to_string(port) + " disconnected before login");
    }
}

void WorkerReactor::handle_client_readable(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }

    char buffer[config::kBufferSize];

    while (true) {
        ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);

        if (n > 0) {
            it->second->in_buffer().append(buffer, static_cast<size_t>(n));

            while (true) {
                std::string& inbuf = it->second->in_buffer();
                size_t pos = inbuf.find('\n');
                if (pos == std::string::npos) {
                    break;
                }

                std::string line = inbuf.substr(0, pos);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                inbuf.erase(0, pos + 1);

                bool keep = chat_service_.handle_line(
                    *it->second,
                    line,
                    send_to_fd_,
                    broadcast_);

                if (!keep) {
                    auto close_it = clients_.find(client_fd);
                    if (close_it != clients_.end()) {
                        close_it->second->set_closing(true);
                    }
                    break;
                }

                auto check_it = clients_.find(client_fd);
                if (check_it == clients_.end()) {
                    return;
                }

                if (check_it->second->closing()) {
                    break;
                }

                it = check_it;
            }

            auto check_it = clients_.find(client_fd);
            if (check_it == clients_.end()) {
                return;
            }

            if (check_it->second->closing()) {
                break;
            }

            continue;
        }

        if (n == 0) {
            auto close_it = clients_.find(client_fd);
            if (close_it != clients_.end()) {
                close_it->second->set_closing(true);
            }
            break;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        auto close_it = clients_.find(client_fd);
        if (close_it != clients_.end()) {
            close_it->second->set_closing(true);
        }
        break;
    }

    it = clients_.find(client_fd);
    if (it != clients_.end()) {
        if (it->second->closing() && it->second->out_buffer().empty()) {
            remove_client(client_fd);
        } else {
            update_client_events(client_fd);
        }
    }
}

void WorkerReactor::handle_client_writable(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }

    if (!flush_output(client_fd)) {
        it = clients_.find(client_fd);
        if (it != clients_.end()) {
            it->second->set_closing(true);
        }
    }

    it = clients_.find(client_fd);
    if (it != clients_.end()) {
        if (it->second->closing() && it->second->out_buffer().empty()) {
            remove_client(client_fd);
        } else {
            update_client_events(client_fd);
        }
    }
}

void WorkerReactor::drain_tasks() {
    while (true) {
        uint64_t cnt = 0;
        ssize_t n = read(wake_fd_, &cnt, sizeof(cnt));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            break;
        }

        if (n == 0) {
            break;
        }

        std::queue<std::function<void()>> local_tasks;
        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            std::swap(local_tasks, tasks_);
        }

        while (!local_tasks.empty()) {
            auto task = std::move(local_tasks.front());
            local_tasks.pop();
            task();
        }
    }
}

void WorkerReactor::close_all_clients_for_shutdown() {
    std::vector<int> fds;
    fds.reserve(clients_.size());

    for (const auto& pair : clients_) {
        fds.push_back(pair.first);
    }

    for (int fd : fds) {
        auto it = clients_.find(fd);
        if (it != clients_.end()) {
            it->second->set_closing(true);
            if (!it->second->out_buffer().empty()) {
                flush_output(fd);
            }
        }
        remove_client(fd);
    }
}

void WorkerReactor::run() {
    epoll_event events[config::kMaxEvents];

    while (true) {
        int nready = epoll_wait(epoll_fd_, events, config::kMaxEvents, -1);
        if (nready < 0) {
            if (errno == EINTR) {
                continue;
            }

            LOG_ERROR("worker epoll_wait failed");
            break;
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == wake_fd_) {
                drain_tasks();
                continue;
            }

            auto it = clients_.find(fd);
            if (it == clients_.end()) {
                continue;
            }

            if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                it->second->set_closing(true);
            }

            if ((ev & EPOLLIN) && clients_.find(fd) != clients_.end()) {
                handle_client_readable(fd);
            }

            if ((ev & EPOLLOUT) && clients_.find(fd) != clients_.end()) {
                handle_client_writable(fd);
            }

            it = clients_.find(fd);
            if (it != clients_.end() && it->second->closing() && it->second->out_buffer().empty()) {
                remove_client(fd);
            }
        }

        if (stopping_) {
            drain_tasks();
            close_all_clients_for_shutdown();
            break;
        }
    }

    if (wake_fd_ >= 0) {
        close(wake_fd_);
        wake_fd_ = -1;
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

