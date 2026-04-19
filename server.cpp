#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/resource.h>

#include <chrono>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define PORT 8888
#define BUFFER_SIZE 4096
#define MAX_EVENTS 64
#define WORKER_COUNT 4

struct ClientInfo {
    int fd = -1;
    std::string username;
    std::string ip;
    int port = 0;
    bool logged_in = false;
    std::string inbuf;
    std::string outbuf;
    bool closing = false;
    int worker_id = -1;
};

struct ServerStats {
    std::atomic<uint64_t> accept_count{0};
    std::atomic<uint64_t> current_connections{0};
    std::atomic<uint64_t> peak_connections{0};

    std::atomic<uint64_t> disconnect_count{0};
};

class ChatServer;

class WorkerReactor {
public:
    WorkerReactor(ChatServer* server, int id)
        : server_(server), id_(id) {}

    ~WorkerReactor() = default;

    bool init() {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            std::cerr << "worker epoll_create1 failed\n";
            return false;
        }

        wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd_ < 0) {
            std::cerr << "worker eventfd failed\n";
            return false;
        }

        if (!add_epoll_fd(epoll_fd_, wake_fd_, EPOLLIN)) {
            std::cerr << "worker add wake_fd failed\n";
            return false;
        }

        return true;
    }

    void start() {
        thread_ = std::thread(&WorkerReactor::run, this);
    }

    void stop() {
        stopping_ = true;
        post_task([]() {});
    }

    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    int id() const {
        return id_;
    }

    void post_task(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            tasks_.push(std::move(task));
        }

        uint64_t one = 1;
        ssize_t n = write(wake_fd_, &one, sizeof(one));
        (void)n; // 消除 unused variable 警告
    }

    void add_client(int client_fd, const std::string& client_ip, int client_port) {
        post_task([this, client_fd, client_ip, client_port]() {
            ClientInfo info;
            info.fd = client_fd;
            info.ip = client_ip;
            info.port = client_port;
            info.logged_in = false;
            info.worker_id = id_;

            clients_[client_fd] = info;

            if (!add_epoll_fd(epoll_fd_, client_fd, EPOLLIN | EPOLLRDHUP)) {
                std::cerr << "epoll add client failed\n";
                close(client_fd);
                clients_.erase(client_fd);
                on_client_force_removed(client_fd);
                return;
            }

            queue_message_local(client_fd, "[system] connected. please enter your username\n");
        });
    }

    void enqueue_message(int fd, const std::string& text) {
        post_task([this, fd, text]() {
            queue_message_local(fd, text);
        });
    }

    void enqueue_broadcast_message(const std::vector<int>& fds, const std::string& text) {
        post_task([this, fds, text]() {
            for (int fd : fds) {
                queue_message_local(fd, text);
            }
        });
    }

private:
    ChatServer* server_ = nullptr;
    int id_ = -1;
    int epoll_fd_ = -1;
    int wake_fd_ = -1;
    std::thread thread_;
    std::atomic<bool> stopping_{false};

    std::unordered_map<int, ClientInfo> clients_;

    std::mutex task_mutex_;
    std::queue<std::function<void()>> tasks_;

private:
    static int set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            return -1;
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            return -1;
        }
        return 0;
    }

    static bool add_epoll_fd(int epoll_fd, int fd, uint32_t events) {
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0; // 成功返回true
    }

    static bool mod_epoll_fd(int epoll_fd, int fd, uint32_t events) {
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0;
    }

    static void del_epoll_fd(int epoll_fd, int fd) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    }

    void update_client_events(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) {
            return;
        }

        uint32_t events = EPOLLIN | EPOLLRDHUP;
        if (!it->second.outbuf.empty()) {
            events |= EPOLLOUT;
        }

        mod_epoll_fd(epoll_fd_, fd, events);
    }

    //确保发送完整信息
    bool flush_output(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) {
            return false;
        }

        std::string& out = it->second.outbuf;

        while (!out.empty()) {
            int flags = 0;
    #ifdef MSG_NOSIGNAL  //为了防止用户断开连接直接杀掉进程
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
                    update_client_events(fd);
                    return true;
                }

                return false;
            }

            return false;
        }

        update_client_events(fd);
        return true;
    }

    void queue_message_local(int fd, const std::string& text) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) {
            return;
        }

        it->second.outbuf += text;

        if (!flush_output(fd)) {
            it = clients_.find(fd);
            if (it != clients_.end()) {
                it->second.closing = true;
            }
        }
    }

    //发私信
    bool send_private_message(const std::string& from, const std::string& to, const std::string& content);

    //核心业务：处理用户输入（命令、信息）
    bool process_line(int client_fd, const std::string& line);

    void remove_client(int client_fd) {
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) {
            return;
        }

        std::string username = it->second.username;
        std::string ip = it->second.ip;
        int port = it->second.port;
        bool logged_in = it->second.logged_in;

        if (logged_in) {
            server_remove_logged_in_user(username, client_fd);
        } else {
            server_remove_fd_mapping(client_fd);
        }

        del_epoll_fd(epoll_fd_, client_fd);
        close(client_fd);
        clients_.erase(it);
        server_on_disconnect();

        if (logged_in) {
            std::string leave_msg = "[system] " + username + " left the chat\n";
            std::cout << leave_msg;
            server_broadcast_message(leave_msg, client_fd, false);
        } else {
            std::cout << "[system] " << ip << ":" << port << " disconnected before login\n";
        }
    }

    void handle_client_readable(int client_fd) {
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) {
            return;
        }

        char buffer[BUFFER_SIZE];

        while (true) {
            ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);

            if (n > 0) {
                it->second.inbuf.append(buffer, static_cast<size_t>(n));

                while (true) {
                    size_t pos = it->second.inbuf.find('\n');
                    if (pos == std::string::npos) {
                        break;
                    }

                    std::string line = it->second.inbuf.substr(0, pos);

                    if (!line.empty() && line.back() == '\r') {
                        line.pop_back();
                    }

                    it->second.inbuf.erase(0, pos + 1);

                    if (!process_line(client_fd, line)) {
                        auto close_it = clients_.find(client_fd);
                        if (close_it != clients_.end()) {
                            close_it->second.closing = true;
                        }
                        break;
                    }

                    auto check_it = clients_.find(client_fd);
                    if (check_it == clients_.end()) {
                        return;
                    }

                    if (check_it->second.closing) {
                        break;
                    }

                    it = check_it;
                }

                auto check_it = clients_.find(client_fd);
                if (check_it == clients_.end()) {
                    return;
                }

                if (check_it->second.closing) {
                    break;
                }

                continue;
            }

            if (n == 0) {
                auto close_it = clients_.find(client_fd);
                if (close_it != clients_.end()) {
                    close_it->second.closing = true;
                }
                break;
            }

            if(n < 0) {
                if (errno == EINTR) {
                    continue;
                }

                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
            }

            auto close_it = clients_.find(client_fd);
            if (close_it != clients_.end()) {
                close_it->second.closing = true;
            }
            break;
        }

        it = clients_.find(client_fd);
        if (it != clients_.end()) {
            if (it->second.closing && it->second.outbuf.empty()) {
                remove_client(client_fd);
            } else {
                update_client_events(client_fd);
            }
        }
    }

    void handle_client_writable(int client_fd) {
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) {
            return;
        }

        if (!flush_output(client_fd)) {
            it = clients_.find(client_fd);
            if (it != clients_.end()) {
                it->second.closing = true;
            }
        }

        it = clients_.find(client_fd);
        if (it != clients_.end()) {
            if (it->second.closing && it->second.outbuf.empty()) {
                remove_client(client_fd);
            } else {
                update_client_events(client_fd);
            }
        }
    }

    void drain_tasks() {
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

    void close_all_clients_for_shutdown() {
        std::vector<int> fds;
        for (const auto& pair : clients_) {
            fds.push_back(pair.first);
        }

        for (int fd : fds) {
            auto it = clients_.find(fd);
            if (it != clients_.end()) {
                it->second.closing = true;
                if (!it->second.outbuf.empty()) {
                    flush_output(fd);
                }
            }
            remove_client(fd);
        }
    }

    void run() {
        epoll_event events[MAX_EVENTS];

        while (true) {
            int nready = epoll_wait(epoll_fd_, events, MAX_EVENTS, -1);
            if (nready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::cerr << "worker epoll_wait failed\n";
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
                    it->second.closing = true;
                }

                if ((ev & EPOLLIN) && clients_.find(fd) != clients_.end()) {
                    handle_client_readable(fd);
                }

                if ((ev & EPOLLOUT) && clients_.find(fd) != clients_.end()) {
                    handle_client_writable(fd);
                }

                it = clients_.find(fd);
                if (it != clients_.end() && it->second.closing && it->second.outbuf.empty()) {
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

    void on_client_force_removed(int fd);
    bool server_register_username(int fd, const std::string& username, std::string& error);
    bool server_rename_username(const std::string& oldname, int fd, const std::string& newname, std::string& error);
    void server_remove_logged_in_user(const std::string& username, int fd);
    void server_remove_fd_mapping(int fd);
    void server_broadcast_message(const std::string& message, int sender_fd, bool include_sender = false);
    void server_on_disconnect();

};

class ChatServer {
public:
    ChatServer()
        : server_fd_(-1), epoll_fd_(-1), next_worker_(0), stopping_(false) {}

    ~ChatServer() {
        shutdown();
    }

    bool init() {
        std::signal(SIGPIPE, SIG_IGN); // 忽略 SIGPIPE 信号
        g_server = this;
        std::signal(SIGINT, ChatServer::handle_signal);
        std::signal(SIGTERM, ChatServer::handle_signal);

        server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd_ == -1) {
            std::cerr << "socket creation failed\n";
            return false;
        }

        int opt = 1;
        if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            std::cerr << "setsockopt failed\n";
            return false;
        }

        if (set_nonblocking(server_fd_) < 0) {
            std::cerr << "set_nonblocking failed for server\n";
            return false;
        }

        sockaddr_in server_addr;
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(PORT);

        if (bind(server_fd_, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
            std::cerr << "bind failed\n";
            return false;
        }

        if (listen(server_fd_, 65535) < 0) {
            std::cerr << "listen failed\n";
            return false;
        }

        for (int i = 0; i < WORKER_COUNT; ++i) {
            workers_.emplace_back(new WorkerReactor(this, i));
            if (!workers_.back()->init()) {
                return false;
            }
        }

        for (auto& worker : workers_) {
            worker->start();
        }

        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            std::cerr << "epoll_create1 failed\n";
            return false;
        }

        if (!add_epoll_fd(epoll_fd_, server_fd_, EPOLLIN)) {
            std::cerr << "epoll add server failed\n";
            return false;
        }
        metrics_thread_ = std::thread(&ChatServer::metrics_loop, this);

        std::cout << "chat server started on port " << PORT << "\n";
        return true;
    }

    void run() {
        epoll_event events[MAX_EVENTS];

        while (!stopping_) {
            int nready = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);
            if (nready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::cerr << "epoll_wait failed\n";
                break;
            }

            for (int i = 0; i < nready; ++i) {
                int fd = events[i].data.fd;

                if (fd == server_fd_) {
                    accept_new_clients();
                    continue;
                }
            }
        }

        shutdown();
    }

    void shutdown() {
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

    }

    void request_stop() {
        stopping_ = true;
    }

    bool register_username(int fd, const std::string& username, std::string& error) {
        if (username.empty()) {
            error = "username cannot be empty";
            return false;
        }

        if (username.size() > 20) {
            error = "username is too long";
            return false;
        }

        if (username[0] == '/') {
            error = "username cannot start with '/'";
            return false;
        }

        if (username.find_first_of(" \t") != std::string::npos) {
            error = "username cannot contain spaces";
            return false;
        }

        std::lock_guard<std::mutex> lock(global_mutex_);

        if (username_to_fd_.find(username) != username_to_fd_.end()) {
            error = "username already exists";
            return false;
        }

        username_to_fd_[username] = fd;
        logged_in_fds_.insert(fd);

        return true;
    }

    bool rename_username(const std::string& oldname, int fd, const std::string& newname, std::string& error) {
        if (newname.empty()) {
            error = "username cannot be empty";
            return false;
        }

        if (newname.size() > 20) {
            error = "username is too long";
            return false;
        }

        if (newname[0] == '/') {
            error = "username cannot start with '/'";
            return false;
        }

        if (newname.find_first_of(" \t") != std::string::npos) {
            error = "username cannot contain spaces";
            return false;
        }

        std::lock_guard<std::mutex> lock(global_mutex_);

        if (username_to_fd_.find(newname) != username_to_fd_.end()) {
            error = "username already exists";
            return false;
        }

        auto it = username_to_fd_.find(oldname);
        if (it != username_to_fd_.end()) {
            username_to_fd_.erase(it);
        }

        username_to_fd_[newname] = fd;
        return true;
    }

    void remove_logged_in_user(const std::string& username, int fd) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        if (!username.empty()) {
            username_to_fd_.erase(username);
        }
        logged_in_fds_.erase(fd);
        fd_to_worker_.erase(fd);
    }

    void remove_fd_mapping(int fd) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        fd_to_worker_.erase(fd);
        logged_in_fds_.erase(fd);
    }

    bool find_user_fd_and_worker(const std::string& username, int& target_fd, int& worker_id) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        auto it = username_to_fd_.find(username);
        if (it == username_to_fd_.end()) {
            return false;
        }

        target_fd = it->second;
        auto wit = fd_to_worker_.find(target_fd);
        if (wit == fd_to_worker_.end()) {
            return false;
        }

        worker_id = wit->second;
        return true;
    }

    void broadcast_message(const std::string& message, int sender_fd, bool include_sender = false) {
        std::unordered_map<int, std::vector<int>> worker_to_fds;

        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            for (int fd : logged_in_fds_) {
                if (!include_sender && fd == sender_fd) {
                    continue;
                }

                auto it = fd_to_worker_.find(fd);
                if (it == fd_to_worker_.end()) {
                    continue;
                }

                worker_to_fds[it->second].push_back(fd);
            }
        }

        for (auto& pair : worker_to_fds) {
            int worker_id = pair.first;
            workers_[worker_id]->enqueue_broadcast_message(pair.second, message);
        }
    }

    void queue_message(int fd, const std::string& text) {
        int worker_id = -1;
        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            auto it = fd_to_worker_.find(fd);
            if (it == fd_to_worker_.end()) {
                return;
            }
            worker_id = it->second;
        }

        workers_[worker_id]->enqueue_message(fd, text);
    }

    void bind_fd_worker(int fd, int worker_id) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        fd_to_worker_[fd] = worker_id;
    }

    void force_remove_fd_mapping(int fd) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        fd_to_worker_.erase(fd);
        logged_in_fds_.erase(fd);

        for (auto it = username_to_fd_.begin(); it != username_to_fd_.end(); ) {
            if (it->second == fd) {
                it = username_to_fd_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void on_accept() {
        auto now = stats_.current_connections.fetch_add(1) + 1;
        stats_.accept_count.fetch_add(1);

        uint64_t peak = stats_.peak_connections.load();
        while (now > peak && !stats_.peak_connections.compare_exchange_weak(peak, now)) {}
    }

    void on_disconnect() {
        stats_.disconnect_count.fetch_add(1);
        stats_.current_connections.fetch_sub(1);
    }   
private:
    int server_fd_;
    int epoll_fd_;
    std::vector<std::unique_ptr<WorkerReactor>> workers_;
    std::unordered_map<std::string, int> username_to_fd_;
    std::unordered_map<int, int> fd_to_worker_;
    std::unordered_set<int> logged_in_fds_;
    std::mutex global_mutex_;
    std::atomic<size_t> next_worker_;
    std::atomic<bool> stopping_;
    bool shutdown_done_ = false;
    ServerStats stats_;
    std::thread metrics_thread_;
    std::atomic<bool> metrics_stop_{false};

private:
    static ChatServer* g_server;

    static void handle_signal(int) {
        if (g_server) {
            g_server->request_stop();
        }
    }

    static int set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            return -1;
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            return -1;
        }
        return 0;
    }

    static bool add_epoll_fd(int epoll_fd, int fd, uint32_t events) {
        epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0; // 成功返回true
    }

    void accept_new_clients() {
        while (true) {
            sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);

            int client_fd = accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }

                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }

                std::cerr << "accept failed\n";
                break;
            }
            on_accept();

            if (set_nonblocking(client_fd) < 0) {
                std::cerr << "set_nonblocking failed for client\n";
                close(client_fd);
                continue;
            }

            std::string client_ip = inet_ntoa(client_addr.sin_addr);
            int client_port = ntohs(client_addr.sin_port);

            int worker_id = static_cast<int>(next_worker_.fetch_add(1) % workers_.size());
            bind_fd_worker(client_fd, worker_id);

            std::cout << "[system] new connection from " << client_ip << ":" << client_port << "\n";
            workers_[worker_id]->add_client(client_fd, client_ip, client_port);
        }
    }

    void metrics_loop() {
        uint64_t last_accept = 0;
        uint64_t last_disconnect = 0;

        while (!metrics_stop_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            uint64_t accept = stats_.accept_count.load();
            uint64_t conn = stats_.current_connections.load();
            uint64_t peak_conn = stats_.peak_connections.load();
            uint64_t disconnects = stats_.disconnect_count.load();

            std::cout
                << "[metrics] "
                << "conn=" << conn
                << " peak_conn=" << peak_conn
                << " accept/s=" << (accept - last_accept)
                << " disconnect/s=" << (disconnects - last_disconnect)
                << "\n";

            last_accept = accept;
            last_disconnect = disconnects;
        }
    }


    friend class WorkerReactor;
};

ChatServer* ChatServer::g_server = nullptr;

bool WorkerReactor::server_register_username(int fd, const std::string& username, std::string& error) {
    return server_->register_username(fd, username, error);
}

bool WorkerReactor::server_rename_username(const std::string& oldname, int fd, const std::string& newname, std::string& error) {
    return server_->rename_username(oldname, fd, newname, error);
}

void WorkerReactor::server_remove_logged_in_user(const std::string& username, int fd) {
    server_->remove_logged_in_user(username, fd);
}

void WorkerReactor::server_remove_fd_mapping(int fd) {
    server_->remove_fd_mapping(fd);
}

void WorkerReactor::server_broadcast_message(const std::string& message, int sender_fd, bool include_sender) {
    server_->broadcast_message(message, sender_fd, include_sender);
}

void WorkerReactor::on_client_force_removed(int fd) {
    server_->force_remove_fd_mapping(fd);
}
void WorkerReactor::server_on_disconnect() {
    server_->on_disconnect();
}

//发私信
bool WorkerReactor::send_private_message(const std::string& from, const std::string& to, const std::string& content) {
    int target_fd = -1;
    int worker_id = -1;

    if (!server_->find_user_fd_and_worker(to, target_fd, worker_id)) {
        return false;
    }

    std::string msg = "[private][" + from + "]: " + content + "\n";
    server_->queue_message(target_fd, msg);
    return true;
}

//核心业务：处理用户输入（命令、信息）
bool WorkerReactor::process_line(int client_fd, const std::string& line) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return false;
    }

    bool logged_in = it->second.logged_in;
    std::string username = it->second.username;

    if (!logged_in) {
        std::string error;

        if (!server_register_username(client_fd, line, error)) {
            queue_message_local(client_fd, "[system] login failed: " + error + "\n");
            return true;
        }

        it = clients_.find(client_fd);
        if (it == clients_.end()) {
            return false;
        }

        it->second.username = line;
        it->second.logged_in = true;

        std::string welcome;
        welcome += "[system] login success\n";
        welcome += "[system] commands: /list, /msg <username> <content>, /quit\n";

        queue_message_local(client_fd, welcome);

        std::string join_msg = "[system] " + line + " joined the chat\n";
        std::cout << join_msg;
        server_broadcast_message(join_msg, client_fd, false);

        return true;
    }

    if (line.empty()) {
        return true;
    }

    if (line == "/list") {
        std::vector<std::string> names;
        {
            std::lock_guard<std::mutex> lock(server_->global_mutex_);
            for (const auto& pair : server_->username_to_fd_) {
                names.push_back(pair.first);
            }
        }

        std::sort(names.begin(), names.end());

        std::string result = "[system] online users (" + std::to_string(names.size()) + "): ";
        for (size_t i = 0; i < names.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += names[i];
        }
        result += "\n";

        queue_message_local(client_fd, result);
        return true;
    }

    if(line.compare(0, 8, "/rename ") == 0) {
        std::istringstream iss(line);
        std::string cmd;
        std::string newname;
        iss >> cmd >> newname;

        std::string error;
        if(!server_rename_username(username, client_fd, newname, error)) {
            queue_message_local(client_fd, "[system] rename failed: " + error + "\n");
            return true;
        }

        std::string msg = "[system] " + username + " changed his/her name to " + newname + "\n";
        server_broadcast_message(msg, client_fd, false);
        std::cout << msg;

        queue_message_local(client_fd, "[system] rename success\n");

        auto self_it = clients_.find(client_fd);
        if (self_it != clients_.end()) {
            self_it->second.username = newname;
        }

        return true;
    }

    if (line == "/quit") {
        queue_message_local(client_fd, "[system] bye\n");
        auto self_it = clients_.find(client_fd);
        if (self_it != clients_.end()) {
            self_it->second.closing = true;
        }
        return true;
    }

    if (line.compare(0, 5, "/msg ") == 0) {
        std::istringstream iss(line);
        std::string cmd;
        std::string target;
        iss >> cmd >> target;

        std::string content;
        std::getline(iss, content);

        if (!content.empty() && content[0] == ' ') {
            content.erase(0, 1);
        }

        if (target.empty() || content.empty()) {
            queue_message_local(client_fd, "[system] usage: /msg <username> <content>\n");
            return true;
        }

        if (target == username) {
            queue_message_local(client_fd, "[system] cannot send private message to yourself\n");
            return true;
        }

        if (!send_private_message(username, target, content)) {
            queue_message_local(client_fd, "[system] user not found or disconnected\n");
        } else {
            queue_message_local(client_fd, "[private][to " + target + "]: " + content + "\n");
        }

        return true;
    }

    if (line[0] == '/') {
        queue_message_local(client_fd, "[system] unknown command\n");
        return true;
    }

    std::string msg = "[" + username + "]: " + line + "\n";
    std::cout << msg;
    server_broadcast_message(msg, client_fd, true);

    return true;
}

int main() {
    ChatServer server;
    if (!server.init()) {
        return 1;
    }
    server.run();
    return 0;
}
