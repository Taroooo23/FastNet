#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#define PORT 8888
#define BUFFER_SIZE 4096
#define MAX_EVENTS 64

struct ClientInfo {
    int fd = -1;
    std::string username;
    std::string ip;
    int port = 0;
    bool logged_in = false;
    std::string inbuf;
    std::string outbuf;
    bool closing = false;
};

std::unordered_map<int, ClientInfo> clients;
std::unordered_map<std::string, int> username_to_fd;

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }
    return 0;
}

bool add_epoll_fd(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0; // 成功返回true
}

bool mod_epoll_fd(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == 0;
}

void del_epoll_fd(int epoll_fd, int fd) {
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
}

void update_client_events(int epoll_fd, int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) {
        return;
    }

    uint32_t events = EPOLLIN;
    if (!it->second.outbuf.empty()) {
        events |= EPOLLOUT;
    }

    mod_epoll_fd(epoll_fd, fd, events);
}

//确保发送完整信息
bool flush_output(int epoll_fd, int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) {
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
                update_client_events(epoll_fd, fd);
                return true;
            }

            return false;
        }

        return false;
    }

    update_client_events(epoll_fd, fd);
    return true;
}

void queue_message(int epoll_fd, int fd, const std::string& text) {
    auto it = clients.find(fd);
    if (it == clients.end()) {
        return;
    }

    it->second.outbuf += text;

    if (!flush_output(epoll_fd, fd)) {
        it = clients.find(fd);
        if (it != clients.end()) {
            it->second.closing = true;
        }
    }
}

void remove_client(int epoll_fd, int client_fd);

//发送信息
void broadcast_message(int epoll_fd, const std::string& message, int sender_fd, bool include_sender = false) {
    std::vector<int> targets;

    for (const auto& pair : clients) {
        const ClientInfo& client = pair.second;

        if (!client.logged_in || client.closing) {
            continue;
        }

        if (!include_sender && client.fd == sender_fd) {
            continue;
        }

        targets.push_back(client.fd);
    }

    for (int fd : targets) {
        queue_message(epoll_fd, fd, message);
    }
}

//输出用户名单
std::string make_user_list() {
    std::vector<std::string> names;

    for (const auto& pair : clients) {
        const ClientInfo& client = pair.second;
        if (client.logged_in && !client.closing) {
            names.push_back(client.username);
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
    return result;
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

    auto client_it = clients.find(fd);
    if (client_it == clients.end()) {
        error = "client not found";
        return false;
    }

    if (client_it->second.logged_in) {
        error = "already logged in";
        return false;
    }

    if (username_to_fd.find(username) != username_to_fd.end()) {
        error = "username already exists";
        return false;
    }

    client_it->second.username = username;
    client_it->second.logged_in = true;
    username_to_fd[username] = fd;

    return true;
}

bool rename(int epoll_fd, int fd, const std::string& username, std::string& error) {
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

    auto client_it = clients.find(fd);
    if (client_it == clients.end()) {
        error = "client not found";
        return false;
    }

    if (username_to_fd.find(username) != username_to_fd.end()) {
        error = "username already exists";
        return false;
    }

    std::string msg = "[system] " + client_it->second.username + " changed his/her name to " + username + "\n";
    broadcast_message(epoll_fd, msg, fd, false);
    std::cout << msg;

    msg = "[system] rename success\n";
    queue_message(epoll_fd, fd, msg);

    auto it = username_to_fd.find(client_it->second.username);
    if(it != username_to_fd.end()) {
        username_to_fd.erase(it);
    }

    client_it->second.username = username;
    username_to_fd[username] = fd;

    
    return true;
}

//发私信
bool send_private_message(int epoll_fd, const std::string& from, const std::string& to, const std::string& content) {
    auto it = username_to_fd.find(to);
    if (it == username_to_fd.end()) {
        return false;
    }

    int target_fd = it->second;

    auto client_it = clients.find(target_fd);
    if (client_it == clients.end() || client_it->second.closing || !client_it->second.logged_in) {
        return false;
    }

    std::string msg = "[private][" + from + "]: " + content + "\n";
    queue_message(epoll_fd, target_fd, msg);

    client_it = clients.find(target_fd);
    if (client_it == clients.end() || client_it->second.closing) {
        return false;
    }

    return true;
}

//核心业务：处理用户输入（命令、信息）
bool process_line(int epoll_fd, int client_fd, const std::string& line) {
    auto it = clients.find(client_fd);
    if (it == clients.end()) {
        return false;
    }

    bool logged_in = it->second.logged_in;
    std::string username = it->second.username;

    if (!logged_in) {
        std::string error;

        if (!register_username(client_fd, line, error)) {
            queue_message(epoll_fd, client_fd, "[system] login failed: " + error + "\n");
            return true;
        }

        std::string welcome;
        welcome += "[system] login success\n";
        welcome += "[system] commands: /list, /msg <username> <content>, /quit\n";

        queue_message(epoll_fd, client_fd, welcome);

        std::string join_msg = "[system] " + line + " joined the chat\n";
        std::cout << join_msg;
        broadcast_message(epoll_fd, join_msg, client_fd, false);

        return true;
    }

    if (line.empty()) {
        return true;
    }

    if (line == "/list") {
        queue_message(epoll_fd, client_fd, make_user_list());
        return true;
    }

    if(line.compare(0, 8, "/rename ") == 0) {
        std::istringstream iss(line);
        std::string cmd;
        std::string newname;
        iss >> cmd >> newname;

        std::string error;
        if(!rename(epoll_fd, client_fd, newname, error)) {
            queue_message(epoll_fd, client_fd, "[system] rename failed: " + error + "\n");
            return true;
        }
        return true;
    }

    if (line == "/quit") {
        queue_message(epoll_fd, client_fd, "[system] bye\n");
        auto self_it = clients.find(client_fd);
        if (self_it != clients.end()) {
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
            queue_message(epoll_fd, client_fd, "[system] usage: /msg <username> <content>\n");
            return true;
        }

        if (target == username) {
            queue_message(epoll_fd, client_fd, "[system] cannot send private message to yourself\n");
            return true;
        }

        if (!send_private_message(epoll_fd, username, target, content)) {
            queue_message(epoll_fd, client_fd, "[system] user not found or disconnected\n");
        } else {
            queue_message(epoll_fd, client_fd, "[private][to " + target + "]: " + content + "\n");
        }

        return true;
    }

    if (line[0] == '/') {
        queue_message(epoll_fd, client_fd, "[system] unknown command\n");
        return true;
    }

    std::string msg = "[" + username + "]: " + line + "\n";
    std::cout << msg;
    broadcast_message(epoll_fd, msg, client_fd, true);

    return true;
}

void remove_client(int epoll_fd, int client_fd) {
    auto it = clients.find(client_fd);
    if (it == clients.end()) {
        return;
    }

    std::string username = it->second.username;
    std::string ip = it->second.ip;
    int port = it->second.port;
    bool logged_in = it->second.logged_in;

    if (!username.empty()) {
        username_to_fd.erase(username);
    }

    del_epoll_fd(epoll_fd, client_fd);
    close(client_fd);
    clients.erase(it);

    if (logged_in) {
        std::string leave_msg = "[system] " + username + " left the chat\n";
        std::cout << leave_msg;
        broadcast_message(epoll_fd, leave_msg, client_fd, false);
    } else {
        std::cout << "[system] " << ip << ":" << port << " disconnected before login\n";
    }
}

void handle_client_readable(int epoll_fd, int client_fd) {
    auto it = clients.find(client_fd);
    if (it == clients.end()) {
        return;
    }

    char buffer[BUFFER_SIZE];

    while (true) {
        ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);

        if (n > 0) {
            // it = clients.find(client_fd);
            // if (it == clients.end()) {
            //     return;
            // }

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

                if (!process_line(epoll_fd, client_fd, line)) {
                    auto close_it = clients.find(client_fd);
                    if (close_it != clients.end()) {
                        close_it->second.closing = true;
                    }
                    break;
                }

                auto check_it = clients.find(client_fd);
                if (check_it == clients.end()) {
                    return;
                }

                if (check_it->second.closing) {
                    break;
                }

                it = check_it;
            }

            auto check_it = clients.find(client_fd);
            if (check_it == clients.end()) {
                return;
            }

            if (check_it->second.closing) {
                break;
            }

            continue;
        }

        if (n == 0) {
            auto close_it = clients.find(client_fd);
            if (close_it != clients.end()) {
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
        
        auto close_it = clients.find(client_fd);
        if (close_it != clients.end()) {
            close_it->second.closing = true;
        }
        break;
    }

    it = clients.find(client_fd);
    if (it != clients.end()) {
        if (it->second.closing && it->second.outbuf.empty()) {
            remove_client(epoll_fd, client_fd);
        } else {
            update_client_events(epoll_fd, client_fd);
        }
    }
}

void handle_client_writable(int epoll_fd, int client_fd) {
    auto it = clients.find(client_fd);
    if (it == clients.end()) {
        return;
    }

    if (!flush_output(epoll_fd, client_fd)) {
        it = clients.find(client_fd);
        if (it != clients.end()) {
            it->second.closing = true;
        }
    }

    it = clients.find(client_fd);
    if (it != clients.end()) {
        if (it->second.closing && it->second.outbuf.empty()) {
            remove_client(epoll_fd, client_fd);
        } else {
            update_client_events(epoll_fd, client_fd);
        }
    }
}

void accept_new_clients(int epoll_fd, int server_fd) {
    while (true) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
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

        if (set_nonblocking(client_fd) < 0) {
            std::cerr << "set_nonblocking failed for client\n";
            close(client_fd);
            continue;
        }

        std::string client_ip = inet_ntoa(client_addr.sin_addr);
        int client_port = ntohs(client_addr.sin_port);

        ClientInfo info;
        info.fd = client_fd;
        info.ip = client_ip;
        info.port = client_port;
        info.logged_in = false;

        clients[client_fd] = info;

        if (!add_epoll_fd(epoll_fd, client_fd, EPOLLIN)) {
            std::cerr << "epoll add client failed\n";
            close(client_fd);
            clients.erase(client_fd);
            continue;
        }

        std::cout << "[system] new connection from " << client_ip << ":" << client_port << "\n";
        queue_message(epoll_fd, client_fd, "[system] connected. please enter your username\n");
    }
}

int main() {
    std::signal(SIGPIPE, SIG_IGN); // 忽略 SIGPIPE 信号

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        std::cerr << "socket creation failed\n";
        return 1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "setsockopt failed\n";
        close(server_fd);
        return 1;
    }

    if (set_nonblocking(server_fd) < 0) {
        std::cerr << "set_nonblocking failed for server\n";
        close(server_fd);
        return 1;
    }

    sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        std::cerr << "bind failed\n";
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 128) < 0) {
        std::cerr << "listen failed\n";
        close(server_fd);
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        std::cerr << "epoll_create1 failed\n";
        close(server_fd);
        return 1;
    }

    if (!add_epoll_fd(epoll_fd, server_fd, EPOLLIN)) {
        std::cerr << "epoll add server failed\n";
        close(epoll_fd);
        close(server_fd);
        return 1;
    }

    std::cout << "chat server started on port " << PORT << "\n";

    epoll_event events[MAX_EVENTS];

    while (true) {
        int nready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nready < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait failed\n";
            break;
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == server_fd) {
                accept_new_clients(epoll_fd, server_fd);
                continue;
            }

            auto it = clients.find(fd);
            if (it == clients.end()) {
                continue;
            }

            if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                it->second.closing = true;
            }

            if ((ev & EPOLLIN) && clients.find(fd) != clients.end()) {
                handle_client_readable(epoll_fd, fd);
            }

            if ((ev & EPOLLOUT) && clients.find(fd) != clients.end()) {
                handle_client_writable(epoll_fd, fd);
            }

            it = clients.find(fd);
            if (it != clients.end() && it->second.closing && it->second.outbuf.empty()) {
                remove_client(epoll_fd, fd);
            }
        }
    }

    std::vector<int> fds;
    for (const auto& pair : clients) {
        fds.push_back(pair.first);
    }

    for (int fd : fds) {
        remove_client(epoll_fd, fd);
    }

    close(epoll_fd);
    close(server_fd);
    return 0;
}

