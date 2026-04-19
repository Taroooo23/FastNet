#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define PORT 8888
#define BUFFER_SIZE 4096

struct ClientInfo {
    int fd = -1;
    std::string username;
    std::string ip;
    int port = 0;
    bool logged_in = false;
    std::string inbuf;
    bool closing = false;
};

std::unordered_map<int, ClientInfo> clients;
std::unordered_map<std::string, int> username_to_fd;
std::mutex clients_mutex;

//确保发送完整信息
bool send_all(int fd, const std::string& text) {
    size_t total = 0;

    while (total < text.size()) {
        int flags = 0;
#ifdef MSG_NOSIGNAL  //为了防止用户断开连接直接杀掉进程
        flags = MSG_NOSIGNAL;
#endif
        ssize_t n = send(fd, text.c_str() + total, text.size() - total, flags);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        if (n == 0) {
            return false;
        }

        total += static_cast<size_t>(n);
    }

    return true;
}

void remove_client(int client_fd);

//发送信息
void broadcast_message(const std::string& message, int sender_fd, bool include_sender = false) {
    std::vector<int> targets;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        for (const auto& pair : clients) {
            const ClientInfo& client = pair.second;

            if (!client.logged_in) {
                continue;
            }

            if (!include_sender && client.fd == sender_fd) {
                continue;
            }

            targets.push_back(client.fd);
        }
    }

    std::vector<int> failed_fds;

    for (int fd : targets) {
        if (!send_all(fd, message)) {
            failed_fds.push_back(fd);
        }
    }

    for (int fd : failed_fds) {
        remove_client(fd);
    }
}

//输出用户名单
std::string make_user_list() {
    std::vector<std::string> names;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        for (const auto& pair : clients) {
            const ClientInfo& client = pair.second;
            if (client.logged_in) {
                names.push_back(client.username);
            }
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

    std::lock_guard<std::mutex> lock(clients_mutex);

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

//发私信
bool send_private_message(const std::string& from, const std::string& to, const std::string& content) {
    int target_fd = -1;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        auto it = username_to_fd.find(to);
        if (it == username_to_fd.end()) {
            return false;
        }

        target_fd = it->second;
    }

    std::string msg = "[private][" + from + "]: " + content + "\n";

    if (!send_all(target_fd, msg)) {
        remove_client(target_fd);
        return false;
    }

    return true;
}

//核心业务：处理用户输入（命令、信息）
bool process_line(int client_fd, const std::string& line) {
    bool logged_in = false;
    std::string username;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        auto it = clients.find(client_fd);
        if (it == clients.end()) {
            return false;
        }

        logged_in = it->second.logged_in;
        username = it->second.username;
    }

    if (!logged_in) {
        std::string error;

        if (!register_username(client_fd, line, error)) {
            send_all(client_fd, "[system] login failed: " + error + "\n");
            return true;
        }

        std::string welcome;
        welcome += "[system] login success\n";
        welcome += "[system] commands: /list, /msg <username> <content>, /quit\n";  

        send_all(client_fd, welcome);

        std::string join_msg = "[system] " + line + " joined the chat\n";
        std::cout << join_msg;
        broadcast_message(join_msg, client_fd, false);

        return true;
    }

    if (line.empty()) {
        return true;
    }

    if (line == "/list") {
        send_all(client_fd, make_user_list());
        return true;
    }

    if (line == "/quit") {
        send_all(client_fd, "[system] bye\n");
        return false;
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
            send_all(client_fd, "[system] usage: /msg <username> <content>\n");
            return true;
        }

        if (target == username) {
            send_all(client_fd, "[system] cannot send private message to yourself\n");
            return true;
        }

        if (!send_private_message(username, target, content)) {
            send_all(client_fd, "[system] user not found or disconnected\n");
        } else {
            send_all(client_fd, "[private][to " + target + "]: " + content + "\n");
        }

        return true;
    }

    if (line[0] == '/') {
        send_all(client_fd, "[system] unknown command\n");
        return true;
    }

    std::string msg = "[" + username + "]: " + line + "\n";
    std::cout << msg;
    broadcast_message(msg, client_fd, true);

    return true;
}

void remove_client(int client_fd) {
    std::string username;
    std::string ip;
    int port = 0;
    bool logged_in = false;

    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        auto it = clients.find(client_fd);
        if (it == clients.end()) {
            return;
        }

        username = it->second.username;
        ip = it->second.ip;
        port = it->second.port;
        logged_in = it->second.logged_in;

        if (!username.empty()) {
            username_to_fd.erase(username);
        }

        clients.erase(it);
    }

    shutdown(client_fd, SHUT_RDWR);  //关闭读写
    close(client_fd);

    if (logged_in) {
        std::string leave_msg = "[system] " + username + " left the chat\n";
        std::cout << leave_msg;
        broadcast_message(leave_msg, client_fd, false);
    } else {
        std::cout << "[system] " << ip << ":" << port << " disconnected before login\n";
    }
}

void handle_client(int client_fd) {
    char buffer[BUFFER_SIZE];

    while (true) {
        ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);

        if (n <= 0) {
            if (errno == EINTR) {
                continue;
            }

            remove_client(client_fd);
            return;
        }

        std::vector<std::string> lines;

        {
            std::lock_guard<std::mutex> lock(clients_mutex);

            auto it = clients.find(client_fd);
            if (it == clients.end()) {
                return;
            }

            it->second.inbuf.append(buffer, n);

            size_t pos = 0;
            while ((pos = it->second.inbuf.find('\n')) != std::string::npos) {
                std::string line = it->second.inbuf.substr(0, pos);

                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                lines.push_back(line);
                it->second.inbuf.erase(0, pos + 1);
            }
        }

        for (const std::string& line : lines) {
            if (!process_line(client_fd, line)) {
                remove_client(client_fd);
                return;
            }
        }
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
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) { // 允许端口复用，避免服务器重启时“端口被占用”问题
        std::cerr << "setsockopt failed\n";
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

    if (listen(server_fd, 10) < 0) {
        std::cerr << "listen failed\n";
        close(server_fd);
        return 1;
    }

    std::cout << "chat server started on port " << PORT << "\n";

    while (true) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            std::cerr << "accept failed\n";
            continue;
        }

        std::string client_ip = inet_ntoa(client_addr.sin_addr);
        int client_port = ntohs(client_addr.sin_port);

        {
            std::lock_guard<std::mutex> lock(clients_mutex);

            ClientInfo info;
            info.fd = client_fd;
            info.ip = client_ip;
            info.port = client_port;
            info.logged_in = false;

            clients[client_fd] = info;
        }

        std::cout << "[system] new connection from " << client_ip << ":" << client_port << "\n";
        send_all(client_fd, "[system] connected. please enter your username\n");

        std::thread t(handle_client, client_fd);
        t.detach();
    }

    close(server_fd);
    return 0;
}

