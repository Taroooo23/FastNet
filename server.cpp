#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#define PORT 8888
#define BUFFER_SIZE 1024

struct ClientInfo {
    int fd;
    std::string username;
    std::string ip;
    int port;
};

std::vector<ClientInfo> clients;      // 保存所有客户端socket
std::mutex clients_mutex;      // 保护clients，防止多线程冲突

// 广播消息给除了发送者以外的所有客户端
void broadcast_message(const std::string& message, int sender_fd) {
    std::lock_guard<std::mutex> lock(clients_mutex);

    for (const auto& client : clients) {
        if (client.fd != sender_fd) {
            send(client.fd, message.c_str(), message.size(), 0);
        }
    }
}

// 删除客户端
void remove_client(int client_fd) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    clients.erase(
        std::remove_if(clients.begin(), clients.end(),
                       [client_fd](const ClientInfo& c) { return c.fd == client_fd; }),
        clients.end());
}


// 处理某一个客户端
void handle_client(int client_fd, std::string client_ip, int client_port) {
    char buffer[BUFFER_SIZE];

    memset(buffer, 0, sizeof(buffer));

    ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received <= 0) {
        std::string leave_msg = "[" + client_ip + ":" + std::to_string(client_port) + "] join and left the chat, and didn't leave his/her name\n";
        std::cout << leave_msg;
        broadcast_message(leave_msg, client_fd);

        close(client_fd);

        remove_client(client_fd); // 内部已加锁

        return ;
    }

    // 去掉可能的换行符
    std::string username(buffer);
    if (!username.empty() && username.back() == '\n') {
        username.pop_back();
    }
    if (!username.empty() && username.back() == '\r') {
        username.pop_back();
    }

    // 保存用户名
    {
        std::lock_guard<std::mutex> lock(clients_mutex);
        for (auto& client : clients) {
            if (client.fd == client_fd) {
                client.username = username;
                break;
            }
        }
    }

    std::string join_msg = "[" + username + "] joined the chat\n";
    std::cout << join_msg;
    broadcast_message(join_msg, client_fd);

    while (true) {
        memset(buffer, 0, sizeof(buffer));

        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

        if (bytes_received <= 0) {
            std::string leave_msg = "[" + username + "] left the chat\n";
            std::cout << leave_msg;
            broadcast_message(leave_msg, client_fd);

            close(client_fd);

            remove_client(client_fd); // 内部已加锁
            //clients.erase(std::remove(clients.begin(), clients.end(), client_fd), clients.end());
            break;
        }

        std::string msg = "[" + username + "]: " + std::string(buffer);
        std::cout << msg;
        broadcast_message(msg, client_fd);
    }
}

int main() {
    // 1. 创建socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        std::cerr << "socket creation failed\n";
        return 1;
    }

    // 允许端口快速复用，常开
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 2. 配置服务端地址
    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;           // IPv4
    server_addr.sin_addr.s_addr = INADDR_ANY;  // 监听本机所有网卡
    server_addr.sin_port = htons(PORT);        // 端口号

    // 3. 绑定
    if (bind(server_fd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "bind failed\n";
        close(server_fd);
        return 1;
    }

    // 4. 监听
    if (listen(server_fd, 10) < 0) {
        std::cerr << "listen failed\n";
        close(server_fd);
        return 1;
    }

    std::cout << "Chat server started on port " << PORT << "...\n";

    // 5. 不断接收客户端连接
    while (true) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            std::cerr << "accept failed\n";
            continue;
        }

        std::string client_ip = inet_ntoa(client_addr.sin_addr);
        int client_port = ntohs(client_addr.sin_port);

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            clients.push_back({client_fd, "", client_ip, client_port});        
        }

        // 6. 为每个客户端创建线程
        std::thread t(handle_client, client_fd, client_ip, client_port);
        t.detach();
    }

    close(server_fd);
    return 0;
}

