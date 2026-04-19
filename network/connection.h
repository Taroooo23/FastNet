#pragma once

#include <string>

// Connection 表示“一个客户端连接”的状态。
// 它不关心 epoll，也不关心业务规则，只保存连接本身的信息。
class Connection {
public:
    Connection(int fd, std::string ip, int port, int worker_id);

    int fd() const;

    const std::string& username() const;
    void set_username(const std::string& username);

    const std::string& ip() const;
    int port() const;
    int worker_id() const;

    bool logged_in() const;
    void set_logged_in(bool value);

    bool closing() const;
    void set_closing(bool value);

    std::string& in_buffer();
    std::string& out_buffer();

private:
    int fd_ = -1;
    std::string username_;
    std::string ip_;
    int port_ = 0;
    int worker_id_ = -1;

    bool logged_in_ = false;
    bool closing_ = false;

    std::string inbuf_;
    std::string outbuf_;
};

