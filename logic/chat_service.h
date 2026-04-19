#pragma once

#include <functional>
#include <string>

#include "logic/command_parser.h"
#include "logic/session_manager.h"
#include "network/connection.h"

// ChatService 负责“聊天业务”。
// 比如：登录、重命名、私聊、群聊、/list、/quit。
// 它不直接做 epoll 和 socket 读写，而是通过回调把结果交回 reactor。
class ChatService {
public:
    using SendToFdFn = std::function<void(int fd, const std::string& text)>;
    using BroadcastFn = std::function<void(const std::string& message, int sender_fd, bool include_sender)>;

    explicit ChatService(SessionManager& session_manager);

    // 处理一整行用户输入。
    // 返回 false 表示业务上建议关闭这个连接。
    bool handle_line(Connection& conn,
                     const std::string& line,
                     const SendToFdFn& send_to_fd,
                     const BroadcastFn& broadcast);

private:
    SessionManager& session_manager_;
    CommandParser parser_;
};

