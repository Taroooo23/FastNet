#pragma once

#include <string>

enum class CommandType {
    Login,
    List,
    Rename,
    Quit,
    PrivateMsg,
    PublicMsg,
    Unknown,
    Empty
};

struct Command {
    CommandType type = CommandType::Unknown;
    std::string target;
    std::string content;
    std::string raw;
};

// 只负责“把一行输入解析成命令”。
// 不负责执行命令。
class CommandParser {
public:
    Command parse(const std::string& line, bool logged_in) const;
};

