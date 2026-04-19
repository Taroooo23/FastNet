#include "logic/command_parser.h"

#include <sstream>

Command CommandParser::parse(const std::string& line, bool logged_in) const {
    Command cmd;
    cmd.raw = line;

    if (line.empty()) {
        cmd.type = CommandType::Empty;
        return cmd;
    }

    // 未登录时，第一行就当作用户名
    if (!logged_in) {
        cmd.type = CommandType::Login;
        cmd.content = line;
        return cmd;
    }

    if (line == "/list") {
        cmd.type = CommandType::List;
        return cmd;
    }

    if (line == "/quit") {
        cmd.type = CommandType::Quit;
        return cmd;
    }

    if (line.compare(0, 8, "/rename ") == 0) {
        std::istringstream iss(line);
        std::string op;
        iss >> op >> cmd.target;

        cmd.type = cmd.target.empty() ? CommandType::Unknown : CommandType::Rename;
        return cmd;
    }

    if (line.compare(0, 5, "/msg ") == 0) {
        std::istringstream iss(line);
        std::string op;
        iss >> op >> cmd.target;
        std::getline(iss, cmd.content);

        if (!cmd.content.empty() && cmd.content[0] == ' ') {
            cmd.content.erase(0, 1);
        }

        if (cmd.target.empty() || cmd.content.empty()) {
            cmd.type = CommandType::Unknown;
        } else {
            cmd.type = CommandType::PrivateMsg;
        }
        return cmd;
    }

    if (!line.empty() && line[0] == '/') {
        cmd.type = CommandType::Unknown;
        return cmd;
    }

    cmd.type = CommandType::PublicMsg;
    cmd.content = line;
    return cmd;
}

