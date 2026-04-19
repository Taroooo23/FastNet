#include "logic/chat_service.h"

#include <sstream>

ChatService::ChatService(SessionManager& session_manager)
    : session_manager_(session_manager) {}

bool ChatService::handle_line(Connection& conn,
                              const std::string& line,
                              const SendToFdFn& send_to_fd,
                              const BroadcastFn& broadcast) {
    Command cmd = parser_.parse(line, conn.logged_in());

    switch (cmd.type) {
        case CommandType::Empty: {
            return true;
        }

        case CommandType::Login: {
            std::string error;
            if (!session_manager_.register_username(conn.fd(), cmd.content, error)) {
                send_to_fd(conn.fd(), "[system] login failed: " + error + "\n");
                return true;
            }

            conn.set_username(cmd.content);
            conn.set_logged_in(true);

            std::string welcome;
            welcome += "[system] login success\n";
            welcome += "[system] commands: /list, /rename <newname>, /msg <username> <content>, /quit\n";
            send_to_fd(conn.fd(), welcome);

            std::string join_msg = "[system] " + conn.username() + " joined the chat\n";
            broadcast(join_msg, conn.fd(), false);
            return true;
        }

        case CommandType::List: {
            auto names = session_manager_.list_online_users();

            std::string result = "[system] online users (" + std::to_string(names.size()) + "): ";
            for (size_t i = 0; i < names.size(); ++i) {
                if (i > 0) {
                    result += ", ";
                }
                result += names[i];
            }
            result += "\n";

            send_to_fd(conn.fd(), result);
            return true;
        }

        case CommandType::Rename: {
            std::string oldname = conn.username();
            std::string error;

            if (!session_manager_.rename_username(oldname, conn.fd(), cmd.target, error)) {
                send_to_fd(conn.fd(), "[system] rename failed: " + error + "\n");
                return true;
            }

            conn.set_username(cmd.target);
            send_to_fd(conn.fd(), "[system] rename success\n");

            std::string msg = "[system] " + oldname + " changed name to " + cmd.target + "\n";
            broadcast(msg, conn.fd(), false);
            return true;
        }

        case CommandType::Quit: {
            send_to_fd(conn.fd(), "[system] bye\n");
            return false;
        }

        case CommandType::PrivateMsg: {
            if (cmd.target == conn.username()) {
                send_to_fd(conn.fd(), "[system] cannot send private message to yourself\n");
                return true;
            }

            int target_fd = -1;
            int worker_id = -1;
            (void)worker_id;  // 这里暂时不直接用，但查找时会顺便验证目标所在 worker 存在

            if (!session_manager_.find_user_fd_and_worker(cmd.target, target_fd, worker_id)) {
                send_to_fd(conn.fd(), "[system] user not found or disconnected\n");
                return true;
            }

            std::string to_target = "[private][" + conn.username() + "]: " + cmd.content + "\n";
            send_to_fd(target_fd, to_target);

            std::string to_self = "[private][to " + cmd.target + "]: " + cmd.content + "\n";
            send_to_fd(conn.fd(), to_self);
            return true;
        }

        case CommandType::PublicMsg: {
            std::string msg = "[" + conn.username() + "]: " + cmd.content + "\n";
            broadcast(msg, conn.fd(), true);
            return true;
        }

        case CommandType::Unknown:
        default: {
            if (line.compare(0, 5, "/msg ") == 0) {
                send_to_fd(conn.fd(), "[system] usage: /msg <username> <content>\n");
            } else {
                send_to_fd(conn.fd(), "[system] unknown command\n");
            }
            return true;
        }
    }
}

