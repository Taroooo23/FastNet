#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SessionManager 专门负责“在线会话状态”。
// 这层不做 socket 收发，只做用户、fd、worker 的映射管理。
class SessionManager {
public:
    void bind_fd_worker(int fd, int worker_id);

    bool register_username(int fd, const std::string& username, std::string& error);

    bool rename_username(const std::string& oldname,
                         int fd,
                         const std::string& newname,
                         std::string& error);

    void remove_logged_in_user(const std::string& username, int fd);

    void remove_fd_mapping(int fd);

    void force_remove_fd_mapping(int fd);

    bool find_user_fd_and_worker(const std::string& username,
                                 int& target_fd,
                                 int& worker_id) const;

    bool get_worker_id_by_fd(int fd, int& worker_id) const;

    std::vector<std::string> list_online_users() const;

    std::unordered_map<int, std::vector<int>> collect_online_fds_grouped_by_worker(
        int exclude_fd,
        bool include_sender) const;

private:
    bool is_valid_username(const std::string& username, std::string& error) const;

private:
    mutable std::mutex mutex_;

    // username -> fd
    std::unordered_map<std::string, int> username_to_fd_;

    // fd -> worker_id
    std::unordered_map<int, int> fd_to_worker_;

    // 已登录的 fd
    std::unordered_set<int> logged_in_fds_;
};

