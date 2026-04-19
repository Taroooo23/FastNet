#include "logic/session_manager.h"

#include <algorithm>

bool SessionManager::is_valid_username(const std::string& username, std::string& error) const {
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

    return true;
}

void SessionManager::bind_fd_worker(int fd, int worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    fd_to_worker_[fd] = worker_id;
}

bool SessionManager::register_username(int fd, const std::string& username, std::string& error) {
    if (!is_valid_username(username, error)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (username_to_fd_.find(username) != username_to_fd_.end()) {
        error = "username already exists";
        return false;
    }

    username_to_fd_[username] = fd;
    logged_in_fds_.insert(fd);
    return true;
}

bool SessionManager::rename_username(const std::string& oldname,
                                     int fd,
                                     const std::string& newname,
                                     std::string& error) {
    if (!is_valid_username(newname, error)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

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

void SessionManager::remove_logged_in_user(const std::string& username, int fd) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!username.empty()) {
        username_to_fd_.erase(username);
    }

    logged_in_fds_.erase(fd);
    fd_to_worker_.erase(fd);
}

void SessionManager::remove_fd_mapping(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    fd_to_worker_.erase(fd);
    logged_in_fds_.erase(fd);
}

void SessionManager::force_remove_fd_mapping(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);

    fd_to_worker_.erase(fd);
    logged_in_fds_.erase(fd);

    for (auto it = username_to_fd_.begin(); it != username_to_fd_.end();) {
        if (it->second == fd) {
            it = username_to_fd_.erase(it);
        } else {
            ++it;
        }
    }
}

bool SessionManager::find_user_fd_and_worker(const std::string& username,
                                             int& target_fd,
                                             int& worker_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

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

bool SessionManager::get_worker_id_by_fd(int fd, int& worker_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = fd_to_worker_.find(fd);
    if (it == fd_to_worker_.end()) {
        return false;
    }

    worker_id = it->second;
    return true;
}

std::vector<std::string> SessionManager::list_online_users() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> names;
    names.reserve(username_to_fd_.size());

    for (const auto& pair : username_to_fd_) {
        names.push_back(pair.first);
    }

    std::sort(names.begin(), names.end());
    return names;
}

std::unordered_map<int, std::vector<int>> SessionManager::collect_online_fds_grouped_by_worker(
    int exclude_fd,
    bool include_sender) const {
    std::unordered_map<int, std::vector<int>> worker_to_fds;

    std::lock_guard<std::mutex> lock(mutex_);

    for (int fd : logged_in_fds_) {
        if (!include_sender && fd == exclude_fd) {
            continue;
        }

        auto it = fd_to_worker_.find(fd);
        if (it == fd_to_worker_.end()) {
            continue;
        }

        worker_to_fds[it->second].push_back(fd);
    }

    return worker_to_fds;
}

