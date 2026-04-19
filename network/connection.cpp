#include "network/connection.h"

Connection::Connection(int fd, std::string ip, int port, int worker_id)
    : fd_(fd), ip_(std::move(ip)), port_(port), worker_id_(worker_id) {}

int Connection::fd() const {
    return fd_;
}

const std::string& Connection::username() const {
    return username_;
}

void Connection::set_username(const std::string& username) {
    username_ = username;
}

const std::string& Connection::ip() const {
    return ip_;
}

int Connection::port() const {
    return port_;
}

int Connection::worker_id() const {
    return worker_id_;
}

bool Connection::logged_in() const {
    return logged_in_;
}

void Connection::set_logged_in(bool value) {
    logged_in_ = value;
}

bool Connection::closing() const {
    return closing_;
}

void Connection::set_closing(bool value) {
    closing_ = value;
}

std::string& Connection::in_buffer() {
    return inbuf_;
}

std::string& Connection::out_buffer() {
    return outbuf_;
}

