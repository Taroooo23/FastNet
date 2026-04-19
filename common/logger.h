#pragma once

#include <mutex>
#include <string>

enum class LogLevel {
    Debug,
    Info,
    Warn,
    Error
};

// 单例模式
// 一个很轻量的线程安全日志器。
// 目前只打印到 stdout / stderr，后面你可以很容易改成写文件。
class Logger {
public:
    static Logger& instance();

    void log(LogLevel level, const std::string& message);

private:
    Logger() = default;
    std::mutex mutex_;
};

// 为了调用更方便，提供几个简单宏
#define LOG_DEBUG(msg) Logger::instance().log(LogLevel::Debug, (msg))
#define LOG_INFO(msg)  Logger::instance().log(LogLevel::Info,  (msg))
#define LOG_WARN(msg)  Logger::instance().log(LogLevel::Warn,  (msg))
#define LOG_ERROR(msg) Logger::instance().log(LogLevel::Error, (msg))

