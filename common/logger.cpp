#include "common/logger.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

// 匿名命名空间，让其中函数只在当前文件内部可见
namespace {

const char* level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
        default:              return "UNKWN";
    }
}

std::string now_string() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now); // t是1970-01-01 00:00:00 UTC 到现在的秒数

    std::tm tm_buf {};
#if defined(_WIN32)
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf); // 把时间写到tm_buf里
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S"); // 把时间写到oss里
    return oss.str();
}

}  // namespace

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

void Logger::log(LogLevel level, const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ostream& os = (level == LogLevel::Error) ? std::cerr : std::cout;
    os << "[" << now_string() << "]"
       << "[" << level_to_string(level) << "]"
       << "[tid=" << std::this_thread::get_id() << "] "
       << message << "\n";
}

