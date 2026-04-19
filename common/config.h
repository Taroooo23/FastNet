#pragma once

namespace config {

// 服务端监听端口
constexpr int kPort = 8888;

// 单次读缓冲大小
constexpr int kBufferSize = 4096;

// epoll_wait 每次最多取多少事件
constexpr int kMaxEvents = 64;

// worker reactor 数量
constexpr int kWorkerCount = 4;

// listen backlog
constexpr int kListenBacklog = 65535;

// 主 reactor epoll_wait 超时，便于优雅退出
constexpr int kMainReactorWaitMs = 1000;

}  // namespace config

