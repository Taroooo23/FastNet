cmake_minimum_required(VERSION 3.10)
project(chat_server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(chat_server
    main.cpp
    common/logger.cpp
    logic/session_manager.cpp
    logic/command_parser.cpp
    logic/chat_service.cpp
    network/connection.cpp
    reactor/main_reactor.cpp
    reactor/worker_reactor.cpp
)

target_include_directories(chat_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(chat_server PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -O2
)

find_package(Threads REQUIRED)
target_link_libraries(chat_server PRIVATE Threads::Threads)

