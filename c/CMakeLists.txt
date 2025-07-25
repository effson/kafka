cmake_minimum_required(VERSION 3.10)
project(kafkaConsumer)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rdkafka C++ 接口头文件路径
include_directories(
    ../src-cpp        # 包含 rdkafkacpp.h
    ../src            # 一些依赖的核心头文件在这里
)

# 链接的库路径（可根据实际编译位置调整）
link_directories(
    ../src-cpp        # 编译生成的 librdkafka++.so
    ../src            # 编译生成的 librdkafka.so
)

# 源文件
set(SOURCES
    KafkaConsumer.cpp
    main.cpp
)

# 编译可执行文件
add_executable(kafkaConsumer ${SOURCES})

# 链接静态或动态库（根据你是否编译了 .a 或 .so）
target_link_libraries(kafkaConsumer
    rdkafka++
    rdkafka
    pthread    # librdkafka依赖
)
