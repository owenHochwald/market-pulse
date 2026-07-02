cmake_minimum_required(VERSION 3.20)

project(market_pulse VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(market_pulse_lib
    src/simulation.cpp
)
target_include_directories(market_pulse_lib PUBLIC include)
target_compile_options(market_pulse_lib PRIVATE -Wall -Wextra -Wpedantic)

add_executable(market-pulse src/main.cpp)
target_link_libraries(market-pulse PRIVATE market_pulse_lib)
target_compile_options(market-pulse PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()

add_executable(market_pulse_tests
    tests/test_main.cpp
)
target_link_libraries(market_pulse_tests PRIVATE market_pulse_lib)
target_compile_options(market_pulse_tests PRIVATE -Wall -Wextra -Wpedantic)

add_test(NAME market_pulse_tests COMMAND market_pulse_tests)
