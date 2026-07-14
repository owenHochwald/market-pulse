cmake_minimum_required(VERSION 3.20)

project(market_pulse VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

option(MARKET_PULSE_NATIVE "Build for the current CPU" OFF)
option(MARKET_PULSE_IPO "Enable interprocedural optimization" OFF)
set(MARKET_PULSE_PGO "OFF" CACHE STRING "PGO mode: OFF, GENERATE, or USE")
set(MARKET_PULSE_PGO_PROFILE "${CMAKE_BINARY_DIR}/market-pulse.profdata" CACHE FILEPATH "Merged PGO profile")
set_property(CACHE MARKET_PULSE_PGO PROPERTY STRINGS OFF GENERATE USE)
set(MARKET_PULSE_SANITIZER "OFF" CACHE STRING "Sanitizer: OFF, address, undefined, or thread")
set_property(CACHE MARKET_PULSE_SANITIZER PROPERTY STRINGS OFF address undefined thread)

if(NOT MARKET_PULSE_SANITIZER STREQUAL "OFF")
    if(NOT MARKET_PULSE_SANITIZER MATCHES "^(address|undefined|thread)$")
        message(FATAL_ERROR "Unsupported sanitizer: ${MARKET_PULSE_SANITIZER}")
    endif()
    add_compile_options(-fsanitize=${MARKET_PULSE_SANITIZER} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${MARKET_PULSE_SANITIZER})
endif()

if(MARKET_PULSE_PGO STREQUAL "GENERATE")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "The PGO presets currently require Clang")
    endif()
    add_compile_options(-fprofile-instr-generate=${CMAKE_BINARY_DIR}/market-pulse-%p.profraw)
    add_link_options(-fprofile-instr-generate=${CMAKE_BINARY_DIR}/market-pulse-%p.profraw)
elseif(MARKET_PULSE_PGO STREQUAL "USE")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "The PGO presets currently require Clang")
    endif()
    if(NOT EXISTS "${MARKET_PULSE_PGO_PROFILE}")
        message(FATAL_ERROR "PGO profile not found: ${MARKET_PULSE_PGO_PROFILE}")
    endif()
    add_compile_options(-fprofile-instr-use=${MARKET_PULSE_PGO_PROFILE})
    add_link_options(-fprofile-instr-use=${MARKET_PULSE_PGO_PROFILE})
elseif(NOT MARKET_PULSE_PGO STREQUAL "OFF")
    message(FATAL_ERROR "MARKET_PULSE_PGO must be OFF, GENERATE, or USE")
endif()

add_library(market_pulse_lib
    src/platform.cpp
    src/simulation.cpp
)
target_include_directories(market_pulse_lib PUBLIC include)
target_link_libraries(market_pulse_lib PUBLIC Threads::Threads)
target_compile_options(market_pulse_lib PRIVATE -Wall -Wextra -Wpedantic)

if(MARKET_PULSE_NATIVE)
    target_compile_options(market_pulse_lib PUBLIC -march=native)
endif()

if(MARKET_PULSE_IPO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT MARKET_PULSE_IPO_SUPPORTED OUTPUT MARKET_PULSE_IPO_ERROR)
    if(NOT MARKET_PULSE_IPO_SUPPORTED)
        message(FATAL_ERROR "IPO is not supported: ${MARKET_PULSE_IPO_ERROR}")
    endif()
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    set_property(TARGET market_pulse_lib PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

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
set_tests_properties(market_pulse_tests PROPERTIES TIMEOUT 30)
add_test(NAME market_pulse_cli_help COMMAND market-pulse --help)
add_test(NAME market_pulse_cli_benchmark
         COMMAND market-pulse benchmark --symbols 8 --events 10000 --producers 2 --warmup 1000 --repeats 1)
set_tests_properties(market_pulse_cli_benchmark PROPERTIES TIMEOUT 10)
add_test(NAME market_pulse_cli_rejects_short_cpu_list
         COMMAND market-pulse benchmark --symbols 8 --events 100 --producers 2 --cpus 0)
set_tests_properties(market_pulse_cli_rejects_short_cpu_list PROPERTIES WILL_FAIL TRUE TIMEOUT 10)
