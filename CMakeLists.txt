cmake_minimum_required(VERSION 3.14)
project(fast_processor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

find_package(pybind11 CONFIG REQUIRED HINTS "${pybind11_DIR}")

pybind11_add_module(fast_processor src/cpp/DataProcessor.cpp src/cpp/bindings.cpp)

set_target_properties(fast_processor PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
