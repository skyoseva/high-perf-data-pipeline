#!/bin/bash

echo "Building module in debug mode"

rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=DEBUG ..
cmake --build .
