#!/usr/bin/env bash

git clone https://github.com/datastax/cpp-driver.git
git checkout 1dc1ee5378961717264e0f708e86604da5d03e50
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make