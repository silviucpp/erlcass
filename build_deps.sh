#!/usr/bin/env bash

git clone https://github.com/silviucpp/cpp-driver.git
git checkout ae56f5e4cd08feefc50383345d00b2dd604cec75
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make