#!/usr/bin/env bash

git clone https://github.com/silviucpp/cpp-driver.git
git checkout 040cefabadab32391b840e95f3cde26bd80e8058
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make