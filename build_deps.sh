#!/usr/bin/env bash

git clone https://github.com/datastax/cpp-driver
git checkout 40221db6be9103cc21f169e70b55e440000624e7
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make
