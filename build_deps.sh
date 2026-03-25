#!/usr/bin/env bash

DEPS_LOCATION=_build/deps
CPP_DRIVER_REPO=https://github.com/apache/cassandra-cpp-driver.git
CPP_DRIVER_BRANCH=trunk
CPP_DRIVER_REV=f49a413409a5af3c60b73ac6d7d89a97cb368d46   #v2.7.0
CPP_DRIVER_DESTINATION=cpp-driver

if [ -f "$DEPS_LOCATION/${CPP_DRIVER_DESTINATION}/build/libcassandra_static.a" ]; then
    echo "cpp-driver fork already exist. delete $DEPS_LOCATION/${CPP_DRIVER_DESTINATION} for a fresh checkout."
    exit 0
fi

CPUS=`getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu`
OS=$(uname -s)
if [ -x "$(command -v lsb_release)" ]; then
    KERNEL=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
elif [ -f /etc/os-release ]; then
    . /etc/os-release
    KERNEL=$(echo $ID)
else
    KERNEL=unknown
fi

# Enable ccache based on ERLCASS_USE_CCACHE environment variable

CCACHE_ENABLED=0
if [ "$ERLCASS_USE_CCACHE" = "1" ]; then
    if command -v ccache &> /dev/null; then
        echo "ccache is enabled and will be used."
        CCACHE_ENABLED=1
    else
        echo "ccache is not installed. Proceeding without ccache..."
    fi
fi

fail_check()
{
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
}

case $OS in
    Linux)
        case $KERNEL in
            centos)
                echo "Linux, CentOS"
                sudo yum -y install automake cmake gcc-c++ git libtool openssl-devel wget
                OUTPUT=`ldconfig -p | grep libuv`
                if [[ $(echo $OUTPUT) != "" ]]
                    then echo "libuv has already been installed"
                else
                    pushd /tmp
                    wget https://dist.libuv.org/dist/v1.18.0/libuv-v1.18.0.tar.gz
                    tar xzf libuv-v1.18.0.tar.gz
                    pushd libuv-v1.18.0
                    sh autogen.sh
                    ./configure
                    sudo make install
                    popd
                    popd

                    sudo grep -q -F '/usr/local/lib' /etc/ld.so.conf.d/usrlocal.conf || echo '/usr/local/lib' | sudo tee --append /etc/ld.so.conf.d/usrlocal.conf > /dev/null
                    sudo ldconfig -v
                fi
            ;;

            ubuntu)
                echo "Linux, Ubuntu"
                # check ubuntu version
                UBUNTU_VSN=$(lsb_release -sr)
                if [[ $UBUNTU_VSN == "14.04" ]]; then
                    # system is Ubuntu 14.04, need to install PPA and possibly override libuv
                    sudo add-apt-repository ppa:acooks/libwebsockets6
                    LIBUDEV_PACKAGE_NAME=libuv1.dev
                else
                    LIBUDEV_PACKAGE_NAME=libuv1-dev
                fi
                sudo apt-get -y update
                sudo apt-get -y install g++ make cmake libssl-dev $LIBUDEV_PACKAGE_NAME
            ;;
            *) echo "Your system $KERNEL is not supported"
        esac
        export CFLAGS="-fPIC -Wno-class-memaccess"
        export CXXFLAGS="-fPIC -Wno-class-memaccess"
    ;;

    Darwin)
        export HOMEBREW_NO_INSTALL_UPGRADE=true
        export HOMEBREW_NO_INSTALL_CLEANUP=true
        export HOMEBREW_NO_AUTO_UPDATE=1
        brew install libuv cmake openssl
        export OPENSSL_ROOT_DIR=$(brew --prefix openssl)
        export OPENSSL_INCLUDE_DIR=$OPENSSL_ROOT_DIR/include/
        export OPENSSL_LIBRARIES=$OPENSSL_ROOT_DIR/lib
        ;;

    *) echo "Your system $OS is not supported"
esac

mkdir -p $DEPS_LOCATION

#checkout repo

pushd $DEPS_LOCATION

if [ ! -d "${CPP_DRIVER_DESTINATION}" ]; then
    fail_check git clone --filter=blob:none -b ${CPP_DRIVER_BRANCH} ${CPP_DRIVER_REPO} ${CPP_DRIVER_DESTINATION}
fi

pushd ${CPP_DRIVER_DESTINATION}
fail_check git checkout ${CPP_DRIVER_REV}
popd
popd

#build

CCACHE_FLAGS=""
if [ "$CCACHE_ENABLED" = "1" ]; then
    CCACHE_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
fi

mkdir -p $DEPS_LOCATION/${CPP_DRIVER_DESTINATION}/build
pushd $DEPS_LOCATION/${CPP_DRIVER_DESTINATION}/build
fail_check cmake .. -DCASS_BUILD_STATIC=ON -DCMAKE_BUILD_TYPE=RELEASE $CCACHE_FLAGS
fail_check make -j $CPUS
popd
