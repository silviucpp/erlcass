#!/usr/bin/env bash

DEPS_LOCATION=_build/deps

if [ -d "$DEPS_LOCATION" ]; then
    echo "cpp-driver fork already exist. delete $DEPS_LOCATION for a fresh checkout."
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

#echo $OS
#echo $KERNEL

CPP_DRIVER_REPO=https://github.com/datastax/cpp-driver.git
CPP_DRIVER_REV=$1

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
git clone -b ${CPP_DRIVER_REV} --depth=1 ${CPP_DRIVER_REPO}
popd

#build

mkdir -p $DEPS_LOCATION/cpp-driver/build
pushd $DEPS_LOCATION/cpp-driver/build
cmake .. -DCASS_BUILD_STATIC=ON -DCMAKE_BUILD_TYPE=RELEASE
make -j $CPUS
popd
