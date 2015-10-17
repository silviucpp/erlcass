#!/usr/bin/env bash
OS=$(uname -s)
KERNEL=$(echo $(lsb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 | awk '{print $1;}') | awk '{print $1;}')

##echo $OS
##echo $KERNEL

CPP_DRIVER_REPO=$1
CPP_DRIVER_REV=$2

case $OS in
    Linux)
        case $KERNEL in
            CentOS)

                echo "Linux, CentOS"
                sudo yum -y install automake cmake gcc-c++ git libtool openssl-devel wget
                OUTPUT=`ldconfig -p | grep libuv`
                if [[ $(echo $OUTPUT) != "" ]]
                    then echo "libuv has already been installed"
                else
                    pushd /tmp
                    wget http://dist.libuv.org/dist/v1.4.2/libuv-v1.4.2.tar.gz
                    tar xzf libuv-v1.4.2.tar.gz
                    pushd libuv-v1.4.2
                    sh autogen.sh
                    ./configure
                    sudo make install
                    popd
                    popd

                    sudo grep -q -F '/usr/local/lib' /etc/ld.so.conf.d/usrlocal.conf || echo '/usr/local/lib' | sudo tee --append /etc/ld.so.conf.d/usrlocal.conf > /dev/null
                    sudo ldconfig -v

                fi
            ;;

            Ubuntu)

                echo "Linux, Ubuntu"

                sudo apt-add-repository -y ppa:linuxjedi/ppa
                sudo apt-get -y update
                sudo apt-get -y install g++ make cmake libuv-dev libssl-dev
            ;;

            *) echo "Your system $KERNEL is not supported"
        esac
    ;;

    Darwin)
        brew install libuv cmake
        ;;

    *) echo "Your system $OS is not supported"
esac

mkdir -p deps

#checkout repo

pushd deps
git clone ${CPP_DRIVER_REPO}
pushd cpp-driver
git checkout ${CPP_DRIVER_REV}
popd
popd

#build

mkdir -p deps/cpp-driver/build
pushd deps/cpp-driver/build
cmake ..
make
popd
