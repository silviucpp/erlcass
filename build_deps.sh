#!/usr/bin/env bash
OS=$(uname -s)
KERNEL=$(echo $(lsb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 | awk '{print $1;}') | awk '{print $1;}')

##echo $OS
##echo $KERNEL

case $OS in
  Linux)
     case $KERNEL in
       CentOS) 
          echo "Linux, CentOS"
          sudo yum install automake cmake gcc-c++ git libtool openssl-devel wget
          if echo "$(ldconfig -p | grep libuv)"
            then echo "libuv has already been installed"
          else
           pushd /tmp
           wget http://libuv.org/dist/v1.4.2/libuv-v1.4.2.tar.gz
           tar xzf libuv-v1.4.2.tar.gz
           pushd libuv-v1.4.2
           sh autogen.sh
           ./configure
           sudo make install
           popd
           popd
           sudo grep -q -F '/usr/local/lib' /etc/ld.so.conf.d/usrlocal.conf || echo '/usr/local/lib' | sudo tee --append /etc/ld.so.conf.d/usrlocal.conf > /dev/null
           fi          
          ;;
       Ubuntu)
         echo "Linux, Ubuntu"
         
         sudo apt-add-repository ppa:linuxjedi/ppa
         sudo apt-get update
         sudo apt-get install g++ make cmake libuv-dev libssl-dev
         ;;
       *) echo "Your system $KERNEL is not supported"
     esac
     ;;
  Darwin)
     sudo brew install libuv cmake
     ;;
  *) echo "Your system $OS is not supported"
esac


