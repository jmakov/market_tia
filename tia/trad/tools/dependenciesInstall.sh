#!/bin/bash


echo "Install the necessary tools"

sudo apt-get install make flex bison libtool libevent-dev automake pkg-config libssl-dev libboost-all-dev libbz2-dev build-essential g++ python-dev git

echo "git cloning Thrift"

git clone https://github.com/apache/thrift.git
pushd thrift
        git fetch
        git branch -a
        git checkout 0.8.x # latest version at this time
        # install jdk and ant if you need the java code generator
        sudo apt-get install openjdk-7-jdk ant
        ./bootstrap.sh #ignore warning
        ./configure
        make -j4
        sudo make install

        # at thrift directory
        pushd lib/py
                sudo python setup.py install
        popd


        #------------------------------------------
        # TESTING Thrift
        #------------------------------------------
        echo "Testing Thrift"
                pushd tutorial
                #in thrift/tutorial directory
                thrift -r -v --gen java tutorial.thrift
                echo "It willll generate a gen-java folder with sources if successful."
        popd




        #------------------------------------------
        # Installing fb303
        #------------------------------------------
        echo "Now installing fb303..."
        pushd contrib/fb303
                #in thrift/contrib/fb303 directory
                ./bootstrap.sh
                ./configure CPPFLAGS="-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H"
                make -j4
                sudo make install
                echo "Install Python Module for Thrift and fb303"

                pushd py
                        sudo python setup.py install
                        echo "To check that the python modules have been installed properly, run:"
                        python -c 'import thrift' ; python -c 'import fb303'
                popd
        popd


popd
#------------------------------------------
# End Thrift
#------------------------------------------

echo "Installing Scribe..."
git clone https://github.com/facebook/scribe.git
pushd scribe
        ./bootstrap.sh
        ./configure CPPFLAGS="-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DBOOST_FILESYSTEM_VERSION=2" LIBS="-lboost_system -lboost_filesystem"
        make -j4
        sudo make install
        echo "Export an environment variable"
        export LD_LIBRARY_PATH=/usr/local/lib
        echo "Install Python Module for Scribe"

        pushd lib/py
                sudo python setup.py install
                echo "poor-man's Checking that the python modules have been install properly"
                python -c 'import scribe'
        popd

        export LD_LIBRARY_PATH=/usr/local/lib
        echo "export LD_LIBRARY_PATH=/usr/local/lib" >> ~/.bashrc

popd



echo "PHEWWWWW..... finally"
echo "DONE. "


echo "TESTING THIS BB"
scribed
