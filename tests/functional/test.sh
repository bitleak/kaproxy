#!/bin/sh

getAddress() {
    platform=`uname`
    if [ $platform == "Linux" ]; then
        ifconfig | awk '/inet addr/{print substr($2,6)}' | awk '$0!="127.0.0.1" && count==0 {print $0;count++}'
    elif [ $platform == "Darwin" ]; then
        ifconfig | awk '/inet /{print $2}' | awk '$0!="127.0.0.1" && count==0 {print $0;count++}'
    fi
}

address=`getAddress`
sed -e "s/__IP_ADDRESS__/$address/"  \
-e "s/kafka/127.0.0.1/" \
-e "s/zookeeper/127.0.0.1/" \
test.tmpl > test.toml

go test -v
rm -rf log
rm pidFile
rm test.toml
