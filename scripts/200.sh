#!/bin/bash

platform=`uname`
if [ $platform == "Darwin" ]; then
host=`ifconfig |grep "inet "|grep -v "127.0.0.1"|awk '{print $2}'|head -1`
elif [ $platform == "Linux" ]; then
host=`ifconfig |grep inet|grep -E '192\.168|10\.'|awk -F: '{print $2}'|awk '{print $1}'|head -1`
else
host="127.0.0.1"
fi

out=`curl -X POST -i -s -o /dev/null -w %{http_code} \
    "http://$host:8080/devops/status?status=enable" \
    2>/dev/null`
if [ $out -eq 200 ]; then
    echo "Set 200 success"
    exit 0
else
    echo "Set 200 failed"
    exit 1
fi
