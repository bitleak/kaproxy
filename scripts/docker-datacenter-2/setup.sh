#!/bin/sh

ZK="127.0.0.1:2182"

docker-compose -p kaproxy2 up -d
while [ `nc -z 127.0.0.1 2182` ]; do echo "zk is not ready"; sleep 1; done
zkCli -server $ZK create /proxy ""
zkCli -server $ZK create /proxy/app ""
zkCli -server $ZK create /proxy/app/acl ""
zkCli -server $ZK create /proxy/app/acl/test '{"topics": ["test"], "groups": ["pull-test","push-test"]}'
zkCli -server $ZK create /proxy/app/acl/admin '{"role": "admin"}'
zkCli -server $ZK create /consumers/pull-test '{"owner":"kaproxy","topics":["test"],"method":"pull","semantics":"atMostOnce"}'
