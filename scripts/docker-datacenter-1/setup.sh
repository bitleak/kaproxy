#!/bin/sh

docker-compose -p kaproxy up -d
until nc -z 127.0.0.1 2181; do echo "zk is not ready"; sleep 1; done
zkCli create /proxy ""
zkCli create /proxy/app ""
zkCli create /proxy/app/acl ""
zkCli create /proxy/app/acl/test '{"topics": ["test"], "groups": ["pull-test","push-test"]}'
zkCli create /proxy/app/acl/admin '{"role": "admin"}'
zkCli create /consumers ""
zkCli create /consumers/pull-test '{"owner":"kaproxy","topics":["test"],"method":"pull","semantics":"atMostOnce"}'
