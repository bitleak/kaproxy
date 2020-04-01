#!/bin/sh

DKCM="docker-compose -p kaproxy_test"

setup_script=`cat <<EOF
./bin/zkCli.sh create /proxy "";
./bin/zkCli.sh create /proxy/app "";
./bin/zkCli.sh create /proxy/app/acl "";
./bin/zkCli.sh create /proxy/app/acl/test '{"topics": ["test"], "groups": ["pull-test","push-test"]}';
./bin/zkCli.sh create /proxy/app/acl/admin '{"role": "admin"}';
./bin/zkCli.sh create /consumers "";
./bin/zkCli.sh create /consumers/pull-test '{"owner":"kaproxy","topics":["test"],"method":"pull","semantics":"atMostOnce"}';
EOF
`

$DKCM up -d
$DKCM exec -T zookeeper sh -c 'until nc -z 127.0.0.1 2181; do echo "zk is not ready"; sleep 1; done'
$DKCM exec -T zookeeper sh -c "$setup_script"
