#!/bin/sh

docker_script_dir="`pwd`/scripts/docker-datacenter-1"
functional_test_dir="`pwd`/tests/functional"

cd $docker_script_dir && sh setup.sh
cd $functional_test_dir && sh test.sh 
cd $docker_script_dir && sh teardown.sh
