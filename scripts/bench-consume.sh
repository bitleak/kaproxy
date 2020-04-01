#!/bin/sh

redis-benchmark -p 16379 -c 10 -n 1000 -a test brpop "{g1}-{test}" 1000
#redis-benchmark -p 16379 -c 100 -n 1000 -a test rpop "{g1}-{test}"
