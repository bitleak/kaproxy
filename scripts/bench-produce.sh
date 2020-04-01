#!/bin/sh

wrk -c 1 -t 1 -d 3s -s bench-produce-http.lua http://localhost:8080/topic/test
