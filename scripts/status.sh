#!/bin/bash
supervisorctl status kaproxy
status=`curl -s http://127.0.0.1:8080/devops/status`
echo "Current HTTP Status: "${status}
