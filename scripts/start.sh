#!/bin/bash
supervisorctl start kaproxy && sleep 3 && /www/kaproxy/scripts/200.sh
