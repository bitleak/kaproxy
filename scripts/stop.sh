#!/bin/bash
/www/kaproxy/scripts/503.sh && sleep 3 && supervisorctl stop kaproxy
