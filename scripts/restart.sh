#!/bin/bash
/www/kaproxy/scripts/503.sh && supervisorctl restart kaproxy && sleep 3 && /www/kaproxy/scripts/200.sh
