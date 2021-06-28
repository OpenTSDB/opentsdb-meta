#!/bin/sh
sleep 60
ROCKET_ENV=development RUST_BACKTRACE=1 /usr/bin/myst >> /var/log/myst/myst.log 2>&1 
