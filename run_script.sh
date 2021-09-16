#!/bin/sh
sleep 5
if [ "server" = $1 ]; then
    exec /usr/share/myst/bin/myst-server >> stdout.out 2> stderr.out
fi
if [ "segment-gen" = $1 ]; then
    exec /usr/share/myst/bin/myst-segment-gen >> stdout.out 2> stderr.out
fi
