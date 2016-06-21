#!/bin/bash
function cleanup()
{
        local pids=`jobs -p`
        if [[ "$pids" != "" ]]; then
                kill $pids >/dev/null 2>/dev/null
        fi
}

trap cleanup EXIT
/log-sink >> /var/log/go_log/log-sink.out 2>&1