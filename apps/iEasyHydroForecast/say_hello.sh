#!/bin/sh -l

echo "Hello $1"
time=$(date)
echo "The current time is : $time"

# Then run a long-running process
exec tail -f /dev/null



