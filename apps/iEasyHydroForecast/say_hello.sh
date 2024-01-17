#!/bin/sh -l
echo "Now let us see if tag latest is working"
echo "Hello $1! Greetings from watchtower!"
time=$(date)
echo "The current time is : $time"

# Then run a long-running process
exec tail -f /dev/null



