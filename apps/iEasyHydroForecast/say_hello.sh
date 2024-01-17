#!/bin/sh -l
echo "Let us see if watchtower montioring works"
echo "Hello $1! Greetings from watchtower!"
time=$(date)
echo "The current time is : $time"

# Then run a long-running process
exec tail -f /dev/null



