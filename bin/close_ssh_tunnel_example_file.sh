#!/bin/bash

PORT="1234"  # Change this to the port you want to close
lsof -ti:$PORT | xargs kill