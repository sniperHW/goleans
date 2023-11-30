#! /bin/sh

ps -ef | grep goleans | grep -v "grep"| awk '{print $2}' | xargs kill -9