#! /bin/sh

#./build.sh

docker run -it --rm --network bridge -v `pwd`/config.yaml:/app/config.yaml -v `pwd`/bot.py:/app/bot.py scb "${@}"
