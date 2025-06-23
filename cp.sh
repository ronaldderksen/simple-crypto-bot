for c in $(docker ps --format json |jq -r .Names |grep scb)
do
  #db=$(docker exec ${c} bash -c "ls /app/*db")
  #docker cp "${c}:${db}" $(basename $db)
  docker cp ${c}:/app/bot.py /tmp/bot.py
  if ! diff -q bot.py /tmp/bot.py; then
    docker cp bot.py ${c}:/app/bot.py
    docker restart ${c}
  fi
done
