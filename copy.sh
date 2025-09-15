for file in $(ssh strato3 ls -d /gluster/scb/*/bot.py)
do
  echo "+ scp bot.py strato3:${file}"
  scp bot.py strato3:${file}
done
