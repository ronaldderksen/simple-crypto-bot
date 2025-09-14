docker tag scb registry.derksen-it.nl/scp:latest
docker push registry.derksen-it.nl/scp:latest

for file in $(ls -d /gluster/scb/*/bot.py)
do
  echo "+ cp bot.py ${file}"
  cp bot.py ${file}
done
