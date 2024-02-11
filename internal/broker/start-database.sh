docker stop postgres-broker
docker rm postgres-broker
rm -rf ../../data
mkdir ../../data
docker compose up -d --force-recreate
sleep 2