mvn package

docker build -t zacchaeus2000/executor-sum:testv1.1 ./executor
docker build -t zacchaeus2000/iosidecar:testv1.1 ./iosidecar
docker build -t zacchaeus2000/statesidecar:testv1.1 ./statesidecar

docker push zacchaeus2000/executor-sum:testv1.1
docker push zacchaeus2000/iosidecar:testv1.1
docker push zacchaeus2000/statesidecar:testv1.1