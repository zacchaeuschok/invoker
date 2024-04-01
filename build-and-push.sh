mvn package

docker build -t ruohang/executor-sum:testv1.1 ./executor
docker build -t ruohang/iosidecar:testv1.1 ./iosidecar
docker build -t ruohang/statesidecar:testv1.1 ./statesidecar

docker push ruohang/executor-sum:testv1.1
docker push ruohang/iosidecar:testv1.1
docker push ruohang/statesidecar:testv1.1