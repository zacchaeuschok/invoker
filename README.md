# Basic deployment

## Dependencies

### Kafka
1. Setup Local cluster: Follow till step 2 of [Quickstart Guide](https://kafka.apache.org/quickstart). Zookeeper mode is fine.

2. Create input and output topic: Default input topic is "test". Default output topic is "output". Can be set in sidecar ENV as `INPUT_TOPIC`. 
```shell
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic output --bootstrap-server localhost:9092
```

3. Create cli producer for input topic
```shell
bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
# add flags --property "parse.key=true" --property "key.separator=:" to send key with key:value
```

4. Create consumer for output topic
```shell
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
```
### Docker

Install docker to run your Kubernetes cluster and build docker images if you haven't.
https://docs.docker.com/get-docker/

### Creating Local Kubernetes cluster (Kind)
Install kind. https://kind.sigs.k8s.io/docs/user/quick-start/

To start a kubernetes cluster. 
1. Find a tag that says v1.29 from https://hub.docker.com/r/kindest/node/tags and pull the docker image from it with the command provided. 
2. Then create the cluster with the image tag as the image flag.
```shell
kind create cluster --image=kindest/node:v1.29.2

# Verify kubernetes version
kubectl version
# Should see: Server Version: v1.29.2
```

## Building the application (Kind)

Kind exposes localhost of your machine same way as docker: using `host.docker.internal`. In Minikube, you have to locate the IP of your host machine by using `host.minikube.internal` or finding the host ip with `ifconfig`.
```shell
mvn package
# Ensure that each module has a target folder with 1.0-SNAPSHOT.jar

docker build ./executor -t executor-word-count:testv1
docker build ./iosidecar -t iosidecar:testv1
docker build ./statesidecar -t statesidecar:testv1
docker build ./producersidecar -t producersidecar:testv1

kind load docker-image executor-word-count:testv1 executor-sum:testv1 iosidecar:testv1 statesidecar:testv1 producersidecar:testv1```

## Running pod on cluster

```shell
kubectl apply -f pod-sample.yaml

# To monitor status:
kubectl get pods
# To check the logs for each container:
# CONTAINER name corresponds to name under initContainers in yaml
# You can check the System.out.println() for sidecars this way 
# Remove the -c flag to just see the main container
kubectl logs pod/executor -c ${CONTAINER} 
```

Afterwards, you should see output in your consumer after writing messages in the producer. Note: When restarting consumer, consumer group rebalancing may take a bit, best to check via the balancing messages shown by the broker.