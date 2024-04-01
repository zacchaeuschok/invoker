# Basic deployment

## Dependencies

### Kafka
1. Get Kafka. Step 1 of [Quickstart Guide](https://kafka.apache.org/quickstart).
2. Start Zookeeper
```shell
bin/zookeeper-server-start.sh config/zookeeper.properties
```
3. Change `config/server.properties` field `advertised.listeners` to your network public ip
```el
advertised.listeners=PLAINTEXT://YOUR_PUBLIC_IP:9092
```
4. Start Kafka Broker
```shell
bin/kafka-server-start.sh config/server.properties
```
5. Create input and output topic: Default input topic is "test". Default output topic is "output". Can be set in sidecar ENV as `INPUT_TOPIC`. 
```shell
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic output --bootstrap-server localhost:9092
```

6. Create cli producer for input topic
```shell
bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
# add flags --property "parse.key=true" --property "key.separator=:" to send key with key:value
```

7. Create consumer for output topic
```shell
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic output  --formatter kafka.tools.DefaultMessageFormatter --property print.key=true  --property print.value=true
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

### Creating Local Kubernetes cluster (Microk8s)
Follow the microk8s guide. No need to enable anything.
https://microk8s.io/#install-microk8s

### Creating Local Kubernetes cluster (Minikube)
Follow the minikube guide.
https://minikube.sigs.k8s.io/docs/start/

## Building the application
Building the docker application requires set up of Maven and docker. Afterwards, the docker image is abled to be pushed to dockerhub using the following script.
The script may need to be modified to change the version or the repository.
```shell
./build-and-push.sh
```

## Running the application

### Finding the localhost ip for Kafka broker
To connect the kubernetes pod to Kafka, the `kafka.broker` address in the config.yaml must be setup correctly.
This section details what you need to do if your Kafka broker is running on `localhost:9092`.
If Kafka is already setup in another IP reachable from the K8S cluster, replacing `kafka.broker` value will be enough.

Kind exposes localhost of your machine same way as docker: using `host.docker.internal`.

In Minikube and Microk8s the process is more complicated.
#### On MacOS
Use ifconfig to find the localhost ip for your host machine.
```shell
ifconfig
```
Under `en0`, the ipv4 address next to the `inet` row should be usable.
#### On Linux
//TODO
#### On Windows
//TODO

After finding the IP, replace the value field of `kafka.broker`. 

### Running the pod

```shell
# Applying kubernetes configmap and pod
kubectl apply -f sample.yaml

# To monitor status:
kubectl get pods
# To check the logs for each container:
# CONTAINER name corresponds to name under initContainers in yaml
# You can check the System.out.println() for sidecars this way 
# Remove the -c flag to just see the main container
kubectl logs pod/executor -c ${CONTAINER} 
# To check if the input sidecar is setup correctly and the kafka broker is reachable.
kubectl logs pod/executor -c input-sidecar
# If everything is setup correctly main container logs should say Connection Established
```

Afterwards, you should see output in your consumer after writing messages in the producer. Note: When restarting consumer, consumer group rebalancing may take a second, best to check via the balancing messages shown by the broker.