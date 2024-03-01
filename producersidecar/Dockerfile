FROM openjdk:8-jre
RUN apt-get update && \
    apt-get install -y \
    dnsutils \
    iputils-ping
WORKDIR /
COPY target/statesidecar-1.0-SNAPSHOT.jar /
CMD ["java", "-jar","statesidecar-1.0-SNAPSHOT.jar"]