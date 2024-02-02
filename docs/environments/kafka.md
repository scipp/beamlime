# Kafka

## Clients(Producer, Consumer and Admin)
We are using [``confluent-kafka``](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#)
since it is the most actively maintained python kafka api for now.

## Broker
You can use [``docker``](https://docs.docker.com/get-started/overview/) to start kafka server.

### Permission
The kafka service may fail due to the insufficient permission.

Try adding the super user into the `docker` group so that you can use `docker` command without `sudo`.

### Start Server
```bash
docker-compose up # add `-d` to start the service in the background.
```
Copy and paste the following [docker compose recipe](#docker-compose-recipe) and run this command in the same location.

### Docker-compose Recipe
```{admonition} Unusual Settings
:class: warning
This docker recipe contains options that adjust maximum size of the buffers/messages. It is for benchmark testing with large size data.
```

```yaml
version: '3.5'
services:

  zookeeper:
    container_name: zookeeper
    hostname: zookeeper
    image: confluentinc/cp-zookeeper:7.3.2
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - beamlime-test

  kafka:
    container_name: kafka-broker
    hostname: kafka-broker
    image: confluentinc/cp-server:7.3.2
    ports:
      - "9092:9092"
      - "9099:9099"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_MESSAGE_MAX_BYTES: 209715200  # ~200 MB
      KAFKA_SOCKET_REQUEST_MAX_BYTES: 209715200  # ~200 MB
      KAFKA_OPTS: "-XX:MaxDirectMemorySize=6G"  # Related to DirectMemoryError
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      ## listeners
      KAFKA_LISTENERS: "PLAINTEXT://:9092,PLAINTEXT_INTERNAL://:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://broker:29092"
    networks:
      - beamlime-test

networks:
  beamlime-test:

```
> References: [confluent](https://developer.confluent.io/quickstart/kafka-docker/)

### Topic Management
There is a simple topic managing tool in test helper package, ``tests/executables/kafka_topic_gui.py`` for **local testing**.

This tool shows all topics that do not start with ``'_'``,
which is a prefix for hidden topics.

It can only delete selected topics.

```bash
# With ``tests/helpers`` included in ``sys.path``.

python -m tests.executables.kafka_topic_gui
```

If you need more complicated tools, consider using other kafka ui tools such as [kafka topic analyzer](https://github.com/xenji/kafka-topic-analyzer) or [kafka ui](https://github.com/provectus/kafka-ui).
