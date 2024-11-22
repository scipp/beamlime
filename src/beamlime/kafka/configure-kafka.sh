#!/bin/bash

docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --delete --topic '.*'
docker exec kafka-broker kafka-leader-election --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions

docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic beamlime-control \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=100

docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic monitor-counts \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config retention.ms=30000 \
  --config segment.bytes=10485760 \
  --config min.compaction.lag.ms=1000 \
  --config delete.retention.ms=100 \
  --config max.message.bytes=32768 \

docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic detector-counts \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000 \
  --config retention.ms=300000 \
  --config segment.bytes=536870912 \
  --config min.compaction.lag.ms=5000 \
  --config delete.retention.ms=100 \
  --config max.message.bytes=17825792