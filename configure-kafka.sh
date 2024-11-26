#!/bin/bash

# Some cleanup to deal with message schema changes during development, etc.
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --delete --topic '.*'
docker exec kafka-broker kafka-leader-election --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions

# Create beamlime.control topic - for long-lived configuration data
docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic beamlime.control \
  --config cleanup.policy=compact \
  --config retention.ms=-1 \
  --config min.compaction.lag.ms=86400000 \
  --config min.cleanable.dirty.ratio=0.5 \
  --config delete.retention.ms=86400000 \
  --config max.message.bytes=1048576 \
  --config retention.bytes=-1

# Create beamlime.monitor.counts topic - for monitor count data
docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic beamlime.monitor.counts \
  --config cleanup.policy=delete \
  --config delete.retention.ms=60000 \
  --config max.message.bytes=32768 \
  --config retention.bytes=1073741824 \
  --config retention.ms=30000 \
  --config segment.bytes=10485760 \
  --config segment.ms=60000

# Create beamlime.detector.counts topic - for detector image data
docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
  --topic beamlime.detector.counts \
  --config cleanup.policy=delete \
  --config delete.retention.ms=60000 \
  --config max.message.bytes=104857600 \
  --config retention.bytes=10737418240 \
  --config retention.ms=30000 \
  --config segment.bytes=104857600 \
  --config segment.ms=60000

#docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
#  --topic beamlime-control \
#  --config cleanup.policy=compact \
#  --config min.cleanable.dirty.ratio=0.01 \
#  --config segment.ms=100 \
#  --config min.compaction.lag.ms=0 \
#
#docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
#  --topic monitor-counts \
#  --config cleanup.policy=compact \
#  --config min.cleanable.dirty.ratio=0.01 \
#  --config segment.ms=1000 \
#  --config retention.ms=30000 \
#  --config segment.bytes=10485760 \
#  --config min.compaction.lag.ms=0 \
#  --config delete.retention.ms=100 \
#  --config max.message.bytes=32768

#docker exec kafka-broker kafka-topics --create --bootstrap-server localhost:9092 \
#  --topic detector-counts \
#  --config cleanup.policy=compact \
#  --config min.cleanable.dirty.ratio=0.01 \
#  --config segment.ms=10000 \
#  --config retention.ms=300000 \
#  --config segment.bytes=536870912 \
#  --config min.compaction.lag.ms=5000 \
#  --config delete.retention.ms=100 \
#  --config max.message.bytes=17825792