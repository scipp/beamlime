#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <BEAMLIME_INSTRUMENT>"
  exit 1
fi

BEAMLIME_INSTRUMENT=$1

kafka-topics --bootstrap-server kafka:29092 --delete --topic '.*' || true
kafka-leader-election --bootstrap-server kafka:29092 --election-type PREFERRED --all-topic-partitions

kafka-topics --create --bootstrap-server kafka:29092 \
  --topic ${BEAMLIME_INSTRUMENT}_detector \
  --config cleanup.policy=delete \
  --config delete.retention.ms=60000 \
  --config max.message.bytes=104857600 \
  --config retention.bytes=10737418240 \
  --config retention.ms=30000 \
  --config segment.bytes=104857600 \
  --config segment.ms=60000

kafka-topics --create --bootstrap-server kafka:29092 \
  --topic ${BEAMLIME_INSTRUMENT}_beam_monitor \
  --config cleanup.policy=delete \
  --config delete.retention.ms=60000 \
  --config max.message.bytes=104857600 \
  --config retention.bytes=10737418240 \
  --config retention.ms=30000 \
  --config segment.bytes=104857600 \
  --config segment.ms=60000

kafka-topics --create --bootstrap-server kafka:29092 \
  --topic ${BEAMLIME_INSTRUMENT}_beamlime_commands \
  --config cleanup.policy=compact \
  --config retention.ms=-1 \
  --config min.compaction.lag.ms=86400000 \
  --config min.cleanable.dirty.ratio=0.5 \
  --config delete.retention.ms=86400000 \
  --config max.message.bytes=1048576 \
  --config retention.bytes=-1

kafka-topics --create --bootstrap-server kafka:29092 \
  --topic ${BEAMLIME_INSTRUMENT}_beamlime_data \
  --config cleanup.policy=delete \
  --config delete.retention.ms=60000 \
  --config max.message.bytes=104857600 \
  --config retention.bytes=1073741824 \
  --config retention.ms=30000 \
  --config segment.bytes=104857600 \
  --config segment.ms=60000
