# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Any

import confluent_kafka as kafka


def assign_partitions(consumer, topic):
    """Assign all partitions of a topic to a consumer."""
    partitions = consumer.list_topics(topic).topics[topic].partitions
    consumer.assign([kafka.TopicPartition(topic, p) for p in partitions])


kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'beamlime-monitor-data',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'fetch.min.bytes': 1,
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 2000,
}


def make_bare_consumer(topics: list[str], config: dict[str, Any]) -> kafka.Consumer:
    """Create a bare confluent_kafka.Consumer that can be used by KafkaMessageSource."""
    consumer = kafka.Consumer(config=config)
    consumer.subscribe(topics)
    for topic in topics:
        assign_partitions(consumer, topic)
    return consumer
