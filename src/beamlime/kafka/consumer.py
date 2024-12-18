# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Any

import confluent_kafka as kafka
from confluent_kafka.error import KafkaException


def validate_topics_exist(consumer: kafka.Consumer, topics: list[str]) -> None:
    """Check if all topics exist and are accessible."""
    try:
        cluster_metadata = consumer.list_topics(timeout=5.0)
        available_topics = cluster_metadata.topics
        missing_topics = [topic for topic in topics if topic not in available_topics]
        if missing_topics:
            raise ValueError(f"Topics not found: {missing_topics}")
    except KafkaException as e:
        raise ValueError(f"Failed to fetch topic metadata: {e}") from e


def assign_partitions(consumer: kafka.Consumer, topic: str) -> None:
    """Assign all partitions of a topic to a consumer."""
    try:
        partitions = consumer.list_topics(topic).topics[topic].partitions
        if not partitions:
            raise ValueError(f"Topic '{topic}' exists but has no partitions")
        consumer.assign([kafka.TopicPartition(topic, p) for p in partitions])
    except KafkaException as e:
        raise ValueError(f"Failed to assign partitions for topic '{topic}': {e}") from e


def make_bare_consumer(topics: list[str], config: dict[str, Any]) -> kafka.Consumer:
    """Create a bare confluent_kafka.Consumer that can be used by KafkaMessageSource."""
    consumer = kafka.Consumer(config)
    validate_topics_exist(consumer, topics)

    try:
        consumer.subscribe(topics)
    except KafkaException as e:
        raise ValueError(f"Failed to subscribe to topics: {e}") from e

    for topic in topics:
        assign_partitions(consumer, topic)

    return consumer
