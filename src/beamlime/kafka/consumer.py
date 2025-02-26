# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import confluent_kafka as kafka
from confluent_kafka.error import KafkaException

from ..config.config_loader import load_config
from .helpers import topic_for_instrument


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


@contextmanager
def make_bare_consumer(
    topics: list[str], config: dict[str, Any]
) -> Generator[kafka.Consumer, None, None]:
    """Create a bare confluent_kafka.Consumer that can be used by KafkaMessageSource."""
    consumer = kafka.Consumer(config)
    try:
        validate_topics_exist(consumer, topics)
        consumer.subscribe(topics)
        for topic in topics:
            assign_partitions(consumer, topic)
        yield consumer
    finally:
        consumer.close()


@contextmanager
def make_consumer_from_config(
    *,
    topics: list[str],
    config: dict[str, Any],
    instrument: str,
    group: str,
    unique_group_id: bool = True,
) -> Generator[kafka.Consumer, None, None]:
    """Create a Kafka consumer from a configuration dictionary."""
    if unique_group_id:
        config['group.id'] = f'{instrument}_{group}_{uuid.uuid4()}'
    with make_bare_consumer(
        config=config,
        topics=topic_for_instrument(topic=topics, instrument=instrument),
    ) as consumer:
        yield consumer


@contextmanager
def make_control_consumer(*, instrument: str) -> Generator[kafka.Consumer, None, None]:
    control_config = load_config(namespace='control_consumer', env='')
    kafka_downstream_config = load_config(namespace='kafka_downstream')
    with make_consumer_from_config(
        topics=['beamlime_commands'],
        config={**control_config, **kafka_downstream_config},
        instrument=instrument,
        group='beamlime_commands',
    ) as consumer:
        yield consumer
