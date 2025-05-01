# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import confluent_kafka as kafka

from .. import StreamKind
from ..config.config_loader import load_config
from ..config.models import ConfigKey, WorkflowSpecs
from ..config.streams import stream_kind_to_topic


@contextmanager
def make_producer(config: dict[str, Any]) -> Generator[kafka.Producer, None, None]:
    """Create a Kafka producer from a configuration dictionary."""
    producer = kafka.Producer(config)
    try:
        yield producer
    finally:
        producer.flush()


def publish_workflow_specs(
    *,
    instrument: str,
    workflow_specs: WorkflowSpecs,
    service_name: str,
    logger: logging.Logger | None = None,
) -> None:
    """
    Publish WorkflowSpecs to the BEAMLIME_CONFIG topic.

    Parameters
    ----------
    instrument
        The instrument identifier used to determine the correct topic
    workflow_specs
        The workflow specifications to publish
    service_name
        The service name to include in the config key
    logger
        Optional logger instance
    """
    logger = logger or logging.getLogger(__name__)
    kafka_config = load_config(namespace='kafka_downstream')

    topic = stream_kind_to_topic(instrument=instrument, kind=StreamKind.BEAMLIME_CONFIG)

    # Create a config key for the workflow specs
    config_key = ConfigKey(service_name=service_name, key="workflow_specs")

    key_bytes = str(config_key).encode('utf-8')
    value_bytes = json.dumps(workflow_specs.model_dump()).encode('utf-8')

    def delivery_callback(err, msg):
        if err:
            logger.error("Failed to publish workflow specs: %s", err)
        else:
            logger.info(
                "Successfully published workflow specs to %s at offset %s",
                msg.topic(),
                msg.offset(),
            )

    with make_producer(config=kafka_config) as producer:
        producer.produce(
            topic, key=key_bytes, value=value_bytes, callback=delivery_callback
        )
        logger.info("Publishing workflow specs to %s with key %s", topic, config_key)
