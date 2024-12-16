# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import Any

import confluent_kafka as kafka
import scipp as sc
from streaming_data_types import dataarray_da00

from ..core.message import Message, MessageSink
from .scipp_da00_compat import scipp_to_da00


class KafkaSink(MessageSink[sc.DataArray]):
    def __init__(
        self, *, logger: logging.Logger | None = None, kafka_config: dict[str, Any]
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._producer = kafka.Producer(kafka_config)

    def publish_messages(self, messages: Message[sc.DataArray]) -> None:
        def delivery_callback(err, msg):
            if err is not None:
                self._logger.error(
                    "Failed to deliver message to %s: %s", msg.topic(), err
                )

        for msg in messages:
            topic = msg.key.topic
            try:
                da00 = dataarray_da00.serialise_da00(
                    source_name=msg.key.source_name,
                    timestamp_ns=time.time_ns(),
                    data=scipp_to_da00(msg.value),
                )
            except (ValueError, TypeError) as e:
                self._logger.error("Failed to serialize message: %s", e)
                continue
            try:
                self._producer.produce(
                    topic=topic, value=da00, callback=delivery_callback
                )
                self._producer.poll(0)
            except kafka.KafkaException as e:
                self._logger.error("Failed to publish message to %s: %s", topic, e)

        try:
            self._producer.flush(timeout=3)
        except kafka.KafkaException as e:
            self._logger.error("Error flushing producer: %s", e)
