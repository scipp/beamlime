# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import json
import pathlib
from collections.abc import Generator
from typing import NewType

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from beamlime import Factory, LogMixin, ProviderGroup
from beamlime.constructors.providers import merge as merge_providers
from beamlime.logging import BeamlimeLogger

from ..applications._nexus_helpers import (
    StreamModuleKey,
    StreamModuleValue,
)
from .options import build_arg_parser
from .prototypes import instantiate_from_args

KafkaConfig = NewType("KafkaConfig", dict)
StreamingModules = NewType("StreamingModules", dict[StreamModuleKey, StreamModuleValue])


def _mock_event_data_parent(path: str) -> dict:
    return {
        "name": pathlib.Path(path).name,
        "type": "group",
        "children": [],
        "attributes": [{"name": "NX_class", "values": "NXevent_data"}],
    }


_ADMIN_SHARED_CONFIG_KEYS = (
    "bootstrap.servers",
    "security.protocol",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
)


def _collect_all_topic_partitions(
    admin: AdminClient, topic: str
) -> list[TopicPartition]:
    """Retrieve the number of partitions for a given topic."""
    topic_metadata = admin.list_topics(topic=topic).topics[topic]
    return [
        TopicPartition(topic, partition)
        for partition in topic_metadata.partitions.keys()
    ]


class EventListener(LogMixin):
    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        streaming_modules: StreamingModules,
        kafka_config: KafkaConfig,
    ) -> None:
        self.logger = logger
        logger.info("Event topics: %s", {key.topic for key in streaming_modules.keys()})
        self.streaming_modules = streaming_modules
        admin_config = {key: kafka_config[key] for key in _ADMIN_SHARED_CONFIG_KEYS}
        self.admin = AdminClient(admin_config)
        try:
            logger.info("Connecting to Kafka server.")
            self.admin.list_topics(timeout=1)  # Check if the connection is successful.
        except Exception as e:
            err_msg = "Failed to connect to a Kafka server."
            logger.error(err_msg)
            raise RuntimeError(err_msg) from e

        self.logger.info("Retrieving the number of partitions for each topic.")
        self.topic_partitions = []
        for topic in {key.topic for key in streaming_modules.keys()}:
            self.topic_partitions += _collect_all_topic_partitions(self.admin, topic)
        self.logger.info("Collected partitions: %s", self.topic_partitions)

        self.consumer = Consumer(kafka_config)
        self.consumer.assign(self.topic_partitions)
        self.consumer.subscribe([key.topic for key in self.streaming_modules.keys()])

    def __del__(self) -> None:
        """Clean up the resources."""
        self.logger.info("Closing the Kafka consumer.")
        if hasattr(self, 'consumer') and self.consumer is not None:
            self.consumer.close()

    def run(self) -> Generator: ...

    @staticmethod
    def add_argument_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group('Event Listener Configuration')
        group.add_argument(
            "--config",
            help="Path to the json file that has kafka configuration.",
            type=str,
            required=True,
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "EventListener":
        json_file_path = pathlib.Path(args.config)
        config_dict = json.loads(json_file_path.read_text())
        streaming_modules_list = config_dict["streaming_modules"]
        streaming_modules = {
            StreamModuleKey(
                module_type='ev44', topic=item['topic'], source=item['source']
            ): StreamModuleValue(
                path=item['path'], parent=_mock_event_data_parent(item['path'])
            )
            for item in streaming_modules_list
        }

        return EventListener(
            logger=logger,
            streaming_modules=StreamingModules(streaming_modules),
            kafka_config=KafkaConfig(config_dict["kafka_config"]),
        )


def listener_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> EventListener:
    return instantiate_from_args(logger, args, EventListener)


def collect_show_detector_providers() -> ProviderGroup:
    from beamlime.logging.providers import log_providers

    app_providers = ProviderGroup(listener_from_args)

    return merge_providers(log_providers, app_providers)


def run_show_detector(factory: Factory, arg_name_space: argparse.Namespace) -> None:
    factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
    factory[BeamlimeLogger].info("Start showing detector hits.")
    with factory.constant_provider(argparse.Namespace, arg_name_space):
        factory[EventListener]


def main() -> None:
    """Entry point of the ``show-detector`` command."""
    factory = Factory(collect_show_detector_providers())
    arg_parser = build_arg_parser(EventListener)
    args = arg_parser.parse_args()
    run_show_detector(factory, args)
