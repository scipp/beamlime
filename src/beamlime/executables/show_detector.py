# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import json
import pathlib
from collections.abc import AsyncGenerator
from typing import NewType

from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, TopicPartition
from confluent_kafka.admin import AdminClient
from streaming_data_types.eventdata_ev44 import EventData, deserialise_ev44

from .. import Factory, ProviderGroup
from ..applications._nexus_helpers import (
    StreamModuleKey,
    StreamModuleValue,
)
from ..applications.base import (
    Application,
    DaemonInterface,
    MessageProtocol,
    MessageRouter,
)
from ..applications.daemons import (
    DataPiece,
    DataPieceReceived,
    FakeListener,
)
from ..applications.handlers import PlotSaver, RawCountHandler, WorkflowResultUpdate
from ..constructors import SingletonProvider
from ..constructors.providers import merge as merge_providers
from ..logging import BeamlimeLogger
from .options import build_minimum_arg_parser
from .prototypes import fake_listener_from_args, instantiate_from_args

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
        TopicPartition(topic, partition, OFFSET_BEGINNING)
        for partition in topic_metadata.partitions.keys()
    ]


def _wrap_event_msg_to_data_piece(topic: str, deserialized: EventData) -> DataPiece:
    key = StreamModuleKey(
        module_type='ev44', topic=topic, source=deserialized['source_name']
    )
    return DataPiece(key=key, deserialized=deserialized)


def _is_event_msg_valid(msg: Message) -> bool:
    return (
        msg is not None
        and msg.error() is None
        and (msg.value()[4:8].decode() == "ev44")
    )


class EventListener(DaemonInterface):
    def __init__(
        self,
        *,
        logger: BeamlimeLogger,
        streaming_modules: StreamingModules,
        kafka_config: KafkaConfig,
    ) -> None:
        self.logger = logger
        self.info("Event topics: %s", {key.topic for key in streaming_modules.keys()})
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

        self.debug("Retrieving the number of partitions for each topic.")
        self.topic_partitions = []
        for topic in {key.topic for key in streaming_modules.keys()}:
            self.topic_partitions += _collect_all_topic_partitions(self.admin, topic)
        self.debug("Collected partitions: %s", self.topic_partitions)

        self.consumer = Consumer(kafka_config)
        self.consumer.assign(self.topic_partitions)

    def __del__(self) -> None:
        """Clean up the resources."""
        self.info("Closing the Kafka consumer.")
        if hasattr(self, 'consumer') and self.consumer is not None:
            self.consumer.close()

    async def run(self) -> AsyncGenerator[MessageProtocol | None, None]:
        while True:
            msg = self.consumer.poll(0.5)
            if _is_event_msg_valid(msg):
                deserialized = deserialise_ev44(msg.value())._asdict()
                self.debug("%s", deserialized)
                yield DataPieceReceived(
                    content=_wrap_event_msg_to_data_piece(msg.topic(), deserialized)
                )
            elif msg is not None:
                self.error("Unexpected message: %s", msg.value().decode())
            yield None

    @staticmethod
    def add_argument_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group('Event Listener Configuration')
        group.add_argument(
            "--config",
            help="Path to the json file that has kafka configuration.",
            type=str,
        )

    @classmethod
    def from_args(
        cls, logger: BeamlimeLogger, args: argparse.Namespace
    ) -> "EventListener":
        if args.config is None:
            # This option is not set as a required argument in the `add_argument_group`
            # method, because it is only required if fake listener is not used.
            raise ValueError(
                "The path to the config file is not provided."
                "Use --config option to set it."
            )

        json_file_path = pathlib.Path(args.config)
        config_dict = json.loads(json_file_path.read_text())
        streaming_modules = streaming_modules_from_config(config_dict)
        return EventListener(
            logger=logger,
            streaming_modules=StreamingModules(streaming_modules),
            kafka_config=KafkaConfig(config_dict["kafka_config"]),
        )


def streaming_modules_from_config(config_dict: dict) -> StreamingModules:
    streaming_modules_list = config_dict["streaming_modules"]
    return StreamingModules(
        {
            StreamModuleKey(
                module_type='ev44', topic=item['topic'], source=item['source']
            ): StreamModuleValue(
                path=item['path'], parent=_mock_event_data_parent(item['path'])
            )
            for item in streaming_modules_list
        }
    )


def listener_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> EventListener:
    return instantiate_from_args(logger, args, EventListener)


def raw_detector_counter_from_args(
    logger: BeamlimeLogger, args: argparse.Namespace
) -> RawCountHandler:
    return instantiate_from_args(logger, args, RawCountHandler)


def plot_saver_from_args(logger: BeamlimeLogger, args: argparse.Namespace) -> PlotSaver:
    return instantiate_from_args(logger, args, PlotSaver)


def collect_show_detector_providers() -> ProviderGroup:
    from ..logging.providers import log_providers

    app_providers = ProviderGroup(
        listener_from_args,
        raw_detector_counter_from_args,
        fake_listener_from_args,
        SingletonProvider(plot_saver_from_args),
        SingletonProvider(ShowDetectorApp),
        MessageRouter,
    )

    return merge_providers(log_providers, app_providers)


class ShowDetectorApp(Application):
    def run(self) -> None:
        try:
            super().run()
        except KeyboardInterrupt:
            self.info("Received a keyboard interrupt. Exiting...")
            self.message_router.message_pipe.put_nowait(Application.Stop(content=None))


def run_show_detector(factory: Factory, arg_name_space: argparse.Namespace) -> None:
    factory[BeamlimeLogger].setLevel(arg_name_space.log_level.upper())
    factory[BeamlimeLogger].info("Start showing detector hits.")
    with factory.constant_provider(argparse.Namespace, arg_name_space):
        if arg_name_space.fake_listener:
            event_listener = factory[FakeListener]
        else:
            event_listener = factory[EventListener]

        raw_detector_counter = factory[RawCountHandler]
        plot_saver = factory[PlotSaver]
        app = factory[ShowDetectorApp]

    app.register_daemon(event_listener)
    app.register_handling_method(DataPieceReceived, raw_detector_counter.handle)
    app.register_handling_method(WorkflowResultUpdate, plot_saver.save_histogram)
    app.run()


def main() -> None:
    """Entry point of the ``show-detector`` command."""
    factory = Factory(collect_show_detector_providers())
    arg_parser = build_minimum_arg_parser(
        EventListener, PlotSaver, RawCountHandler, FakeListener
    )
    arg_parser.add_argument(
        "--fake-listener",
        action="store_true",
        help="Use fake listener instead of real listener.",
        default=False,
    )
    args = arg_parser.parse_args()
    run_show_detector(factory, args)
