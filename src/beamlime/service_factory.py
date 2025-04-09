# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from contextlib import ExitStack
from typing import Any, Generic, NoReturn, TypeVar

from .config import config_names
from .config.config_loader import load_config
from .core import MessageSink, StreamProcessor
from .core.handler import HandlerFactory, HandlerRegistry
from .core.message import MessageSource, StreamId
from .core.service import Service
from .kafka import KafkaTopic
from .kafka import consumer as kafka_consumer
from .kafka.message_adapter import AdaptingMessageSource, MessageAdapter
from .kafka.sink import KafkaSink, UnrollingSinkAdapter
from .kafka.source import KafkaConsumer, KafkaMessageSource, MultiConsumer
from .sinks import PlotToPngSink

Traw = TypeVar("Traw")
Tin = TypeVar("Tin")
Tout = TypeVar("Tout")


class DataServiceBuilder(Generic[Traw, Tin, Tout]):
    def __init__(
        self,
        *,
        instrument: str,
        name: str,
        log_level: int = logging.INFO,
        adapter: MessageAdapter | None = None,
        handler_factory: HandlerFactory[Tin, Tout],
    ):
        self._name = f'{instrument}_{name}'
        self._log_level = log_level
        self._topics: list[KafkaTopic] | None = None
        self._instrument = instrument
        self._adapter = adapter
        self._handler_registry = HandlerRegistry(factory=handler_factory)

    @property
    def instrument(self) -> str:
        """Returns the instrument name."""
        return self._instrument

    def add_handler(self, key: StreamId, handler: HandlerFactory[Tin, Tout]) -> None:
        """Add specific handler to use for given key, instead of using the factory."""
        self._handler_registry.register_handler(key=key, handler=handler)

    def from_consumer_config(
        self,
        kafka_config: dict[str, Any],
        sink: MessageSink[Tout],
        raise_on_adapter_error: bool = False,
    ) -> Service:
        """Create a service from a consumer config."""
        resources = ExitStack()
        try:
            config_topic, config_consumer = resources.enter_context(
                kafka_consumer.make_control_consumer(instrument=self._instrument)
            )
            topics = self._adapter.topics
            data_topics = [topic for topic in topics if topic != config_topic]
            data_consumer = resources.enter_context(
                kafka_consumer.make_consumer_from_config(
                    topics=data_topics, config=kafka_config, group=self._name
                )
            )
            consumer = MultiConsumer([config_consumer, data_consumer])

            # Ownership of resource stack transferred to the service
            return self.from_source(
                source=KafkaMessageSource(consumer=consumer),
                sink=sink,
                resources=resources.pop_all(),
                raise_on_adapter_error=raise_on_adapter_error,
            )
        except Exception:
            resources.close()
            raise

    def from_consumer(
        self,
        consumer: KafkaConsumer,
        sink: MessageSink[Tout],
        resources: ExitStack | None = None,
        raise_on_adapter_error: bool = False,
    ) -> Service:
        return self.from_source(
            source=KafkaMessageSource(consumer=consumer),
            sink=sink,
            resources=resources,
            raise_on_adapter_error=raise_on_adapter_error,
        )

    def from_source(
        self,
        source: MessageSource,
        sink: MessageSink[Tout],
        resources: ExitStack | None = None,
        raise_on_adapter_error: bool = False,
    ) -> Service:
        processor = StreamProcessor(
            source=source
            if self._adapter is None
            else AdaptingMessageSource(
                source=source,
                adapter=self._adapter,
                raise_on_error=raise_on_adapter_error,
            ),
            sink=sink,
            handler_registry=self._handler_registry,
        )
        return Service(
            processor=processor,
            name=self._name,
            log_level=self._log_level,
            resources=resources,
        )


class DataServiceRunner:
    def __init__(
        self,
        *,
        pretty_name: str,
        make_builder: Callable[..., DataServiceBuilder],
    ) -> None:
        self._make_builder = make_builder
        self._parser = Service.setup_arg_parser(description=f'{pretty_name} Service')
        self._parser.add_argument(
            '--sink-type',
            choices=['kafka', 'png'],
            default='kafka',
            help='Select sink type: kafka or png',
        )

    @property
    def parser(self) -> argparse.ArgumentParser:
        """
        Returns the argument parser.

        Use this to add extra arguments the `make_builder` function needs.
        """
        return self._parser

    def run(
        self,
    ) -> NoReturn:
        args = vars(self._parser.parse_args())
        consumer_config = load_config(namespace=config_names.raw_data_consumer, env='')
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        kafka_upstream_config = load_config(namespace=config_names.kafka_upstream)

        sink_type = args.pop('sink_type')
        builder = self._make_builder(**args)

        if sink_type == 'kafka':
            sink = KafkaSink(
                instrument=builder.instrument, kafka_config=kafka_downstream_config
            )
        else:
            sink = PlotToPngSink()
        sink = UnrollingSinkAdapter(sink)

        with builder.from_consumer_config(
            kafka_config={**consumer_config, **kafka_upstream_config}, sink=sink
        ) as service:
            service.start()
