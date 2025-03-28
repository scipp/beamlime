# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

import scipp as sc
from ess.reduce.streaming import StreamProcessor
from sciline.typing import Key

from ..core.handler import (
    Accumulator,
    Config,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import Message, MessageKey
from .accumulators import DetectorEvents, ToNXevent_data
from .to_nx_log import ToNXlog


class NullHandler(Handler[Any, None]):
    def handle(self, messages: list[Message[Any]]) -> list[Message[None]]:
        return []


class ReductionHandlerFactory(
    HandlerFactory[DetectorEvents, sc.DataGroup[sc.DataArray]]
):
    """
    Factory for data reduction handlers.
    """

    def __init__(
        self,
        *,
        instrument_config: ModuleType,
        logger: logging.Logger | None = None,
        config: Config,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._instrument_config = instrument_config
        self._processors = self._instrument_config.make_stream_processors()
        self._source_to_key = self._instrument_config.source_to_key
        self._attrs_registry = self._instrument_config.f144_attribute_registry

    def _is_nxlog(self, key: MessageKey) -> bool:
        return key.topic.split('_', maxsplit=1)[1] in ('motion',)

    def make_handler(
        self, key: MessageKey
    ) -> Handler[DetectorEvents, sc.DataGroup[sc.DataArray]]:
        self._logger.info("Creating handler for %s", key)
        wf_key = self._source_to_key.get(key.source_name)
        if wf_key is None:
            self._logger.warning(
                "No workflow key found for source name %s, using null handler",
                key.source_name,
            )
            return NullHandler(logger=self._logger, config=self._config)

        if is_context := self._is_nxlog(key):
            preprocessor = ToNXlog(attrs=self._attrs_registry[key.source_name])
        else:
            preprocessor = ToNXevent_data()
        self._logger.info(
            "Preprocessor %s is used for source name %s",
            preprocessor.__class__.__name__,
            key.source_name,
        )

        if (processor := self._processors.get(key.source_name)) is not None:
            accumulator = StreamProcessorProxy(processor, key=wf_key)
            self._logger.info(
                "Source name %s is mapped to input %s of stream processor %s",
                key.source_name,
                wf_key,
                key.source_name,
            )
        else:
            # Note the inefficiency here, of processing these sources in multiple
            # workflows. This is typically once per detector. If monitors are large this
            # can turn into a problem. At the same time, we want to keep flexible to
            # allow for
            #
            # 1. Different workflows for different detector banks, e.g., for diffraction
            #    and SANS detectors.
            # 2. Simple scaling, by processing different detectors on different nodes.
            #
            # Both could probably also be achieved with a non-duplicate processing of
            # monitors, but we keep it simple until proven to be necessary. Note that
            # an alternative would be to move some cost into the preprocessor, which
            # could, e.g., histogram large monitors to reduce the duplicate cost in the
            # stream processors.
            accumulator = MultiplexingProxy(
                list(self._processors.values()), key=wf_key, is_context=is_context
            )
            self._logger.info(
                "Source name %s is mapped to input %s in all stream processors",
                key.source_name,
                wf_key,
            )
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators={f'reduced/{key.source_name}': accumulator},
        )


class MultiplexingProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(
        self, stream_processors: list[StreamProcessor], key: Key, is_context: bool
    ) -> None:
        self._stream_processors = stream_processors
        self._key = key
        self._is_context = is_context

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        if self._is_context:
            for stream_processor in self._stream_processors:
                stream_processor.set_context({self._key: data})
        else:
            for stream_processor in self._stream_processors:
                stream_processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        return sc.DataGroup()

    def clear(self) -> None:
        # Clearing would be ok, but should be redundant since the stream processors are
        # cleared for each detector in the non-multiplexing proxies.
        pass


class StreamProcessorProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(self, processor: StreamProcessor, *, key: type) -> None:
        self._processor = processor
        self._key = key

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        self._processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        return sc.DataGroup(
            {str(key): val for key, val in self._processor.finalize().items()}
        )

    def clear(self) -> None:
        self._processor.clear()
