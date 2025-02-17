# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from sciline.typing import Key

import scipp as sc
from ess.reduce.streaming import StreamProcessor

from ..config import models
from ..config.raw_detectors import get_detector_config
from ..core.handler import (
    Accumulator,
    Config,
    ConfigModelAccessor,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import MessageKey
from .accumulators import (
    DetectorEvents,
    ToNXevent_data,
    GroupIntoPixels,
    NullAccumulator,
    ROIBasedTOAHistogram,
)
from ..kafka.helpers import beam_monitor_topic, detector_topic
from .monitor_data_handler import MonitorDataPreprocessor


# Idea:
# We need to handle monitors and detectors with separate source names. The need to be
# passed into the same StreamProcessor (or rather one per detector, multiplixing the
# monitors). We thus need several handlers that set the data, but one leader that
# performs the actual reduction. The other handlers should generally not return any
# messages.
# What if we get monitors before detectors?

# We probably want to avoid duplicate processing of monitors. We can either use a single
# Scline workflow for all detector banks, or somehow extract the monitor workflow.
# Both is awkward, since it forces some synchronization.


# Alternatively, we could return the same handler *instance* for all keys, but how do we
# avoid duplicate `get` calls for each message?
# Or is PeriodicAccumulatingHandler simply not the right abstraction in this case?
# Let us check, what does it do:
# 1. preprocess all messages.
# 2. preprocess.get(). This could perform merging of ev44.
# 3. accumulate preprocessed. <-> StreamProcessor.accumulate
# 4. IF UPDATE: accumulator.get() and return results <-> StreamProcessor.finalize

# backwards need many preprocessors, one accumulator, one finalizer.
# current is opposite: one preprocessor, many accumulators, many finalizers.

# ... but we can have multiple handlers, giving us multiple preprocessors.
# The accumulators across all handlers should feed into the same underlying
# StreamProcessor.accumulate. The question is how to trigger this.
# Also, StreamProcessor is not really supporting mapped keys, do we need to run one
# wf per detector? Can we extract the monitor workflow?
#
# Let us assume StreamProcessor was more powerful, allowing for more independent updates
# of keys.
#
# 1. Monitor handlers: We want to run the part of the workflow that does not depend on
#    any detector (dynamic or static).
# 2. Detectors: We would like to insert monitor wf results into each detector wf.

# Note scipp/essreduce#191


class ReductionHandlerFactory(
    HandlerFactory[DetectorEvents, sc.DataGroup[sc.DataArray]]
):
    """
    Factory for detector data handlers.

    Handlers are created based on the instrument name in the message key which should
    identify the detector name. Depending on the configured detector views a NeXus file
    with geometry information may be required to setup the view. Currently the NeXus
    files are always obtained via Pooch. Note that this may need to be refactored to a
    more dynamic approach in the future.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        processors: dict[Key, StreamProcessor],
        source_to_key: dict[str, Key],
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        # TODO need a copy for every detector! muliplex monitor data into each!
        # copying stream processor not trivial accumulators need to be able to be copied
        # actually we may need different workflow for each detector (say diffraction vs
        # sans, different mask files, ...), so automatic mechanism may be futile anyway.
        self._processors = processors
        self._source_to_key = source_to_key

    def _is_monitor(self, key: MessageKey) -> bool:
        return key.topic == beam_monitor_topic(self._instrument)

    def _is_detector(self, key: MessageKey) -> bool:
        return key.topic == detector_topic(self._instrument)

    def _workflow_key(self, key: MessageKey) -> str:
        return self._source_to_key[key.source_name]

    def make_handler(
        self, key: MessageKey
    ) -> Handler[DetectorEvents, sc.DataGroup[sc.DataArray]]:
        self._logger.info(f"Creating handler for {key}")
        wf_key = self._workflow_key(key)
        if (processor := self._processors.get(key.source_name)) is not None:
            accumulator = StreamProcessorProxy(processor, key=wf_key)
            self._logger.info(f"Using existing processor for {wf_key}")
        else:
            accumulator = MultiplexingProxy(list(self._processors.values()), key=wf_key)
            self._logger.info(f"Creating multiplexing processor for {wf_key}")
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=ToNXevent_data(),
            accumulators={'reduced': accumulator},
        )


class MultiplexingProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(self, stream_processors: list[StreamProcessor], key: Key) -> None:
        self._stream_processors = stream_processors
        self._key = key

    def append(self, stream_processor: StreamProcessor) -> None:
        self._stream_processors.append(stream_processor)

    def accumulate(self, data: sc.DataArray) -> None:
        for stream_processor in self._stream_processors:
            stream_processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        return sc.DataGroup()


class StreamProcessorProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(self, processor: StreamProcessor, *, key: type) -> None:
        self._processor = processor
        self._key = key

    def accumulate(self, data: sc.DataArray) -> None:
        self._processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        return self._processor.finalize()
