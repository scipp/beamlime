# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

import scipp as sc
from ess.reduce.streaming import StreamProcessor
from sciline.typing import Key

from ..core.handler import Accumulator


class WorkflowManager:
    def __init__(
        self,
        *,
        instrument_config: ModuleType,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._instrument_config = instrument_config
        self._source_to_key = instrument_config.source_to_key
        self._processors = instrument_config.make_stream_processors()
        self._attrs_registry = instrument_config.f144_attribute_registry
        self._context_keys = instrument_config.context_keys

    def attrs_for_f144(self, source_name: str) -> dict[str, Any] | None:
        """
        Get the attributes for a given source name.

        Parameters
        ----------
        source_name:
            The source name to get the attributes for.
        Returns
        -------
        :
            The attributes for the given source name, or None if not found.
        """
        return self._attrs_registry.get(source_name)

    def get_proxy_for_key(
        self, source_name: str
    ) -> MultiplexingProxy | StreamProcessorProxy | None:
        wf_key = self._source_to_key.get(source_name)
        if wf_key is None:
            return None
        is_context = wf_key in self._context_keys
        if (processor := self._processors.get(source_name)) is not None:
            self._logger.info(
                "Source name %s is mapped to input %s of stream processor %s",
                source_name,
                wf_key,
                source_name,
            )
            return StreamProcessorProxy(processor, key=wf_key)
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
            self._logger.info(
                "Source name %s is mapped to input %s in all stream processors",
                source_name,
                wf_key,
            )
            return MultiplexingProxy(
                list(self._processors.values()), key=wf_key, is_context=is_context
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
