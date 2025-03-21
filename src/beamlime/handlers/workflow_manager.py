# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Callable, Iterator, MutableMapping, Sequence
from dataclasses import dataclass

import sciline
import scipp as sc
from ess.reduce.streaming import StreamProcessor
from sciline.typing import Key

from ..core.handler import Accumulator


# ... so we can recreate the StreamProcessor if workflow parameters change.
@dataclass
class DynamicWorkflow:
    workflow: sciline.Pipeline
    dynamic_keys: tuple[Key, ...]
    target_keys: tuple[Key, ...]
    accumulators: dict[Key, Accumulator | Callable[..., Accumulator]] | tuple[Key, ...]
    context_keys: tuple[Key, ...] = ()

    def make_stream_processor(self) -> StreamProcessor:
        """Create a StreamProcessor from the workflow."""
        # TODO Need to copy accumulators if they are instances!
        return StreamProcessor(
            self.workflow,
            dynamic_keys=self.dynamic_keys,
            context_keys=self.context_keys,
            target_keys=self.target_keys,
            accumulators=self.accumulators,
        )


# Should have list of possible workflows, config which to use while running, for given
# bank?

# 1. message -> create handler with proxy -> proxy without processor
# 2. set workflow parameters -> create new processor -> update proxy
# 2.b If we do this via Kafka, it can auto-recover after restart?

# Logic problems if some keys are context in some but dynamic in others.
# Disallow that?
# make sets of dynamic keys and context keys, and check that they are disjoint.


class ProcessorRegistry(MutableMapping[str, StreamProcessor]):
    def __init__(self) -> None:
        self._processors: dict[str, StreamProcessor] = {}

    def __getitem__(self, key: str) -> StreamProcessor:
        if key not in self._processors:
            raise KeyError(f"Processor {key} not found")
        return self._processors[key]

    def __setitem__(self, key: str, value: StreamProcessor) -> None:
        self._processors[key] = value

    def __delitem__(self, key: str) -> None:
        if key not in self._processors:
            raise KeyError(f"Processor {key} not found")
        del self._processors[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._processors)

    def __len__(self) -> int:
        return len(self._processors)


class WorkflowManager:
    def __init__(
        self, *, workflow_names: Sequence[str], source_to_key: dict[str, Key]
    ) -> None:
        # Why this init? We want the service to keep running, but be able to configure
        # workflows after startup.
        self._source_to_key = source_to_key
        self._workflows: dict[str, DynamicWorkflow | None] = {
            name: None for name in workflow_names
        }
        self._processors = ProcessorRegistry()
        self._proxies: dict[str, StreamProcessorProxy] = {}
        for name in workflow_names:
            self.set_worklow(name, None)

    def set_worklow(self, name: str, workflow: DynamicWorkflow | None) -> None:
        """
        Add a workflow to the manager.

        Parameters
        ----------
        name:
            The name to identify the workflow.
        workflow:
            The workflow to add.
        """
        if name not in self._workflows:
            raise ValueError(f"Workflow {name} was not defined in the manager.")
        self._workflows[name] = workflow
        if workflow is None:
            self._processors.pop(name, None)
        else:
            self._processors[name] = workflow.make_stream_processor()
        if (proxy := self._proxies.get(name)) is not None:
            proxy.set_processor(self._processors.get(name))

    def get_accumulator(
        self, source_name: str
    ) -> MultiplexingProxy | StreamProcessorProxy | None:
        wf_key = self._source_to_key.get(source_name)
        if wf_key is None:
            return None
        if source_name in self._workflows:
            # Note that the processor may be 'None' at this point.
            proxy = StreamProcessorProxy(self._processors.get(source_name), key=wf_key)
            self._proxies[source_name] = proxy
            return proxy
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
            return MultiplexingProxy(self._processors, key=wf_key)


class MultiplexingProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(self, stream_processors: ProcessorRegistry, key: Key) -> None:
        self._stream_processors = stream_processors
        self._key = key

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._key})"

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        for stream_processor in self._stream_processors.values():
            if self._key in stream_processor._context_keys:
                stream_processor.set_context({self._key: data})
            else:
                stream_processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        return sc.DataGroup()

    def clear(self) -> None:
        # Clearing would be ok, but should be redundant since the stream processors are
        # cleared for each detector in the non-multiplexing proxies.
        pass


class StreamProcessorProxy(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    def __init__(self, processor: StreamProcessor | None = None, *, key: type) -> None:
        self._processor = processor
        self._key = key

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._key})"

    def set_processor(self, processor: StreamProcessor | None) -> None:
        """Set the processor to use for this proxy."""
        self._processor = processor

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        if self._processor is not None:
            self._processor.accumulate({self._key: data})

    def get(self) -> sc.DataGroup[sc.DataArray]:
        if self._processor is None:
            return sc.DataGroup()
        return sc.DataGroup(
            {str(key): val for key, val in self._processor.finalize().items()}
        )

    def clear(self) -> None:
        if self._processor is not None:
            self._processor.clear()
