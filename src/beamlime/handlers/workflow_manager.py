# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import inspect
from collections.abc import Callable, Iterator, MutableMapping, Sequence
from functools import wraps
from typing import Any

import scipp as sc
from ess.reduce.streaming import StreamProcessor
from sciline.typing import Key

from ..config.models import WorkflowControl
from ..core.handler import Accumulator


class StreamProcessorFactory:
    def __init__(self) -> None:
        self._factories: dict[str, Callable[[], StreamProcessor]] = {}

    def get_available(self) -> tuple[str, ...]:
        """Return a tuple of available factory names."""
        return tuple(self._factories.keys())

    def register(
        self, name: str
    ) -> Callable[[Callable[[], StreamProcessor]], Callable[[], StreamProcessor]]:
        """
        Decorator to register a factory function for creating StreamProcessors.

        Parameters
        ----------
        name:
            Name to register the factory under.

        Returns
        -------
        Decorator function that registers the factory and returns it unchanged.
        """

        def decorator(
            factory: Callable[[], StreamProcessor],
        ) -> Callable[[], StreamProcessor]:
            @wraps(factory)
            def wrapper() -> StreamProcessor:
                return factory()

            if name in self._factories:
                raise ValueError(f"Factory for {name} already registered")
            self._factories[name] = factory
            return wrapper

        return decorator

    def create(self, *, workflow_name: str, source_name: str) -> StreamProcessor:
        """Create a StreamProcessor using the registered factory."""
        factory = self._factories[workflow_name]
        sig = inspect.signature(factory)
        if 'source_name' in sig.parameters:
            return factory(source_name=source_name)
        else:
            return factory()


processor_factory = StreamProcessorFactory()


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
        self,
        *,
        source_names: Sequence[str],
        source_to_key: dict[str, Key],
    ) -> None:
        """
        Parameters
        ----------
        source_names:
            List of source names to attach workflows to. These need to be passed
            explicitly, so we can distinguish source names that should not be handled
            (to proxy and handler will be created) from source names that may later be
            configured to use a workflow.
        source_to_key:
            Dictionary mapping source names to workflow input keys.
        dynamic_workflows:
            Dictionary mapping source names to dynamic workflows.
        """
        self._source_names = source_names
        self._source_to_key = source_to_key
        self._processors = ProcessorRegistry()
        self._proxies: dict[str, StreamProcessorProxy] = {}
        for name in source_names:
            self.set_worklow(name, None)

    def set_worklow(self, source_name: str, processor: StreamProcessor | None) -> None:
        """
        Add a workflow to the manager.

        Parameters
        ----------
        source_name:
            Source name to attach the workflow to.
        workflow:
            The workflow to attach to the source name. If None, the workflow is removed.
        """
        if source_name not in self._source_names:
            raise ValueError(f"Workflow {source_name} was not defined in the manager.")
        if processor is None:
            self._processors.pop(source_name, None)
        else:
            self._processors[source_name] = processor
        if (proxy := self._proxies.get(source_name)) is not None:
            proxy.set_processor(self._processors.get(source_name))

    def set_workflow_from_command(self, command: Any) -> None:
        decoded = WorkflowControl.model_validate(command)
        if decoded.workflow_name is None:
            processor = None
        else:
            processor = processor_factory.create(
                workflow_name=decoded.workflow_name, source_name=decoded.source_name
            )
        self.set_worklow(decoded.source_name, processor)

    def get_accumulator(
        self, source_name: str
    ) -> MultiplexingProxy | StreamProcessorProxy | None:
        wf_key = self._source_to_key.get(source_name)
        if wf_key is None:
            return None
        if source_name in self._source_names:
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
            elif self._key in stream_processor._dynamic_keys:
                stream_processor.accumulate({self._key: data})
            else:
                # Might be unused by this particular workflow
                pass

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
