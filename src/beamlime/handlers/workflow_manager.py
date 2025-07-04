# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Iterator, MutableMapping

import scipp as sc
from ess.reduce.streaming import StreamProcessor
from sciline.typing import Key

from beamlime.handlers.stream_processor_factory import StreamProcessorFactory

from ..config.models import ConfigKey
from ..config.workflow_spec import (
    WorkflowConfig,
    WorkflowSpecs,
    WorkflowStatus,
    WorkflowStatusType,
)
from ..core.handler import Accumulator


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
        processor_factory: StreamProcessorFactory,
        source_to_key: dict[str, Key],
    ) -> None:
        """
        Parameters
        ----------
        processor_factory:
            Factory for creating stream processors.
        source_to_key:
            Dictionary mapping source names to workflow input keys.
        """
        self._processor_factory = processor_factory
        self._source_to_key = source_to_key
        self._processors = ProcessorRegistry()
        self._proxies: dict[str, StreamProcessorProxy] = {}

    def get_workflow_specs(self) -> WorkflowSpecs:
        """
        Get the workflow specifications for the available workflows.

        Returns
        -------
        WorkflowSpecs
            The workflow specifications.
        """
        return WorkflowSpecs(workflows=dict(self._processor_factory.items()))

    def set_workflow(self, source_name: str, processor: StreamProcessor | None) -> None:
        """
        Add a workflow to the manager.

        Parameters
        ----------
        source_name:
            Source name to attach the workflow to.
        workflow:
            The workflow to attach to the source name. If None, the workflow is removed.
        """
        if processor is None:
            self._processors.pop(source_name, None)
        else:
            self._processors[source_name] = processor
        if (proxy := self._proxies.get(source_name)) is not None:
            proxy.set_processor(self._processors.get(source_name))

    def set_workflow_with_config(
        self, source_name: str | None, value: dict | None
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            results = []
            for name in self._processor_factory.source_names:
                results.extend(self.set_workflow_with_config(name, value))
            return results

        config_key = ConfigKey(source_name=source_name, key="workflow_status")
        if value is None:  # Legacy way to stop/remove a workflow.
            self.set_workflow(source_name, None)
            status = WorkflowStatus(
                source_name=source_name, status=WorkflowStatusType.STOPPED
            )
            return [(config_key, status)]

        # TODO This will be the model now, access registry here (deser with reg?)
        config = WorkflowConfig.model_validate(value)
        if config.identifier is None:  # New way to stop/remove a workflow.
            self.set_workflow(source_name, None)
            status = WorkflowStatus(
                source_name=source_name, status=WorkflowStatusType.STOPPED
            )
            return [(config_key, status)]

        try:
            processor = self._processor_factory.create(
                workflow_id=config.identifier,
                source_name=source_name,
                workflow_params=config.values,
            )
        except Exception as e:
            # TODO This system is a bit flawed: If we have a workflow running already
            # it will keep running, but we need to notify about startup errors. Frontend
            # will not be able to display the correct workflow status. Need to come up
            # with a better way to handle this.
            status = WorkflowStatus(
                source_name=source_name,
                status=WorkflowStatusType.STARTUP_ERROR,
                message=str(e),
            )
            return [(config_key, status)]

        self.set_workflow(source_name, processor=processor)
        status = WorkflowStatus(
            source_name=source_name,
            status=WorkflowStatusType.RUNNING,
            workflow_id=config.identifier,
        )
        return [(config_key, status)]

    def get_accumulator(
        self, source_name: str
    ) -> MultiplexingProxy | StreamProcessorProxy | None:
        wf_key = self._source_to_key.get(source_name)
        if wf_key is None:
            return None
        if source_name in self._processor_factory.source_names:
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
