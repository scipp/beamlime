# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Hashable, Protocol

import scipp as sc

from ..config.workflow_spec import WorkflowConfig, WorkflowId, WorkflowSpec
from ..handlers.workflow_manager import WorkflowManager as LegacyWorkflowManager
from .handler import output_stream_name
from .message import Message, StreamId, StreamKind, Tin, Tout


class StreamProcessor(Protocol):
    """
    Protocol matching ess.reduce.streaming.StreamProcessor, used by :py:class:`Job`.

    There will be other implementations, in particular for non-data-reduction jobs.
    """

    def accumulate(self, data: dict[Hashable, Any]) -> None: ...
    def finalize(self) -> dict[Hashable, Any]: ...
    def clear(self) -> None: ...


@dataclass(slots=True, kw_only=True)
class WorkflowData:
    start_time: int
    end_time: int
    data: dict[StreamId, Any]


class Job:
    def __init__(
        self,
        *,
        workflow_name: str,
        source_name: str,
        processor: StreamProcessor,
        source_mapping: dict[str, Hashable],
    ) -> None:
        self._workflow_name = workflow_name
        self._source_name = source_name
        self._processor = processor
        self._source_mapping = source_mapping
        self._start_time = -1
        self._end_time = -1

    @property
    def start_time(self) -> int:
        return self._start_time

    @property
    def end_time(self) -> int:
        return self._end_time

    def add(self, data: WorkflowData) -> None:
        if self._start_time == -1:
            self._start_time = data.start_time
        self._end_time = data.end_time
        update: dict[Hashable, Any] = {}
        for stream, value in data.data.items():
            if stream.name not in self._source_mapping:
                continue
            key = self._source_mapping[stream.name]
            update[key] = value
        self._processor.accumulate(update)

    def get(self) -> JobResult | None:
        data = sc.DataGroup(
            {str(key): val for key, val in self._processor.finalize().items()}
        )
        return JobResult(
            start_time=self.start_time,
            end_time=self.end_time,
            source_name=self._source_name,
            name=self._workflow_name,
            data=data,
        )

    def reset(self) -> None:
        """Reset the processor for this job."""
        self._processor.clear()
        self._start_time = -1
        self._end_time = -1


@dataclass
class JobResult:
    start_time: int
    end_time: int
    source_name: str
    name: str
    data: sc.DataArray | sc.DataGroup

    def to_message(self, service_name: str) -> Message:
        """
        Convert the workflow result to a message for publishing.

        Parameters
        ----------
        service_name:
            The name of the service to which the message belongs.
        """
        # TODO I think we should include the Job ID in the message.
        stream_name = output_stream_name(
            service_name=service_name,
            stream_name=self.source_name,
            signal_name=self.name,
        )
        return Message(
            timestamp=self.start_time,
            stream=StreamId(kind=StreamKind.BEAMLIME_DATA, name=stream_name),
            value=self.data,
        )


class JobFactory:
    def __init__(self, legacy_manager: LegacyWorkflowManager) -> None:
        self._workflow_specs: dict[WorkflowId, WorkflowSpec] = {}
        self._legacy_manager = legacy_manager

    def create(self, *, source_name: str, config: WorkflowConfig) -> Job:
        # Note that this initializes the job immediately, i.e., we pay startup cost now.
        legacy_factory = self._legacy_manager._processor_factory
        stream_processor = legacy_factory.create(source_name=source_name, config=config)
        workflow_id = config.identifier
        workflow_spec = legacy_factory._workflow_specs[workflow_id]
        source_to_key = self._legacy_manager._source_to_key
        source_mapping = {
            source: source_to_key[source] for source in workflow_spec.aux_source_names
        }
        source_mapping[source_name] = source_to_key[source_name]
        return Job(
            workflow_name=workflow_spec.name,
            source_name=source_name,
            processor=stream_processor,
            source_mapping=source_mapping,
        )


JobId = int


class JobManager:
    def __init__(self, job_factory: JobFactory) -> None:
        self.service_name = 'data_reduction'
        self._last_update: int = 0
        self._job_factory = job_factory
        self._active_jobs: dict[JobId, Job] = {}
        self._scheduled_jobs: dict[JobId, Job] = {}
        self._finishing_jobs: list[JobId] = []
        self._next_job_id: JobId = 0

    @property
    def active_jobs(self) -> list[Job]:
        """Get the list of active jobs."""
        return list(self._active_jobs.values())

    def _schedule_job(self, workflow: Job) -> None:
        """Start a new job with the given workflow."""
        job_id = self._next_job_id
        self._next_job_id += 1
        self._scheduled_jobs[job_id] = workflow

    def _start_job(self, job: JobId) -> None:
        """Start a new job with the given workflow."""
        workflow = self._scheduled_jobs.pop(job, None)
        if workflow is None:
            raise KeyError(f"Job {job} not found in scheduled jobs.")
        self._active_jobs[job] = workflow

    def _advance_to_time(self, start_time: int, end_time: int) -> None:
        to_activate = [
            job
            for job, wf in self._scheduled_jobs.items()
            if wf.start_time <= start_time
        ]
        to_finish = [
            job for job, wf in self._active_jobs.items() if wf.end_time <= end_time
        ]
        for job in to_activate:
            self._start_job(job)
        # Do not remove from active jobs yet, we need to compute results.
        self._finishing_jobs.extend(to_finish)

    def handle_config_messages(self, config_messages: list[Message[Tin]]) -> None:
        """
        Handle configuration messages to update the workflow state.
        This may include resetting accumulators or changing their configuration.
        """
        # Config messages contain WorkflowConfig, which WorkflowFactory can use to
        # create a workflow. This may also schedule a workflow stop. In that case we
        # still need to compute a final result?

    def handle_data_messages(self, data: WorkflowData) -> None:
        """
        Handle data messages by passing them to the appropriate accumulators.
        This may include updating the workflow state based on the preprocessed data.
        """
        self._advance_to_time(data.start_time, data.end_time)
        for workflow in self.active_jobs:
            workflow.add(data)

    def compute_results(self) -> list[Message[Tout]]:
        """
        Compute results from the accumulated data and return them as messages.
        This may include processing the accumulated data and preparing it for output.
        """
        results = [job.get() for job in self.active_jobs]
        results = [result for result in results if result is not None]
        for job in self._finishing_jobs:
            _ = self._active_jobs.pop(job, None)
        self._finishing_jobs.clear()
        return [result.to_message(service_name=self.service_name) for result in results]
