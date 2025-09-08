# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

import pydantic
import scipp as sc

from beamlime.config.instrument import Instrument
from beamlime.handlers.workflow_factory import Workflow

from ..config.workflow_spec import (
    JobId,
    JobSchedule,
    ResultKey,
    WorkflowConfig,
    WorkflowId,
)
from .message import StreamId


class DifferentInstrument(Exception):
    """
    Raised when a workflow id does not match the instrument of this worker.

    This is not considered an error, but rather a signal that the workflow should be
    handled by a different worker.
    """


class WorkflowNotFoundError(Exception):
    """Raised when a workflow specification is not found in this worker."""


@dataclass(slots=True, kw_only=True)
class WorkflowData:
    """
    Data to be processed by a workflow.

    All timestamps are in nanoseconds since the epoch (UTC) and reference the timestamps
    of the raw data being processed (as opposed to when it was processed).
    """

    start_time: int
    end_time: int
    data: dict[StreamId, Any]


@dataclass(slots=True, kw_only=True)
class JobResult:
    job_id: JobId
    workflow_id: WorkflowId
    output_name: str | None = None
    # Should this be included in the data instead?
    start_time: int | None
    end_time: int | None
    data: sc.DataArray | sc.DataGroup | None = None
    error_message: str | None = None

    @property
    def stream_name(self) -> str:
        """Get the stream name associated with this job result."""
        return ResultKey(
            workflow_id=self.workflow_id,
            job_id=self.job_id,
            output_name=self.output_name,
        ).model_dump_json()


@dataclass
class JobError:
    job_id: JobId
    error_message: str | None = None

    @property
    def has_error(self) -> bool:
        """Check if the job status indicates an error."""
        return self.error_message is not None


class JobAction(str, Enum):
    pause = "pause"
    resume = "resume"
    reset = "reset"
    stop = "stop"


class JobCommand(pydantic.BaseModel):
    key: ClassVar[str] = "job_command"
    job_id: JobId | None = pydantic.Field(
        default=None, description="ID of the job to control."
    )
    workflow_id: WorkflowId | None = pydantic.Field(
        default=None, description="Workflow ID to cancel jobs for."
    )
    action: JobAction = pydantic.Field(description="Action to perform on the job.")


class Job:
    def __init__(
        self,
        *,
        job_id: JobId,
        workflow_id: WorkflowId,
        processor: Workflow,
        source_mapping: Mapping[str, Hashable],
    ) -> None:
        self._job_id = job_id
        self._workflow_id = workflow_id
        self._processor = processor
        self._source_mapping = source_mapping
        self._start_time: int | None = None
        self._end_time: int | None = None

    @property
    def job_id(self) -> JobId:
        return self._job_id

    @property
    def workflow_id(self) -> WorkflowId:
        return self._workflow_id

    @property
    def start_time(self) -> int | None:
        return self._start_time

    @property
    def end_time(self) -> int | None:
        return self._end_time

    def add(self, data: WorkflowData) -> JobError:
        try:
            if self._start_time is None:
                self._start_time = data.start_time
            self._end_time = data.end_time
            update: dict[Hashable, Any] = {}
            for stream, value in data.data.items():
                if stream.name not in self._source_mapping:
                    continue
                key = self._source_mapping[stream.name]
                update[key] = value
            if update:
                self._processor.accumulate(update)
            return JobError(job_id=self._job_id)
        except Exception as e:
            error_msg = f"Error processing data for job {self._job_id}: {e}"
            return JobError(job_id=self._job_id, error_message=error_msg)

    def get(self) -> JobResult:
        try:
            data = sc.DataGroup(
                {str(key): val for key, val in self._processor.finalize().items()}
            )
            return JobResult(
                job_id=self._job_id,
                workflow_id=self._workflow_id,
                start_time=self.start_time,
                end_time=self.end_time,
                data=data,
            )
        except Exception as e:
            error_msg = f"Error finalizing job {self._job_id}: {e}"
            return JobResult(
                job_id=self._job_id,
                workflow_id=self._workflow_id,
                start_time=self.start_time,
                end_time=self.end_time,
                error_message=error_msg,
            )

    def reset(self) -> None:
        """Reset the processor for this job."""
        self._processor.clear()
        self._start_time = None
        self._end_time = None


class JobFactory:
    def __init__(self, instrument: Instrument) -> None:
        self._instrument = instrument

    def create(self, *, job_id: JobId, config: WorkflowConfig) -> Job:
        workflow_id = config.identifier
        if workflow_id is None:
            raise ValueError("WorkflowConfig must have an identifier to create a Job")
        if (workflow_id.instrument != self._instrument.name) or (
            workflow_id.namespace != self._instrument.active_namespace
        ):
            raise DifferentInstrument()

        factory = self._instrument.workflow_factory
        if (workflow_spec := factory.get(workflow_id)) is None:
            raise WorkflowNotFoundError(f"WorkflowSpec with Id {workflow_id} not found")
        # Note that this initializes the job immediately, i.e., we pay startup cost now.
        stream_processor = factory.create(source_name=job_id.source_name, config=config)
        source_to_key = self._instrument.source_to_key
        source_mapping = {
            source: source_to_key[source] for source in workflow_spec.aux_source_names
        }
        source_mapping[job_id.source_name] = source_to_key.get(
            job_id.source_name, job_id.source_name
        )
        return Job(
            job_id=job_id,
            workflow_id=workflow_id,
            processor=stream_processor,
            source_mapping=source_mapping,
        )


class JobManager:
    def __init__(self, job_factory: JobFactory) -> None:
        self.service_name = 'data_reduction'
        self._last_update: int = 0
        self._job_factory = job_factory
        self._active_jobs: dict[JobId, Job] = {}
        self._scheduled_jobs: dict[JobId, Job] = {}
        self._finishing_jobs: list[JobId] = []
        self._job_schedules: dict[JobId, JobSchedule] = {}

    @property
    def all_jobs(self) -> list[Job]:
        """Get a list of all jobs, both active and scheduled."""
        return [*self._active_jobs.values(), *self._scheduled_jobs.values()]

    @property
    def active_jobs(self) -> list[Job]:
        """Get the list of active jobs."""
        return list(self._active_jobs.values())

    def _start_job(self, job_id: JobId) -> None:
        """Start a new job with the given workflow."""
        workflow = self._scheduled_jobs.pop(job_id, None)
        if workflow is None:
            raise KeyError(f"Job {job_id} not found in scheduled jobs.")
        self._active_jobs[job_id] = workflow

    def _advance_to_time(self, start_time: int, end_time: int) -> None:
        """Activate jobs that should start and mark jobs that should finish."""
        to_activate = [
            job_id
            for job_id in self._scheduled_jobs.keys()
            if self._job_schedules[job_id].should_start(start_time)
        ]

        # Activate jobs first
        for job_id in to_activate:
            self._start_job(job_id)

        # Now check for jobs to finish (including newly activated ones)
        to_finish = [
            job_id
            for job_id in self._active_jobs.keys()
            if (schedule := self._job_schedules[job_id]).end_time is not None
            and schedule.end_time <= end_time
        ]

        # Do not remove from active jobs yet, we need to compute results.
        self._finishing_jobs.extend(to_finish)

    def schedule_job(self, source_name: str, config: WorkflowConfig) -> JobId:
        """
        Schedule a new job based on the provided configuration.
        """
        job_id = JobId(
            job_number=config.job_number or uuid.uuid4(), source_name=source_name
        )
        job = self._job_factory.create(job_id=job_id, config=config)
        self._job_schedules[job_id] = config.schedule
        self._scheduled_jobs[job_id] = job
        return job_id

    def stop_job(self, job_id: JobId) -> None:
        """Stop a job with the given ID immediately."""
        was_active = self._active_jobs.pop(job_id, None)
        _ = self._job_schedules.pop(job_id, None)
        was_schedule = self._scheduled_jobs.pop(job_id, None)
        if was_active is None and was_schedule is None:
            raise KeyError(f"Job {job_id} not found in active or scheduled jobs.")

    def job_command(self, command: JobCommand) -> None:
        if command.job_id is not None:
            self._perform_job_action(job_id=command.job_id, action=command.action)
        elif command.workflow_id is not None:
            self._perform_action(
                action=command.action,
                sel=lambda job: job.workflow_id == command.workflow_id,
            )
        else:
            self._perform_action(action=command.action, sel=lambda job: True)

    def _perform_action(self, action: JobAction, sel: Callable[[Job], bool]) -> None:
        jobs_to_control = [job.job_id for job in self.active_jobs if sel(job)]
        for job_id in jobs_to_control:
            self._perform_job_action(job_id=job_id, action=action)

    def _perform_job_action(self, job_id: JobId, action: JobAction) -> None:
        match action:
            case JobAction.reset:
                self.reset_job(job_id)
            case JobAction.stop:
                self.stop_job(job_id)
            case JobAction.pause:
                raise NotImplementedError("Pause action not implemented yet")
            case JobAction.resume:
                raise NotImplementedError("Resume action not implemented yet")
            case _:
                raise ValueError(f"Unknown job action: {action}")

    def reset_job(self, job_id: JobId) -> None:
        """
        Reset a job with the given ID.
        This will clear the processor and reset the start and end times.
        """
        if (job := self._active_jobs.get(job_id)) is not None:
            job.reset()
        elif (job := self._scheduled_jobs.get(job_id)) is not None:
            # Currently meaningless but might become relevant, e.g., if we add support
            # for pausing jobs?
            job.reset()
        else:
            raise KeyError(f"Job {job_id} not found in active or scheduled jobs.")

    def push_data(self, data: WorkflowData) -> list[JobError]:
        """Push data into the active jobs and return status for each job."""
        self._advance_to_time(data.start_time, data.end_time)
        job_statuses: list[JobError] = []
        for job in self.active_jobs:
            status = job.add(data)
            job_statuses.append(status)
        return job_statuses

    def compute_results(self) -> list[JobResult]:
        """
        Compute results from the accumulated data and return them as messages.
        This may include processing the accumulated data and preparing it for output.
        """
        results = [job.get() for job in self.active_jobs]
        self._finish_jobs()
        return results

    def _finish_jobs(self):
        for job_id in self._finishing_jobs:
            _ = self._active_jobs.pop(job_id, None)
            _ = self._job_schedules.pop(job_id, None)  # Clean up schedule
        self._finishing_jobs.clear()

    def format_job_error(self, status: JobError) -> str:
        """Format a job error message with meaningful job information."""
        job = self._active_jobs.get(status.job_id) or self._scheduled_jobs.get(
            status.job_id
        )
        if job is None:
            return f"Job {status.job_id} error: {status.error_message}"

        return (
            f"Job {job._workflow_id}/{status.job_id.source_name} "
            f"error: {status.error_message}"
        )
