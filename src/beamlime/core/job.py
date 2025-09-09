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
class JobStatus:
    """Complete status information for a job."""

    job_id: JobId
    workflow_id: WorkflowId
    state: JobState
    error_message: str | None = None
    warning_message: str | None = None
    start_time: int | None = None
    end_time: int | None = None

    @property
    def has_error(self) -> bool:
        """Check if the job status indicates an error."""
        return self.state == JobState.error

    @property
    def has_warning(self) -> bool:
        """Check if the job status indicates a warning."""
        return self.state == JobState.warning or self.warning_message is not None


@dataclass
class JobError:
    """Error information for a job operation."""

    job_id: JobId
    error_message: str | None = None

    @property
    def has_error(self) -> bool:
        """Check if the job status indicates an error."""
        return self.error_message is not None


class JobState(str, Enum):
    scheduled = "scheduled"
    active = "active"
    paused = "paused"
    finishing = "finishing"
    stopped = "stopped"
    error = "error"
    warning = "warning"


class JobAction(str, Enum):
    pause = "pause"
    resume = "resume"
    reset = "reset"
    stop = "stop"
    remove = "remove"


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
        self._stopped_jobs: dict[JobId, Job] = {}
        self._finishing_jobs: list[JobId] = []
        self._job_schedules: dict[JobId, JobSchedule] = {}
        # Track job states and messages in the manager
        self._job_states: dict[JobId, JobState] = {}
        self._job_error_messages: dict[JobId, str] = {}
        self._job_warning_messages: dict[JobId, str] = {}

    @property
    def all_jobs(self) -> list[Job]:
        """Get a list of all jobs, both active and scheduled."""
        return [
            *self._active_jobs.values(),
            *self._scheduled_jobs.values(),
            *self._stopped_jobs.values(),
        ]

    @property
    def active_jobs(self) -> list[Job]:
        """Get the list of active jobs."""
        return list(self._active_jobs.values())

    def _start_job(self, job_id: JobId) -> None:
        """Start a new job with the given workflow."""
        job = self._scheduled_jobs.pop(job_id, None)
        if job is None:
            raise KeyError(f"Job {job_id} not found in scheduled jobs.")
        self._job_states[job_id] = JobState.active
        self._active_jobs[job_id] = job

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
        self._job_states[job_id] = JobState.scheduled
        self._scheduled_jobs[job_id] = job
        return job_id

    def stop_job(self, job_id: JobId) -> None:
        """Stop a job with the given ID immediately and move it to stopped state."""
        was_active = self._active_jobs.pop(job_id, None)
        was_schedule = self._scheduled_jobs.pop(job_id, None)

        job = was_active or was_schedule
        if job is None:
            raise KeyError(f"Job {job_id} not found in active or scheduled jobs.")

        # Move to stopped jobs and update state
        self._stopped_jobs[job_id] = job
        self._job_states[job_id] = JobState.stopped

    def remove_job(self, job_id: JobId) -> None:
        """Remove a job with the given ID completely from the system."""
        was_active = self._active_jobs.pop(job_id, None)
        was_scheduled = self._scheduled_jobs.pop(job_id, None)
        was_stopped = self._stopped_jobs.pop(job_id, None)

        job = was_active or was_scheduled or was_stopped
        if job is None:
            raise KeyError(f"Job {job_id} not found.")

        # Clean up all tracking
        self._job_schedules.pop(job_id, None)
        self._job_states.pop(job_id, None)
        self._job_error_messages.pop(job_id, None)
        self._job_warning_messages.pop(job_id, None)

        # Remove from finishing jobs if present
        if job_id in self._finishing_jobs:
            self._finishing_jobs.remove(job_id)

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
            case JobAction.remove:
                self.remove_job(job_id)
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
            job.reset()
        elif (job := self._stopped_jobs.get(job_id)) is not None:
            job.reset()
        else:
            raise KeyError(
                f"Job {job_id} not found in active, scheduled, or stopped jobs."
            )

        # Clear error/warning state when resetting
        self._job_error_messages.pop(job_id, None)
        self._job_warning_messages.pop(job_id, None)
        # Reset state to scheduled unless it's currently active
        if job_id in self._active_jobs:
            self._job_states[job_id] = JobState.active
        else:
            self._job_states[job_id] = JobState.scheduled

    def push_data(self, data: WorkflowData) -> list[JobError]:
        """Push data into the active jobs and return status for each job."""
        self._advance_to_time(data.start_time, data.end_time)
        job_statuses: list[JobError] = []
        for job in self.active_jobs:
            status = job.add(data)
            job_statuses.append(status)
            # Track warnings from job operations, or clear them on success
            if status.has_error and status.error_message is not None:
                self._job_warning_messages[job.job_id] = status.error_message
                self._job_states[job.job_id] = JobState.warning
            else:
                # Clear warning state on successful data processing
                self._job_warning_messages.pop(job.job_id, None)
                # Only update state if it was warning (preserve error state)
                if self._job_states.get(job.job_id) == JobState.warning:
                    self._job_states[job.job_id] = JobState.active
        return job_statuses

    def compute_results(self) -> list[JobResult]:
        """
        Compute results from the accumulated data and return them as messages.
        This may include processing the accumulated data and preparing it for output.
        """
        results = []
        for job in self.active_jobs:
            result = job.get()
            results.append(result)
            # Track errors from job finalization, or clear them on success
            if result.error_message is not None:
                self._job_error_messages[job.job_id] = result.error_message
                self._job_states[job.job_id] = JobState.error
            else:
                # Clear error state on successful finalization
                self._job_error_messages.pop(job.job_id, None)
                # Update state based on current status (may still have warnings)
                if job.job_id in self._job_warning_messages:
                    self._job_states[job.job_id] = JobState.warning
                else:
                    self._job_states[job.job_id] = JobState.active
        self._finish_jobs()
        return results

    def _finish_jobs(self):
        for job_id in self._finishing_jobs:
            job = self._active_jobs.pop(job_id, None)
            if job is not None:
                self._stopped_jobs[job_id] = job
                self._job_states[job_id] = JobState.stopped
            _ = self._job_schedules.pop(job_id, None)  # Clean up schedule
        self._finishing_jobs.clear()

    def get_job_status(self, job_id: JobId) -> JobStatus | None:
        """Get the status of a specific job by its ID."""
        job = (
            self._active_jobs.get(job_id)
            or self._scheduled_jobs.get(job_id)
            or self._stopped_jobs.get(job_id)
        )
        if job is None:
            return None

        # Determine current state based on job's location in manager
        if job_id in self._active_jobs:
            if job_id in self._finishing_jobs:
                current_state = JobState.finishing
            else:
                # Use tracked state (may be warning/error from operations)
                current_state = self._job_states.get(job_id, JobState.active)
        elif job_id in self._scheduled_jobs:
            # Use tracked state (may be error/warning from previous operations)
            current_state = self._job_states.get(job_id, JobState.scheduled)
        elif job_id in self._stopped_jobs:
            current_state = JobState.stopped
        else:
            return None

        return JobStatus(
            job_id=job_id,
            workflow_id=job.workflow_id,
            state=current_state,
            error_message=self._job_error_messages.get(job_id),
            warning_message=self._job_warning_messages.get(job_id),
            start_time=job.start_time,
            end_time=job.end_time,
        )

    def get_all_job_statuses(self) -> list[JobStatus]:
        """Get the status of all jobs in the manager."""
        all_job_ids = (
            list(self._active_jobs.keys())
            + list(self._scheduled_jobs.keys())
            + list(self._stopped_jobs.keys())
        )
        statuses = []
        for job_id in all_job_ids:
            status = self.get_job_status(job_id)
            if status is not None:
                statuses.append(status)
        return statuses

    def get_jobs_by_workflow(self, workflow_id: WorkflowId) -> list[JobStatus]:
        """Get all jobs for a specific workflow ID."""
        return [
            status
            for status in self.get_all_job_statuses()
            if status.workflow_id == workflow_id
        ]

    def get_jobs_by_state(self, state: JobState) -> list[JobStatus]:
        """Get all jobs in a specific state."""
        return [
            status for status in self.get_all_job_statuses() if status.state == state
        ]

    def format_job_error(self, status: JobError) -> str:
        """Format a job error message with meaningful job information."""
        job = (
            self._active_jobs.get(status.job_id)
            or self._scheduled_jobs.get(status.job_id)
            or self._stopped_jobs.get(status.job_id)
        )
        if job is None:
            return f"Job {status.job_id} error: {status.error_message}"

        return (
            f"Job {job._workflow_id}/{status.job_id.source_name} "
            f"error: {status.error_message}"
        )
