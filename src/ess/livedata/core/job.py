# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any

import scipp as sc

from ess.livedata.handlers.workflow_factory import Workflow

from ..config.workflow_spec import JobId, ResultKey, WorkflowId
from .message import StreamId


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


class Job:
    def __init__(
        self,
        *,
        job_id: JobId,
        workflow_id: WorkflowId,
        processor: Workflow,
        source_names: list[str],
        aux_source_names: list[str] | None = None,
    ) -> None:
        """
        Initialize a Job with the given parameters.

        Parameters
        ----------
        job_id:
            The unique identifier for this job.
        workflow_id:
            The identifier of the workflow this job is running.
        processor:
            The Workflow instance that will process data for this job.
        source_names:
            The names of the primary data sources for this job.
        aux_source_names:
            The names of any auxiliary data sources for this job.
        """
        self._job_id = job_id
        self._workflow_id = workflow_id
        self._processor = processor
        self._start_time: int | None = None
        self._end_time: int | None = None
        self._source_names = source_names
        self._aux_source_names = aux_source_names or []

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
            primary: dict[str, Any] = {}
            aux: dict[str, Any] = {}
            for stream, value in data.data.items():
                if stream.name in self._source_names:
                    primary[stream.name] = value
                elif stream.name in self._aux_source_names:
                    aux[stream.name] = value
            if primary:
                # Only "start" on first valid data
                if self._start_time is None:
                    self._start_time = data.start_time
                self._end_time = data.end_time
            if primary or aux:
                self._processor.accumulate({**primary, **aux})
            return JobError(job_id=self._job_id)
        except Exception:
            tb = traceback.format_exc()
            message = f"Job failed to process latest data.\n\n{tb}"
            return JobError(job_id=self._job_id, error_message=message)

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
        except Exception:
            tb = traceback.format_exc()
            message = f"Job failed to compute result.\n\n{tb}"
            return JobResult(
                job_id=self._job_id,
                workflow_id=self._workflow_id,
                start_time=self.start_time,
                end_time=self.end_time,
                error_message=message,
            )

    def reset(self) -> None:
        """Reset the processor for this job."""
        self._processor.clear()
        self._start_time = None
        self._end_time = None
