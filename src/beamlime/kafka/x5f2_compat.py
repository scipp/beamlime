# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from enum import Enum

import pydantic
from pydantic import field_serializer, field_validator
from streaming_data_types import deserialise_x5f2, serialise_x5f2

from beamlime.config.workflow_spec import WorkflowId
from beamlime.core.job import JobId, JobState, JobStatus


class ServiceId(pydantic.BaseModel):
    """Helper class for handling service_id in source_name:job_number format."""

    job_id: JobId = pydantic.Field(description="Job identifier")

    @classmethod
    def from_string(cls, service_id: str) -> ServiceId:
        """Parse service_id string in format 'source_name:job_number'."""
        try:
            source_name, job_number_str = service_id.split(':', 1)
            job_id = JobId(
                source_name=source_name, job_number=uuid.UUID(job_number_str)
            )
            return cls(job_id=job_id)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid service_id format '{service_id}'. "
                "Expected 'source_name:job_number'"
            ) from e

    @classmethod
    def from_job_id(cls, job_id: JobId) -> ServiceId:
        """Create ServiceId from JobId."""
        return cls(job_id=job_id)

    def to_string(self) -> str:
        """Convert to service_id string in format 'source_name:job_number'."""
        return f"{self.job_id.source_name}:{self.job_id.job_number}"

    def __str__(self) -> str:
        return self.to_string()


class Message(pydantic.BaseModel):
    state: JobState = pydantic.Field(description="Current state of the job")
    warning: str | None = pydantic.Field(
        default=None, description="Warning message if any"
    )
    error: str | None = pydantic.Field(default=None, description="Error message if any")
    job_id: JobId = pydantic.Field(description="Job identifier")
    workflow_id: str = pydantic.Field(description="Workflow identifier as string")
    start_time: int | None = pydantic.Field(
        default=None, description="Job start time in nanoseconds since epoch"
    )
    end_time: int | None = pydantic.Field(
        default=None, description="Job end time in nanoseconds since epoch"
    )


class NicosStatus(int, Enum):
    OK = 200
    WARNING = 210
    ERROR = 240
    DISABLED = 235
    UNKNOWN = 999


def job_state_to_nicos_status_constant(state: JobState) -> NicosStatus:
    match state:
        case JobState.active:
            return NicosStatus.OK
        case JobState.error:
            return NicosStatus.ERROR
        case JobState.finishing:
            return NicosStatus.OK
        case JobState.paused:
            return NicosStatus.DISABLED
        case JobState.scheduled:
            return NicosStatus.DISABLED
        case JobState.warning:
            return NicosStatus.WARNING
        case _:
            return NicosStatus.UNKNOWN


class StatusJSON(pydantic.BaseModel):
    status: NicosStatus = pydantic.Field(description="Status code")
    message: Message = pydantic.Field(description="Status message")


class StatusMessage(pydantic.BaseModel):
    software_name: str = pydantic.Field(
        default='beamlime', description="Name of the software"
    )
    software_version: str = pydantic.Field(
        default='0.0.0', description="Version of the software"
    )
    service_id: ServiceId = pydantic.Field(
        description="Service identifier defined as source_name:job_number"
    )
    host_name: str = pydantic.Field(default='', description="Host name")
    process_id: int = pydantic.Field(default=-1, description="Process ID")
    update_interval: int = pydantic.Field(
        default=1000, description="Update interval in milliseconds"
    )
    status_json: StatusJSON = pydantic.Field(
        description="Status information in JSON format"
    )

    @field_validator('service_id', mode='before')
    @classmethod
    def validate_service_id(cls, v):
        """Convert string service_id to ServiceId object during validation."""
        if isinstance(v, str):
            return ServiceId.from_string(v)
        return v

    @field_serializer('service_id')
    def serialize_service_id(self, service_id: ServiceId) -> str:
        """Serialize ServiceId to string format."""
        return service_id.to_string()

    @staticmethod
    def from_job_status(status: JobStatus) -> StatusMessage:
        return StatusMessage(
            service_id=ServiceId.from_job_id(status.job_id),
            status_json=StatusJSON(
                status=job_state_to_nicos_status_constant(status.state),
                message=Message(
                    state=status.state,
                    warning=status.warning_message,
                    error=status.error_message,
                    job_id=status.job_id,
                    workflow_id=str(status.workflow_id),
                    start_time=status.start_time,
                    end_time=status.end_time,
                ),
            ),
        )

    def to_job_status(self) -> JobStatus:
        """Convert StatusMessage to JobStatus."""
        message = self.status_json.message
        return JobStatus(
            job_id=message.job_id,
            workflow_id=WorkflowId.from_string(message.workflow_id),
            state=message.state,
            error_message=message.error,
            warning_message=message.warning,
            start_time=message.start_time,
            end_time=message.end_time,
        )


def x5f2_to_job_status(x5f2_status: bytes) -> JobStatus:
    """Deserialize x5f2 status message to JobStatus."""
    status_msg = deserialise_x5f2(x5f2_status)
    status_message = StatusMessage.model_validate(status_msg)
    return status_message.to_job_status()


def job_status_to_x5f2(status: JobStatus) -> bytes:
    """Serialize JobStatus to x5f2 status message."""
    status_message = StatusMessage.from_job_status(status)
    return serialise_x5f2(**status_message.model_dump())
