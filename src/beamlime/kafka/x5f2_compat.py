# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations
from enum import Enum
import pydantic
from streaming_data_types import serialise_x5f2, deserialise_x5f2, status_x5f2

from beamlime.core.job import JobState, JobStatus


class Message(pydantic.BaseModel):
    state: JobState = pydantic.Field(description="Current state of the job")
    warning: str | None = pydantic.Field(
        default=None, description="Warning message if any"
    )
    error: str | None = pydantic.Field(default=None, description="Error message if any")


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
    service_id: str = pydantic.Field(
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

    @staticmethod
    def from_job_status(status: JobStatus) -> StatusMessage:
        # Colon-separated source_name:job_number that can be parsed by Nicos
        return StatusMessage(
            service_id=f"{status.job_id.source_name}:{status.job_id.job_number}",
            status_json=StatusJSON(
                status=job_state_to_nicos_status_constant(status.state),
                message=Message(
                    state=status.state,
                    warning=status.warning_message,
                    error=status.error_message,
                ),
            ),
        )


def x5f2_to_job_status(x5f2_status: str) -> JobError:
    pass


def job_status_to_x5f2(status: JobError) -> str:
    pass
