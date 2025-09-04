# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from beamlime.config.models import ConfigKey
from beamlime.config.workflow_spec import JobId, JobNumber, WorkflowId
from beamlime.core.job import JobAction, JobCommand
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.job_service import JobService


class JobController:
    def __init__(self, config_service: ConfigService, job_service: JobService) -> None:
        self._config_service = config_service
        self._job_service = job_service

    def _config_key(self, key: str) -> ConfigKey:
        return ConfigKey(key=key)

    def send_global_action(self, action: JobAction) -> None:
        command = JobCommand(action=action)
        self._config_service.update_config(self._config_key(JobCommand.key), command)

    def get_workflow_ids(self) -> list[WorkflowId]:
        """Get list of all workflow IDs from active jobs."""
        return list(set(self._job_service.job_info.values()))

    def get_job_ids(self, workflow_id: WorkflowId | None = None) -> list[JobId]:
        """Get list of job IDs, optionally filtered by workflow ID."""
        job_ids = []
        for job_number, source_data in self._job_service.job_data.items():
            # Check if this job matches the workflow filter
            if workflow_id is not None:
                job_workflow_id = self._job_service.job_info.get(job_number)
                if job_workflow_id != workflow_id:
                    continue

            # Add JobId for each source in this job
            job_ids.extend(
                JobId(job_number=job_number, source_name=source_name)
                for source_name in source_data.keys()
            )

        return job_ids

    def get_job_numbers(self, workflow_id: WorkflowId | None = None) -> list[JobNumber]:
        """Get list of job numbers, optionally filtered by workflow ID."""
        if workflow_id is None:
            return list(self._job_service.job_info.keys())

        return [
            job_number
            for job_number, wf_id in self._job_service.job_info.items()
            if wf_id == workflow_id
        ]

    def send_job_action(self, job_id: JobId, action: JobAction) -> None:
        """Send action for a specific job ID."""
        command = JobCommand(job_id=job_id, action=action)
        self._config_service.update_config(self._config_key(JobCommand.key), command)

    def send_workflow_action(self, workflow_id: WorkflowId, action: JobAction) -> None:
        """Send action for a specific workflow ID."""
        command = JobCommand(workflow_id=workflow_id, action=action)
        self._config_service.update_config(self._config_key(JobCommand.key), command)
