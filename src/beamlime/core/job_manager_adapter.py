# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

from ..config.models import ConfigKey, StartTime
from ..config.workflow_spec import WorkflowConfig, WorkflowStatus, WorkflowStatusType
from .job import DifferentInstrument, JobCommand, JobId, JobManager


class JobManagerAdapter:
    """
    Adapter to convert calls to JobManager into ConfigHandler actions.

    This has two purposes:

    1. We can keep using ConfigHandler until we have fully refactored everything.
    2. We keep the legacy one-source-one-job behavior, replacing old jobs if a new one
       is started. The long-term goal is to change this to a more flexible mechanism,
       but this, too, would require frontend changes.
    """

    def __init__(self, *, job_manager: JobManager, logger: logging.Logger) -> None:
        self._logger = logger
        self._job_manager = job_manager
        self._jobs: dict[str, JobId] = {}

    def job_command(self, source_name: str, value: dict) -> None:
        _ = source_name  # Legacy, not used.
        command = JobCommand.model_validate(value)
        self._job_manager.job_command(command)

    def reset_job(
        self, source_name: str | None, value: dict
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            for source in self._jobs:
                self.reset_job(source_name=source, value=value)
            return []
        # TODO Can we use the start_time or should we change the schema?
        _ = StartTime.model_validate(value)
        self._job_manager.reset_job(job_id=self._jobs[source_name])
        return []

    def set_workflow_with_config(
        self, source_name: str | None, value: dict | None
    ) -> list[tuple[ConfigKey, WorkflowStatus]]:
        if source_name is None:
            raise ValueError("source_name cannot be None for set_workflow_with_config")

        config_key = ConfigKey(
            service_name="job_server", source_name=source_name, key="workflow_status"
        )

        config = WorkflowConfig.model_validate(value)
        if config.identifier is None:  # New way to stop/remove a workflow.
            if (job_id := self._jobs.pop(source_name, None)) is not None:
                self._job_manager.stop_job(job_id)
                # TODO Not stopped yet, is returning status here the wrong approach?
                status = WorkflowStatus(
                    source_name=source_name, status=WorkflowStatusType.STOPPED
                )
                return [(config_key, status)]
            return []

        try:
            job_id = self._job_manager.schedule_job(
                source_name=source_name, config=config
            )
            if source_name in self._jobs:
                # If we have a job for this source, we stop it first.
                self._job_manager.stop_job(self._jobs[source_name])
            self._jobs[source_name] = job_id
        except DifferentInstrument:
            # We have multiple backend services that handle jobs, e.g., data_reduction
            # and monitor_data. The frontend simply sends a WorkflowConfig message and
            # does not make assumptions which service will handle it. The workflows
            # for each backend are part of a different instrument, e.g., 'dream' for
            # data_reduction and 'dream_beam_monitors' for monitor_data, which is
            # included in the identifier. This should thus work safely, but the question
            # is whether it should be filtered out earlier.
            self._logger.debug(
                "Workflow %s not found, assuming it is handled by another worker",
                config.identifier,
            )
            return []
        except Exception as e:
            # TODO This system is a bit flawed: If we have a workflow running already
            # it will keep running, but we need to notify about startup errors. Frontend
            # will not be able to display the correct workflow status. Need to come up
            # with a better way to handle this.
            # NOTE This can be fixed using the new JobManager approach, provided that
            # the frontend can display a per-job status, instead of per-source.
            # But maybe the key insight is that status-reporting should be decoupled
            # from reply messages. Maybe status should just be emitted periodically for
            # all jobs?
            status = WorkflowStatus(
                source_name=source_name,
                status=WorkflowStatusType.STARTUP_ERROR,
                message=str(e),
            )
            return [(config_key, status)]

        status = WorkflowStatus(
            source_name=source_name,
            status=WorkflowStatusType.RUNNING,
            workflow_id=config.identifier,
        )
        return [(config_key, status)]
