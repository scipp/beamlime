# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable
from typing import TypeVar

import scipp as sc

from beamlime.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId

from .data_service import DataService

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')

# Next:
# - Remove WorkflowConfig.identifier = None "stop mechanism"
# - Move some methods from WorkflowController to JobService
# - WorkflowController is for getting workflows and launching jobs
# - JobService is for tracking jobs and stopping them
# - Should we have a separate JobController?
# - Make a new widgets or refactor WorkflowStatusListWidget

SourceName = str
SourceData = sc.DataArray | dict[str, sc.DataArray]


class JobService:
    def __init__(
        self,
        *,
        data_service: DataService[ResultKey, sc.DataArray],
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._data_service = data_service
        self._job_data: dict[JobNumber, dict[SourceName, SourceData]] = {}
        self._job_info: dict[JobNumber, WorkflowId] = {}
        self._data_service.register_subscriber(self.data_updated)

    @property
    def job_data(self) -> dict[JobNumber, dict[SourceName, SourceData]]:
        return self._job_data

    @property
    def job_info(self) -> dict[JobNumber, WorkflowId]:
        return self._job_info

    def data_updated(self, updated_keys: set[ResultKey]) -> None:
        notify_job_update = False
        for key in updated_keys:
            value = self._data_service[key]
            job_number = key.job_id.job_number
            source_name = key.job_id.source_name
            if (workflow_id := self._job_info.get(job_number)) is None:
                self._job_info[job_number] = key.workflow_id
                notify_job_update = True
            elif workflow_id != key.workflow_id:
                self._logger.warning(
                    "Workflow ID mismatch for job %s: existing %s, new %s. ",
                    key.job_id,
                    workflow_id,
                    key.workflow_id,
                )
            all_source_data = self._job_data.setdefault(job_number, {})
            # Store in DataService for access by plots etc.
            if key.output_name is None:
                # Single output, store directly
                all_source_data[source_name] = value
            else:
                # Multiple outputs, store in a dict
                source_data = all_source_data.setdefault(source_name, {})
                if not isinstance(source_data, dict):
                    # This should never happen in practice, but we check to be safe
                    raise ValueError(
                        f"Expected dict for multiple outputs, got {type(source_data)}"
                    )
                source_data[key.output_name] = value
        if notify_job_update:
            self._notify_job_update()

    def _notify_job_update(self) -> None:
        """Notify listeners about job updates."""
        # For now we just log the job updates. In future we may want to have a more
        # sophisticated notification mechanism.
        self._logger.info("Job data updated for jobs: %s", list(self._job_info.keys()))

    # To move from WorkflowController
    def stop_job(self, job: JobId) -> None: ...
    def remove_job(self, job: JobId) -> None: ...
