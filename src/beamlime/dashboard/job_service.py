# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Callable, Hashable
from typing import TypeVar

import scipp as sc

from beamlime.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId

from .data_service import DataService

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')

SourceName = str
SourceData = dict[str | None, sc.DataArray]


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
        self._job_update_subscribers: list[Callable[[], None]] = []
        self._data_service.register_subscriber(self.data_updated)

    @property
    def job_data(self) -> dict[JobNumber, dict[SourceName, SourceData]]:
        return self._job_data

    @property
    def job_info(self) -> dict[JobNumber, WorkflowId]:
        return self._job_info

    def register_job_update_subscriber(self, callback: Callable[[], None]) -> None:
        """Register a callback to be called when job data is updated."""
        self._job_update_subscribers.append(callback)
        # Immediately notify the new subscriber of current state
        try:
            callback()
        except Exception as e:
            self._logger.error("Error in job update callback: %s", e)

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
            # Single or multiple outputs, store in a dict. If only one output, then the
            # output_name is None.
            source_data = all_source_data.setdefault(source_name, {})
            source_data[key.output_name] = value
        if notify_job_update:
            self._notify_job_update()

    def _notify_job_update(self) -> None:
        """Notify listeners about job updates."""
        # For now we just log the job updates. In future we may want to have a more
        # sophisticated notification mechanism.
        self._logger.info("Job data updated for jobs: %s", list(self._job_info.keys()))

        # Notify all subscribers
        for callback in self._job_update_subscribers:
            try:
                callback()
            except Exception as e:
                self._logger.error("Error in job update callback: %s", e)

    # To move from WorkflowController
    def stop_job(self, job: JobId) -> None: ...

    def remove_job(self, job: JobId) -> None: ...
