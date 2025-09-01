# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable
from typing import TypeVar

import scipp as sc

from beamlime.config.workflow_spec import JobId, ResultKey, WorkflowId

from .data_service import DataService

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')

# Next:
# - Remove WorkflowConfig.identifier = None "stop mechanism"
# - Pass WorkflowConfigService to JobService, so we can send stop messages
# - Move some methods from WorkflowController to JobService
# - WorkflowController is for getting workflows and launching jobs
# - JobService is for tracking jobs and stopping them
# - Should we have a separate JobController?
# - Make a new widgets or refactor WorkflowStatusListWidget


class JobService:
    def __init__(
        self,
        *,
        data_service: DataService[ResultKey, sc.DataArray],
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._data_service = data_service
        self._job_data: dict[JobId, sc.DataArray | sc.DataGroup] = {}
        self._job_info: dict[JobId, WorkflowId] = {}
        self._data_service.register_subscriber(self.data_updated)

    def data_updated(self, updated_keys: set[ResultKey]) -> None:
        notify_job_update = False
        for key in updated_keys:
            value = self._data_service[key]
            # If we have multiple outputs then multiple updated keys will update this,
            # but they should all have the same workflow_id for the same job_id. Since
            # currently JobId is not unique across backend restarts we cannot raise on
            # mismatch, but we log a warning if it happens.
            if (workflow_id := self._job_info.get(key.job_id)) is None:
                self._job_info[key.job_id] = key.workflow_id
                notify_job_update = True
            elif workflow_id != key.workflow_id:
                self._logger.warning(
                    "Workflow ID mismatch for job %s: existing %s, new %s. "
                    "This may happen if the backend was restarted.",
                    key.job_id,
                    workflow_id,
                    key.workflow_id,
                )
            if key.output_name is None:
                # Single output, store directly
                self._job_data[key.job_id] = value
            else:
                # Multiple outputs, store in a DataGroup
                job_data = self._job_data.setdefault(key.job_id, sc.DataGroup())
                if not isinstance(job_data, sc.DataGroup):
                    # This should never happen in practice, but we check to be safe
                    raise ValueError(
                        f"Expected DataGroup for job {key.job_id}, got {type(job_data)}"
                    )
                job_data[key.output_name] = value
        if notify_job_update:
            self._notify_job_update()

    def _notify_job_update(self) -> None:
        """Notify listeners about job updates."""
        # For now we just log the job updates. In future we may want to have a more
        # sophisticated notification mechanism.
        self._logger.info("Job data updated for jobs: %s", list(self._job_info.keys()))
