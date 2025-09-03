# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable
from typing import Protocol, TypeVar

import holoviews as hv
import pydantic
import scipp as sc

from beamlime.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId

from .job_service import JobService
from .plotting import PlotterSpec, plotter_registry
from .stream_manager import StreamManager

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')

# TODO
# - Add last update column to job table
# - Add "stop job" button to job table
# - Allow for passing options such as log scale when creating plot, configure in widget


class Plot(Protocol):
    def __init__(self) -> None: ...

    def __call__(
        self, data: sc.DataArray | dict[ResultKey, sc.DataArray]
    ) -> hv.Element: ...


class PlotService:
    def __init__(
        self,
        job_service: JobService,
        stream_manager: StreamManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_service = job_service
        self._stream_manager = stream_manager
        self._logger = logger or logging.getLogger(__name__)
        self._plot_fns: dict[
            tuple[WorkflowId, str | None],
            list[type[Plot]],
        ] = {}

    def register_plot(
        self, workflow_id: WorkflowId, output_name: str | None, plot_cls: type[Plot]
    ) -> None:
        # output_name = None means the workflow produces a single output
        key = (workflow_id, output_name)
        plots = self._plot_fns.setdefault(key, [])
        plots.append(plot_cls)

    def get_available_plots(
        self, job_number: JobNumber, output_name: str | None
    ) -> dict[str, PlotterSpec]:
        """Get all available plots for a given job and output."""
        workflow_id = self._job_service.job_info[job_number]
        key = (workflow_id, output_name)
        # TODO Filter based on data properties?
        return plotter_registry.get_specs()
        return self._plot_fns.get(key, [])

    def create_plot(
        self,
        job_number: JobNumber,
        source_names: list[str],
        output_name: str | None,
        plot_name: str,
    ) -> hv.DynamicMap:
        workflow_id = self._job_service.job_info[job_number]
        keys = {
            ResultKey(
                workflow_id=workflow_id,
                job_id=JobId(job_number=job_number, source_name=source_name),
                output_name=output_name,
            )
            for source_name in source_names
        }
        pipe = self._stream_manager.make_merging_stream(keys)

        class Dummy(pydantic.BaseModel):
            pass

        plot = plotter_registry.create_plotter(plot_name, params=Dummy())
        dmap = hv.DynamicMap(plot, streams=[pipe], cache_size=1).opts(shared_axes=False)
        return dmap
