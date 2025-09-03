# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable
from typing import TypeVar

import holoviews as hv
import pydantic

from beamlime.config.workflow_spec import JobId, JobNumber, ResultKey

from .job_service import JobService
from .plotting import PlotterSpec, plotter_registry
from .stream_manager import StreamManager

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')


class PlottingController:
    def __init__(
        self,
        job_service: JobService,
        stream_manager: StreamManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_service = job_service
        self._stream_manager = stream_manager
        self._logger = logger or logging.getLogger(__name__)

    def get_available_plotters(
        self, job_number: JobNumber, output_name: str | None
    ) -> dict[str, PlotterSpec]:
        """Get all available plotters for a given job and output."""
        job_data = self._job_service.job_data[job_number]
        data = {k: v[output_name] for k, v in job_data.items()}
        return plotter_registry.get_compatible_plotters(data)

    def get_spec(self, plot_name: str) -> PlotterSpec:
        """Get the parameter model for a given plotter name."""
        return plotter_registry.get_spec(plot_name)

    def create_plot(
        self,
        job_number: JobNumber,
        source_names: list[str],
        output_name: str | None,
        plot_name: str,
        params: pydantic.BaseModel,
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
        plot = plotter_registry.create_plotter(plot_name, params=params)
        dmap = hv.DynamicMap(plot, streams=[pipe], cache_size=1).opts(shared_axes=False)
        return dmap
