# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable
from typing import TypeVar

import holoviews as hv
import pydantic
from holoviews import opts, streams

from ess.livedata.config.workflow_spec import JobId, JobNumber, ResultKey

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
        self._box_streams: dict[ResultKey, streams.BoxEdit] = {}

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

    def get_result_key(
        self, job_number: JobNumber, source_name: str, output_name: str | None
    ) -> ResultKey:
        """Get the ResultKey for a given job number and source name."""
        workflow_id = self._job_service.job_info[job_number]
        return ResultKey(
            workflow_id=workflow_id,
            job_id=JobId(job_number=job_number, source_name=source_name),
            output_name=output_name,
        )

    def _log_box_stream(self, key: ResultKey, event) -> None:
        self._logger.info("Box stream for %s: %s", key, event.new)

    def create_plot(
        self,
        job_number: JobNumber,
        source_names: list[str],
        output_name: str | None,
        plot_name: str,
        params: pydantic.BaseModel,
    ) -> hv.DynamicMap:
        items = {
            self.get_result_key(
                job_number=job_number, source_name=source_name, output_name=output_name
            ): self._job_service.job_data[job_number][source_name][output_name]
            for source_name in source_names
        }
        pipe = self._stream_manager.make_merging_stream(items)
        plotter = plotter_registry.create_plotter(plot_name, params=params)
        # It seems we need to compose the box edit with the dynamic map, not what the
        # dmap wraps. However, this does not seem to work if it wraps hv.Overlay or
        # hv.Layout. For now we just bail.
        if len(source_names) != 1:
            return hv.DynamicMap(plotter, streams=[pipe], cache_size=1).opts(
                shared_axes=False
            )

        # We might want to "share" the boxes between equivalent plots, such as the
        # cumulative and current plot of a given detector bank. While sharing the
        # rectangles is no problem, the boxes drawn on a plot do not update when
        # making changes to the boxes in another plot.
        boxes = hv.Rectangles([])
        box_stream = streams.BoxEdit(
            source=boxes, num_objects=3, styles={'fill_color': ['red', 'green', 'blue']}
        )
        result_key = next(iter(items.keys()))
        box_stream.param.watch(
            lambda event: self._log_box_stream(result_key, event), 'data'
        )
        self._box_streams[result_key] = box_stream

        rect_opts = opts.Rectangles(fill_alpha=0.3)
        interactive_boxes = boxes.opts(rect_opts)
        return (
            hv.DynamicMap(plotter, streams=[pipe], cache_size=1).opts(shared_axes=False)
            * interactive_boxes
        )
