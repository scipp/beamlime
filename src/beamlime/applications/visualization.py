# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

import plopp as pp
from scipp import DataArray

from .interfaces import BeamlimeApplicationInterface


class RealtimePlot(BeamlimeApplicationInterface):
    def __init__(self, config: dict = None, logger=None, **kwargs) -> None:
        super().__init__(config, logger, **kwargs)
        self._plottable_objects = dict()
        self._stream_nodes = dict()
        self._figs = dict()

    def pause(self) -> None:
        pass

    def start(self) -> None:
        pass

    def resume(self) -> None:
        pass

    def __del__(self) -> None:
        self._plottable_objects.clear()
        self._stream_nodes.clear()

    def parse_config(self, config: dict) -> None:
        if config is None:
            pass

    @property
    def plottable_objects(self):
        return self._plottable_objects

    @property
    def stream_nodes(self):
        return self._stream_nodes

    @property
    def figs(self):
        return self._figs

    @staticmethod
    def handover(obj):
        return obj

    async def process(self, new_data):
        for workflow, result in new_data.items():
            if isinstance(result, DataArray):
                if workflow not in self.plottable_objects:
                    self.plottable_objects[workflow] = result
                    myfunc = partial(
                        self.handover, obj=self.plottable_objects[workflow]
                    )
                    self.stream_nodes[workflow] = pp.Node(myfunc)
                    self.figs[workflow] = pp.figure2d(self.stream_nodes[workflow])
                else:
                    self.plottable_objects[workflow].values = result.values
                    self.stream_nodes[workflow].notify_children("update")
        frame_number = new_data["frame-number-counting"]
        return self.info("value updated with frame number %s", frame_number)

    async def _run(self):
        new_data = await self.receive_data(timeout=20, wait_interval=1)
        while new_data is not None:
            self.info("Received new data. Updating plot ...")
            result = await self.process(new_data)
            self.debug("Processed new data: %s", str(result))
            new_data = await self.receive_data(timeout=20, wait_interval=1)
        self.info("Finishing visualisation ...")
