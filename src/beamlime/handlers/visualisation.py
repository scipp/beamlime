# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from logging import Logger

import plopp as pp
from scipp import DataArray

from ..applications.interfaces import BeamlimeApplicationInterface
from ..communication.broker import CommunicationBroker


class RealtimePlot(BeamlimeApplicationInterface):
    def __init__(
        self,
        /,
        app_name: str,
        broker: CommunicationBroker = None,
        config: dict = None,
        logger: Logger = None,
        timeout: float = 1,
        wait_interval: float = 0.1,
    ) -> None:
        super().__init__(
            app_name=app_name,
            broker=broker,
            config=config,
            logger=logger,
            timeout=timeout,
            wait_interval=wait_interval,
        )
        self._plottable_objects = dict()
        self._stream_nodes = dict()
        self._figs = dict()

    def __del__(self) -> None:
        self.plottable_objects.clear()
        self.stream_nodes.clear()
        super().__del__()

    @property
    def plottable_objects(self) -> dict:
        return self._plottable_objects

    @property
    def stream_nodes(self) -> dict:
        return self._stream_nodes

    @property
    def figs(self) -> dict:
        return self._figs

    @staticmethod
    def handover(obj):
        return obj

    async def update_plot(self, new_data: dict) -> None:
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
        self.info("value updated with frame number %s", frame_number)

    async def _run(self):
        new_data = await self.get()
        while new_data is not None and await self.should_proceed(wait=False):
            self.info("Received new data. Updating plot ...")
            await self.update_plot(new_data)
            self.debug("Processed new data: %s", str(self.plottable_objects))
            new_data = await self.get()
        self.info("Finishing visualisation ...")
