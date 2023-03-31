import asyncio
from functools import partial

import plopp as pp
from colorama import Style
from scipp import DataArray

from ..core.application import BeamlimeApplicationInterface


class RealtimePlot(BeamlimeApplicationInterface):
    def __init__(
        self,
        config: dict = None,
        verbose: bool = False,
        verbose_option: str = Style.RESET_ALL,
    ) -> None:
        self._plottable_objects = dict()
        self._stream_nodes = dict()
        self._figs = dict()
        super().__init__(config, verbose, verbose_option)

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

    def process(self, new_data):
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
        return f"value updated with frame number {frame_number}"

    @staticmethod
    async def _run(self):
        await asyncio.sleep(2)
        new_data = await self.receive_data()
        while new_data is not None:
            await asyncio.sleep(0.5)
            result = self.process(new_data)
            print(result)
            new_data = await self.receive_data()
