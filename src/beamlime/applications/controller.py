# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from ..applications.interfaces import BeamlimeApplicationInterface, ControlInterface
from ..empty_factory import empty_app_factory as app_factory
from .data_generator import DataGenerator
from .data_reduction import DataFeeder
from .data_visualization import DataPlotter


@app_factory.provider
class Controller(BeamlimeApplicationInterface):
    remote_ctrl: ControlInterface
    data_generator: DataGenerator
    data_feeder: DataFeeder
    data_plotter: DataPlotter

    def run(self):
        from asyncio import create_task, get_running_loop

        self.remote_ctrl.start()
        try:
            event_loop = get_running_loop()
        except RuntimeError:
            from asyncio import run, wait

            async def _run():
                await wait(
                    (
                        create_task(self.data_feeder.run()),
                        create_task(self.data_generator.run()),
                        create_task(self.data_plotter.run()),
                    )
                )

            run(_run())
        else:
            event_loop.create_task(self.data_feeder.run())
            event_loop.create_task(self.data_generator.run())
            event_loop.create_task(self.data_plotter.run())
