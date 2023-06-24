# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from ..applications.interfaces import BeamlimeApplicationInterface, ControlInterface
from ..empty_factory import empty_app_factory
from .daemons import DataPlotter, DataReduction, DataStreamSimulator


@empty_app_factory.provider
class Controller(BeamlimeApplicationInterface):
    remote_ctrl: ControlInterface
    data_generator: DataStreamSimulator
    data_reduction: DataReduction
    data_plotter: DataPlotter

    def run(self):
        from asyncio import create_task, get_running_loop

        daemons: list[BeamlimeApplicationInterface]
        daemons = [self.data_generator, self.data_reduction, self.data_plotter]
        self.remote_ctrl.start()
        try:
            event_loop = get_running_loop()
        except RuntimeError:
            from asyncio import run, wait

            async def _run():
                await wait([create_task(daemon.run()) for daemon in daemons])

            run(_run())
        else:
            for daemon in daemons:
                event_loop.create_task(daemon.run())
