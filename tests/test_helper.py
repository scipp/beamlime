# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from beamlime.applications.interfaces import BeamlimeApplicationInterface


class DummyApp(BeamlimeApplicationInterface):
    def parse_config(self, _: dict) -> None:
        ...

    async def _run(self) -> None:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...
