# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from beamlime.applications.interfaces import BeamlimeApplicationInterface


class DummyApp(BeamlimeApplicationInterface):
    def __del__(self):
        ...

    async def _run(self) -> None:
        ...
