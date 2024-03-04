# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Protocol
import scipp as sc
from scippneutron.io.nexus.load_nexus import JSONGroup


class StatelessWorkflow(Protocol):
    def __call__(self, group: JSONGroup) -> dict[str, sc.DataArray]:
        pass
