# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Protocol

import scipp as sc
from scippneutron.io.nexus.load_nexus import JSONGroup

WorkflowResult = dict[str, sc.DataArray]


class StatelessWorkflow(Protocol):
    def __call__(self, group: JSONGroup) -> WorkflowResult:
        pass


class DummyWorkflow:
    """
    Dummy workflow for testing purposes, returning single result with random counts.
    """

    def __init__(self):
        import numpy as np

        self.rng = np.random.default_rng()
        self.x = sc.array(dims=['x'], values=np.arange(10), unit='m')

    def __call__(self, group: JSONGroup) -> WorkflowResult:
        return {
            'random counts': sc.DataArray(
                data=sc.array(dims=['x'], values=self.rng.random(10), unit='counts'),
                coords={'x': self.x},
            )
        }


def provide_stateless_workflow() -> StatelessWorkflow:
    # TODO implement plugin mechanism here, see #132
    return DummyWorkflow()
