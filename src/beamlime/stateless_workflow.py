# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from importlib.metadata import entry_points
from typing import NewType, Protocol

import scipp as sc
from scippneutron.io.nexus.load_nexus import JSONGroup

Workflow = NewType('Workflow', str)
'''Name of the workflow plugin to use'''

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


def provide_stateless_workflow(workflow: Workflow) -> StatelessWorkflow:
    stateless_workflows = entry_points(group='beamlime.stateless')
    return stateless_workflows[workflow].load()


dummy_workflow = DummyWorkflow()
