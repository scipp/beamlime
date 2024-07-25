# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from importlib.metadata import entry_points
from typing import NewType, Protocol

try:
    import scipp as sc
    from scippneutron.io.nexus.load_nexus import JSONGroup
except ImportError as e:
    raise ImportError(
        "Please install the scipp and "
        "scippneutron packages to use the ``DummyWorkflow``."
    ) from e


Workflow = NewType('Workflow', str)
'''Name of the workflow plugin to use'''

WorkflowResult = dict[str, sc.DataArray]
"""Result of a workflow, a dictionary of scipp DataArrays."""


class StatelessWorkflow(Protocol):
    """
    Protocol for stateless workflows.

    Can be implemented by a class or a function in plugins, in the
    `beamlime.stateless` entry point group.
    """

    def __call__(
        self,
        nexus_filename: Path,
        nxevent_data: dict[str, JSONGroup],
        nxlog: dict[str, JSONGroup],
    ) -> WorkflowResult:
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
            f'random-counts-{i}': sc.DataArray(
                data=sc.array(dims=['x'], values=self.rng.random(10), unit='counts'),
                coords={'x': self.x},
            )
            for i in range(4)
        }


def provide_stateless_workflow(workflow: Workflow) -> StatelessWorkflow:
    stateless_workflows = entry_points(group='beamlime.stateless')
    return stateless_workflows[workflow].load()


dummy_workflow = DummyWorkflow()
