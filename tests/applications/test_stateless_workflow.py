# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable
from importlib.metadata import entry_points

import pytest

from beamlime.stateless_workflow import StatelessWorkflow

stateless_workflows = entry_points(group='beamlime.stateless')


def get_func_signature(func: Callable) -> dict[str, type]:
    from inspect import getfullargspec

    return {
        arg_name: arg_sig
        for arg_name, arg_sig in getfullargspec(func).annotations.items()
        if arg_name != 'self'
    }


@pytest.mark.parametrize(
    'workflow',
    [
        stateless_workflows[workflow_name].load()
        for workflow_name in stateless_workflows.names
    ],
)
def test_stateless_workflow(workflow: StatelessWorkflow) -> None:
    assert isinstance(workflow, StatelessWorkflow)

    # Manually check methods signature
    # since Protocol instance check does not include member signatures

    assert get_func_signature(workflow.__call__) == get_func_signature(
        StatelessWorkflow.__call__
    )
