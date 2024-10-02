# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from collections.abc import Callable
from importlib.metadata import entry_points

import pytest

from beamlime import LiveWorkflow

stateless_workflows = entry_points(group='beamlime.workflow_plugin')


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
def test_stateless_workflow(workflow: LiveWorkflow) -> None:
    assert isinstance(workflow, LiveWorkflow)

    # Manually check methods signature
    # since Protocol instance check does not include member signatures
    constructor_signature = {
        # Can't use get_func_signature(LiveWorkflow.__init__)
        # Since LiveWorkflow is a Protocol and does not show __init__ signature
        'nexus_filename': pathlib.Path,
        'return': None,
    }
    assert get_func_signature(workflow.__init__) == constructor_signature
    assert get_func_signature(workflow.__call__) == get_func_signature(
        LiveWorkflow.__call__
    )
