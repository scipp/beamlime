# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime import StatelessWorkflow


def test_dummy_workflow_protocol() -> None:
    from pathlib import Path

    from beamlime.stateless_workflow import DummyWorkflow

    dummy_workflow = DummyWorkflow()
    result = dummy_workflow(
        nexus_filename=Path("test.nxs"),
        nxevent_data={"event1": {}, "event2": {}},
        nxlog={},
    )
    assert len(result) == 2
    assert result.keys() == {"event1", "event2"}
    assert isinstance(dummy_workflow, StatelessWorkflow)
