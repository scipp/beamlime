# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowId,
)


@pytest.fixture
def sample_workflow_id() -> WorkflowId:
    return WorkflowId(
        instrument="INSTRUMENT",
        namespace="NAMESPACE",
        name="NAME",
        version=1,
    )


@pytest.fixture
def sample_workflow_config(sample_workflow_id: WorkflowId) -> WorkflowConfig:
    return WorkflowConfig(
        identifier=sample_workflow_id,
        params={"param1": 10, "param2": "value"},
    )


class TestPersistentWorkflowConfig:
    def test_ser_deser_empty(self) -> None:
        configs = PersistentWorkflowConfigs()
        dumped = configs.model_dump()
        loaded = PersistentWorkflowConfigs.model_validate(dumped)
        assert configs == loaded

    def test_ser_deser_single(
        self, sample_workflow_id: WorkflowId, sample_workflow_config: WorkflowConfig
    ) -> None:
        pwc = PersistentWorkflowConfig(
            source_names=["source1"], config=sample_workflow_config
        )
        configs = PersistentWorkflowConfigs(configs={sample_workflow_id: pwc})
        dumped = configs.model_dump()
        loaded = PersistentWorkflowConfigs.model_validate(dumped)
        assert configs == loaded

    def test_ser_deser_multiple(
        self, sample_workflow_id: WorkflowId, sample_workflow_config: WorkflowConfig
    ) -> None:
        pwc1 = PersistentWorkflowConfig(
            source_names=["source1"], config=sample_workflow_config
        )
        pwc2 = PersistentWorkflowConfig(
            source_names=["source2", "source3"],
            config=WorkflowConfig(
                identifier=WorkflowId(
                    instrument="INSTRUMENT2",
                    namespace="NAMESPACE2",
                    name="NAME2",
                    version=2,
                ),
                params={"paramA": 5.0},
            ),
        )
        configs = PersistentWorkflowConfigs(
            configs={sample_workflow_id: pwc1, pwc2.config.identifier: pwc2}
        )
        dumped = configs.model_dump()
        loaded = PersistentWorkflowConfigs.model_validate(dumped)
        assert configs == loaded
