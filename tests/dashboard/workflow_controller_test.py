# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
    WorkflowStatus,
    WorkflowStatusType,
)
from beamlime.dashboard.workflow_config_service import WorkflowConfigService
from beamlime.dashboard.workflow_controller import WorkflowController


class FakeWorkflowConfigService(WorkflowConfigService):
    """Fake service for testing WorkflowController."""

    def __init__(self):
        self._workflow_specs = WorkflowSpecs()
        self._persistent_configs = PersistentWorkflowConfigs()
        self._sent_configs: list[tuple[str, WorkflowConfig]] = []
        self._workflow_specs_callbacks: list[callable] = []
        self._status_callbacks: dict[str, list[callable]] = {}

    def get_workflow_specs(self) -> WorkflowSpecs:
        return self._workflow_specs

    def set_workflow_specs(self, specs: WorkflowSpecs) -> None:
        """Test helper to set workflow specs."""
        self._workflow_specs = specs
        for callback in self._workflow_specs_callbacks:
            callback(specs)

    def get_persistent_configs(self) -> PersistentWorkflowConfigs:
        return self._persistent_configs

    def save_persistent_configs(self, configs: PersistentWorkflowConfigs) -> None:
        self._persistent_configs = configs

    def send_workflow_config(self, source_name: str, config: WorkflowConfig) -> None:
        self._sent_configs.append((source_name, config))

    def subscribe_to_workflow_specs(self, callback: callable) -> None:
        self._workflow_specs_callbacks.append(callback)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: callable
    ) -> None:
        if source_name not in self._status_callbacks:
            self._status_callbacks[source_name] = []
        self._status_callbacks[source_name].append(callback)

    def simulate_status_update(self, status: WorkflowStatus) -> None:
        """Test helper to simulate status updates."""
        for callback in self._status_callbacks.get(status.source_name, []):
            callback(status)

    def get_sent_configs(self) -> list[tuple[str, WorkflowConfig]]:
        """Test helper to get sent configs."""
        return self._sent_configs.copy()

    def clear_sent_configs(self) -> None:
        """Test helper to clear sent configs."""
        self._sent_configs.clear()


@pytest.fixture
def source_names() -> list[str]:
    """Test source names."""
    return ["detector_1", "detector_2"]


@pytest.fixture
def workflow_id() -> WorkflowId:
    """Test workflow ID."""
    return "test_workflow"


@pytest.fixture
def workflow_spec(workflow_id: WorkflowId) -> WorkflowSpec:
    """Test workflow specification."""
    return WorkflowSpec(
        name="Test Workflow",
        description="A test workflow for unit testing",
        source_names=["detector_1", "detector_2"],
        parameters=[
            Parameter(
                name="threshold",
                description="Detection threshold",
                param_type=ParameterType.FLOAT,
                default=100.0,
                unit="counts",
            ),
            Parameter(
                name="mode",
                description="Processing mode",
                param_type=ParameterType.OPTIONS,
                default="fast",
                options=["fast", "accurate"],
            ),
        ],
    )


@pytest.fixture
def workflow_specs(
    workflow_id: WorkflowId, workflow_spec: WorkflowSpec
) -> WorkflowSpecs:
    """Test workflow specifications."""
    return WorkflowSpecs(workflows={workflow_id: workflow_spec})


@pytest.fixture
def fake_service() -> FakeWorkflowConfigService:
    """Fake service for testing."""
    return FakeWorkflowConfigService()


@pytest.fixture
def workflow_controller(
    fake_service: FakeWorkflowConfigService, source_names: list[str]
) -> tuple[WorkflowController, FakeWorkflowConfigService]:
    """Workflow controller instance for testing."""
    controller = WorkflowController(fake_service, source_names)
    return controller, fake_service


class TestWorkflowController:
    def test_start_workflow_sends_config_to_sources(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that start_workflow sends configuration to all specified sources."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {"threshold": 150.0, "mode": "accurate"}

        # Act
        result = controller.start_workflow(workflow_id, source_names, config)

        # Assert
        assert result is True
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == len(source_names)

        for source_name in source_names:
            # Find the config for this source
            source_config = next(
                (sc for sc in sent_configs if sc[0] == source_name), None
            )
            assert source_config is not None
            assert source_config[1].identifier == workflow_id
            assert source_config[1].values == config

    def test_start_workflow_saves_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that start_workflow saves persistent configuration."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {"threshold": 200.0, "mode": "fast"}

        # Act
        result = controller.start_workflow(workflow_id, source_names, config)

        # Assert
        assert result is True
        persistent_configs = service.get_persistent_configs()
        assert workflow_id in persistent_configs.configs

        workflow_config = persistent_configs.configs[workflow_id]
        assert workflow_config.source_names == source_names
        assert workflow_config.config.identifier == workflow_id
        assert workflow_config.config.values == config

    def test_start_workflow_updates_status_to_starting(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that start_workflow immediately updates status to STARTING."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {"threshold": 75.0}

        # Act
        result = controller.start_workflow(workflow_id, source_names, config)

        # Assert
        assert result is True
        all_status = controller.get_all_workflow_status()

        for source_name in source_names:
            status = all_status[source_name]
            assert status.source_name == source_name
            assert status.workflow_id == workflow_id
            assert status.status == WorkflowStatusType.STARTING

    def test_start_workflow_with_empty_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that start_workflow works with empty configuration."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {}

        # Act
        result = controller.start_workflow(workflow_id, source_names, config)

        # Assert
        assert result is True
        sent_configs = service.get_sent_configs()
        for _, workflow_config in sent_configs:
            assert workflow_config.identifier == workflow_id
            assert workflow_config.values == {}

    def test_start_workflow_with_single_source(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
    ):
        """Test that start_workflow works with a single source."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        single_source = ["detector_1"]
        config = {"threshold": 300.0}

        # Act
        result = controller.start_workflow(workflow_id, single_source, config)

        # Assert
        assert result is True
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 1
        assert sent_configs[0][0] == "detector_1"

        # Check status
        all_status = controller.get_all_workflow_status()
        assert all_status["detector_1"].status == WorkflowStatusType.STARTING
        assert all_status["detector_1"].workflow_id == workflow_id

    def test_start_workflow_returns_false_for_nonexistent_workflow(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        source_names: list[str],
    ):
        """Test that start_workflow returns False for non-existent workflow."""
        controller, service = workflow_controller
        nonexistent_workflow_id = "nonexistent_workflow"
        config = {"threshold": 100.0}

        # Act
        result = controller.start_workflow(
            nonexistent_workflow_id, source_names, config
        )

        # Assert
        assert result is False
        # Should not have sent any configs
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 0

    def test_persistent_config_stores_multiple_workflows(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that multiple workflow configurations can be stored persistently."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)

        workflow_id_1 = "workflow_1"
        workflow_id_2 = "workflow_2"
        config_1 = {"threshold": 100.0, "mode": "fast"}
        config_2 = {"threshold": 200.0, "mode": "accurate"}
        sources_1 = ["detector_1"]
        sources_2 = ["detector_2"]

        # Add specs for both workflows
        extended_specs = WorkflowSpecs(
            workflows={
                **workflow_specs.workflows,
                workflow_id_1: WorkflowSpec(
                    name="Workflow 1",
                    description="First workflow",
                    source_names=sources_1,
                ),
                workflow_id_2: WorkflowSpec(
                    name="Workflow 2",
                    description="Second workflow",
                    source_names=sources_2,
                ),
            }
        )
        service.set_workflow_specs(extended_specs)

        # Start both workflows
        result1 = controller.start_workflow(workflow_id_1, sources_1, config_1)
        result2 = controller.start_workflow(workflow_id_2, sources_2, config_2)

        # Assert
        assert result1 is True
        assert result2 is True
        persistent_configs = service.get_persistent_configs()
        assert len(persistent_configs.configs) == 2

        # Check first workflow config
        config_1_data = persistent_configs.configs[workflow_id_1]
        assert config_1_data.source_names == sources_1
        assert config_1_data.config.values == config_1

        # Check second workflow config
        config_2_data = persistent_configs.configs[workflow_id_2]
        assert config_2_data.source_names == sources_2
        assert config_2_data.config.values == config_2

    def test_persistent_config_replaces_existing_workflow(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
    ):
        """Test that starting a workflow replaces existing persistent configuration."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)

        # Start workflow with initial config
        initial_config = {"threshold": 100.0, "mode": "fast"}
        initial_sources = ["detector_1"]
        result1 = controller.start_workflow(
            workflow_id, initial_sources, initial_config
        )

        # Start same workflow with different config
        updated_config = {"threshold": 300.0, "mode": "accurate"}
        updated_sources = ["detector_1", "detector_2"]
        result2 = controller.start_workflow(
            workflow_id, updated_sources, updated_config
        )

        # Assert
        assert result1 is True
        assert result2 is True
        persistent_configs = service.get_persistent_configs()
        assert len(persistent_configs.configs) == 1

        # Should have the updated values
        workflow_config = persistent_configs.configs[workflow_id]
        assert workflow_config.source_names == updated_sources
        assert workflow_config.config.values == updated_config

    def test_cleanup_persistent_configs_removes_obsolete_workflows(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that cleanup removes configs for workflows that no longer exist."""
        controller, service = workflow_controller
        service.set_workflow_specs(
            WorkflowSpecs(
                workflows={
                    "workflow_1": WorkflowSpec(
                        name="Workflow 1",
                        description="First workflow",
                        source_names=["detector_1"],
                    ),
                    "workflow_2": WorkflowSpec(
                        name="Workflow 2",
                        description="Second workflow",
                        source_names=["detector_2"],
                    ),
                }
            )
        )

        # Start workflows to create persistent configs
        controller.start_workflow("workflow_1", ["detector_1"], {})
        controller.start_workflow("workflow_2", ["detector_2"], {})

        # Simulate workflow specs update with only one workflow remaining
        remaining_workflows = WorkflowSpecs(
            workflows={
                "workflow_1": WorkflowSpec(
                    name="Remaining Workflow",
                    description="Only this one remains",
                    source_names=["detector_1"],
                )
            }
        )
        service.set_workflow_specs(remaining_workflows)

        # Check that cleanup occurred
        persistent_configs = service.get_persistent_configs()
        assert "workflow_1" in persistent_configs.configs
        assert "workflow_2" not in persistent_configs.configs
        assert len(persistent_configs.configs) == 1

    def test_status_updates_from_service(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that controller handles status updates from service."""
        controller, service = workflow_controller

        # Simulate status update from service
        new_status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(new_status)

        # Check that controller received the update
        all_status = controller.get_all_workflow_status()
        assert all_status["detector_1"].status == WorkflowStatusType.RUNNING
        assert all_status["detector_1"].workflow_id == workflow_id
