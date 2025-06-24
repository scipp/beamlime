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
from beamlime.dashboard.workflow_controller import (
    WorkflowConfigService,
    WorkflowController,
)


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
        source_names: list[str],
    ):
        """Test that start_workflow sends configuration to all specified sources."""
        controller, service = workflow_controller
        config = {"threshold": 150.0, "mode": "accurate"}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert
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
        source_names: list[str],
    ):
        """Test that start_workflow saves persistent configuration."""
        controller, service = workflow_controller
        config = {"threshold": 200.0, "mode": "fast"}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert
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
        source_names: list[str],
    ):
        """Test that start_workflow immediately updates status to STARTING."""
        controller, service = workflow_controller
        config = {"threshold": 75.0}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert
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
        source_names: list[str],
    ):
        """Test that start_workflow works with empty configuration."""
        controller, service = workflow_controller
        config = {}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert
        sent_configs = service.get_sent_configs()
        for _, workflow_config in sent_configs:
            assert workflow_config.identifier == workflow_id
            assert workflow_config.values == {}

    def test_start_workflow_with_single_source(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that start_workflow works with a single source."""
        controller, service = workflow_controller
        single_source = ["detector_1"]
        config = {"threshold": 300.0}

        # Act
        controller.start_workflow(workflow_id, single_source, config)

        # Assert
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 1
        assert sent_configs[0][0] == "detector_1"

        # Check status
        all_status = controller.get_all_workflow_status()
        assert all_status["detector_1"].status == WorkflowStatusType.STARTING
        assert all_status["detector_1"].workflow_id == workflow_id

    def test_persistent_config_stores_multiple_workflows(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        source_names: list[str],
    ):
        """Test that multiple workflow configurations can be stored persistently."""
        controller, service = workflow_controller

        workflow_id_1 = "workflow_1"
        workflow_id_2 = "workflow_2"
        config_1 = {"threshold": 100.0, "mode": "fast"}
        config_2 = {"threshold": 200.0, "mode": "accurate"}
        sources_1 = ["detector_1"]
        sources_2 = ["detector_2"]

        # Start both workflows
        controller.start_workflow(workflow_id_1, sources_1, config_1)
        controller.start_workflow(workflow_id_2, sources_2, config_2)

        # Assert
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
    ):
        """Test that starting a workflow replaces existing persistent configuration."""
        controller, service = workflow_controller

        # Start workflow with initial config
        initial_config = {"threshold": 100.0, "mode": "fast"}
        initial_sources = ["detector_1"]
        controller.start_workflow(workflow_id, initial_sources, initial_config)

        # Start same workflow with different config
        updated_config = {"threshold": 300.0, "mode": "accurate"}
        updated_sources = ["detector_1", "detector_2"]
        controller.start_workflow(workflow_id, updated_sources, updated_config)

        # Assert
        persistent_configs = service.get_persistent_configs()
        assert len(persistent_configs.configs) == 1

        # Should have the updated values
        workflow_config = persistent_configs.configs[workflow_id]
        assert workflow_config.source_names == updated_sources
        assert workflow_config.config.values == updated_config

    def test_get_initial_parameter_values_uses_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that get_initial_parameter_values uses saved persistent config."""
        controller, service = workflow_controller

        # Set up workflow specs
        service.set_workflow_specs(workflow_specs)

        # Start workflow with custom config
        custom_config = {"threshold": 250.0, "mode": "accurate"}
        controller.start_workflow(workflow_id, source_names, custom_config)

        # Get initial values - should return the saved values, not defaults
        initial_values = controller.get_initial_parameter_values(workflow_id)

        assert initial_values["threshold"] == 250.0
        assert initial_values["mode"] == "accurate"

    def test_get_initial_parameter_values_uses_defaults_when_no_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
    ):
        """Test that get_initial_parameter_values uses defaults when no saved config."""
        controller, service = workflow_controller

        # Set up workflow specs
        service.set_workflow_specs(workflow_specs)

        # Get initial values without starting workflow first
        initial_values = controller.get_initial_parameter_values(workflow_id)

        # Should return defaults from workflow spec
        assert initial_values["threshold"] == 100.0  # Default from fixture
        assert initial_values["mode"] == "fast"  # Default from fixture

    def test_get_initial_source_names_uses_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that get_initial_source_names returns saved source names."""
        controller, service = workflow_controller
        saved_sources = ["detector_2"]
        config = {"threshold": 100.0}

        # Start workflow to save source names
        controller.start_workflow(workflow_id, saved_sources, config)

        # Get initial source names
        initial_sources = controller.get_initial_source_names(workflow_id)

        assert initial_sources == saved_sources

    def test_get_initial_source_names_returns_empty_when_no_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test get_initial_source_names returns empty list when no saved config."""
        controller, service = workflow_controller

        # Get initial source names without starting workflow first
        initial_sources = controller.get_initial_source_names(workflow_id)

        assert initial_sources == []

    def test_cleanup_persistent_configs_removes_obsolete_workflows(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that cleanup removes configs for workflows that no longer exist."""
        controller, service = workflow_controller

        # Start workflows to create persistent configs
        controller.start_workflow("workflow_1", ["detector_1"], {"param": "value1"})
        controller.start_workflow("workflow_2", ["detector_2"], {"param": "value2"})

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
