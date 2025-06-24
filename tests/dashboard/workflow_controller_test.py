# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.models import ConfigKey
from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
    WorkflowStatusType,
)
from beamlime.dashboard.config_service import (
    ConfigSchemaManager,
    ConfigService,
    FakeMessageBridge,
)
from beamlime.dashboard.workflow_controller import WorkflowController
from beamlime.dashboard.workflow_controller_base import WorkflowControllerBase


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
def config_service_with_bridge():
    """Config service with fake message bridge for testing."""
    schemas = ConfigSchemaManager()
    bridge = FakeMessageBridge()
    service = ConfigService(schema_validator=schemas, message_bridge=bridge)
    return service, bridge


@pytest.fixture
def workflow_controller(
    config_service_with_bridge, source_names: list[str]
) -> tuple[WorkflowControllerBase, FakeMessageBridge]:
    """Workflow controller instance for testing."""
    service, bridge = config_service_with_bridge
    controller = WorkflowController(service, source_names)
    return controller, bridge


class TestWorkflowController:
    def test_start_workflow_publishes_config_to_sources(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that start_workflow publishes configuration to all specified sources."""
        controller, bridge = workflow_controller
        config = {"threshold": 150.0, "mode": "accurate"}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert - should have published WorkflowConfig to each source
        published_messages = bridge.get_published_messages()

        # Should have messages for each source plus persistent config
        assert len(published_messages) >= len(source_names)

        # Check that workflow config was sent to each source
        source_configs = [
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey) and msg[0].key == "workflow_config"
        ]
        assert len(source_configs) == len(source_names)

        for source_name in source_names:
            # Find the config message for this source
            source_config = next(
                msg for msg in source_configs if msg[0].source_name == source_name
            )
            config_data = source_config[1]
            assert config_data["identifier"] == workflow_id
            assert config_data["values"] == config

    def test_start_workflow_saves_persistent_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that start_workflow saves persistent configuration."""
        controller, bridge = workflow_controller
        config = {"threshold": 200.0, "mode": "fast"}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert - should have published persistent config
        published_messages = bridge.get_published_messages()

        # Find persistent config message
        persistent_config_msg = next(
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey)
            and msg[0].key == "persistent_workflow_configs"
        )

        persistent_data = persistent_config_msg[1]
        assert workflow_id in persistent_data["configs"]

        workflow_config = persistent_data["configs"][workflow_id]
        assert workflow_config["source_names"] == source_names
        assert workflow_config["config"]["identifier"] == workflow_id
        assert workflow_config["config"]["values"] == config

    def test_start_workflow_updates_status_to_starting(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that start_workflow immediately updates status to STARTING."""
        controller, _ = workflow_controller
        config = {"threshold": 75.0}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert - check status through the protocol interface
        all_status = controller.get_all_workflow_status()

        for source_name in source_names:
            status = all_status[source_name]
            assert status.source_name == source_name
            assert status.workflow_id == workflow_id
            assert status.status == WorkflowStatusType.STARTING

    def test_start_workflow_with_empty_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that start_workflow works with empty configuration."""
        controller, bridge = workflow_controller
        config = {}

        # Act
        controller.start_workflow(workflow_id, source_names, config)

        # Assert
        published_messages = bridge.get_published_messages()
        source_configs = [
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey) and msg[0].key == "workflow_config"
        ]

        for source_config in source_configs:
            config_data = source_config[1]
            assert config_data["identifier"] == workflow_id
            assert config_data["values"] == {}

    def test_start_workflow_with_single_source(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
    ):
        """Test that start_workflow works with a single source."""
        controller, bridge = workflow_controller
        single_source = ["detector_1"]
        config = {"threshold": 300.0}

        # Act
        controller.start_workflow(workflow_id, single_source, config)

        # Assert
        published_messages = bridge.get_published_messages()
        source_configs = [
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey) and msg[0].key == "workflow_config"
        ]

        assert len(source_configs) == 1
        assert source_configs[0][0].source_name == "detector_1"

        # Check status
        all_status = controller.get_all_workflow_status()
        assert all_status["detector_1"].status == WorkflowStatusType.STARTING
        assert all_status["detector_1"].workflow_id == workflow_id

    def test_persistent_config_stores_multiple_workflows(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        source_names: list[str],
    ):
        """Test that multiple workflow configurations can be stored persistently."""
        controller, bridge = workflow_controller

        workflow_id_1 = "workflow_1"
        workflow_id_2 = "workflow_2"
        config_1 = {"threshold": 100.0, "mode": "fast"}
        config_2 = {"threshold": 200.0, "mode": "accurate"}
        sources_1 = ["detector_1"]
        sources_2 = ["detector_2"]

        # Start first workflow
        controller.start_workflow(workflow_id_1, sources_1, config_1)
        bridge.clear()  # Clear to isolate second workflow messages

        # Start second workflow
        controller.start_workflow(workflow_id_2, sources_2, config_2)

        # Get the latest persistent config message
        published_messages = bridge.get_published_messages()
        persistent_config_msg = next(
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey)
            and msg[0].key == "persistent_workflow_configs"
        )

        persistent_data = persistent_config_msg[1]
        assert len(persistent_data["configs"]) == 2

        # Check first workflow config
        assert workflow_id_1 in persistent_data["configs"]
        config_1_data = persistent_data["configs"][workflow_id_1]
        assert config_1_data["source_names"] == sources_1
        assert config_1_data["config"]["values"] == config_1

        # Check second workflow config
        assert workflow_id_2 in persistent_data["configs"]
        config_2_data = persistent_data["configs"][workflow_id_2]
        assert config_2_data["source_names"] == sources_2
        assert config_2_data["config"]["values"] == config_2

    def test_persistent_config_replaces_existing_workflow(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
    ):
        """Test that starting a workflow replaces existing persistent configuration."""
        controller, bridge = workflow_controller

        # Start workflow with initial config
        initial_config = {"threshold": 100.0, "mode": "fast"}
        initial_sources = ["detector_1"]
        controller.start_workflow(workflow_id, initial_sources, initial_config)

        bridge.clear()

        # Start same workflow with different config
        updated_config = {"threshold": 300.0, "mode": "accurate"}
        updated_sources = ["detector_1", "detector_2"]
        controller.start_workflow(workflow_id, updated_sources, updated_config)

        # Get the latest persistent config
        published_messages = bridge.get_published_messages()
        persistent_config_msg = next(
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey)
            and msg[0].key == "persistent_workflow_configs"
        )

        persistent_data = persistent_config_msg[1]
        assert len(persistent_data["configs"]) == 1  # Only one config for this workflow

        # Should have the updated values
        workflow_config = persistent_data["configs"][workflow_id]
        assert workflow_config["source_names"] == updated_sources
        assert workflow_config["config"]["values"] == updated_config

    def test_get_initial_parameter_values_uses_persistent_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that get_initial_parameter_values uses saved persistent config."""
        controller, bridge = workflow_controller

        # Set up workflow specs in controller
        controller._on_workflow_specs_updated(workflow_specs)

        # Start workflow with custom config
        custom_config = {"threshold": 250.0, "mode": "accurate"}
        controller.start_workflow(workflow_id, source_names, custom_config)

        # Get initial values - should return the saved values, not defaults
        initial_values = controller.get_initial_parameter_values(workflow_id)

        assert initial_values["threshold"] == 250.0
        assert initial_values["mode"] == "accurate"

    def test_get_initial_parameter_values_uses_defaults_when_no_persistent_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
    ):
        """Test that get_initial_parameter_values uses defaults when no saved config."""
        controller, _ = workflow_controller

        # Set up workflow specs in controller
        controller._on_workflow_specs_updated(workflow_specs)

        # Get initial values without starting workflow first
        initial_values = controller.get_initial_parameter_values(workflow_id)

        # Should return defaults from workflow spec
        assert initial_values["threshold"] == 100.0  # Default from fixture
        assert initial_values["mode"] == "fast"  # Default from fixture

    def test_get_initial_source_names_uses_persistent_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
    ):
        """Test that get_initial_source_names returns saved source names."""
        controller, _ = workflow_controller
        saved_sources = ["detector_2"]
        config = {"threshold": 100.0}

        # Start workflow to save source names
        controller.start_workflow(workflow_id, saved_sources, config)

        # Get initial source names
        initial_sources = controller.get_initial_source_names(workflow_id)

        assert initial_sources == saved_sources

    def test_get_initial_source_names_returns_empty_when_no_persistent_config(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
        workflow_id: WorkflowId,
    ):
        """Test get_initial_source_names returns empty list when no saved config."""
        controller, _ = workflow_controller

        # Get initial source names without starting workflow first
        initial_sources = controller.get_initial_source_names(workflow_id)

        assert initial_sources == []

    def test_cleanup_persistent_configs_removes_obsolete_workflows(
        self,
        workflow_controller: tuple[WorkflowControllerBase, FakeMessageBridge],
    ):
        """Test that cleanup removes configs for workflows that no longer exist."""
        controller, bridge = workflow_controller

        # Start workflows to create persistent configs
        controller.start_workflow("workflow_1", ["detector_1"], {"param": "value1"})
        controller.start_workflow("workflow_2", ["detector_2"], {"param": "value2"})

        bridge.clear()

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

        controller._on_workflow_specs_updated(remaining_workflows)

        # Should have published updated persistent config without workflow_2
        published_messages = bridge.get_published_messages()
        persistent_config_msgs = [
            msg
            for msg in published_messages
            if isinstance(msg[0], ConfigKey)
            and msg[0].key == "persistent_workflow_configs"
        ]

        # Should have exactly one cleanup message
        assert len(persistent_config_msgs) == 1
        persistent_data = persistent_config_msgs[0][1]

        # Should only contain workflow_1, workflow_2 should be cleaned up
        assert "workflow_1" in persistent_data["configs"]
        assert "workflow_2" not in persistent_data["configs"]
        assert len(persistent_data["configs"]) == 1
