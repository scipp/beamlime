# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable

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
        self._workflow_specs_callbacks: list[Callable[[WorkflowSpecs], None]] = []
        self._status_callbacks: dict[str, list[Callable[[WorkflowStatus], None]]] = {}

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

    def subscribe_to_workflow_specs(
        self, callback: Callable[[WorkflowSpecs], None]
    ) -> None:
        self._workflow_specs_callbacks.append(callback)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
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

        # Set up callback to capture status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)
        captured_status.clear()  # Clear initial callback

        # Act
        result = controller.start_workflow(workflow_id, source_names, config)

        # Assert
        assert result is True
        for source_name in source_names:
            status = captured_status[source_name]
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

        # Set up callback to capture status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)
        captured_status.clear()  # Clear initial callback

        # Act
        result = controller.start_workflow(workflow_id, single_source, config)

        # Assert
        assert result is True
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 1
        assert sent_configs[0][0] == "detector_1"

        # Check status
        assert captured_status["detector_1"].status == WorkflowStatusType.STARTING
        assert captured_status["detector_1"].workflow_id == workflow_id

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

        # Set up callback to capture status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)

        # Simulate status update from service
        new_status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(new_status)

        # Check that controller received the update
        assert captured_status["detector_1"].status == WorkflowStatusType.RUNNING
        assert captured_status["detector_1"].workflow_id == workflow_id

    def test_stop_workflow_for_source_sends_none_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that stop_workflow_for_source sends None workflow config."""
        controller, service = workflow_controller
        source_name = "detector_1"

        # Act
        controller.stop_workflow_for_source(source_name)

        # Assert
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 1
        assert sent_configs[0][0] == source_name
        assert sent_configs[0][1].identifier is None

    def test_stop_workflow_for_source_updates_status_to_stopping(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that stop_workflow_for_source updates status to STOPPING."""
        controller, service = workflow_controller
        source_name = "detector_1"

        # Set up callback to capture status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)
        captured_status.clear()  # Clear initial callback

        # Act
        controller.stop_workflow_for_source(source_name)

        # Assert
        assert captured_status[source_name].status == WorkflowStatusType.STOPPING
        assert captured_status[source_name].source_name == source_name

    def test_remove_workflow_for_source_resets_status(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that remove_workflow_for_source resets status to UNKNOWN."""
        controller, service = workflow_controller
        source_name = "detector_1"

        # Set up initial status
        initial_status = WorkflowStatus(
            source_name=source_name,
            workflow_id=workflow_id,
            status=WorkflowStatusType.STOPPED,
        )
        service.simulate_status_update(initial_status)

        # Set up callback to capture status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)
        captured_status.clear()  # Clear initial callback

        # Act
        controller.remove_workflow_for_source(source_name)

        # Assert
        assert captured_status[source_name].status == WorkflowStatusType.UNKNOWN
        assert captured_status[source_name].workflow_id is None

    def test_get_workflow_spec_returns_correct_spec(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_spec: WorkflowSpec,
        workflow_specs: WorkflowSpecs,
    ):
        """Test that get_workflow_spec returns the correct specification."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)

        # Act
        result = controller.get_workflow_spec(workflow_id)

        # Assert
        assert result == workflow_spec
        assert result.name == "Test Workflow"
        assert result.description == "A test workflow for unit testing"

    def test_get_workflow_spec_returns_none_for_nonexistent(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that get_workflow_spec returns None for non-existent workflow."""
        controller, service = workflow_controller

        # Act
        result = controller.get_workflow_spec("nonexistent_workflow")

        # Assert
        assert result is None

    def test_get_workflow_config_returns_persistent_config(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
        source_names: list[str],
    ):
        """Test that get_workflow_config returns saved persistent configuration."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {"threshold": 150.0, "mode": "accurate"}

        # Start workflow to create persistent config
        controller.start_workflow(workflow_id, source_names, config)

        # Act
        result = controller.get_workflow_config(workflow_id)

        # Assert
        assert result is not None
        assert result.source_names == source_names
        assert result.config.identifier == workflow_id
        assert result.config.values == config

    def test_get_workflow_config_returns_none_for_nonexistent(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that get_workflow_config returns None for non-existent workflow."""
        controller, service = workflow_controller

        # Act
        result = controller.get_workflow_config("nonexistent_workflow")

        # Assert
        assert result is None

    def test_subscribe_to_workflow_updates_calls_callback_on_specs_change(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
        workflow_spec: WorkflowSpec,
    ):
        """Test that workflow updates subscription works correctly."""
        controller, service = workflow_controller
        callback_called = []

        def test_callback(workflows: dict[WorkflowId, WorkflowSpec]):
            callback_called.append(workflows)

        controller.subscribe_to_workflow_updates(test_callback)
        assert len(callback_called) == 1
        # Initial callback should have empty workflows dict
        assert callback_called[0] == {}

        service.set_workflow_specs(workflow_specs)
        assert len(callback_called) == 2
        # Second callback should have the workflow specs
        assert len(callback_called[1]) == 1
        assert workflow_id in callback_called[1]
        assert callback_called[1][workflow_id] == workflow_spec

    def test_subscribe_to_workflow_status_updates_calls_callback_immediately(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
    ):
        """Test that status updates subscription calls callback immediately."""
        controller, service = workflow_controller
        callback_called = []

        def test_callback(all_status):
            callback_called.append(all_status)

        # Act - subscribe should trigger immediate callback
        controller.subscribe_to_workflow_status_updates(test_callback)

        # Assert
        assert len(callback_called) == 1
        # Should contain initial status for all sources
        assert len(callback_called[0]) == 2  # detector_1, detector_2

    def test_subscribe_to_workflow_status_updates_calls_callback_on_status_change(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that status updates subscription works correctly."""
        controller, service = workflow_controller
        callback_called = []

        def test_callback(all_status):
            callback_called.append(all_status)

        # Subscribe (will trigger immediate callback)
        controller.subscribe_to_workflow_status_updates(test_callback)
        initial_calls = len(callback_called)

        # Trigger status update
        status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(status)

        # Assert
        assert len(callback_called) == initial_calls + 1
        # Check that the status was updated
        latest_status = callback_called[-1]
        assert latest_status["detector_1"].status == WorkflowStatusType.RUNNING

    def test_controller_initializes_all_sources_with_unknown_status(
        self,
        fake_service: FakeWorkflowConfigService,
    ):
        """Test that controller initializes all sources with UNKNOWN status."""
        source_names = ["detector_1", "detector_2", "detector_3"]
        controller = WorkflowController(fake_service, source_names)

        # Set up callback to capture initial status
        captured_status = {}

        def capture_status(all_status):
            captured_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)

        # Assert
        assert len(captured_status) == 3
        for source_name in source_names:
            assert source_name in captured_status
            assert captured_status[source_name].status == WorkflowStatusType.UNKNOWN
            assert captured_status[source_name].source_name == source_name
            assert captured_status[source_name].workflow_id is None

    def test_workflow_specs_callback_exception_handling(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
    ):
        """Test that exceptions in workflow specs callbacks are handled gracefully."""
        controller, service = workflow_controller

        def failing_callback(workflows: dict[WorkflowId, WorkflowSpec]):
            raise Exception("Test exception")

        def working_callback(workflows: dict[WorkflowId, WorkflowSpec]):
            working_callback.called = True
            working_callback.received_workflows = workflows

        working_callback.called = False
        working_callback.received_workflows = {}

        # Subscribe both callbacks
        controller.subscribe_to_workflow_updates(failing_callback)
        controller.subscribe_to_workflow_updates(working_callback)

        # Reset after initial subscription calls
        working_callback.called = False

        # Trigger update - should not crash and should call working callback
        service.set_workflow_specs(workflow_specs)

        # Assert working callback was still called despite exception in failing one
        assert working_callback.called is True
        assert len(working_callback.received_workflows) == 1

    def test_workflow_status_callback_exception_handling(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that exceptions in workflow status callbacks are handled gracefully."""
        controller, service = workflow_controller

        def failing_callback(all_status: dict[str, WorkflowStatus]):
            raise Exception("Test exception")

        def working_callback(all_status: dict[str, WorkflowStatus]):
            working_callback.called = True
            working_callback.received_status = all_status

        working_callback.called = False
        working_callback.received_status = {}

        # Subscribe both callbacks
        controller.subscribe_to_workflow_status_updates(failing_callback)
        controller.subscribe_to_workflow_status_updates(working_callback)

        # Reset call count after initial subscription calls
        working_callback.called = False

        # Trigger status update - should not crash and should call working callback
        status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(status)

        # Assert working callback was still called despite exception in failing one
        assert working_callback.called is True
        assert "detector_1" in working_callback.received_status
        assert (
            working_callback.received_status["detector_1"].status
            == WorkflowStatusType.RUNNING
        )

    def test_multiple_status_subscriptions_work_correctly(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
    ):
        """Test that multiple status update subscriptions work correctly."""
        controller, service = workflow_controller
        callback1_calls = []
        callback2_calls = []

        def callback1(all_status: dict[str, WorkflowStatus]):
            callback1_calls.append(all_status)

        def callback2(all_status: dict[str, WorkflowStatus]):
            callback2_calls.append(all_status)

        # Subscribe both
        controller.subscribe_to_workflow_status_updates(callback1)
        controller.subscribe_to_workflow_status_updates(callback2)

        # Clear initial calls
        callback1_calls.clear()
        callback2_calls.clear()

        # Trigger update
        status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(status)

        # Assert both were called
        assert len(callback1_calls) == 1
        assert len(callback2_calls) == 1
        # Check that both received the same status and validate structure
        assert "detector_1" in callback1_calls[0]
        assert "detector_2" in callback1_calls[0]  # Should contain all sources
        assert callback1_calls[0]["detector_1"].status == WorkflowStatusType.RUNNING
        assert callback2_calls[0]["detector_1"].status == WorkflowStatusType.RUNNING
        # Verify the callbacks receive copies (not the same dict instance)
        assert callback1_calls[0] is not callback2_calls[0]

    def test_start_workflow_with_empty_source_names_list(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        workflow_specs: WorkflowSpecs,
    ):
        """Test that start_workflow works with empty source names list."""
        controller, service = workflow_controller
        service.set_workflow_specs(workflow_specs)
        config = {"threshold": 100.0}

        # Act
        result = controller.start_workflow(workflow_id, [], config)

        # Assert
        assert result is True
        sent_configs = service.get_sent_configs()
        assert len(sent_configs) == 0  # No configs sent to sources

        # Should still save persistent config
        persistent_configs = service.get_persistent_configs()
        assert workflow_id in persistent_configs.configs
        assert persistent_configs.configs[workflow_id].source_names == []

    def test_callback_receives_complete_workflow_status_dict(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_id: WorkflowId,
        source_names: list[str],
    ):
        """Test that status callbacks receive complete status dict for all sources."""
        controller, service = workflow_controller
        received_status = {}

        def capture_status(all_status: dict[str, WorkflowStatus]):
            received_status.update(all_status)

        controller.subscribe_to_workflow_status_updates(capture_status)

        # Verify initial state contains all sources
        assert len(received_status) == len(source_names)
        for source_name in source_names:
            assert source_name in received_status
            assert received_status[source_name].status == WorkflowStatusType.UNKNOWN

        # Clear and trigger update for one source
        received_status.clear()
        status = WorkflowStatus(
            source_name="detector_1",
            workflow_id=workflow_id,
            status=WorkflowStatusType.RUNNING,
        )
        service.simulate_status_update(status)

        # Should still receive status for all sources, not just the updated one
        assert len(received_status) == len(source_names)
        assert received_status["detector_1"].status == WorkflowStatusType.RUNNING
        assert received_status["detector_2"].status == WorkflowStatusType.UNKNOWN

    def test_callback_receives_workflow_specs_copy(
        self,
        workflow_controller: tuple[WorkflowController, FakeWorkflowConfigService],
        workflow_specs: WorkflowSpecs,
        workflow_id: WorkflowId,
    ):
        """Test that workflow specs callbacks receive a copy of the workflows dict."""
        controller, service = workflow_controller
        received_workflows = []

        def capture_workflows(workflows: dict[WorkflowId, WorkflowSpec]):
            received_workflows.append(workflows)

        controller.subscribe_to_workflow_updates(capture_workflows)
        service.set_workflow_specs(workflow_specs)

        # Should have received two callbacks (initial empty + update)
        assert len(received_workflows) == 2

        # Verify the second callback contains the expected workflow
        final_workflows = received_workflows[1]
        assert workflow_id in final_workflows

        # Verify it's a copy by modifying the received dict
        original_count = len(final_workflows)
        final_workflows["test_modification"] = workflow_specs.workflows[workflow_id]

        # Trigger another update to verify the modification didn't affect the controller
        service.set_workflow_specs(workflow_specs)
        assert len(received_workflows) == 3
        # The new callback should not contain our test modification
        assert "test_modification" not in received_workflows[2]
        assert len(received_workflows[2]) == original_count
