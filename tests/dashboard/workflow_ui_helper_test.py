# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    PersistentWorkflowConfig,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
)
from beamlime.dashboard.workflow_controller_base import WorkflowUIHelper


class MockWorkflowController:
    """Mock workflow controller for testing WorkflowUIHelper."""

    def __init__(self):
        self._specs: dict[WorkflowId, WorkflowSpec] = {}
        self._configs: dict[WorkflowId, PersistentWorkflowConfig] = {}

    def get_workflow_specs(self) -> dict[WorkflowId, WorkflowSpec]:
        return self._specs.copy()

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        return self._specs.get(workflow_id)

    def get_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        return self._configs.get(workflow_id)

    def set_workflow_spec(self, workflow_id: WorkflowId, spec: WorkflowSpec) -> None:
        """Test helper to set workflow spec."""
        self._specs[workflow_id] = spec

    def set_workflow_config(
        self, workflow_id: WorkflowId, config: PersistentWorkflowConfig
    ) -> None:
        """Test helper to set workflow config."""
        self._configs[workflow_id] = config


@pytest.fixture
def mock_controller() -> MockWorkflowController:
    """Mock controller for testing."""
    return MockWorkflowController()


@pytest.fixture
def workflow_spec() -> WorkflowSpec:
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
            Parameter(
                name="enabled",
                description="Enable processing",
                param_type=ParameterType.BOOL,
                default=True,
            ),
        ],
    )


@pytest.fixture
def ui_helper(mock_controller: MockWorkflowController) -> WorkflowUIHelper:
    """UI helper instance for testing."""
    return WorkflowUIHelper(mock_controller)


class TestWorkflowUIHelper:
    def test_no_selection_is_consistent_across_instances(
        self, mock_controller: MockWorkflowController
    ):
        """Test that no_selection object is consistent across different helper
        instances."""
        helper1 = WorkflowUIHelper(mock_controller)
        helper2 = WorkflowUIHelper(mock_controller)

        # Get no_selection from both helpers
        no_selection_1 = helper1.get_default_workflow_selection()
        no_selection_2 = helper2.get_default_workflow_selection()

        # They should be the same object (class attribute)
        assert no_selection_1 is no_selection_2
        assert no_selection_1 is WorkflowUIHelper.no_selection

        # is_no_selection should work across instances
        assert helper1.is_no_selection(no_selection_2) is True
        assert helper2.is_no_selection(no_selection_1) is True

    def test_get_workflow_options_with_empty_specs(self, ui_helper: WorkflowUIHelper):
        """Test get_workflow_options with no workflow specs."""
        options = ui_helper.get_workflow_options()

        assert len(options) == 1
        assert "--- Click to select a workflow ---" in options
        assert (
            options["--- Click to select a workflow ---"]
            is WorkflowUIHelper.no_selection
        )

    def test_get_workflow_options_with_single_spec(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_workflow_options with a single workflow spec."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        options = ui_helper.get_workflow_options()

        assert len(options) == 2
        assert "--- Click to select a workflow ---" in options
        assert "Test Workflow" in options
        assert options["Test Workflow"] == workflow_id
        assert (
            options["--- Click to select a workflow ---"]
            is WorkflowUIHelper.no_selection
        )

    def test_get_workflow_options_with_multiple_specs(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_workflow_options with multiple workflow specs."""
        spec1 = WorkflowSpec(name="Workflow One", description="First workflow")
        spec2 = WorkflowSpec(name="Workflow Two", description="Second workflow")
        spec3 = WorkflowSpec(name="Another Workflow", description="Third workflow")

        mock_controller.set_workflow_spec("workflow_1", spec1)
        mock_controller.set_workflow_spec("workflow_2", spec2)
        mock_controller.set_workflow_spec("workflow_3", spec3)

        options = ui_helper.get_workflow_options()

        assert len(options) == 4
        assert "--- Click to select a workflow ---" in options
        assert "Workflow One" in options
        assert "Workflow Two" in options
        assert "Another Workflow" in options
        assert options["Workflow One"] == "workflow_1"
        assert options["Workflow Two"] == "workflow_2"
        assert options["Another Workflow"] == "workflow_3"

    def test_is_no_selection_with_no_selection_value(self, ui_helper: WorkflowUIHelper):
        """Test is_no_selection returns True for no_selection object."""
        no_selection = ui_helper.get_default_workflow_selection()
        assert ui_helper.is_no_selection(no_selection) is True
        assert ui_helper.is_no_selection(WorkflowUIHelper.no_selection) is True

    def test_is_no_selection_with_workflow_id(self, ui_helper: WorkflowUIHelper):
        """Test is_no_selection returns False for actual workflow IDs."""
        assert ui_helper.is_no_selection("test_workflow") is False
        assert ui_helper.is_no_selection("") is False
        assert ui_helper.is_no_selection(None) is False

    def test_is_no_selection_with_other_objects(self, ui_helper: WorkflowUIHelper):
        """Test is_no_selection returns False for other objects."""
        assert ui_helper.is_no_selection("some_string") is False
        assert ui_helper.is_no_selection(123) is False
        assert ui_helper.is_no_selection([]) is False
        assert ui_helper.is_no_selection({}) is False
        assert ui_helper.is_no_selection(object()) is False

    def test_get_default_workflow_selection(self, ui_helper: WorkflowUIHelper):
        """Test get_default_workflow_selection returns no_selection object."""
        default = ui_helper.get_default_workflow_selection()
        assert default is WorkflowUIHelper.no_selection
        assert ui_helper.is_no_selection(default) is True

    def test_get_workflow_name_with_existing_workflow(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_workflow_name with existing workflow."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        name = ui_helper.get_workflow_name(workflow_id)
        assert name == "Test Workflow"

    def test_get_workflow_name_with_nonexistent_workflow(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_workflow_name with non-existent workflow."""
        name = ui_helper.get_workflow_name("nonexistent_workflow")
        assert name == "nonexistent_workflow"

    def test_get_workflow_name_with_none(self, ui_helper: WorkflowUIHelper):
        """Test get_workflow_name with None."""
        name = ui_helper.get_workflow_name(None)
        assert name == "None"

    def test_get_workflow_description_with_existing_workflow(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_workflow_description with existing workflow."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        description = ui_helper.get_workflow_description(workflow_id)
        assert description == "A test workflow for unit testing"

    def test_get_workflow_description_with_nonexistent_workflow(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_workflow_description with non-existent workflow."""
        description = ui_helper.get_workflow_description("nonexistent_workflow")
        assert description is None

    def test_get_workflow_description_with_no_selection(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_workflow_description with no_selection object."""
        no_selection = ui_helper.get_default_workflow_selection()
        description = ui_helper.get_workflow_description(no_selection)
        assert description is None

    def test_get_workflow_description_with_workflow_spec_without_description(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_workflow_description with workflow spec that has no description."""
        spec = WorkflowSpec(name="No Description Workflow", description="")
        workflow_id = "no_desc_workflow"
        mock_controller.set_workflow_spec(workflow_id, spec)

        description = ui_helper.get_workflow_description(workflow_id)
        assert description == ""

    def test_get_initial_parameter_values_with_existing_workflow(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_initial_parameter_values with existing workflow."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "threshold": 100.0,
            "mode": "fast",
            "enabled": True,
        }
        assert values == expected

    def test_get_initial_parameter_values_with_nonexistent_workflow(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_initial_parameter_values with non-existent workflow."""
        values = ui_helper.get_initial_parameter_values("nonexistent_workflow")
        assert values == {}

    def test_get_initial_parameter_values_with_persistent_config_override(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_initial_parameter_values with persistent config override."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        # Set persistent config with different values
        persistent_config = PersistentWorkflowConfig(
            source_names=["detector_1"],
            config=WorkflowConfig(
                identifier=workflow_id,
                values={"threshold": 200.0, "mode": "accurate", "enabled": False},
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "threshold": 200.0,  # Override from persistent config
            "mode": "accurate",  # Override from persistent config
            "enabled": False,  # Override from persistent config
        }
        assert values == expected

    def test_get_initial_parameter_values_with_partial_persistent_config_override(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test get_initial_parameter_values with partial persistent config override."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        # Set persistent config with only some values
        persistent_config = PersistentWorkflowConfig(
            source_names=["detector_1"],
            config=WorkflowConfig(
                identifier=workflow_id,
                values={"threshold": 300.0},  # Only override threshold
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "threshold": 300.0,  # Override from persistent config
            "mode": "fast",  # Default from spec
            "enabled": True,  # Default from spec
        }
        assert values == expected

    def test_get_initial_parameter_values_with_workflow_without_parameters(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_initial_parameter_values with workflow that has no parameters."""
        spec = WorkflowSpec(
            name="No Parameters Workflow",
            description="Workflow with no parameters",
            parameters=[],
        )
        workflow_id = "no_params_workflow"
        mock_controller.set_workflow_spec(workflow_id, spec)

        values = ui_helper.get_initial_parameter_values(workflow_id)
        assert values == {}

    def test_get_initial_source_names_with_persistent_config(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_initial_source_names with persistent config."""
        workflow_id = "test_workflow"
        source_names = ["detector_1", "detector_3"]

        persistent_config = PersistentWorkflowConfig(
            source_names=source_names,
            config=WorkflowConfig(identifier=workflow_id, values={}),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        result = ui_helper.get_initial_source_names(workflow_id)
        assert result == source_names

    def test_get_initial_source_names_without_persistent_config(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_initial_source_names without persistent config."""
        result = ui_helper.get_initial_source_names("nonexistent_workflow")
        assert result == []

    def test_get_initial_source_names_with_empty_source_names(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_initial_source_names with empty source names in persistent
        config."""
        workflow_id = "test_workflow"

        persistent_config = PersistentWorkflowConfig(
            source_names=[],
            config=WorkflowConfig(identifier=workflow_id, values={}),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        result = ui_helper.get_initial_source_names(workflow_id)
        assert result == []

    def test_different_helper_instances_share_no_selection_state(
        self, mock_controller: MockWorkflowController
    ):
        """Test that different helper instances share the same no_selection state."""
        helper1 = WorkflowUIHelper(mock_controller)
        helper2 = WorkflowUIHelper(mock_controller)
        helper3 = WorkflowUIHelper(mock_controller)

        # Get no_selection from different helpers
        no_sel_1 = helper1.get_default_workflow_selection()
        no_sel_2 = helper2.get_default_workflow_selection()
        no_sel_3 = helper3.get_default_workflow_selection()

        # All should be the same object
        assert no_sel_1 is no_sel_2
        assert no_sel_2 is no_sel_3
        assert no_sel_1 is WorkflowUIHelper.no_selection

        # Cross-helper is_no_selection should work
        assert helper1.is_no_selection(no_sel_2) is True
        assert helper2.is_no_selection(no_sel_3) is True
        assert helper3.is_no_selection(no_sel_1) is True

    def test_workflow_options_consistency_across_instances(
        self,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test that workflow options are consistent across helper instances."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        helper1 = WorkflowUIHelper(mock_controller)
        helper2 = WorkflowUIHelper(mock_controller)

        options1 = helper1.get_workflow_options()
        options2 = helper2.get_workflow_options()

        # Options should be equal
        assert options1 == options2

        # no_selection objects should be identical
        no_sel_1 = next(iter(options1.values()))  # First value is no_selection
        no_sel_2 = next(iter(options2.values()))
        assert no_sel_1 is no_sel_2

    def test_parameter_values_with_various_parameter_types(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_initial_parameter_values with various parameter types."""
        spec = WorkflowSpec(
            name="Multi-Type Parameters Workflow",
            description="Workflow with various parameter types",
            parameters=[
                Parameter(
                    name="int_param",
                    description="Integer parameter",
                    param_type=ParameterType.INT,
                    default=42,
                ),
                Parameter(
                    name="str_param",
                    description="String parameter",
                    param_type=ParameterType.STRING,
                    default="hello",
                ),
                Parameter(
                    name="float_param",
                    description="Float parameter",
                    param_type=ParameterType.FLOAT,
                    default=3.14,
                ),
                Parameter(
                    name="bool_param",
                    description="Boolean parameter",
                    param_type=ParameterType.BOOL,
                    default=False,
                ),
                Parameter(
                    name="option_param",
                    description="Options parameter",
                    param_type=ParameterType.OPTIONS,
                    default="option2",
                    options=["option1", "option2", "option3"],
                ),
            ],
        )
        workflow_id = "multi_type_workflow"
        mock_controller.set_workflow_spec(workflow_id, spec)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "int_param": 42,
            "str_param": "hello",
            "float_param": 3.14,
            "bool_param": False,
            "option_param": "option2",
        }
        assert values == expected

    def test_persistent_config_overrides_with_extra_parameters(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
        workflow_spec: WorkflowSpec,
    ):
        """Test persistent config with extra parameters not in spec."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        # Persistent config with extra parameter
        persistent_config = PersistentWorkflowConfig(
            source_names=["detector_1"],
            config=WorkflowConfig(
                identifier=workflow_id,
                values={
                    "threshold": 500.0,
                    "extra_param": "extra_value",  # Not in spec
                },
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        # Should include both spec defaults and persistent config values
        expected = {
            "threshold": 500.0,  # Override from persistent config
            "mode": "fast",  # Default from spec
            "enabled": True,  # Default from spec
            "extra_param": "extra_value",  # Extra from persistent config
        }
        assert values == expected

    def test_no_selection_object_identity_after_class_access(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test that no_selection object identity is maintained after direct
        class access."""
        # Access via class attribute
        class_no_selection = WorkflowUIHelper.no_selection

        # Access via instance method
        instance_no_selection = ui_helper.get_default_workflow_selection()

        # Should be the same object
        assert class_no_selection is instance_no_selection

        # is_no_selection should work with both
        assert ui_helper.is_no_selection(class_no_selection) is True
        assert ui_helper.is_no_selection(instance_no_selection) is True
