# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pydantic
import pytest

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
)
from beamlime.dashboard.widgets.workflow_ui_helper import WorkflowUIHelper


# Example Pydantic models for testing - matching the expected structure
class ThresholdParams(pydantic.BaseModel):
    value: float = 100.0
    unit: str = "counts"


class ModeParams(pydantic.BaseModel):
    setting: str = "fast"
    accuracy: float = 0.95


class EnabledParams(pydantic.BaseModel):
    active: bool = True
    priority: int = 1


class SomeParams(pydantic.BaseModel):
    threshold: ThresholdParams = pydantic.Field(default_factory=ThresholdParams)
    mode: ModeParams = pydantic.Field(default_factory=ModeParams)
    enabled: EnabledParams = pydantic.Field(default_factory=EnabledParams)


class IntParamGroup(pydantic.BaseModel):
    value: int = 42


class StrParamGroup(pydantic.BaseModel):
    value: str = "hello"


class FloatParamGroup(pydantic.BaseModel):
    value: float = 3.14


class BoolParamGroup(pydantic.BaseModel):
    value: bool = False


class OptionParamGroup(pydantic.BaseModel):
    value: str = "option2"


class MultiTypeParams(pydantic.BaseModel):
    int_param: IntParamGroup = pydantic.Field(default_factory=IntParamGroup)
    str_param: StrParamGroup = pydantic.Field(default_factory=StrParamGroup)
    float_param: FloatParamGroup = pydantic.Field(default_factory=FloatParamGroup)
    bool_param: BoolParamGroup = pydantic.Field(default_factory=BoolParamGroup)
    option_param: OptionParamGroup = pydantic.Field(default_factory=OptionParamGroup)


class NestedParamGroup(pydantic.BaseModel):
    nested_value: float = 1.0
    nested_flag: bool = True


class GroupParams(pydantic.BaseModel):
    simple_param: str = "test"


class NestedParams(pydantic.BaseModel):
    simple_param: GroupParams = pydantic.Field(default_factory=GroupParams)
    group: NestedParamGroup = pydantic.Field(default_factory=NestedParamGroup)


class MockWorkflowController:
    """Mock workflow controller for testing WorkflowUIHelper."""

    def __init__(self):
        self._specs: dict[WorkflowId, WorkflowSpec] = {}
        self._configs: dict[WorkflowId, PersistentWorkflowConfig] = {}
        self._params: dict[WorkflowId, type[pydantic.BaseModel]] = {}

    def get_workflow_specs(self) -> dict[WorkflowId, WorkflowSpec]:
        return self._specs.copy()

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        return self._specs.get(workflow_id)

    def get_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        return self._configs.get(workflow_id)

    def get_workflow_params(
        self, workflow_id: WorkflowId
    ) -> type[pydantic.BaseModel] | None:
        return self._params.get(workflow_id)

    def set_workflow_spec(self, workflow_id: WorkflowId, spec: WorkflowSpec) -> None:
        """Test helper to set workflow spec."""
        self._specs[workflow_id] = spec

    def set_workflow_config(
        self, workflow_id: WorkflowId, config: PersistentWorkflowConfig
    ) -> None:
        """Test helper to set workflow config."""
        self._configs[workflow_id] = config

    def set_workflow_params(
        self, workflow_id: WorkflowId, params: type[pydantic.BaseModel]
    ) -> None:
        """Test helper to set workflow parameters."""
        self._params[workflow_id] = params


@pytest.fixture
def mock_controller() -> MockWorkflowController:
    """Mock controller for testing."""
    return MockWorkflowController()


@pytest.fixture
def workflow_spec() -> WorkflowSpec:
    """Test workflow specification."""
    return WorkflowSpec(
        instrument="test_instrument",
        name="test_workflow",
        version=1,
        title="Test Workflow",
        description="A test workflow for unit testing",
        source_names=["detector_1", "detector_2"],
        params=SomeParams,
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
        options = ui_helper.make_workflow_options()

        assert len(options) == 1
        assert "--- Click to select a workflow ---" in options
        assert (
            options["--- Click to select a workflow ---"]
            is WorkflowUIHelper.no_selection
        )

    def test_make_workflow_options_with_single_spec(self, workflow_spec: WorkflowSpec):
        """Test get_workflow_options with a single workflow spec."""
        workflow_id = "test_workflow"

        options = WorkflowUIHelper.make_workflow_options({workflow_id: workflow_spec})

        assert len(options) == 2
        assert "--- Click to select a workflow ---" in options
        assert "Test Workflow" in options
        assert options["Test Workflow"] == workflow_id
        assert (
            options["--- Click to select a workflow ---"]
            is WorkflowUIHelper.no_selection
        )

    def test_make_workflow_options_with_multiple_specs(
        self,
    ):
        """Test get_workflow_options with multiple workflow specs."""
        spec1 = WorkflowSpec(
            instrument="test_instrument",
            name="Workflow One",
            version=1,
            title="Workflow One",
            description="First workflow",
            params=None,
        )
        spec2 = WorkflowSpec(
            instrument="test_instrument",
            name="Workflow Two",
            version=1,
            title="Workflow Two",
            description="Second workflow",
            params=None,
        )
        spec3 = WorkflowSpec(
            instrument="test_instrument",
            name="Another Workflow",
            version=1,
            title="Another Workflow",
            description="Third workflow",
            params=None,
        )

        specs = {"workflow_1": spec1, "workflow_2": spec2, "workflow_3": spec3}
        options = WorkflowUIHelper.make_workflow_options(specs)

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

        name = ui_helper.get_workflow_title(workflow_id)
        assert name == "Test Workflow"

    def test_get_workflow_name_with_nonexistent_workflow(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_workflow_name with non-existent workflow."""
        name = ui_helper.get_workflow_title("nonexistent_workflow")
        assert name == "nonexistent_workflow"

    def test_get_workflow_name_with_none(self, ui_helper: WorkflowUIHelper):
        """Test get_workflow_name with None."""
        name = ui_helper.get_workflow_title(None)
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

    def test_get_workflow_description_with_empty_workflow_description_returns_empty(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        workflow_id = "empty_desc_workflow"
        empty_spec = WorkflowSpec(
            instrument="test_instrument",
            name="worklow",
            version=1,
            title="Empty Description Workflow",
            description="",
            params=None,
        )
        mock_controller.set_workflow_spec(workflow_id, empty_spec)

        description = ui_helper.get_workflow_description(workflow_id)
        assert description == ""  # not None, but empty string

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
        spec = WorkflowSpec(
            instrument="test_instrument",
            name="workflow",
            version=1,
            title="No Description Workflow",
            description="",
            params=None,
        )
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
        workflow_id = "test_workflow"
        mock_controller.set_workflow_spec(workflow_id, workflow_spec)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        # Should return empty dict when no persistent config exists
        assert values == {}

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
                params={"threshold": 200.0, "mode": "accurate", "enabled": False},
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
                params={"threshold": 300.0},  # Only override threshold
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "threshold": 300.0,  # Override from persistent config
        }
        assert values == expected

    def test_get_initial_parameter_values_with_workflow_without_parameters(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_initial_parameter_values with workflow that has no parameters."""
        spec = WorkflowSpec(
            instrument="test_instrument",
            name="workflow",
            version=1,
            title="No Parameters Workflow",
            description="Workflow with no parameters",
            params=None,
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
            config=WorkflowConfig(identifier=workflow_id, params={}),
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
            config=WorkflowConfig(identifier=workflow_id, params={}),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        result = ui_helper.get_initial_source_names(workflow_id)
        assert result == []

    def test_get_workflow_model_class_with_existing_workflow(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_workflow_model_class with existing workflow."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        model_class = ui_helper.get_workflow_model_class(workflow_id)
        assert model_class is SomeParams

    def test_get_workflow_model_class_with_nonexistent_workflow(
        self, ui_helper: WorkflowUIHelper
    ):
        """Test get_workflow_model_class with non-existent workflow."""
        with pytest.raises(
            ValueError,
            match="Workflow parameters for 'nonexistent_workflow' are not defined",
        ):
            ui_helper.get_workflow_model_class("nonexistent_workflow")

    def test_get_parameter_widget_data_with_existing_workflow(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_parameter_widget_data with existing workflow."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        widget_data = ui_helper.get_parameter_widget_data(workflow_id)

        assert "threshold" in widget_data
        assert "mode" in widget_data
        assert "enabled" in widget_data

        # Check structure of widget data
        threshold_data = widget_data["threshold"]
        assert "field_type" in threshold_data
        assert threshold_data["field_type"] is ThresholdParams
        assert "values" in threshold_data
        assert "title" in threshold_data
        assert "description" in threshold_data

        # Check that values include defaults from the nested model
        assert threshold_data["values"]["value"] == 100.0
        assert threshold_data["values"]["unit"] == "counts"

    def test_get_parameter_widget_data_with_persistent_config(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_parameter_widget_data with persistent config values."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        # Set persistent config - this should override nested model values
        persistent_config = PersistentWorkflowConfig(
            source_names=["detector_1"],
            config=WorkflowConfig(
                identifier=workflow_id,
                params={"threshold": {"value": 250.0, "unit": "events"}},
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        widget_data = ui_helper.get_parameter_widget_data(workflow_id)

        # Should include persistent config values
        threshold_data = widget_data["threshold"]
        assert threshold_data["values"]["value"] == 250.0
        assert threshold_data["values"]["unit"] == "events"

    def test_assemble_parameter_values(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test assemble_parameter_values method."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        # Create mock parameter values - these should be the nested model instances
        threshold_params = ThresholdParams(value=200.0, unit="hits")
        mode_params = ModeParams(setting="accurate", accuracy=0.99)
        enabled_params = EnabledParams(active=False, priority=2)

        parameter_values = {
            "threshold": threshold_params,
            "mode": mode_params,
            "enabled": enabled_params,
        }

        result = ui_helper.assemble_parameter_values(workflow_id, parameter_values)

        assert isinstance(result, SomeParams)
        assert result.threshold.value == 200.0
        assert result.threshold.unit == "hits"
        assert result.mode.setting == "accurate"
        assert result.mode.accuracy == 0.99
        assert not result.enabled.active
        assert result.enabled.priority == 2

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

        options1 = helper1.make_workflow_options()
        options2 = helper2.make_workflow_options()

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
        workflow_id = "multi_type_workflow"
        mock_controller.set_workflow_params(workflow_id, MultiTypeParams)

        # Set persistent config with all parameter types
        persistent_config = PersistentWorkflowConfig(
            source_names=["detector_1"],
            config=WorkflowConfig(
                identifier=workflow_id,
                params={
                    "int_param": {"value": 42},
                    "str_param": {"value": "hello"},
                    "float_param": {"value": 3.14},
                    "bool_param": {"value": False},
                    "option_param": {"value": "option2"},
                },
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        expected = {
            "int_param": {"value": 42},
            "str_param": {"value": "hello"},
            "float_param": {"value": 3.14},
            "bool_param": {"value": False},
            "option_param": {"value": "option2"},
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
                params={
                    "threshold": {"value": 500.0, "unit": "counts"},
                    "extra_param": {"value": "extra_value"},  # Not in spec
                },
            ),
        )
        mock_controller.set_workflow_config(workflow_id, persistent_config)

        values = ui_helper.get_initial_parameter_values(workflow_id)

        # Should only include persistent config values
        expected = {
            "threshold": {"value": 500.0, "unit": "counts"},
            "extra_param": {"value": "extra_value"},
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

    def test_get_parameter_widget_data_with_nested_model(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_parameter_widget_data with nested Pydantic model."""
        workflow_id = "nested_workflow"
        mock_controller.set_workflow_params(workflow_id, NestedParams)

        widget_data = ui_helper.get_parameter_widget_data(workflow_id)

        assert "simple_param" in widget_data
        assert "group" in widget_data

        # Check that simple param field data is properly structured
        simple_data = widget_data["simple_param"]
        assert simple_data["field_type"] is GroupParams
        assert simple_data["values"]["simple_param"] == "test"

        # Check that nested field data is properly structured
        group_data = widget_data["group"]
        assert "field_type" in group_data
        assert group_data["field_type"] is NestedParamGroup
        assert "values" in group_data
        assert group_data["values"]["nested_value"] == 1.0
        assert group_data["values"]["nested_flag"]
        assert "title" in group_data
        assert "description" in group_data

    def test_get_parameter_widget_data_with_field_metadata(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_parameter_widget_data respects field metadata."""

        class MetadataParamGroup(pydantic.BaseModel):
            value: str = "default"

        class AnotherParamGroup(pydantic.BaseModel):
            value: str = "default"

        class ParamsWithMetadata(pydantic.BaseModel):
            param_with_title: MetadataParamGroup = pydantic.Field(
                default_factory=MetadataParamGroup,
                title="Custom Title",
                description="Custom description",
            )
            param_without_title: AnotherParamGroup = pydantic.Field(
                default_factory=AnotherParamGroup
            )

        workflow_id = "metadata_workflow"
        mock_controller.set_workflow_params(workflow_id, ParamsWithMetadata)

        widget_data = ui_helper.get_parameter_widget_data(workflow_id)

        # Check custom title and description
        titled_param = widget_data["param_with_title"]
        assert titled_param["title"] == "Custom Title"
        assert titled_param["description"] == "Custom description"

        # Check auto-generated title for field without explicit title
        untitled_param = widget_data["param_without_title"]
        assert (
            untitled_param["title"] == "Param Without Title"
        )  # Auto-generated from field name
        assert untitled_param["description"] is None

    def test_get_parameter_widget_data_structure_with_nested_models(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        """Test get_parameter_widget_data returns correct structure for nested."""
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        widget_data = ui_helper.get_parameter_widget_data(workflow_id)

        # Check that nested model fields have correct structure
        threshold_data = widget_data["threshold"]
        assert threshold_data["field_type"] is ThresholdParams
        # Values should contain defaults from the nested model
        assert threshold_data["values"]["value"] == 100.0
        assert threshold_data["values"]["unit"] == "counts"
        assert threshold_data["title"] == "Threshold"
        assert threshold_data["description"] is None

        mode_data = widget_data["mode"]
        assert mode_data["field_type"] is ModeParams
        assert mode_data["values"]["setting"] == "fast"
        assert mode_data["values"]["accuracy"] == 0.95

    def test_assemble_parameter_values_with_individual_values(
        self,
        ui_helper: WorkflowUIHelper,
        mock_controller: MockWorkflowController,
    ):
        workflow_id = "test_workflow"
        mock_controller.set_workflow_params(workflow_id, SomeParams)

        # Create parameter values as individual values (not model instances)
        parameter_values = {
            "threshold": {"value": 200.0, "unit": "hits"},
            "mode": {"setting": "accurate", "accuracy": 0.99},
            "enabled": {"active": False, "priority": 2},
        }

        result = ui_helper.assemble_parameter_values(workflow_id, parameter_values)

        assert isinstance(result, SomeParams)
        assert result.threshold.value == 200.0
        assert result.threshold.unit == "hits"
        assert result.mode.setting == "accurate"
        assert result.mode.accuracy == 0.99
        assert not result.enabled.active
        assert result.enabled.priority == 2
