"""
Demo script for the reduction widget with fake data and controller.

Run with: python reduction_widget_demo.py
"""

from typing import Any

import panel as pn

from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
)
from beamlime.dashboard.widgets.reduction_widget import (
    ReductionWidget,
    WorkflowStatus,
)
from beamlime.dashboard.WorkflowController import WorkflowController


class FakeWorkflowController(WorkflowController):
    """Fake workflow controller for demonstration purposes."""

    def __init__(self) -> None:
        self._running_workflows: dict[str, WorkflowId] = {}
        self._workflow_status: dict[str, WorkflowStatus] = {}

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> None:
        """Start a workflow with given configuration."""
        print(f"Starting workflow '{workflow_id}' on sources {source_names}")  # noqa: T201
        print(f"Configuration: {config}")  # noqa: T201

        # Start workflow for each source name individually
        for source_name in source_names:
            self._running_workflows[source_name] = workflow_id
            self._workflow_status[source_name] = WorkflowStatus.RUNNING

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        print(f"Stopping workflow for source '{source_name}'")  # noqa: T201
        if source_name in self._running_workflows:
            self._workflow_status[source_name] = WorkflowStatus.STOPPED

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        print(f"Removing workflow for source '{source_name}'")  # noqa: T201
        if source_name in self._running_workflows:
            del self._running_workflows[source_name]
            del self._workflow_status[source_name]

    def get_running_workflows(self) -> dict[str, WorkflowId]:
        """Get currently running workflows mapped by source name."""
        return self._running_workflows.copy()

    def get_workflow_status(self, source_name: str) -> WorkflowStatus | None:
        """Get the status of a workflow for a specific source."""
        return self._workflow_status.get(source_name)


def create_sample_workflow_specs() -> WorkflowSpecs:
    """Create sample workflow specifications for demonstration."""

    # Powder diffraction workflow
    powder_params = [
        Parameter[int](
            name="num_bins",
            description="Number of time-of-flight bins",
            param_type=ParameterType.INT,
            default=1000,
            unit="bins",
        ),
        Parameter[float](
            name="d_spacing_min",
            description="Minimum d-spacing for binning",
            param_type=ParameterType.FLOAT,
            default=0.5,
            unit="Å",
        ),
        Parameter[float](
            name="d_spacing_max",
            description="Maximum d-spacing for binning",
            param_type=ParameterType.FLOAT,
            default=10.0,
            unit="Å",
        ),
        Parameter[bool](
            name="normalize_by_monitor",
            description="Normalize data by beam monitor",
            param_type=ParameterType.BOOL,
            default=True,
        ),
        Parameter[str](
            name="correction_method",
            description="Method for absorption correction",
            param_type=ParameterType.OPTIONS,
            default="PaalmanPings",
            options=["none", "PaalmanPings", "MonteCarloAbsorption"],
        ),
    ]

    powder_spec = WorkflowSpec(
        name="Powder Diffraction",
        description="Standard powder diffraction reduction with d-spacing binning",
        source_names=["detector_1", "detector_2", "detector_3", "detector_4"],
        parameters=powder_params,
    )

    # Single crystal workflow
    crystal_params = [
        Parameter[float](
            name="wavelength_min",
            description="Minimum wavelength for integration",
            param_type=ParameterType.FLOAT,
            default=0.8,
            unit="Å",
        ),
        Parameter[float](
            name="wavelength_max",
            description="Maximum wavelength for integration",
            param_type=ParameterType.FLOAT,
            default=5.0,
            unit="Å",
        ),
        Parameter[int](
            name="peak_radius",
            description="Peak integration radius",
            param_type=ParameterType.INT,
            default=3,
            unit="pixels",
        ),
        Parameter[str](
            name="background_method",
            description="Background subtraction method",
            param_type=ParameterType.OPTIONS,
            default="shell",
            options=["none", "shell", "planar"],
        ),
    ]

    crystal_spec = WorkflowSpec(
        name="Single Crystal",
        description="Single crystal diffraction with peak integration",
        source_names=["detector_1", "detector_2"],
        parameters=crystal_params,
    )

    # Simple monitoring workflow
    monitor_params = [
        Parameter[int](
            name="update_interval",
            description="Update interval for monitoring",
            param_type=ParameterType.INT,
            default=10,
            unit="seconds",
        ),
        Parameter[str](
            name="output_format",
            description="Format for output files",
            param_type=ParameterType.OPTIONS,
            default="nexus",
            options=["nexus", "hdf5", "ascii"],
        ),
    ]

    monitor_spec = WorkflowSpec(
        name="Live Monitoring",
        description="Real-time data monitoring and basic processing",
        source_names=[
            "detector_1",
            "detector_2",
            "detector_3",
            "detector_4",
            "monitor_1",
        ],
        parameters=monitor_params,
    )

    return WorkflowSpecs(
        workflows={
            "powder_diffraction": powder_spec,
            "single_crystal": crystal_spec,
            "live_monitoring": monitor_spec,
        }
    )


def create_sample_config() -> WorkflowConfig:
    """Create a sample initial configuration."""
    return WorkflowConfig(
        identifier="powder_diffraction",
        values={
            "num_bins": 2000,
            "d_spacing_min": 0.8,
            "d_spacing_max": 8.0,
            "normalize_by_monitor": True,
            "correction_method": "MonteCarloAbsorption",
        },
    )


def main():
    """Main function to create and serve the demo application."""
    # Enable Panel extensions
    pn.extension('tabulator', 'modal')

    # Create sample data
    workflow_specs = create_sample_workflow_specs()
    controller = FakeWorkflowController()
    initial_config = create_sample_config()

    # Create the reduction widget
    reduction_widget = ReductionWidget(
        workflow_specs=workflow_specs,
        controller=controller,
        initial_config=initial_config,
    )

    # Create button to simulate updating workflow specs
    def update_specs(event):
        """Simulate receiving new workflow specifications."""
        new_specs = create_sample_workflow_specs()
        del new_specs.workflows["powder_diffraction"]  # Remove one existing workflow
        # Add a new workflow to demonstrate updating
        new_specs.workflows["test_workflow"] = WorkflowSpec(
            name="Test Workflow",
            description="A test workflow added dynamically",
            source_names=["test_source"],
            parameters=[
                Parameter[int](
                    name="test_param",
                    description="A test parameter",
                    param_type=ParameterType.INT,
                    default=42,
                )
            ],
        )
        reduction_widget.update_workflow_specs(new_specs)

    update_button = pn.widgets.Button(
        name="Update Workflow Specs",
        button_type="light",
    )
    update_button.on_click(update_specs)

    # Create the main application layout
    app = pn.template.MaterialTemplate(
        title="Beamlime Reduction Widget Demo",
        sidebar=[
            pn.pane.HTML("""
            <h3>Demo Information</h3>
            <p>This is a demonstration of the Beamlime reduction widget.</p>
            <ul>
                <li>Select a workflow from the dropdown</li>
                <li>Click "Configure & Start" to open configuration modal</li>
                <li>Configure parameters and select sources in the modal</li>
                <li>Click "Start Workflow" to simulate running</li>
                <li>Use "Stop" to stop running workflows (they become grayed out)</li>
                <li>Use "Remove" to remove stopped workflows from the list</li>
                <li>Use "Update Workflow Specs" to simulate receiving new specifications</li>
            </ul>
            <p><strong>Note:</strong> This is a demo with fake data - no actual processing occurs.</p>
            """),  # noqa: E501
            update_button,
        ],
    )

    app.main.append(reduction_widget.widget)

    return app


if __name__ == "__main__":
    # Create and serve the application
    app = main()
    app.show(port=5007, autoreload=True)
