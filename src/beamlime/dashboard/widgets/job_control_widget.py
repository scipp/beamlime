# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import panel as pn

from beamlime.core.job import JobAction
from beamlime.dashboard.job_controller import JobController


class JobControlWidget:
    def __init__(self, job_controller: JobController) -> None:
        self._job_controller = job_controller

        # Create UI components
        self._workflow_select = pn.widgets.Select(
            name="Workflow Filter",
            value=None,
            options={"All Workflows": None},
            width=300,
        )

        self._job_select = pn.widgets.MultiSelect(
            name="Jobs", value=[], options={}, height=200, width=400
        )

        # Action buttons
        self._pause_btn = pn.widgets.Button(
            name="Pause", button_type="primary", width=80
        )
        self._resume_btn = pn.widgets.Button(
            name="Resume", button_type="primary", width=80
        )
        self._reset_btn = pn.widgets.Button(name="Reset", button_type="light", width=80)
        self._stop_btn = pn.widgets.Button(name="Stop", button_type="primary", width=80)

        # Global action buttons
        self._global_pause_btn = pn.widgets.Button(
            name="Pause All", button_type="primary", width=100
        )
        self._global_resume_btn = pn.widgets.Button(
            name="Resume All", button_type="primary", width=100
        )
        self._global_reset_btn = pn.widgets.Button(
            name="Reset All", button_type="light", width=100
        )
        self._global_stop_btn = pn.widgets.Button(
            name="Stop All", button_type="primary", width=100
        )

        # Workflow action buttons
        self._workflow_pause_btn = pn.widgets.Button(
            name="Pause Workflow", button_type="primary", width=120
        )
        self._workflow_resume_btn = pn.widgets.Button(
            name="Resume Workflow", button_type="primary", width=120
        )
        self._workflow_reset_btn = pn.widgets.Button(
            name="Reset Workflow", button_type="light", width=120
        )
        self._workflow_stop_btn = pn.widgets.Button(
            name="Stop Workflow", button_type="primary", width=120
        )

        # Status display
        self._status_text = pn.pane.HTML("<div>Ready</div>", width=400, height=50)

        # Set up event handlers
        self._setup_callbacks()

        # Initialize data
        self._refresh_data()

        # Create layout
        self._layout = self._create_layout()

    def _setup_callbacks(self) -> None:
        """Set up event handlers for widgets."""
        self._workflow_select.param.watch(self._on_workflow_changed, 'value')

        # Job action callbacks
        self._pause_btn.on_click(lambda event: self._on_job_action(JobAction.pause))
        self._resume_btn.on_click(lambda event: self._on_job_action(JobAction.resume))
        self._reset_btn.on_click(lambda event: self._on_job_action(JobAction.reset))
        self._stop_btn.on_click(lambda event: self._on_job_action(JobAction.stop))

        # Global action callbacks
        self._global_pause_btn.on_click(
            lambda event: self._on_global_action(JobAction.pause)
        )
        self._global_resume_btn.on_click(
            lambda event: self._on_global_action(JobAction.resume)
        )
        self._global_reset_btn.on_click(
            lambda event: self._on_global_action(JobAction.reset)
        )
        self._global_stop_btn.on_click(
            lambda event: self._on_global_action(JobAction.stop)
        )

        # Workflow action callbacks
        self._workflow_pause_btn.on_click(
            lambda event: self._on_workflow_action(JobAction.pause)
        )
        self._workflow_resume_btn.on_click(
            lambda event: self._on_workflow_action(JobAction.resume)
        )
        self._workflow_reset_btn.on_click(
            lambda event: self._on_workflow_action(JobAction.reset)
        )
        self._workflow_stop_btn.on_click(
            lambda event: self._on_workflow_action(JobAction.stop)
        )

    def _create_layout(self) -> pn.Column:
        """Create the main widget layout."""
        return pn.Column(
            pn.pane.HTML("<h3>Job Control</h3>"),
            # Workflow selection section
            pn.Row(
                pn.Column(self._workflow_select, margin=(5, 10)),
                pn.Column(
                    pn.pane.HTML("<b>Workflow Actions:</b>"),
                    pn.Row(
                        self._workflow_pause_btn,
                        self._workflow_resume_btn,
                        self._workflow_reset_btn,
                        self._workflow_stop_btn,
                    ),
                    margin=(5, 10),
                ),
            ),
            # Job selection and actions section
            pn.Row(
                pn.Column(self._job_select, margin=(5, 10)),
                pn.Column(
                    pn.pane.HTML("<b>Selected Job Actions:</b>"),
                    pn.Row(
                        self._pause_btn,
                        self._resume_btn,
                        self._reset_btn,
                        self._stop_btn,
                    ),
                    margin=(5, 10, 0, 10),
                ),
            ),
            # Global actions section
            pn.Column(
                pn.pane.HTML("<b>Global Actions:</b>"),
                pn.Row(
                    self._global_pause_btn,
                    self._global_resume_btn,
                    self._global_reset_btn,
                    self._global_stop_btn,
                ),
                margin=(20, 10, 5, 10),
            ),
            # Status section
            pn.Column(
                pn.pane.HTML("<b>Status:</b>"), self._status_text, margin=(10, 10)
            ),
            width=600,
        )

    def _refresh_data(self) -> None:
        """Refresh workflow and job data from the controller."""
        # Update workflow options
        workflow_ids = self._job_controller.get_workflow_ids()
        workflow_options = {"All Workflows": None}
        workflow_options.update({str(wf_id): wf_id for wf_id in workflow_ids})
        self._workflow_select.options = workflow_options

        # Update job list based on current workflow selection
        self._update_job_list()

    def _update_job_list(self) -> None:
        """Update the job list based on current workflow selection."""
        selected_workflow = self._workflow_select.value
        job_ids = self._job_controller.get_job_ids(workflow_id=selected_workflow)

        # Create human-readable job options
        job_options = {}
        for job_id in job_ids:
            display_name = f"{job_id.job_number} - {job_id.source_name}"
            job_options[display_name] = job_id

        self._job_select.options = job_options
        self._job_select.value = []  # Clear selection when options change

    def _on_workflow_changed(self, event) -> None:
        """Handle workflow selection change."""
        self._update_job_list()

        # Update workflow action button states
        has_workflow = self._workflow_select.value is not None
        self._workflow_pause_btn.disabled = not has_workflow
        self._workflow_resume_btn.disabled = not has_workflow
        self._workflow_reset_btn.disabled = not has_workflow
        self._workflow_stop_btn.disabled = not has_workflow

    def _on_job_action(self, action: JobAction) -> None:
        """Handle job-specific actions."""
        selected_jobs = self._job_select.value
        if not selected_jobs:
            self._update_status("No jobs selected", "warning")
            return

        try:
            for job_id in selected_jobs:
                self._job_controller.send_job_action(job_id, action)

            job_count = len(selected_jobs)
            self._update_status(
                f"Sent {action.value} action to {job_count} job(s)", "success"
            )
        except Exception as e:
            self._update_status(f"Error sending job action: {e}", "error")

    def _on_workflow_action(self, action: JobAction) -> None:
        """Handle workflow-specific actions."""
        workflow_id = self._workflow_select.value
        if workflow_id is None:
            self._update_status("No workflow selected", "warning")
            return

        try:
            self._job_controller.send_workflow_action(workflow_id, action)
            self._update_status(
                f"Sent {action.value} action to workflow {workflow_id}", "success"
            )
        except Exception as e:
            self._update_status(f"Error sending workflow action: {e}", "error")

    def _on_global_action(self, action: JobAction) -> None:
        """Handle global actions."""
        try:
            self._job_controller.send_global_action(action)
            self._update_status(f"Sent global {action.value} action", "success")
        except Exception as e:
            self._update_status(f"Error sending global action: {e}", "error")

    def _update_status(self, message: str, status_type: str = "info") -> None:
        """Update the status display."""
        color_map = {
            "success": "green",
            "warning": "orange",
            "error": "red",
            "info": "blue",
        }
        color = color_map.get(status_type, "black")
        self._status_text.object = f'<div style="color: {color};">{message}</div>'

    def refresh(self) -> None:
        """Public method to refresh the widget data."""
        self._refresh_data()

    @property
    def panel(self) -> pn.Column:
        """Get the panel widget for display."""
        return self._layout
