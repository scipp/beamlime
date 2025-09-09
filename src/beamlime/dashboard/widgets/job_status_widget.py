# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from datetime import UTC, datetime

import panel as pn
import param

from beamlime.core.job import JobState, JobStatus
from beamlime.dashboard.job_service import JobService


class JobStatusWidget(param.Parameterized):
    """Widget to display the status of a job."""

    expanded = param.Boolean(default=False, doc="Whether error details are expanded")

    def __init__(self, job_status: JobStatus, **params) -> None:
        super().__init__(**params)
        self._job_status = job_status
        self._setup_widgets()

    def _setup_widgets(self) -> None:
        """Set up the UI components."""
        # Job info row
        source_name = self._job_status.job_id.source_name
        job_number_short = str(self._job_status.job_id.job_number)[:8]
        job_id_text = f"{source_name} ({job_number_short}...)"

        workflow_name = self._job_status.workflow_id.name
        workflow_version = self._job_status.workflow_id.version
        workflow_text = f"{workflow_name} v{workflow_version}"

        self._job_info = pn.pane.HTML(
            f"<b>{job_id_text}</b> | {workflow_text}",
            width=300,
            height=25,
            margin=(5, 10),
        )

        # Status indicator with color
        status_color = self._get_status_color(self._job_status.state)
        status_text = self._job_status.state.value.upper()
        status_style = (
            f"background-color: {status_color}; color: white; "
            f"padding: 2px 8px; border-radius: 3px; text-align: center; "
            f"font-weight: bold;"
        )
        self._status_indicator = pn.pane.HTML(
            f'<div style="{status_style}">{status_text}</div>',
            width=90,
            height=25,
            margin=(5, 5),
        )

        # Timing info
        timing_text = self._format_timing()
        self._timing_info = pn.pane.HTML(
            timing_text, width=150, height=25, margin=(5, 5)
        )

        # Action buttons placeholder (for future stop/reset buttons)
        self._action_buttons = pn.Spacer(width=100, height=25, margin=(5, 5))

        # Error/warning message handling
        self._error_brief = None
        self._error_details = None
        self._expand_button = None

        if self._job_status.has_error or self._job_status.has_warning:
            self._setup_error_display()

    def _get_status_color(self, state: JobState) -> str:
        """Get color for job state."""
        color_map = {
            JobState.scheduled: "#6c757d",  # Gray
            JobState.active: "#28a745",  # Green
            JobState.paused: "#ffc107",  # Yellow
            JobState.finishing: "#17a2b8",  # Blue
            JobState.error: "#dc3545",  # Red
            JobState.warning: "#fd7e14",  # Orange
        }
        return color_map.get(state, "#6c757d")

    def _format_timing(self) -> str:
        """Format timing information."""
        if self._job_status.start_time is None:
            return "Not started"

        start_dt = datetime.fromtimestamp(self._job_status.start_time / 1e9, tz=UTC)
        start_str = start_dt.strftime("%H:%M:%S")

        if self._job_status.end_time is not None:
            duration = (self._job_status.end_time - self._job_status.start_time) / 1e9
            return f"{start_str} ({duration:.1f}s)"
        else:
            return f"Started: {start_str}"

    def _setup_error_display(self) -> None:
        """Set up error/warning message display."""
        message = self._job_status.error_message or self._job_status.warning_message
        if not message:
            return

        # Get first line for brief display
        brief_message = message.split('\n')[0]
        if len(brief_message) > 60:
            brief_message = brief_message[:57] + "..."

        # Brief error display
        error_color = "#dc3545" if self._job_status.has_error else "#fd7e14"
        brief_html = (
            f'<span style="color: {error_color}; font-size: 12px;">'
            f'{brief_message}</span>'
        )
        self._error_brief = pn.pane.HTML(
            brief_html, width=400, height=20, margin=(0, 10)
        )

        # Expand button if message is longer than brief
        if len(message) > len(brief_message) or '\n' in message:
            self._expand_button = pn.widgets.Button(
                name="⊞" if not self.expanded else "⊟",
                button_type="light",
                width=25,
                height=20,
                margin=(0, 5),
            )
            self._expand_button.on_click(self._toggle_error_details)

        # Full error details (initially hidden)
        if self.expanded:
            self._show_error_details(message)

    def _toggle_error_details(self, event) -> None:
        """Toggle the display of full error details."""
        self.expanded = not self.expanded
        self._expand_button.name = "⊟" if self.expanded else "⊞"

        message = self._job_status.error_message or self._job_status.warning_message
        if self.expanded and message:
            self._show_error_details(message)
        else:
            self._hide_error_details()

    def _show_error_details(self, message: str) -> None:
        """Show full error details."""
        error_color = "#dc3545" if self._job_status.has_error else "#fd7e14"
        details_style = (
            f"color: {error_color}; font-size: 11px; margin: 5px 0; "
            "white-space: pre-wrap;"
        )
        self._error_details = pn.pane.HTML(
            f'<pre style="{details_style}">{message}</pre>', width=600, margin=(5, 10)
        )

    def _hide_error_details(self) -> None:
        """Hide error details."""
        self._error_details = None

    def update_status(self, job_status: JobStatus) -> None:
        """Update the widget with new job status."""
        self._job_status = job_status
        self._setup_widgets()

    @property
    def job_id(self):
        """Get the job ID for this widget."""
        return self._job_status.job_id

    def panel(self) -> pn.layout.ListPanel:
        """Get the panel layout for this widget."""
        # Main row with job info, status, timing, and actions
        main_row = pn.Row(
            self._job_info,
            self._status_indicator,
            self._timing_info,
            self._action_buttons,
            sizing_mode="stretch_width",
            height=35,
        )

        layout_items = [main_row]

        # Add error row if present
        if self._error_brief is not None:
            error_row_items = [self._error_brief]
            if self._expand_button is not None:
                error_row_items.append(self._expand_button)

            error_row = pn.Row(*error_row_items, sizing_mode="stretch_width", height=25)
            layout_items.append(error_row)

        # Add error details if expanded
        if self._error_details is not None:
            layout_items.append(self._error_details)

        return pn.Column(
            *layout_items,
            styles={
                "border": "1px solid #dee2e6",
                "border-radius": "4px",
                "margin": "2px",
            },
            sizing_mode="stretch_width",
        )


class JobStatusListWidget(param.Parameterized):
    """Widget to display a list of job statuses with live updates."""

    def __init__(self, job_service: JobService, **params) -> None:
        super().__init__(**params)
        self._job_service = job_service
        self._status_widgets: dict[str, JobStatusWidget] = {}
        self._setup_layout()

        # Subscribe to job status updates
        self._job_service.register_job_status_update_subscriber(self._on_status_update)

    def _setup_layout(self) -> None:
        """Set up the main layout."""
        self._header = pn.pane.HTML("<h3>Job Status</h3>", margin=(10, 10, 5, 10))

        self._job_list = pn.Column(sizing_mode="stretch_width", margin=(0, 10))

        # Initialize with current job statuses
        for job_status in self._job_service.job_statuses.values():
            self._add_or_update_job_widget(job_status)

    def _on_status_update(self) -> None:
        """Handle job status updates from the service."""
        # Get all current job statuses and update widgets accordingly
        current_statuses = self._job_service.job_statuses
        current_job_keys = {
            str(status.job_id.job_number) for status in current_statuses.values()
        }

        # Remove widgets for jobs that no longer exist
        widgets_to_remove = set(self._status_widgets.keys()) - current_job_keys
        for job_key in widgets_to_remove:
            self._remove_job_widget(job_key)

        # Add or update widgets for current jobs
        for job_status in current_statuses.values():
            self._add_or_update_job_widget(job_status)

    def _add_or_update_job_widget(self, job_status: JobStatus) -> None:
        """Add a new job widget or update an existing one."""
        job_key = str(job_status.job_id.job_number)

        if job_key in self._status_widgets:
            # Update existing widget
            self._status_widgets[job_key].update_status(job_status)
        else:
            # Create new widget
            widget = JobStatusWidget(job_status)
            self._status_widgets[job_key] = widget
            self._job_list.append(widget.panel())

    def _remove_job_widget(self, job_id: str) -> None:
        """Remove a job widget."""
        if job_id in self._status_widgets:
            self._status_widgets.pop(job_id)
            # Note: Panel removal would be handled by updating the layout
            # For now, just tracking widgets in the dictionary

    def panel(self) -> pn.layout.Column:
        """Get the main panel for this widget."""
        return pn.Column(self._header, self._job_list, sizing_mode="stretch_width")
