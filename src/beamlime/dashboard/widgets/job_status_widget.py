# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

import panel as pn

from beamlime.core.job import JobAction, JobState, JobStatus
from beamlime.dashboard.job_controller import JobController
from beamlime.dashboard.job_service import JobService


# UI Constants
class UIConstants:
    """Constants for UI styling and sizing."""

    # Colors
    COLORS: ClassVar[dict[JobState, str]] = {
        JobState.scheduled: "#6c757d",  # Gray
        JobState.active: "#28a745",  # Green
        JobState.paused: "#ffc107",  # Yellow
        JobState.finishing: "#17a2b8",  # Blue
        JobState.stopped: "#343a40",  # Dark gray
        JobState.error: "#dc3545",  # Red
        JobState.warning: "#fd7e14",  # Orange
    }
    DEFAULT_COLOR = "#C162F4"
    ERROR_COLOR = "#dc3545"
    WARNING_COLOR = "#fd7e14"

    # Sizes
    JOB_INFO_WIDTH = 300
    JOB_INFO_HEIGHT = 45
    STATUS_INDICATOR_WIDTH = 90
    STATUS_INDICATOR_HEIGHT = 25
    TIMING_INFO_WIDTH = 150
    TIMING_INFO_HEIGHT = 25
    BUTTON_WIDTH = 36
    BUTTON_HEIGHT = 36
    EXPAND_BUTTON_WIDTH = 25
    EXPAND_BUTTON_HEIGHT = 20
    ERROR_BRIEF_WIDTH = 400
    ERROR_BRIEF_HEIGHT = 20
    ERROR_DETAILS_WIDTH = 600

    # Margins
    STANDARD_MARGIN = (5, 5)
    INFO_MARGIN = (5, 10)
    ERROR_MARGIN = (0, 10)
    BUTTON_MARGIN = (0, 2)
    EXPAND_MARGIN = (0, 5)
    DETAILS_MARGIN = (5, 10)

    # Text
    BRIEF_MESSAGE_MAX_LENGTH = 60
    JOB_NUMBER_MAX_LENGTH = 8

    # Button symbols
    RESET_SYMBOL = "↻"
    PAUSE_SYMBOL = "⏸"
    PLAY_SYMBOL = "▶"
    STOP_SYMBOL = "⏹"
    REMOVE_SYMBOL = "🗑"
    EXPAND_SYMBOL = "⊞"
    COLLAPSE_SYMBOL = "⊟"


class JobStatusWidget:
    """Widget to display the status of a job."""

    def __init__(self, job_status: JobStatus, job_controller: JobController) -> None:
        self._job_status = job_status
        self._job_controller = job_controller
        self.expanded = False
        self._setup_widgets()

    def _setup_widgets(self) -> None:
        """Set up the UI components."""
        # Job info row
        source_name = self._job_status.job_id.source_name
        job_number = str(self._job_status.job_id.job_number)
        job_number_short = job_number[: UIConstants.JOB_NUMBER_MAX_LENGTH]
        job_id_text = f"{source_name} ({job_number_short})"

        workflow_name = self._job_status.workflow_id.name
        workflow_version = self._job_status.workflow_id.version
        workflow_text = f"{workflow_name} v{workflow_version}"

        self._job_info = pn.pane.HTML(
            f"<b>{job_id_text}</b><br>{workflow_text}",
            width=UIConstants.JOB_INFO_WIDTH,
            height=UIConstants.JOB_INFO_HEIGHT,
            margin=UIConstants.INFO_MARGIN,
        )

        # Status indicator with color
        self._status_indicator = self._create_status_indicator()

        # Timing info
        self._timing_info = self._create_timing_info()

        # Action buttons
        self._action_buttons = self._create_action_buttons()

        # Error/warning message handling - initialize as None but will be created
        # if needed
        self._error_brief: pn.pane.HTML | None = None
        self._error_details: pn.pane.HTML | None = None
        self._expand_button: pn.widgets.Button | None = None

        if self._job_status.has_error or self._job_status.has_warning:
            self._setup_error_display()

    def _create_status_indicator(self) -> pn.pane.HTML:
        """Create the status indicator widget."""
        status_color = self._get_status_color(self._job_status.state)
        status_text = self._job_status.state.value.upper()
        status_style = self._get_status_style(status_color)
        return pn.pane.HTML(
            f'<div style="{status_style}">{status_text}</div>',
            width=UIConstants.STATUS_INDICATOR_WIDTH,
            height=UIConstants.STATUS_INDICATOR_HEIGHT,
            margin=UIConstants.STANDARD_MARGIN,
        )

    def _create_timing_info(self) -> pn.pane.HTML:
        """Create the timing info widget."""
        timing_text = self._format_timing()
        return pn.pane.HTML(
            timing_text,
            width=UIConstants.TIMING_INFO_WIDTH,
            height=UIConstants.TIMING_INFO_HEIGHT,
            margin=UIConstants.STANDARD_MARGIN,
        )

    def _create_button(self, symbol: str, callback) -> pn.widgets.Button:
        """Create a button with consistent styling."""
        button = pn.widgets.Button(
            name=symbol,
            button_type="light",
            width=UIConstants.BUTTON_WIDTH,
            height=UIConstants.BUTTON_HEIGHT,
            margin=UIConstants.BUTTON_MARGIN,
        )
        button.on_click(callback)
        return button

    def _get_button_widgets(self) -> list[pn.widgets.Button]:
        """Get button widgets based on current job state."""
        buttons = []

        # Reset button - always available for non-removed jobs
        reset_btn = self._create_button(
            UIConstants.RESET_SYMBOL, lambda event: self._send_action(JobAction.reset)
        )
        buttons.append(reset_btn)

        # Pause/Resume button - only for active/paused jobs
        if self._job_status.state in [JobState.active, JobState.paused]:
            if self._job_status.state == JobState.active:
                pause_btn = self._create_button(
                    UIConstants.PAUSE_SYMBOL,
                    lambda event: self._send_action(JobAction.pause),
                )
                buttons.append(pause_btn)
            else:  # paused
                resume_btn = self._create_button(
                    UIConstants.PLAY_SYMBOL,
                    lambda event: self._send_action(JobAction.resume),
                )
                buttons.append(resume_btn)

        # Stop/Remove button - dual purpose
        if self._job_status.state == JobState.stopped:
            remove_btn = self._create_button(
                UIConstants.REMOVE_SYMBOL,
                lambda event: self._send_action(JobAction.remove),
            )
            buttons.append(remove_btn)
        elif self._job_status.state not in [JobState.stopped]:
            stop_btn = self._create_button(
                UIConstants.STOP_SYMBOL, lambda event: self._send_action(JobAction.stop)
            )
            buttons.append(stop_btn)
        return buttons

    def _create_action_buttons(self) -> pn.layout.Row:
        """Create action buttons based on job state."""
        buttons = self._get_button_widgets()
        return pn.Row(*buttons, margin=UIConstants.STANDARD_MARGIN)

    def _send_action(self, action: JobAction) -> None:
        """Send job action via the controller."""
        self._job_controller.send_job_action(self._job_status.job_id, action)

    def _get_status_color(self, state: JobState) -> str:
        """Get color for job state."""
        return UIConstants.COLORS.get(state, UIConstants.DEFAULT_COLOR)

    def _get_status_style(self, color: str) -> str:
        """Get CSS style for status indicator."""
        return (
            f"background-color: {color}; color: white; "
            f"padding: 2px 8px; border-radius: 3px; text-align: center; "
            f"font-weight: bold;"
        )

    def _get_error_style(self, color: str) -> str:
        """Get CSS style for error details."""
        return f"color: {color}; font-size: 11px; margin: 5px 0; white-space: pre-wrap;"

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

    def _get_error_color(self) -> str:
        """Get error color based on error type."""
        return (
            UIConstants.ERROR_COLOR
            if self._job_status.has_error
            else UIConstants.WARNING_COLOR
        )

    def _truncate_error_message(self, message: str) -> str:
        """Truncate error message for brief display."""
        brief_message = message.split('\n')[0]
        if len(brief_message) > UIConstants.BRIEF_MESSAGE_MAX_LENGTH:
            brief_message = (
                brief_message[: UIConstants.BRIEF_MESSAGE_MAX_LENGTH - 3] + "..."
            )
        return brief_message

    def _setup_error_display(self) -> None:
        """Set up error/warning message display."""
        message = self._job_status.error_message or self._job_status.warning_message
        if not message:
            return

        # Get first line for brief display
        brief_message = self._truncate_error_message(message)

        # Brief error display
        error_color = self._get_error_color()
        brief_html = (
            f'<span style="color: {error_color}; font-size: 12px;">'
            f'{brief_message}</span>'
        )
        self._error_brief = pn.pane.HTML(
            brief_html,
            width=UIConstants.ERROR_BRIEF_WIDTH,
            height=UIConstants.ERROR_BRIEF_HEIGHT,
            margin=UIConstants.ERROR_MARGIN,
        )

        # Expand button if message is longer than brief
        if len(message) > len(brief_message) or '\n' in message:
            self._expand_button = pn.widgets.Button(
                name=UIConstants.EXPAND_SYMBOL
                if not self.expanded
                else UIConstants.COLLAPSE_SYMBOL,
                button_type="light",
                width=UIConstants.EXPAND_BUTTON_WIDTH,
                height=UIConstants.EXPAND_BUTTON_HEIGHT,
                margin=UIConstants.EXPAND_MARGIN,
            )
            self._expand_button.on_click(self._toggle_error_details)

        # Full error details (initially hidden)
        if self.expanded:
            self._show_error_details(message)

    def _toggle_error_details(self, event) -> None:
        """Toggle the display of full error details."""
        self.expanded = not self.expanded
        if self._expand_button is not None:
            self._expand_button.name = (
                UIConstants.COLLAPSE_SYMBOL
                if self.expanded
                else UIConstants.EXPAND_SYMBOL
            )

        message = self._job_status.error_message or self._job_status.warning_message
        if self.expanded and message:
            self._show_error_details(message)
        else:
            self._hide_error_details()

    def _show_error_details(self, message: str) -> None:
        """Show full error details."""
        error_color = self._get_error_color()
        details_style = self._get_error_style(error_color)
        self._error_details = pn.pane.HTML(
            f'<pre style="{details_style}">{message}</pre>',
            width=UIConstants.ERROR_DETAILS_WIDTH,
            margin=UIConstants.DETAILS_MARGIN,
        )

    def _hide_error_details(self) -> None:
        """Hide error details."""
        self._error_details = None

    def update_status(self, job_status: JobStatus) -> None:
        """Update the widget with new job status, only changing what's necessary."""
        old_status = self._job_status
        self._job_status = job_status

        # Update status indicator if state changed
        if old_status.state != job_status.state:
            self._update_status_indicator()
            # Update action buttons when state changes
            self._update_action_buttons()

        # Update timing info if times changed
        if (
            old_status.start_time != job_status.start_time
            or old_status.end_time != job_status.end_time
        ):
            self._update_timing_info()

        # Update error/warning display if messages changed
        if (
            old_status.error_message != job_status.error_message
            or old_status.warning_message != job_status.warning_message
        ):
            self._update_error_display()

    def _update_status_indicator(self) -> None:
        """Update just the status indicator."""
        status_color = self._get_status_color(self._job_status.state)
        status_text = self._job_status.state.value.upper()
        status_style = self._get_status_style(status_color)
        self._status_indicator.object = (
            f'<div style="{status_style}">{status_text}</div>'
        )

    def _update_timing_info(self) -> None:
        """Update just the timing information."""
        timing_text = self._format_timing()
        self._timing_info.object = timing_text

    def _update_action_buttons(self) -> None:
        """Update action buttons in place when job state changes."""
        # Clear existing buttons
        self._action_buttons.clear()

        # Add new buttons based on current state
        new_buttons = self._get_button_widgets()
        self._action_buttons.extend(new_buttons)

    def _update_error_display(self) -> None:
        """Update the error/warning display efficiently."""
        has_message = self._job_status.has_error or self._job_status.has_warning

        if not has_message:
            # Clear error display
            if self._error_brief is not None:
                self._error_brief.object = ""
            if self._error_details is not None:
                self._error_details.object = ""
            return

        message = self._job_status.error_message or self._job_status.warning_message
        if not message:
            return

        # Update existing error display or create new one
        brief_message = self._truncate_error_message(message)
        error_color = self._get_error_color()
        brief_html = (
            f'<span style="color: {error_color}; font-size: 12px;">'
            f'{brief_message}</span>'
        )

        if self._error_brief is not None:
            # Update existing brief display
            self._error_brief.object = brief_html
        else:
            # Create new brief display
            self._error_brief = pn.pane.HTML(
                brief_html,
                width=UIConstants.ERROR_BRIEF_WIDTH,
                height=UIConstants.ERROR_BRIEF_HEIGHT,
                margin=UIConstants.ERROR_MARGIN,
            )

        # Handle expand button
        needs_expand_button = len(message) > len(brief_message) or '\n' in message
        if needs_expand_button and self._expand_button is None:
            self._expand_button = pn.widgets.Button(
                name=UIConstants.EXPAND_SYMBOL,
                button_type="light",
                width=UIConstants.EXPAND_BUTTON_WIDTH,
                height=UIConstants.EXPAND_BUTTON_HEIGHT,
                margin=UIConstants.EXPAND_MARGIN,
            )
            self._expand_button.on_click(self._toggle_error_details)

        # Update error details if currently expanded
        if self.expanded and self._error_details is not None:
            details_style = self._get_error_style(error_color)
            self._error_details.object = f'<pre style="{details_style}">{message}</pre>'

    @property
    def job_id(self):
        """Get the job ID for this widget."""
        return self._job_status.job_id

    def panel(self) -> pn.layout.Column:
        """Get the panel layout for this widget."""
        # Main row with job info, status, timing, and actions
        main_row = pn.Row(
            self._job_info,
            self._status_indicator,
            self._timing_info,
            self._action_buttons,
            sizing_mode="stretch_both",
        )

        layout_items: list[pn.viewable.Viewable] = [main_row]

        # Add error row if present
        if (
            self._error_brief is not None
            and hasattr(self._error_brief, 'object')
            and getattr(self._error_brief, 'object', None)
        ):
            error_row_items: list[pn.viewable.Viewable] = [self._error_brief]
            if self._expand_button is not None:
                error_row_items.append(self._expand_button)

            error_row = pn.Row(*error_row_items, sizing_mode="stretch_width", height=25)
            layout_items.append(error_row)

        # Add error details if expanded
        if (
            self._error_details is not None
            and hasattr(self._error_details, 'object')
            and getattr(self._error_details, 'object', None)
        ):
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


class JobStatusListWidget:
    """Widget to display a list of job statuses with live updates."""

    def __init__(self, job_service: JobService, job_controller: JobController) -> None:
        self._job_service = job_service
        self._job_controller = job_controller
        self._status_widgets: dict[str, JobStatusWidget] = {}
        self._widget_panels: dict[str, pn.layout.Column] = {}
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
            f"{status.job_id.source_name}:{status.job_id.job_number}"
            for status in current_statuses.values()
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
        job_key = f"{job_status.job_id.source_name}:{job_status.job_id.job_number}"

        if job_key in self._status_widgets:
            # Update existing widget
            self._status_widgets[job_key].update_status(job_status)
        else:
            # Create new widget
            widget = JobStatusWidget(job_status, self._job_controller)
            widget_panel = widget.panel()
            self._status_widgets[job_key] = widget
            self._widget_panels[job_key] = widget_panel
            self._job_list.append(widget_panel)

    def _remove_job_widget(self, job_key: str) -> None:
        """Remove a job widget."""
        if job_key in self._status_widgets:
            self._status_widgets.pop(job_key)
            if job_key in self._widget_panels:
                widget_panel = self._widget_panels.pop(job_key)
                # Remove from the job list
                try:
                    self._job_list.remove(widget_panel)
                except ValueError:
                    # Panel might not be in the list anymore
                    pass

    def panel(self) -> pn.layout.Column:
        """Get the main panel for this widget."""
        return pn.Column(self._header, self._job_list, sizing_mode="stretch_width")
