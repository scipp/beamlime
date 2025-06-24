from typing import Any

import panel as pn

from beamlime.config.workflow_spec import WorkflowStatus, WorkflowStatusType

from .workflow_controller_base import WorkflowControllerBase


class WorkflowStatusListWidget:
    """Widget for displaying and controlling running workflows."""

    def __init__(self, controller: WorkflowControllerBase) -> None:
        """
        Initialize running workflows widget.

        Parameters
        ----------
        controller
            Controller for workflow operations
        """
        self._controller = controller
        self._workflow_list = pn.Column()
        self._workflow_rows: dict[str, dict[str, Any]] = {}  # Track persistent widgets
        self._widget = self._create_widget()

        # Subscribe to status updates for automatic refresh
        self._controller.subscribe_to_workflow_status_updates(self.refresh)

        # Subscribe to workflow specs updates to refresh workflow names
        self._controller.subscribe_to_workflow_updates(
            lambda workflow_specs: self.refresh()
        )

    def _create_widget(self) -> pn.Column:
        """Create the main widget."""
        return pn.Column(
            pn.pane.HTML("<h4>Workflow status</h4>"),
            self._workflow_list,
        )

    def _create_workflow_row(
        self, source_name: str, status: WorkflowStatus
    ) -> dict[str, Any]:
        """Create a row widget data structure for a single workflow."""
        # Get workflow name from controller
        workflow_name = self._controller.get_workflow_name(status.workflow_id)

        # Create info panel
        info_pane = pn.pane.HTML("", width=220)

        # Create action button
        action_button = pn.widgets.Button(
            name="",
            button_type="primary",
            width=45,
            height=25,
            sizing_mode="fixed",
            margin=(2, 2),
            stylesheets=[
                """
            .bk-btn {
                font-size: 10px !important;
                text-align: center !important;
                padding: 2px !important;
                line-height: 1 !important;
            }
            """
            ],
        )

        # Create inspect button
        inspect_button = pn.widgets.Button(
            name="Inspect",
            button_type="light",
            width=45,
            height=25,
            sizing_mode="fixed",
            margin=(2, 2),
            disabled=True,  # Will be enabled in future versions
            stylesheets=[
                """
            .bk-btn {
                font-size: 10px !important;
                text-align: center !important;
                padding: 2px !important;
                line-height: 1 !important;
            }
            """
            ],
        )

        # Create row widget
        row_widget = pn.Row(
            info_pane,
            pn.Spacer(),
            inspect_button,
            action_button,
            margin=(2, 0),
        )

        # Return widget data structure
        row_data = {
            'widget': row_widget,
            'info_pane': info_pane,
            'action_button': action_button,
            'inspect_button': inspect_button,
            'last_status': None,
            'last_workflow_name': None,
        }

        # Update the row content
        self._update_row_content(source_name, status, workflow_name, row_data)

        return row_data

    def _update_row_content(
        self,
        source_name: str,
        status: WorkflowStatus,
        workflow_name: str,
        row_data: dict[str, Any],
    ) -> None:
        """Update the content of an existing row widget."""
        # Check if update is needed
        if (
            row_data['last_status'] == status.status
            and row_data['last_workflow_name'] == workflow_name
        ):
            return

        # Get display info from controller
        display_info = self._controller.get_status_display_info(status)

        # Update info panel HTML
        info_html = f"""
        <div style="{display_info['opacity_style']}">
            <strong>{source_name}</strong>
            <span style="color: {display_info['color']}; font-size: 0.8em; margin-left: 8px;">● {display_info['text']}</span>
            <br>
            <small>Workflow: {workflow_name}</small>
        </div>
        """  # noqa: E501
        row_data['info_pane'].object = info_html

        # Update button if needed
        if row_data['action_button'].name != display_info['button_name']:
            row_data['action_button'].name = display_info['button_name']
            row_data['action_button'].button_type = display_info['button_type']

            # Clear existing callbacks
            row_data['action_button']._callbacks = {}

            # Set new callback based on status
            if status.status in (
                WorkflowStatusType.STARTING,
                WorkflowStatusType.RUNNING,
            ):

                def stop_callback(event):
                    self._controller.stop_workflow_for_source(source_name)

                row_data['action_button'].on_click(stop_callback)
            else:

                def remove_callback(event):
                    self._controller.remove_workflow_for_source(source_name)

                row_data['action_button'].on_click(remove_callback)

        # Update tracking data
        row_data['last_status'] = status.status
        row_data['last_workflow_name'] = workflow_name

    def refresh(self) -> None:
        """Refresh the list of workflows."""
        all_status = self._controller.get_all_workflow_status()

        if not all_status:
            self._workflow_rows.clear()
            self._workflow_list.objects = [
                pn.pane.HTML(
                    "<p style='color: #6c757d; font-style: italic;'>No workflows</p>"
                )
            ]
            return

        # Get current source names
        current_sources = set(all_status.keys())
        tracked_sources = set(self._workflow_rows.keys())

        # Remove rows for sources that no longer exist
        sources_to_remove = tracked_sources - current_sources
        for source_name in sources_to_remove:
            del self._workflow_rows[source_name]

        # Update or create rows for current sources
        workflow_widgets = []
        for source_name, status in all_status.items():
            # Get workflow name from controller
            workflow_name = self._controller.get_workflow_name(status.workflow_id)

            if source_name in self._workflow_rows:
                # Update existing row
                row_data = self._workflow_rows[source_name]
                self._update_row_content(source_name, status, workflow_name, row_data)
            else:
                # Create new row
                row_data = self._create_workflow_row(source_name, status)
                self._workflow_rows[source_name] = row_data

            workflow_widgets.append(self._workflow_rows[source_name]['widget'])

        # Update the workflow list only if the structure changed
        if (
            len(workflow_widgets) != len(self._workflow_list.objects)
            or sources_to_remove
        ):
            self._workflow_list.objects = workflow_widgets

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget
