# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

from dash import dcc, html


def create_workflow_controls(
    *, source_name: str, workflow_id: str | None, workflow_options: list
) -> list:
    """Create the workflow controls widget."""

    return [
        html.Div(
            f"Configuring workflows for source: {source_name}",
            style={'fontWeight': 'bold', 'marginBottom': '10px'},
        ),
        html.Label('Workflow Name'),
        dcc.Dropdown(
            id='workflow-name',
            options=workflow_options,
            value=workflow_id,
            style={'width': '100%', 'marginBottom': '10px'},
        ),
        # Hidden div to store parameters
        html.Div(
            id='workflow-params-container',
            children=[],
            style={'marginTop': '10px'},
        ),
        html.Button(
            'Apply Workflow',
            id='workflow-apply-button',
            n_clicks=0,
            style={'width': '100%', 'marginTop': '10px'},
        ),
        # Add hidden div to store the source name this control set is for
        html.Div(
            id='workflow-source-storage',
            children=source_name,
            style={'display': 'none'},
        ),
    ]
