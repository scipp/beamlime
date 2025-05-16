# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from dash import ALL, Dash, Input, Output, State, dcc, html


def make_dash_app(name: str, callbacks):
    app = Dash(name)
    _setup_layout(app)
    _setup_callbacks(app, callbacks)
    return app


def _setup_layout(app: Dash) -> None:
    # Add CSS styles using the Dash assets approach
    app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            html, body {
                margin: 0;
                padding: 0;
                overflow-x: hidden;
                height: 100%;
                max-height: 100%;
            }
            #react-entry-point {
                height: 100%;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

    controls = [
        html.Label('Update Speed (ms)'),
        dcc.Slider(
            id='update-speed',
            min=8,
            max=13,
            step=0.5,
            value=10,
            marks={i: {'label': f'{2**i}'} for i in range(8, 14)},
        ),
        dcc.Checklist(
            id='bins-checkbox',
            options=[
                {
                    'label': 'Time-of-arrival bins (WARNING: Clears the history!)',
                    'value': 'confirmed',
                }
            ],
            value=[],
            style={'margin': '10px 0'},
        ),
        dcc.Slider(
            id='num-points',
            min=10,
            max=1000,
            step=10,
            value=100,
            marks={i: str(i) for i in range(0, 1001, 100)},
            disabled=True,
        ),
        html.Label('ROI X-axis Center (%)'),
        dcc.Slider(
            id='roi-x-center',
            min=0,
            max=100,
            step=1,
            value=50,
            marks={i: str(i) for i in range(0, 101, 20)},
        ),
        html.Label('ROI X-axis Width (%)'),
        dcc.Slider(
            id='roi-x-delta',
            min=0,
            max=10,
            step=1,
            value=5,
            marks={i: str(i) for i in range(0, 11, 1)},
        ),
        html.Label('ROI Y-axis Center (%)'),
        dcc.Slider(
            id='roi-y-center',
            min=0,
            max=100,
            step=1,
            value=50,
            marks={i: str(i) for i in range(0, 101, 20)},
        ),
        html.Label('ROI Y-axis Width (%)'),
        dcc.Slider(
            id='roi-y-delta',
            min=0,
            max=10,
            step=1,
            value=5,
            marks={i: str(i) for i in range(0, 11, 1)},
        ),
        dcc.Checklist(
            id='toa-checkbox',
            options=[{'label': 'Filter by time-of-arrival (μs)', 'value': 'enabled'}],
            value=[],
            style={'margin': '10px 0'},
        ),
        html.Label('Time-of-arrival center (μs)'),
        dcc.Slider(
            id='toa-center',
            min=0,
            max=71_000,
            step=100,
            value=35_500,
            marks={i: str(i) for i in range(0, 71_001, 10_000)},
        ),
        html.Label('Time-of-arrival width (μs)'),
        dcc.Slider(
            id='toa-delta',
            min=0,
            max=5_000,
            step=100,
            value=5_000,
            marks={i: str(i) for i in range(0, 5_001, 1000)},
        ),
        dcc.Checklist(
            id='use-weights-checkbox',
            options=[{'label': 'Use weights', 'value': 'enabled'}],
            value=['enabled'],
            style={'margin': '10px 0'},
        ),
        html.Button('Clear', id='clear-button', n_clicks=0),
        html.Hr(style={'margin': '20px 0'}),
        html.H3('Workflow Control', style={'marginTop': '10px'}),
        html.Label('Source Name'),
        dcc.Dropdown(
            id='workflow-source-name',
            options=[],  # Will be populated dynamically
            value=None,
            style={'width': '100%', 'marginBottom': '10px'},
        ),
        html.Div(
            style={
                'display': 'flex',
                'justifyContent': 'space-between',
                'marginTop': '10px',
            },
            children=[
                html.Button(
                    'Stop Workflow',
                    id='workflow-stop-button',
                    n_clicks=0,
                    style={'width': '48%'},
                ),
                html.Button(
                    'Configure Workflow',
                    id='workflow-configure-button',
                    n_clicks=0,
                    style={'width': '48%'},
                ),
            ],
        ),
        html.Div(
            id='workflow-controls-container',
            children=[],  # Will be populated when Configure Workflow is clicked
            style={'marginTop': '10px'},
        ),
        html.Label('Note that workflow changes may take a few seconds to apply.'),
    ]
    app.layout = html.Div(
        [
            html.Div(
                controls,
                style={
                    'width': '300px',
                    'position': 'fixed',
                    'top': '0',
                    'left': '0',
                    'bottom': '0',
                    'padding': '10px',
                    'overflowY': 'auto',
                    'backgroundColor': '#f8f9fa',
                    'borderRight': '1px solid #dee2e6',
                    'zIndex': '1000',
                },
            ),
            html.Div(
                id='plots-container',
                style={
                    'marginLeft': '320px',
                    'padding': '10px 10px 0 10px',  # Remove bottom padding
                    'height': '100vh',
                    'overflowY': 'auto',
                    'boxSizing': 'border-box',
                },
            ),
            dcc.Interval(id='interval-component', interval=200, n_intervals=0),
            # Add interval for workflow updates
            dcc.Interval(id='workflow-update-interval', interval=5000, n_intervals=0),
        ],
        style={
            'height': '100vh',
            'width': '100%',
            'margin': '0',
            'padding': '0',
            'overflow': 'hidden',  # Hide both x and y overflow
            'boxSizing': 'border-box',
            'display': 'block',  # Ensure block display
        },
    )


def _setup_callbacks(app: Dash, self) -> None:
    app.callback(Output('num-points', 'disabled'), Input('bins-checkbox', 'value'))(
        self._toggle_slider
    )

    app.callback(
        Output('plots-container', 'children'),
        Input('interval-component', 'n_intervals'),
    )(self.update_plots)

    app.callback(
        Output('interval-component', 'interval'), Input('update-speed', 'value')
    )(self.update_timing_settings)

    app.callback(Output('num-points', 'value'), Input('num-points', 'value'))(
        self.update_num_points
    )

    app.callback(Output('clear-button', 'n_clicks'), Input('clear-button', 'n_clicks'))(
        self.clear_data
    )

    app.callback(
        Output('roi-x-center', 'value'),
        Output('roi-x-delta', 'value'),
        Output('roi-y-center', 'value'),
        Output('roi-y-delta', 'value'),
        Input('roi-x-center', 'value'),
        Input('roi-x-delta', 'value'),
        Input('roi-y-center', 'value'),
        Input('roi-y-delta', 'value'),
    )(self.update_roi)

    app.callback(
        [Output('toa-center', 'disabled'), Output('toa-delta', 'disabled')],
        Input('toa-checkbox', 'value'),
    )(lambda value: [len(value) == 0, len(value) == 0])

    app.callback(
        Output('toa-center', 'value'),
        Output('toa-delta', 'value'),
        Input('toa-center', 'value'),
        Input('toa-delta', 'value'),
        Input('toa-checkbox', 'value'),
    )(self.update_toa_range)

    app.callback(
        Output('use-weights-checkbox', 'value'),
        Input('use-weights-checkbox', 'value'),
    )(self.update_use_weights)

    app.callback(
        Output('workflow-controls-container', 'children'),
        Input('workflow-configure-button', 'n_clicks'),
        State('workflow-source-name', 'value'),
    )(self.show_workflow_config)
    app.callback(
        Output('workflow-params-container', 'children'),
        Input('workflow-name', 'value'),
        State('workflow-source-name', 'value'),
    )(self.update_workflow_parameters)
    app.callback(
        Output('workflow-stop-button', 'n_clicks'),
        Input('workflow-stop-button', 'n_clicks'),
        Input('workflow-source-name', 'value'),
    )(self.stop_workflow)

    # Update source name dropdown options periodically
    app.callback(
        Output('workflow-source-name', 'options'),
        Input('workflow-update-interval', 'n_intervals'),
        Input('workflow-source-name', 'value'),
    )(self.update_source_dropdown)

    # Set initial value for source dropdown ONLY if empty and only once
    app.callback(
        Output('workflow-source-name', 'value'),
        Input('workflow-source-name', 'options'),
        Input('workflow-source-name', 'value'),
        prevent_initial_call=True,  # Prevent automatic call on initial load
    )(self.set_initial_source)

    # Update workflow apply button callback to collect parameter values
    app.callback(
        Output('workflow-apply-button', 'n_clicks'),
        Input('workflow-apply-button', 'n_clicks'),
        State('workflow-source-storage', 'children'),
        State('workflow-name', 'value'),
        # Use ALL pattern to get all parameter inputs
        State({'type': 'param-input', 'name': ALL}, 'value'),
        State({'type': 'param-type', 'name': ALL}, 'children'),
        State({'type': 'param-input', 'name': ALL}, 'id'),
    )(self.send_workflow_control)
