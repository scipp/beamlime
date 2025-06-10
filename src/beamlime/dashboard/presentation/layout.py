"""Dashboard layout components."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from ..services.configuration import ConfigurationService


def create_dashboard_layout(config_service: ConfigurationService) -> dbc.Container:
    """Create the main dashboard layout."""
    return dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1("Beamlime Dashboard", className="text-center mb-4"),
                        ],
                        width=12,
                    )
                ]
            ),
            dbc.Row(
                [
                    dbc.Col([create_control_panel(config_service)], width=4),
                    dbc.Col([create_plot_tabs()], width=8),
                ]
            ),
        ],
        fluid=True,
    )


def create_control_panel(config_service: ConfigurationService) -> dbc.Card:
    """Create the configuration control panel."""
    config = config_service.get_config_dict()

    return dbc.Card(
        [
            dbc.CardHeader(html.H4("Configuration Panel")),
            dbc.CardBody(
                [
                    html.H5("Processing Parameters"),
                    html.Div(
                        [
                            html.Label("Threshold:"),
                            dcc.Slider(
                                id="threshold-slider",
                                min=0.1,
                                max=5.0,
                                step=0.1,
                                value=config['threshold'],
                                marks={i: str(i) for i in range(1, 6)},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        [
                            html.Label("Smoothing Factor:"),
                            dcc.Slider(
                                id="smoothing-slider",
                                min=0.0,
                                max=1.0,
                                step=0.1,
                                value=config['smoothing_factor'],
                                marks={i / 10: f"{i/10:.1f}" for i in range(0, 11, 2)},
                                tooltip={"placement": "bottom", "always_visible": True},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        [
                            dbc.Switch(
                                id="filtering-switch",
                                label="Enable Filtering",
                                value=config['enable_filtering'],
                            )
                        ],
                        className="mb-3",
                    ),
                    html.Hr(),
                    html.H5("Region of Interest"),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("X Min:"),
                                    dbc.Input(
                                        id="roi-x-min",
                                        type="number",
                                        value=config['roi_x_min'],
                                        min=0,
                                        max=100,
                                    ),
                                ],
                                width=6,
                            ),
                            dbc.Col(
                                [
                                    html.Label("X Max:"),
                                    dbc.Input(
                                        id="roi-x-max",
                                        type="number",
                                        value=config['roi_x_max'],
                                        min=0,
                                        max=100,
                                    ),
                                ],
                                width=6,
                            ),
                        ],
                        className="mb-2",
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Y Min:"),
                                    dbc.Input(
                                        id="roi-y-min",
                                        type="number",
                                        value=config['roi_y_min'],
                                        min=0,
                                        max=100,
                                    ),
                                ],
                                width=6,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Y Max:"),
                                    dbc.Input(
                                        id="roi-y-max",
                                        type="number",
                                        value=config['roi_y_max'],
                                        min=0,
                                        max=100,
                                    ),
                                ],
                                width=6,
                            ),
                        ]
                    ),
                ]
            ),
        ]
    )


def create_plot_tabs() -> dbc.Card:
    """Create the plot visualization tabs."""
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    dbc.Tabs(
                        [
                            dbc.Tab(label="1D Plot", tab_id="1d-tab"),
                            dbc.Tab(label="2D Heatmap", tab_id="2d-tab"),
                            dbc.Tab(label="Statistics", tab_id="stats-tab"),
                        ],
                        id="plot-tabs",
                        active_tab="1d-tab",
                    )
                ]
            ),
            dbc.CardBody([html.Div(id="plot-content")]),
        ]
    )
