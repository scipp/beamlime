"""Dashboard callback controller."""

import logging
from typing import Any

from dash import Input, Output, callback, html
from dash.dash import Dash
from dash.dcc import Graph

from ..services.orchestrator import DashboardOrchestrator

logger = logging.getLogger(__name__)


class CallbackController:
    """Handles dashboard callbacks and user interactions."""

    def __init__(self, orchestrator: DashboardOrchestrator):
        self.orchestrator = orchestrator
        logger.info('Callback controller initialized')

    def register_callbacks(self, app: Dash) -> None:
        """Register all dashboard callbacks."""
        self._register_plot_callbacks(app)
        self._register_config_callbacks(app)
        logger.info('Dashboard callbacks registered')

    def _register_plot_callbacks(self, app: Dash) -> None:
        """Register plot update callbacks."""

        @app.callback(
            Output("plot-content", "children"), Input("plot-tabs", "active_tab")
        )
        def update_plot_content(active_tab: str) -> Any:
            """Update plot content based on active tab."""
            figures = self.orchestrator.get_current_figures()

            if active_tab == "1d-tab":
                return Graph(figure=figures['1d_plot'])
            elif active_tab == "2d-tab":
                return Graph(figure=figures['2d_heatmap'])
            elif active_tab == "stats-tab":
                return Graph(figure=figures['statistics'])
            else:
                return html.Div("Select a tab to view plots")

    def _register_config_callbacks(self, app: Dash) -> None:
        """Register configuration update callbacks."""

        @app.callback(
            Output("threshold-slider", "value", allow_duplicate=True),
            [
                Input("threshold-slider", "value"),
                Input("smoothing-slider", "value"),
                Input("filtering-switch", "value"),
                Input("roi-x-min", "value"),
                Input("roi-x-max", "value"),
                Input("roi-y-min", "value"),
                Input("roi-y-max", "value"),
            ],
            prevent_initial_call=True,
        )
        def update_configuration(
            threshold: float,
            smoothing: float,
            filtering: bool,
            roi_x_min: int,
            roi_x_max: int,
            roi_y_min: int,
            roi_y_max: int,
        ) -> float:
            """Update configuration when controls change."""
            self.orchestrator.update_configuration(
                threshold=threshold,
                smoothing_factor=smoothing,
                enable_filtering=filtering,
                roi_x_min=roi_x_min,
                roi_x_max=roi_x_max,
                roi_y_min=roi_y_min,
                roi_y_max=roi_y_max,
            )
            return threshold
