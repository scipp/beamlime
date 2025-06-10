"""Development dashboard application."""

import logging

import dash_bootstrap_components as dbc
from dash import Dash

from .dependencies import DashboardDependencies
from .models import DashboardConfig
from .presentation.layout import create_dashboard_layout

logger = logging.getLogger(__name__)


def create_dev_dashboard() -> Dash:
    """Create the development dashboard application."""
    config = DashboardConfig(debug_mode=True)
    deps = DashboardDependencies(config)

    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
    )

    app.layout = create_dashboard_layout(deps.config_service)
    deps.callback_controller.register_callbacks(app)

    logger.info('Development dashboard created')
    return app


def main() -> None:
    """Run the development dashboard."""
    logging.basicConfig(level=logging.INFO)
    app = create_dev_dashboard()
    app.run(debug=False)


if __name__ == '__main__':
    main()
