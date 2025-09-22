# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import argparse

import holoviews as hv
import panel as pn

from ess.livedata import Service

from .dashboard import DashboardBase

pn.extension('holoviews', 'modal', template='material')
hv.extension('bokeh')


class ReductionApp(DashboardBase):
    """Reduction dashboard application."""

    def __init__(
        self,
        *,
        instrument: str = 'dummy',
        dev: bool = False,
        log_level: int,
        basic_auth_password: str | None = None,
        basic_auth_cookie_secret: str | None = None,
    ):
        super().__init__(
            instrument=instrument,
            dev=dev,
            log_level=log_level,
            dashboard_name='reduction_dashboard',
            port=5009,  # Default port for reduction dashboard
            basic_auth_password=basic_auth_password,
            basic_auth_cookie_secret=basic_auth_cookie_secret,
        )
        self._logger.info("Reduction dashboard initialized")

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with workflow controls."""
        return pn.Column(
            pn.pane.Markdown("## Data Reduction"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main content area."""
        return pn.Row()


def get_arg_parser() -> argparse.ArgumentParser:
    return Service.setup_arg_parser(
        description='ESSlivedata Dashboard', dashboard_auth=True
    )


def main() -> None:
    parser = get_arg_parser()
    app = ReductionApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
