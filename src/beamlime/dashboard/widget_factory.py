# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass

from dash import Dash, Input, Output, dcc
from dash.development.base_component import Component

from beamlime.core.config_service import ConfigService

# dcc.Slider('update-speed')
# app.callback()  <-- Output :(
# update_timing_settings
# config['update_every']  # models.UpdateEvery(2**update_speep, unit='ms')


class ConfigBackedWidget:
    def make_widget(self) -> Component:
        return dcc.Slider(
            id='update-speed',
            min=8,
            max=13,
            step=0.5,
            value=10,
            marks={i: {'label': f'{2**i}'} for i in range(8, 14)},
        )

    def setup_callback(self, app: Dash) -> None:
        app.callback(
            Output('interval-component', 'interval'), Input('update-speed', 'value')
        )(self.update_timing_settings)

    def update_timing_settings(self, update_speed: float) -> float:
        update_every = models.UpdateEvery(value=2**update_speed, unit='ms')
        config_key = ConfigKey(key="update_every")
        # TODO should config service just be handled via anthoer callback?
        self._config_service.update_config(config_key, update_every.model_dump())
        return 2**update_speed


class WidgetFactory:
    def __init__(self, config_service: ConfigService, app: Dash):
        """
        Initialize the WidgetFactory with a ConfigService and a Dash app.

        Parameters
        ----------
        config_service: ConfigService
            The configuration service to use for managing widget configurations.
        app: Dash
            The Dash app instance to which widgets will be added.
        """
        self._config_service = config_service
        self._app = app

    def register_widget(self, widget_type: str, **kwargs):
        pass
