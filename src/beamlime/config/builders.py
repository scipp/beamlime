# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Union

from ..resources.templates import (
    load_app_subscription_tpl,
    load_application_tpl,
    load_communication_channel_tpl,
    load_config_tpl,
    load_subscription_tpl,
    load_workflow_tpl,
)


def _build_default_application_config(name: str) -> dict:
    tpl = load_application_tpl()
    tpl["name"] = name
    return tpl


def _build_workflow_config(name: str) -> dict:
    tpl = load_workflow_tpl()
    tpl["name"] = name
    tpl.pop("reference")
    tpl.pop("process-args")
    tpl.pop("process-kwargs")
    return tpl


def _build_channel_config(name: str) -> dict:
    tpl = load_communication_channel_tpl()
    tpl["name"] = name
    tpl["type"] = "SQUEUE"
    tpl.pop("options")
    return tpl


def _build_subscription(channel_name: str) -> dict:
    tpl = load_subscription_tpl()
    tpl["name"] = channel_name
    return tpl


def _build_app_subscription(
    app_name: str, in_ch_name: Union[str, None], out_ch_name: Union[str, None]
) -> dict:
    tpl = load_app_subscription_tpl()
    tpl["app-name"] = app_name
    if in_ch_name:
        tpl["input-channels"] = [_build_subscription(in_ch_name)]
    else:
        tpl.pop("input-channels")
    if out_ch_name:
        tpl["output-channels"] = [_build_subscription(out_ch_name)]
    else:
        tpl.pop("output-channels")
    return tpl


def _build_default_data_stream_config() -> dict:
    from beamlime.handlers.data_feeder import Fake2dDetectorImageFeeder as data_feeder
    from beamlime.handlers.data_reduction import (
        BeamLimeDataReductionApplication as data_reduction,
    )
    from beamlime.handlers.visualisation import RealtimePlot as visualisation

    config = dict()
    app_names = ["data-feeder", "data-reduction", "visualisation"]
    # Placeholders
    config["applications"] = [
        _build_default_application_config(app_name) for app_name in app_names
    ]
    config["application-specs"] = {app_name: {} for app_name in app_names}
    # Application configurations
    config["applications"][0]["data-handler"] = ".".join(
        (data_feeder.__module__, data_feeder.__name__)
    )
    config["applications"][0]["timeout"] = 1
    config["applications"][0]["wait-interval"] = 0.2
    config["applications"][1]["data-handler"] = ".".join(
        (data_reduction.__module__, data_reduction.__name__)
    )
    config["applications"][1]["timeout"] = 5
    config["applications"][1]["wait-interval"] = 0.1
    config["applications"][2]["data-handler"] = ".".join(
        (visualisation.__module__, visualisation.__name__)
    )
    config["applications"][2]["timeout"] = 10
    config["applications"][2]["wait-interval"] = 1
    # Application specs
    config["application-specs"]["data-feeder"] = {
        "detector-size": [64, 64],
        "min-intensity": 0.5,
        "num-frame": 32,
        "signal-mu": 0.7,
        "signal-err": 0.2,
        "noise-mu": 0.5,
        "noise-err": 0.2,
    }
    # Communication
    channel_names = ["raw-data", "reduced-data"]
    config["communication-channels"] = [
        _build_channel_config(ch_name) for ch_name in channel_names
    ]
    config["application-subscriptions"] = [
        _build_app_subscription("data-feeder", None, "raw-data"),
        _build_app_subscription("data-reduction", "raw-data", "reduced-data"),
        _build_app_subscription("visualisation", "reduced-data", None),
    ]
    return config


def _build_default_data_reduction_config() -> dict:
    config = dict()
    # workflows
    config["workflows"] = [
        _build_workflow_config(name="heatmap"),
        _build_workflow_config(name="frame-number-counting"),
    ]
    config["workflows"][0]["targets"] = ["fake-2d-image"]
    config["workflows"][0]["process"] = "heatmap_2d"
    config["workflows"][0]["process-kargs"] = {
        "threshold": 0.4,
        "binning_size": [64, 64],
    }
    # heatmap result will be accumulated
    config["workflows"][0]["output-policy"] = "STACK"
    config["workflows"][1]["targets"] = ["frame-number"]
    config["workflows"][1]["process"] = "handover"

    # targets
    config["targets"] = [
        {"name": "fake-2d-image", "index": ["detector-data", "image"]},
        {"name": "frame-number", "index": ["detector-data", "frame-number"]},
    ]
    return config


def build_default_config():
    """
    This default configuration contains three basic applications,
    1. data-generator/feeder
    2. data-reduction
    3. visualisation
    The scenario is that you want to take a picture of
    a little metal plate with 2d-detector, where a lime-shape is carved on.
    """
    # TODO: Detach visualisation application, which is currently glued with
    # data-reduction application via queue.Queue.

    import datetime

    tpl = load_config_tpl()
    # General
    tpl["general"]["user"] = "scipp"
    tpl["general"]["name"] = "default configuration"
    tpl["general"]["last-updated"] = str(datetime.datetime.now())
    # Default logging directory should be retrieved by ``preset_options``.
    tpl["general"].pop("log-dir")

    # Data Stream
    tpl["data-stream"] = _build_default_data_stream_config()

    # Data Reduction
    tpl["data-reduction"] = _build_default_data_reduction_config()
    return tpl
