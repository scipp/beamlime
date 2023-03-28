# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from ..resources.templates import (
    load_config_tpl,
    load_data_stream_interface_tpl,
    load_workflow_tpl,
)


def _build_default_application_config(name: str):
    tpl = load_data_stream_interface_tpl()
    tpl["name"] = name
    return tpl


def _build_workflow_config(name=str):
    tpl = load_workflow_tpl()
    tpl["name"] = name
    tpl.pop("reference")
    tpl.pop("process-args")
    tpl.pop("process-kwargs")
    return tpl


def _build_default_data_stream_config() -> dict:
    config = dict()
    config["applications"] = [
        _build_default_application_config("data-feeder"),
        _build_default_application_config("data-reduction"),
        _build_default_application_config("visualization"),
    ]
    config["applications"][0][
        "data-handler"
    ] = "beamlime.offline.data_feeder.Fake2dDetectorImageFeeder"
    config["applications"][1][
        "data-handler"
    ] = "beamlime.offline.data_reduction.BeamLimeDataReductionApplication"
    config["applications"][2]["data-handler"] = "RealtimePlot"

    config["applications-mapping"] = [
        {"from": "data-feeder", "to": "data-reduction"},
        {"from": "data-reduction", "to": "visualization"},
    ]
    return config


def _build_default_data_reduction_config() -> dict:
    config = dict()
    # workflow-target mapping
    config["workflow-target-mapping"] = [
        {"workflow": "heatmap", "targets": ["fake-2d-image"]},
        {"workflow": "frame-number-counting", "targets": ["frame-number"]},
    ]
    # workflows
    config["workflows"] = [
        _build_workflow_config(name="heatmap"),
        _build_workflow_config(name="frame-number-counting"),
    ]
    config["workflows"][0]["process"] = "heatmap_2d"
    config["workflows"][0]["process-kargs"] = {
        "threshold": 0.4,
        "binning_size": [64, 64],
    }
    config["workflows"][0][
        "output-policy"
    ] = "STACK"  # heatmap result will be accumulated
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
    3. visualization
    The scenario is that you want to take a picture of
    a little metal plate with 2d-detector, where a lime-shape is carved on.
    """
    # TODO: Detach visualization application, which is currently glued with
    # data-reduction application via queue.Queue.

    import datetime

    tpl = load_config_tpl()
    # General
    tpl["general"]["user"] = "scipp"
    tpl["general"]["name"] = "default configuration"
    tpl["general"]["last-updated"] = str(datetime.datetime.now())

    # Data Stream
    tpl["data-stream"] = _build_default_data_stream_config()

    # Data Reduction
    tpl["data-reduction"] = _build_default_data_reduction_config()
    return tpl
