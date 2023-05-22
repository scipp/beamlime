# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from functools import partial
from typing import Any, Callable, Literal, NamedTuple, TypeVar, Union

from ..core.protocols import BeamlimeApplicationProtocol, BeamlimeCommunicationProtocol
from ..core.system import BeamlimeSystem

_MicroServiceTypes = TypeVar(
    "_MicroServiceTypes", BeamlimeApplicationProtocol, BeamlimeCommunicationProtocol
)


class ServiceRecipe(NamedTuple):
    name: str
    constructor: Union[_MicroServiceTypes, str]
    specs: Any = None


def _update_or_pop(config: dict, **kwargs) -> dict:
    for key, item in kwargs.items():
        if not item:
            config.pop(key, None)
        else:
            config[key] = item
    return config


def micro_service_config(
    name: str, constructor: _MicroServiceTypes, specs: Any = None, **extra_info: Any
) -> dict:
    from ..resources.templates import load_minimum_config_tpl

    config = load_minimum_config_tpl()
    config["name"] = name
    config["constructor"] = ".".join((constructor.__module__, constructor.__name__))
    config = _update_or_pop(config, specs=specs, **extra_info)
    return config


def multi_micro_services_config(*recipes: ServiceRecipe) -> list[dict]:
    return [
        micro_service_config(recipe.name, recipe.constructor, recipe.specs)
        for recipe in recipes
    ]


def _workflow_target_config(name: str, idx: Any, *indices: Any) -> dict:
    from ..resources.templates import load_wf_target_tpl

    config = load_wf_target_tpl()
    config["name"] = name
    config["index"] = list((idx, *indices))
    return config


def _workflow_config(
    name: str, process: Callable, targets: list = None, process_specs: dict = None
) -> dict:
    from ..resources.templates import load_workflow_tpl

    tpl = load_workflow_tpl()
    tpl["name"] = name
    tpl["process"] = ".".join((process.__module__, process.__name__))
    _update_or_pop(tpl, targets=targets, process_specs=process_specs)
    tpl.pop("reference")

    return tpl


def _app_subscription(app_name: str, ch_name: str, *ch_names: str) -> dict:
    from ..resources.templates import load_subscription_tpl

    tpl = load_subscription_tpl()
    tpl["app_name"] = app_name
    tpl["channels"] = list((ch_name, *ch_names))

    return tpl


def _fake2d_data_stream_config() -> dict:
    """
    This configuration contains three basic applications,
    1. data-generator/feeder
    2. data-reduction
    3. visualisation
    The scenario is that you want to take a picture of
    a little metal plate with 2d-detector, where a lime-shape is carved on.
    """
    from ..resources.templates import load_application_specs_tpl, load_system_specs_tpl

    config = load_system_specs_tpl()

    def _data_reduction_spec() -> dict:
        reduction_spec = dict()
        # workflows
        from ..handlers.data_reduction import handover, heatmap_2d

        heatmap = _workflow_config(
            name="heatmap",
            process=heatmap_2d,
            targets=["fake-2d-image"],
            process_specs={"threshold": 0.4, "binning_size": [64, 64]},
        )
        counting = _workflow_config(
            name="frame-number-counting", process=handover, targets=["frame-number"]
        )

        reduction_spec["workflows"] = [heatmap, counting]

        # targets
        reduction_spec["targets"] = [
            _workflow_target_config("fake-2d-image", *("detector-data", "image")),
            _workflow_target_config("frame-number", *("detector-data", "frame-number")),
        ]
        return reduction_spec

    # Applications
    from beamlime.handlers.data_feeder import Fake2dDetectorImageFeeder
    from beamlime.handlers.data_reduction import BeamLimeDataReductionApplication
    from beamlime.handlers.visualisation import RealtimePlot

    data_feeder_spec = load_application_specs_tpl()
    data_feeder_spec.update(
        {
            "timeout": 1,
            "wait_interval": 0.2,
            "detector_size": [64, 64],
            "min_intensity": 0.5,
            "num_frame": 32,
            "signal_mu": 0.7,
            "signal_err": 0.2,
            "noise_mu": 0.5,
            "noise_err": 0.2,
        }
    )

    data_reduction_spec = load_application_specs_tpl()
    data_reduction_spec.update({"timeout": 5, "wait_interval": 0.1})
    data_reduction_spec.update(**_data_reduction_spec())

    visualization_spec = load_application_specs_tpl()
    visualization_spec.update({"timeout": 10, "wait_interval": 1})

    config["applications"] = multi_micro_services_config(
        ServiceRecipe("data-feeder", Fake2dDetectorImageFeeder, data_feeder_spec),
        ServiceRecipe(
            "data-reduction", BeamLimeDataReductionApplication, data_reduction_spec
        ),
        ServiceRecipe("visualization", RealtimePlot, visualization_spec),
    )

    # Communications
    from ..communication.interfaces import SingleProcessQueue

    config["communications"] = multi_micro_services_config(
        ServiceRecipe(
            "raw-data",
            SingleProcessQueue,
        ),
        ServiceRecipe(
            "reduced-data",
            SingleProcessQueue,
        ),
    )

    # Subscriptions
    config["subscriptions"] = [
        _app_subscription("data-feeder", "raw-data"),
        _app_subscription("data-reduction", "raw-data", "reduced-data"),
        _app_subscription("visualisation", "reduced-data"),
    ]

    return config


def _kafka_recipe(
    name: str, topic: str, role: Literal["consumer", "producer"]
) -> ServiceRecipe:
    if role == "consumer":
        from ..communication.interfaces import KafkaConsumer as constructor
        from ..resources.templates import (
            load_kafka_consumer_specs_tpl as load_specs_tpl,
        )
    elif role == "producer":
        from ..communication.interfaces import KafkaProducer as constructor
        from ..resources.templates import (
            load_kafka_producer_specs_tpl as load_specs_tpl,
        )
    else:
        raise ValueError("Not supported kafka role: ", role)

    specs = load_specs_tpl()
    specs["topic"] = topic

    return ServiceRecipe(name, constructor, specs)


def _fake_event_data_stream_specs() -> dict:
    """
    This configuration contains 2 temporary applications
    1. data-generator
    2. data-feeder (pre-data-reduction)
    3. data-reduction (real data-reduction)
    4. visualization
    # TODO: Add data-reduction and visualizations.
    """
    from ..resources.templates import load_application_specs_tpl, load_system_specs_tpl

    config = load_system_specs_tpl()

    # Applications
    from beamlime.handlers.data_feeder import FakeEventDataFeeder
    from beamlime.handlers.data_generator import FakeDreamDataGenerator

    data_generator_spec = load_application_specs_tpl()
    data_generator_spec.update(
        {
            "timeout": 5,
            "wait_interval": 0.1,
            "pulse_rate": 100_000,
            "original_file_name:": "DREAM_baseline_all_dets.nxs",
            "keep_tmp_file": False,
            "num_frame": 1_000_000,
            "max_events": 10e8,
            "random_seed": 0,
        }
    )

    data_feeder_spec = load_application_specs_tpl()
    data_feeder_spec.update({"timeout": 10, "wait_interval": 0.5, "chunk_size": 16})

    config["applications"] = multi_micro_services_config(
        ServiceRecipe("data-generator", FakeDreamDataGenerator, data_generator_spec),
        ServiceRecipe("data-feeder", FakeEventDataFeeder, data_feeder_spec),
    )

    # Communications
    from itertools import product

    config["communications"] = multi_micro_services_config(
        *(
            _kafka_recipe(f"kafka-{topic}-{role}", topic, role)
            for role, topic in product(["producer", "consumer"], ["log", "data"])
        )
    )

    # Subscriptions
    config["subscriptions"] = [
        _app_subscription(
            "data-generator", "kafka-log-producer", "kafka-data-producer"
        ),
        _app_subscription("data-feeder", "kafka-log-consumer", "kafka-data-consumer"),
    ]
    return config


def _build_static_configuration(
    name: str = "Beamlime Dashboard",
    constructor: Union[_MicroServiceTypes, str] = BeamlimeSystem,
    specs: Any = None,
) -> dict:
    import datetime

    config = micro_service_config(
        name=name,
        constructor=constructor,
        specs=specs,
        **{"last-updated": str(datetime.datetime.now())},
    )

    return config


build_offline_fake2d_config = partial(
    _build_static_configuration,
    name="Fake Event Stream",
    constructor=BeamlimeSystem,
    specs=_fake2d_data_stream_config(),
)

build_fake_event_stream_config = partial(
    _build_static_configuration,
    name="Fake Event Stream",
    constructor=BeamlimeSystem,
    specs=_fake_event_data_stream_specs(),
)
