# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from typing import Union

from ...config.tools import import_object
from ..loaders import load_yaml


def _replace_preset_symbol(tpl: Union[dict, list]):
    """
    Recursively replace symbols in code block to values in the template.
    This helper is only for templates.
    Do not use this for loading static configuration.
    """

    def _replace(value):
        # If the value is in code block.
        if isinstance(value, str) and value.startswith("``") and value.endswith("``"):
            try:
                obj_path = value.removeprefix("``").removesuffix("``")
                if isinstance((obj := import_object(obj_path)), (str, int, float)):
                    return obj
                else:
                    return str(obj)
            except ModuleNotFoundError:
                ...
        return value

    if isinstance(tpl, dict):
        keys = tpl.keys()
    elif isinstance(tpl, list):
        keys = range(len(tpl))

    for key in keys:
        if isinstance(tpl[key], str):
            tpl[key] = _replace(tpl[key])
        if isinstance(tpl[key], (dict, list)):
            tpl[key] = _replace_preset_symbol(tpl=tpl[key])

    return tpl


def _load_tpl(tpl_name: str) -> dict:
    tpl = load_yaml(tpl_name + ".yaml", module=__package__)

    return _replace_preset_symbol(tpl)


# Minimum configuration of micro services
load_minimum_config_tpl = partial(_load_tpl, tpl_name="minimum-config")
# microservice:System related
load_system_specs_tpl = partial(_load_tpl, tpl_name="system-specs")
load_subscription_tpl = partial(_load_tpl, tpl_name="system-subscription")
# microservice:Application related
load_application_specs_tpl = partial(_load_tpl, tpl_name="application-specs")
# microservice:Communication related
load_kafka_consumer_specs_tpl = partial(_load_tpl, tpl_name="kafka-consumer-specs")
load_kafka_producer_specs_tpl = partial(_load_tpl, tpl_name="kafka-producer-specs")
# Data Reduction
load_workflow_tpl = partial(_load_tpl, tpl_name="workflow")
load_wf_target_tpl = partial(_load_tpl, tpl_name="workflow-target")
