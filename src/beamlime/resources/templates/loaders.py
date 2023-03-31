# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from typing import Union

from ...config.tools import import_object
from ..loaders import load_yaml


def _replace_preset_symbol(tpl: Union[dict, list]):
    """
    Recursively replace symbols to values in the template.
    This helper is only for templates.
    Do not use this for loading static configuration.
    """
    # TODO: Write down when you can use symbol in the yaml template.

    def _replace(value):
        if not isinstance(value, str):
            return value
        preset = import_object(value)
        if hasattr(preset, "DEFAULT"):
            return preset.DEFAULT
        else:
            return str(preset)

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


def _load_tpl(tpl_name: str, replace_symbol: bool = False) -> dict:
    tpl = load_yaml(tpl_name + ".yaml", module=__package__)

    if replace_symbol:
        return _replace_preset_symbol(tpl)

    return tpl


load_config_tpl = partial(_load_tpl, tpl_name="config")
load_data_stream_app_tpl = partial(
    _load_tpl, tpl_name="data-stream-application", replace_symbol=True
)
load_data_stream_app_spec_tpl = partial(_load_tpl, tpl_name="data-stream-application")
load_data_stream_mapping_tpl = partial(_load_tpl, tpl_name="data-stream-mapping")
load_target_tpl = partial(_load_tpl, tpl_name="target")
load_workflow_tpl = partial(_load_tpl, tpl_name="workflow", replace_symbol=True)
