# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

from ..loaders import load_yaml


def _load_tpl(tpl_name):
    return load_yaml(tpl_name + ".yaml", module=__package__)


load_config_tpl = partial(_load_tpl, tpl_name="config")
load_data_stream_app_tpl = partial(_load_tpl, tpl_name="data-stream-application")
load_data_stream_app_spec_tpl = partial(_load_tpl, tpl_name="data-stream-application")
load_data_stream_mapping_tpl = partial(_load_tpl, tpl_name="data-stream-mapping")
load_target_tpl = partial(_load_tpl, tpl_name="target")
load_workflow_tpl = partial(_load_tpl, tpl_name="workflow")
