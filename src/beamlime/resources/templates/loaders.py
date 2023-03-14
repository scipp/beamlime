# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

from .. import load_yaml


def _load_tpl(tpl_name):
    from . import __name__

    return load_yaml(tpl_name + ".yaml", module=__name__)


load_config_tpl = partial(_load_tpl, tpl_name="config")
load_data_stream_interface_tpl = partial(_load_tpl, tpl_name="data-stream-interface")
load_data_stream_mapping_tpl = partial(_load_tpl, tpl_name="data-stream-mapping")
load_internal_stream_tpl = partial(_load_tpl, tpl_name="internal-stream")
load_kafka_tpl = partial(_load_tpl, tpl_name="kafka")
load_target_tpl = partial(_load_tpl, tpl_name="target")
load_workflow_tpl = partial(_load_tpl, tpl_name="workflow")
