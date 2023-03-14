# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

from ..loaders import load_yaml


def _load_generated_yaml(filename):
    from . import __name__

    return load_yaml(filename, module=__name__)


load_static_default_config = partial(
    _load_generated_yaml, filename="default-setting.yaml"
)
load_static_sample_workflow_config = partial(
    _load_generated_yaml, filename="sample-workflow.yaml"
)
