# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

# Original source from
# https://github.com/scipp/scipp/blob/main/src/scipp/html/resources.py

import importlib.resources
from functools import lru_cache, partial


def _read_text(filename):
    if hasattr(importlib.resources, "files"):
        # Use new API added in Python 3.9
        return (
            importlib.resources.files("peek_data.config.resources")
            .joinpath(filename)
            .read_text()
        )
    # Old API, deprecated as of Python 3.11
    return importlib.resources.read_text("peek_data.config.resources", filename)


def _preprocess_yaml(template: str) -> str:
    import re

    # remove comments
    template = re.sub(r"#(.+)", "", template)
    # remove empty spaces at the end
    template = re.sub(r"(\s+)$", "", template)
    # remove unnecessary line breaks
    template = re.sub(r"\n(\n+)", "\n", template)
    return re.sub(r"^\n", "", template)


@lru_cache(maxsize=1)
def load_default_config_yaml() -> str:
    default_config = _read_text("default-setting.yaml")
    return _preprocess_yaml(default_config)


@lru_cache(maxsize=4)
def load_user_config_yaml(path: str) -> str:
    return _preprocess_yaml(_read_text(path))


@lru_cache(maxsize=5)
def _load_config_yaml_tpl(name: str) -> str:
    config_tpl = _read_text(name + ".yaml.template")
    return _preprocess_yaml(config_tpl)


load_config_tpl = partial(_load_config_yaml_tpl, name="config")
load_data_stream_mapping_tpl = partial(
    _load_config_yaml_tpl, name="data-stream-mapping"
)
load_data_stream_interface_tpl = partial(
    _load_config_yaml_tpl, name="data-stream-interface"
)
load_target_tpl = partial(_load_config_yaml_tpl, name="target")
load_workflow_tpl = partial(_load_config_yaml_tpl, name="workflow")
load_kafka_tpl = partial(_load_config_yaml_tpl, name="kafka")
load_internal_tpl = partial(_load_config_yaml_tpl, name="internal-byte-stream")
