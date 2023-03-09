# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

# Original source from
# https://github.com/scipp/scipp/blob/main/src/scipp/html/resources.py

import importlib.resources
from functools import lru_cache, partial
from os import listdir

import yaml


def _read_text(path, module="peek_data.config.resources"):
    return importlib.resources.files(module).joinpath(path).read_text()


@lru_cache(maxsize=1)
def _get_number_of_templates():
    tpl_dir = importlib.resources.files("peek_data.config.resources.templates")
    return len([f_name for f_name in listdir(tpl_dir) if f_name.endswith(".yaml")])


@lru_cache(maxsize=_get_number_of_templates())
def _read_template(tpl_name):
    module = "peek_data.config.resources.templates"
    return _read_text(tpl_name + ".yaml", module=module)


@lru_cache(maxsize=1)
def load_default_config_yaml() -> str:
    default_config = _read_text("default-setting.yaml")
    return default_config


@lru_cache(maxsize=4)
def load_user_config_yaml(path: str) -> str:
    return _read_text(path)


def _load_yaml_tpl(name: str) -> str:
    return yaml.safe_load(_read_template(name))


load_config_tpl = partial(_load_yaml_tpl, name="config")
load_data_stream_mapping_tpl = partial(_load_yaml_tpl, name="data-stream-mapping")
load_data_stream_interface_tpl = partial(_load_yaml_tpl, name="data-stream-interface")
load_target_tpl = partial(_load_yaml_tpl, name="target")
load_workflow_tpl = partial(_load_yaml_tpl, name="workflow")
load_kafka_tpl = partial(_load_yaml_tpl, name="kafka")
load_internal_tpl = partial(_load_yaml_tpl, name="internal-byte-stream")
