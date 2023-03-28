# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo


from beamlime.config.builders import (
    _build_default_application_config,
    build_default_config,
)


def is_config_complete(config: dict):
    for value in config.values():
        if value is None or (isinstance(value, dict) and not is_config_complete(value)):
            return False
    return True


def test_default_config_completion():
    assert is_config_complete(_build_default_application_config("interface-0"))
    assert is_config_complete(build_default_config())
