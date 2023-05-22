# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo


from beamlime.config.builders import (
    build_fake_event_kafka_config,
    build_offline_fake2d_config,
)


def _is_config_complete(config: dict):
    for value in config.values():
        if value is None or (
            isinstance(value, dict) and not _is_config_complete(value)
        ):
            return False
    return True


def test_config_completion():
    assert _is_config_complete(build_offline_fake2d_config())
    assert _is_config_complete(build_fake_event_kafka_config())
