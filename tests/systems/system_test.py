# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from beamlime.core.application import BeamLimeApplicationProtocol
from beamlime.core.system import AsyncApplicationInstanceGroup
from tests.test_helper import DummyApp


def test_system_protocol():
    # ``issubclass`` does not support ``Protocol``s with non-method members.
    ag = AsyncApplicationInstanceGroup(constructor=DummyApp, instance_num=1)
    assert isinstance(ag, AsyncApplicationInstanceGroup) and isinstance(
        ag, BeamLimeApplicationProtocol
    )
