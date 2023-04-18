# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo

from beamlime.core.application import (
    BeamlimeApplicationInterface,
    BeamLimeApplicationProtocol,
)
from tests.test_helper import DummyApp


def test_interface_protocol():
    # ``issubclass`` does not support ``Protocol``s with non-method members.
    app = DummyApp()
    assert isinstance(app, BeamlimeApplicationInterface) and isinstance(
        app, BeamLimeApplicationProtocol
    )
