# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo
# ``issubclass`` does not support ``Protocol``s with non-method members.
from beamlime import protocols as bm_protocol
from tests.test_helper import DummyApp


def test_instace_group_protocol():
    from beamlime.applications import BeamlimeApplicationInstanceGroup

    app_config = {"name": "dummy", "data-handler": "tests.test_helper.DummyApp"}
    ag = BeamlimeApplicationInstanceGroup(app_config=app_config, app_spec={})
    assert isinstance(ag, bm_protocol.BeamlimeApplicationProtocol)


def test_logging_mixin_protocol():
    from beamlime.applications.mixins import LogMixin

    assert isinstance(LogMixin(), bm_protocol.BeamlimeLoggingProtocol)


def test_flag_based_control_mixin_protocol():
    from beamlime.applications.mixins import FlagControlMixin

    assert isinstance(FlagControlMixin(), bm_protocol.BeamlimeControlProtocol)


def test_coroutine_mixin_protocol():
    from beamlime.applications.mixins import CoroutineMixin

    assert isinstance(CoroutineMixin(), bm_protocol.BeamlimeCoroutineProtocol)


def test_application_interface_protocol():
    from beamlime.applications.interfaces import BeamlimeApplicationInterface

    app = DummyApp(app_name="Dummy Application")
    assert isinstance(app, BeamlimeApplicationInterface) and isinstance(
        app, bm_protocol.BeamlimeApplicationProtocol
    )
