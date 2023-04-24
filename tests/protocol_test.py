# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# @author Sunyoung Yoo
# ``issubclass`` does not support ``Protocol``s with non-method members.
from beamlime.core import protocols as bm_protocol
from tests.test_helper import DummyApp

# def test_system_protocol():
#     # TODO: reorganize protocols regarding daemon interfaces.
#     from beamlime.applications.interfaces import BeamlimeApplicationProtocol
#     from beamlime.core.system import DaemonApplicationInstanceGroup

#     ag = DaemonApplicationInstanceGroup(constructor=DummyApp, instance_num=1)
#     assert isinstance(ag, DaemonApplicationInstanceGroup) and isinstance(
#         ag, BeamlimeApplicationProtocol
#     )


def test_logging_mixin_protocol():
    from beamlime.applications.interfaces import _LogMixin

    assert issubclass(_LogMixin, bm_protocol.BeamlimeLoggingProtocol)


def test_flag_based_control_mixin_protocol():
    from beamlime.applications.interfaces import _FlagControlMixin

    assert issubclass(_FlagControlMixin, bm_protocol.BeamlimeApplicationControlProtocol)


def test_daemon_application_interface_protocol():
    from beamlime.applications.interfaces import _DaemonInterface

    assert issubclass(_DaemonInterface, bm_protocol.BeamlimeDaemonAppProtocol)


def test_application_interface_protocol():
    from beamlime.applications.interfaces import BeamlimeApplicationInterface

    app = DummyApp()
    assert isinstance(app, BeamlimeApplicationInterface) and isinstance(
        app, bm_protocol.BeamlimeApplicationProtocol
    )
