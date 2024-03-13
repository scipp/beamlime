# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

from beamlime.applications._nexus_helpers import NexusContainer


def test_nexus_container_initialized_from_path():
    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    NexusContainer.from_template_file(path)
