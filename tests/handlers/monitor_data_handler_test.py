# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import replace

import numpy as np
from scipp.testing import assert_identical

from beamlime.core.handler import FakeConfigRegistry, Message, StreamId, StreamKind
from beamlime.handlers.monitor_data_handler import (
    MonitorEvents,
    MonitorHandlerFactory,
)
