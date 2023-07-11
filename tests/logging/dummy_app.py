# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging
from dataclasses import dataclass

from beamlime.logging.mixins import LogMixin


@dataclass
class LogMixinDummy(LogMixin):
    logger: logging.Logger
