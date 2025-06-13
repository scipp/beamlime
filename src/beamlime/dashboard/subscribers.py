# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import scipp as sc

from .data_key import ComponentDataKey
from .data_service import DataKey
from .data_subscriber import DataSubscriber, Pipe


@dataclass(frozen=True, slots=True, kw_only=True)
class RawData:
    cumulative: sc.DataArray
    current: sc.DataArray


class ComponentDataSubscriber(DataSubscriber):
    """
    Subscriber for processing component (detector or monitor) data.

    This subscriber is responsible for handling "raw" data that has both cumulative and
    current values.
    """

    def __init__(self, *, component_key: ComponentDataKey, pipe: Pipe) -> None:
        self._cumulative_key = component_key.cumulative_key()
        self._current_key = component_key.current_key()
        self._pipe = pipe
        super().__init__({self._cumulative_key, self._current_key})

    def send(self, data: dict[DataKey, Any]) -> None:
        detector_data = RawData(
            cumulative=data[self._cumulative_key], current=data[self._current_key]
        )
        self._pipe.send(detector_data)
