# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

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
        self._mode: Literal['cumulative', 'current'] = 'current'
        self._current_key = component_key.current_key()
        self._pipe = pipe
        super().__init__({self._cumulative_key, self._current_key})

    @property
    def mode(self) -> Literal['cumulative', 'current']:
        """
        The current mode of the subscriber, either 'cumulative' or 'current'.
        """
        return self._mode

    @mode.setter
    def mode(self, value: Literal['cumulative', 'current']) -> None:
        """
        Set the mode of the subscriber. This determines which data key is used for
        sending data.
        """
        if value not in {'cumulative', 'current'}:
            raise ValueError(
                f"Invalid mode: {value}. Must be 'cumulative' or 'current'."
            )
        self._mode = value

    def send(self, data: dict[DataKey, Any]) -> None:
        if self._mode == 'cumulative':
            da = data.get(self._cumulative_key)
        elif self._mode == 'current':
            da = data.get(self._current_key)
        else:
            return
        self._pipe.send(da)
