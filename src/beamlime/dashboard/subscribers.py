# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import scipp as sc

from .data_key import ComponentDataKey
from .data_service import DataKey
from .data_subscriber import StreamAssembler


@dataclass(frozen=True, slots=True, kw_only=True)
class RawData:
    cumulative: sc.DataArray
    current: sc.DataArray


class ComponentStreamAssembler(StreamAssembler):
    """
    Assembler for processing component (detector or monitor) data.

    This assembler handles "raw" data that has both cumulative and current values.
    """

    def __init__(self, component_key: ComponentDataKey) -> None:
        self._cumulative_key = component_key.cumulative_key()
        self._current_key = component_key.current_key()
        super().__init__({self._cumulative_key, self._current_key})

    def assemble(self, data: dict[DataKey, Any]) -> RawData:
        return RawData(
            cumulative=data[self._cumulative_key], current=data[self._current_key]
        )


class MergingStreamAssembler(StreamAssembler):
    """Assembler for merging data from multiple sources into a dict."""

    def assemble(self, data: dict[DataKey, Any]) -> dict[DataKey, Any]:
        return {key: data[key] for key in self.keys if key in data}
