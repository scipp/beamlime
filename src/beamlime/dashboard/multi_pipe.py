# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .data_service import DataKey


class MultiPipe(ABC):
    """Base class for pipes that define their data dependencies."""

    @property
    @abstractmethod
    def keys(self) -> set[DataKey]:
        """Return the set of data keys this pipe depends on."""

    @abstractmethod
    def send(self, data: dict[DataKey, Any]) -> None:
        """
        Send data to the UI component.

        Parameters
        ----------
        data
            Complete data for all keys this pipe depends on.
        """
