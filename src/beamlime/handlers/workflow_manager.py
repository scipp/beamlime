# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from sciline.typing import Key

from beamlime.handlers.stream_processor_factory import StreamProcessorFactory


class WorkflowManager:
    def __init__(
        self,
        *,
        processor_factory: StreamProcessorFactory,
        source_to_key: dict[str, Key],
    ) -> None:
        """
        Parameters
        ----------
        processor_factory:
            Factory for creating stream processors.
        source_to_key:
            Dictionary mapping source names to workflow input keys.
        """
        self.processor_factory = processor_factory
        self.source_to_key = source_to_key
