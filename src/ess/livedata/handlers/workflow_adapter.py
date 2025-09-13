# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Adapt ess.reduce.streaming.StreamProcessor to the Workflow protocol."""

from __future__ import annotations

from collections.abc import Hashable
from typing import Any

import sciline
import sciline.typing

from ess.reduce import streaming

from .workflow_factory import Workflow


class WorkflowAdapter(Workflow):
    def __init__(
        self,
        *,
        dynamic_key_map: dict[Hashable, sciline.typing.Key],
        context_key_map: dict[Hashable, sciline.typing.Key] | None = None,
        stream_processor: streaming.StreamProcessor,
    ) -> None:
        self._stream_processor = stream_processor
        self._dynamic_key_map = dynamic_key_map
        self._context_key_map = context_key_map or {}

    @staticmethod
    def create(
        base_workflow: sciline.Pipeline,
        *,
        dynamic_keys: dict[Hashable, sciline.typing.Key],
        context_keys: dict[Hashable, sciline.typing.Key] | None = None,
        # target_keys: dict[Hashable, sciline.typing.Key] | None = None,
        **kwargs: Any,
    ) -> WorkflowAdapter:
        processor = streaming.StreamProcessor(
            base_workflow,
            dynamic_keys=tuple(dynamic_keys.values()),
            context_keys=tuple(context_keys.values()) if context_keys else (),
            # target_keys=tuple(target_keys.values()) if target_keys else (),
            **kwargs,
        )
        return WorkflowAdapter(
            dynamic_key_map=dynamic_keys,
            context_key_map=context_keys,
            stream_processor=processor,
        )

    def accumulate(self, data: dict[Hashable, Any]) -> None:
        context = {
            sciline_key: data[key]
            for key, sciline_key in self._context_key_map.items()
            if key in data
        }
        dynamic = {
            sciline_key: data[key]
            for key, sciline_key in self._dynamic_key_map.items()
            if key in data
        }
        self._stream_processor.set_context(context)
        self._stream_processor.accumulate(dynamic)

    def finalize(self) -> dict[str, Any]:
        targets = self._stream_processor.finalize()
        return {str(k): v for k, v in targets.items()}

    def clear(self) -> None:
        self._stream_processor.clear()
