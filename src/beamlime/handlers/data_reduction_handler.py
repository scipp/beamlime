# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

import scipp as sc

from ..core.handler import Config, Handler, HandlerFactory, PeriodicAccumulatingHandler
from ..core.message import Message, MessageKey
from .accumulators import DetectorEvents, ToNXevent_data
from .to_nx_log import ToNXlog
from .workflow_manager import WorkflowManager


class NullHandler(Handler[Any, None]):
    def handle(self, messages: list[Message[Any]]) -> list[Message[None]]:
        return []


class ReductionHandlerFactory(
    HandlerFactory[DetectorEvents, sc.DataGroup[sc.DataArray]]
):
    """
    Factory for data reduction handlers.
    """

    def __init__(
        self,
        *,
        instrument_config: ModuleType,
        logger: logging.Logger | None = None,
        config: Config,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._workflow_manager = WorkflowManager(instrument_config=instrument_config)

    def _is_nxlog(self, key: MessageKey) -> bool:
        return key.topic.split('_', maxsplit=1)[1] in ('motion',)

    def make_handler(
        self, key: MessageKey
    ) -> Handler[DetectorEvents, sc.DataGroup[sc.DataArray]]:
        self._logger.info("Creating handler for %s", key)
        accumulator = self._workflow_manager.get_accumulator(key.source_name)
        if accumulator is None:
            self._logger.warning(
                "No workflow key found for source name %s, using null handler",
                key.source_name,
            )
            return NullHandler(logger=self._logger, config=self._config)

        if self._is_nxlog(key):
            attrs = self._workflow_manager.attrs_for_f144(key.source_name)
            preprocessor = ToNXlog(attrs=attrs)
        else:
            preprocessor = ToNXevent_data()
        self._logger.info(
            "Preprocessor %s is used for source name %s",
            preprocessor.__class__.__name__,
            key.source_name,
        )
        self._logger.info(
            "Accumulator %s is used for source name %s", accumulator, key.source_name
        )

        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators={f'reduced/{key.source_name}': accumulator},
        )
