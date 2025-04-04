# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Any

import scipp as sc

from ..core.handler import (
    ConfigRegistry,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import Message, MessageKey
from .accumulators import DetectorEvents, ToNXevent_data
from .monitor_data_handler import MonitorDataPreprocessor
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
        workflow_manager: WorkflowManager,
        f144_attribute_registry: dict[str, dict[str, Any]],
        logger: logging.Logger | None = None,
        config_registry: ConfigRegistry,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config_registry = config_registry
        self._workflow_manager = workflow_manager
        self._f144_attribute_registry = f144_attribute_registry

    def _is_nxlog(self, key: MessageKey) -> bool:
        return key.topic.split('_', maxsplit=1)[1] in ('motion',)

    def _is_monitor(self, key: MessageKey) -> bool:
        return key.topic.split('_', maxsplit=1)[1] in ('beam_monitor',)

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
            return NullHandler(logger=self._logger, config={})

        # Note we are setting config = {} unless we are processing detector data. The
        # config is used, e.g., to implement a "clear" mechanism. For data reduction,
        # clearing is however handled by "clearing" the entire data reduction workflow
        # for a detector, i.e., auxiliary data such as logs and monitors are cleared
        # when the workflow is cleared. This is done via the proxy the workflow manager
        # returns for the detector data accumulator.
        if self._is_nxlog(key):
            attrs = self._f144_attribute_registry[key.source_name]
            preprocessor = ToNXlog(attrs=attrs)
            config = {}
        elif self._is_monitor(key):
            preprocessor = MonitorDataPreprocessor(config={})
            config = {}
        else:
            preprocessor = ToNXevent_data()
            config = self._config_registry.get_config(key.source_name)
        self._logger.info(
            "%s using preprocessor %s", key.source_name, preprocessor.__class__.__name__
        )
        self._logger.info("%s using accumulator %s", key.source_name, accumulator)

        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=config,
            preprocessor=preprocessor,
            accumulators={f'reduced/{key.source_name}': accumulator},
        )
