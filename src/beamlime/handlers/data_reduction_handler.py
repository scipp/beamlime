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
from ..core.message import Message, StreamKey, StreamKind
from .accumulators import DetectorEvents, ToNXevent_data
from .monitor_data_handler import make_monitor_data_preprocessor
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

    def make_handler(
        self, key: StreamKey
    ) -> Handler[DetectorEvents, sc.DataGroup[sc.DataArray]]:
        self._logger.info("Creating handler for %s", key)
        accumulator = self._workflow_manager.get_accumulator(key.name)
        if accumulator is None:
            self._logger.warning(
                "No workflow key found for source name %s, using null handler",
                key.name,
            )
            return NullHandler(logger=self._logger, config={})

        # Note we are setting config = {} unless we are processing detector data. The
        # config is used, e.g., to implement a "clear" mechanism. For data reduction,
        # clearing is however handled by "clearing" the entire data reduction workflow
        # for a detector, i.e., auxiliary data such as logs and monitors are cleared
        # when the workflow is cleared. This is done via the proxy the workflow manager
        # returns for the detector data accumulator.
        match key.kind:
            case StreamKind.MONITOR_EVENTS | StreamKind.MONITOR_COUNTS:
                # TODO The config for the preprocessor may need to be setup differently,
                # e.g., to obtain the desired number of TOA bins. Or maybe we should
                # not histogram in the preprocessor, i.e., not reuse the same
                # preprocessor as in the monitor_data service.
                preprocessor = make_monitor_data_preprocessor(key, config={})
                config = {}
            case StreamKind.LOG:
                attrs = self._f144_attribute_registry[key.name]
                preprocessor = ToNXlog(attrs=attrs)
                config = {}
            case StreamKind.DETECTOR_EVENTS:
                preprocessor = ToNXevent_data()
                config = self._config_registry.get_config(key.name)
            case _:
                raise ValueError(f"Invalid stream kind: {key.kind}")
        self._logger.info(
            "%s using preprocessor %s", key.name, preprocessor.__class__.__name__
        )
        self._logger.info("%s using accumulator %s", key.name, accumulator)

        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=config,
            preprocessor=preprocessor,
            accumulators={f'reduced/{key.name}': accumulator},
        )
