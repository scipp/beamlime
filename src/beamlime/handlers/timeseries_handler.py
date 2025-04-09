# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

import scipp as sc

from ..config.instruments import get_config
from ..core.handler import (
    ConfigRegistry,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import StreamId
from .accumulators import ForwardingAccumulator, LogData
from .to_nx_log import ToNXlog


class LogdataHandlerFactory(HandlerFactory[LogData, sc.DataArray]):
    """
    Factory for creating handlers for log data.

    This factory creates a handler that accumulates log data and returns it as a
    DataArray.
    """

    def __init__(
        self,
        *,
        instrument: str,
        logger: logging.Logger | None = None,
        config_registry: ConfigRegistry,
        attribute_registry: dict[str, dict[str, any]] | None = None,
    ) -> None:
        """
        Initialize the LogdataHandlerFactory.

        Parameters
        ----------
        instrument:
            The name of the instrument.
        logger:
            The logger to use for logging messages.
        config:
            Configuration for the handler.
        attribute_registry:
            A dictionary mapping source names to attributes. This provides essential
            attributes for the values and timestamps in the log data. Log messages do
            not contain this information, so it must be provided externally.
            The keys of the dictionary are source names, and the values are dictionaries
            containing the attributes as they would be found in the fields of an NXlog
            class in a NeXus file.
        """

        self._logger = logger or logging.getLogger(__name__)
        self._config_registry = config_registry
        self._instrument = instrument
        if attribute_registry is None:
            self._attribute_registry = get_config(instrument).f144_attribute_registry
        else:
            self._attribute_registry = attribute_registry

    def make_handler(self, key: StreamId) -> Handler[LogData, sc.DataArray] | None:
        source_name = key.name
        attrs = self._attribute_registry.get(source_name)
        if attrs is None:
            self._logger.warning(
                "No attributes found for source name '%s'. Messages will be dropped.",
                source_name,
            )
            return None

        try:
            to_nx_log = ToNXlog(attrs=attrs)
        except Exception:
            self._logger.exception(
                "Failed to create NXlog for source name '%s'. "
                "Messages will be dropped.",
                source_name,
            )
            return None

        accumulators = {'timeseries': ForwardingAccumulator()}
        config = self._config_registry.get_config(source_name)
        return PeriodicAccumulatingHandler(
            service_name=self._config_registry.service_name,
            logger=self._logger,
            config=config,
            preprocessor=to_nx_log,
            accumulators=accumulators,
        )
