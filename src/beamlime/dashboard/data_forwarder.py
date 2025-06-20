# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from contextlib import ExitStack, contextmanager

import scipp as sc

from .data_key import DataKey
from .data_service import DataService


class DataForwarder:
    """A class to forward data to a data service based on stream names."""

    def __init__(self, data_services: dict[str, DataService]) -> None:
        self._data_services = data_services

    def __contains__(self, data_service_name: str) -> bool:
        """
        Check if a data service with the given name exists.

        Parameters
        ----------
        data_service_name:
            The name of the data service to check.

        Returns
        -------
        :
            True if the data service exists, False otherwise.
        """
        return data_service_name in self._data_services

    @contextmanager
    def transaction(self):
        """Context manager for batching updates across all data services."""
        with ExitStack() as stack:
            for service in self._data_services.values():
                stack.enter_context(service.transaction())
            yield

    def forward(self, stream_name: str, value: sc.DataArray) -> None:
        """
        Forward data to the appropriate data service based on the stream name.

        Parameters
        ----------
        stream_name:
            The name of the stream in the format 'source_name/service_name/suffix'. The
            suffix may contain additional '/' characters which will be ignored.
        value:
            The data to be forwarded.
        """
        try:
            source_name, service_name, key = stream_name.split('/', maxsplit=2)
        except ValueError:
            raise ValueError(
                f"Invalid stream name format '{stream_name}'. Expected format: "
                "'source_name/service_name/key'."
            ) from None
        if (service := self._data_services.get(service_name)) is not None:
            data_key = DataKey(
                service_name=service_name, source_name=source_name, key=key
            )
            service[data_key] = value
