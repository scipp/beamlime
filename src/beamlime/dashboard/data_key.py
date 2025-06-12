# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class DataKey:
    service_name: str
    source_name: str
    key: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ComponentDataKey(ABC):
    component_name: str
    view_name: str

    @property
    @abstractmethod
    def service_name(self) -> str:
        """
        Returns the name of the service this component belongs to.
        This is used to identify the service in the data store.
        """

    def cumulative_key(self) -> DataKey:
        return DataKey(
            service_name=self.service_name,
            source_name=self.component_name,
            key=f'{self.view_name}/cumulative',
        )

    def current_key(self) -> DataKey:
        return DataKey(
            service_name=self.service_name,
            source_name=self.component_name,
            key=f'{self.view_name}/current',
        )


class MonitorDataKey(ComponentDataKey):
    @property
    def service_name(self) -> str:
        return 'monitor_data'


class DetectorDataKey(ComponentDataKey):
    @property
    def service_name(self) -> str:
        return 'detector_data'
