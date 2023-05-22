# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Protocol, TypeVar, runtime_checkable

from beamlime import __name__ as RESERVED_APP_NAME  # noqa F401

from .tools import find_home

PresetValueType = TypeVar("PresetValueType", str, int)


@runtime_checkable
class PresetOptionProtocol(Protocol):
    @property
    @staticmethod
    def default() -> PresetValueType:
        ...


@runtime_checkable
class NumericBoundPresetOptionProtocol(PresetOptionProtocol, Protocol):
    @property
    @staticmethod
    def minimum() -> PresetValueType:
        ...

    @property
    @staticmethod
    def maximum() -> PresetValueType:
        ...


class SystemClass:
    """
    Beamlime system class.
    Later, we might want to have different system classes for special purposes.

    Default
    -------
    ``beamlime.core.system.BeamlimeSystem``

    """

    default = "beamlime.core.system.BeamlimeSystem"


class HomeDirectory:
    """
    Home directory for beamlime system.

    Default
    -------
    ``~/.beamlime``

    """

    default = find_home()


class ConfigurationPath:
    """
    Configuration file path.

    Default
    -------
    ``~/.beamlime/default-config.yaml``
    """

    default = HomeDirectory.default.joinpath("default-config.yaml")


class LogDirectory:
    """
    Directory of collected log files.

    Default
    -------
    ``~/.beamlime/logs/``
    """

    default = HomeDirectory.default.joinpath("logs")


class CustomHandlerDirectory:
    """
    Directory of Custom handlers for beamlime applications.

    Default
    -------
    ``~/.beamlime/custom-handlers/``
    """

    default = HomeDirectory.default.joinpath("custom-handlers")


class InstanceLife:
    """Number of times that an instance can be recreated upon unexpected abortion."""

    minimum = 1
    default = 1
    maximum = 10
    system_maximum = 1_000  # TODO: If revival attempts exceed ``system_maximum``,
    # it should turn down the system.


class Timeout:
    """Timeout preset options in seconds."""

    minimum = 0
    default = 60
    maximum = 600


class WaitInterval:
    """Wait interval (update rate) in seconds."""

    minimum = 1e-2
    default = 1
    maximum = Timeout.maximum


class InstanceNumber:
    """Number of instances for a single application."""

    minimum = 1
    default = 1
    maximum = 3


class NewDataPolicy:
    """
    Expected behaviour on new data.

    Default
    -------
    ``REPLACE``

    Options
    -------

    1. ``REPLACE`` - Default
    Only new data shall be used.
    New data will replace the previous data, and it does not save the ``previous`` data.

    2. ``SKIP``
    New data or previous data shall be used.
    New data will be used only if it is different from the ``previous`` one
    and it will replace the ``previous``.
    ``input`` with ``SKIP`` policy, will work like caching with maximum capacity of 1.
    ``result`` with ``SKIP`` policy, will not update the ``history``
    but the process to retrieve the ``result`` will still have to be executed.

    3. ``STACK``
    Sum of all data collected including the new data shall be used.
    The length of the data shall stay the same, but each value will be updated.

    4. ``AVERAGE``
    Average of all data collected including the new data shall be used.
    # TODO: ``AVERAGE`` and ``APPEND`` may cause too much memory consumption
    so it should be either forbidden or managed.
    Currently ``AVERAGE`` will not increase the memory consumption in the
    ``offline.data_reduction.BeamLimeDataReductionApplication``,
    since the average is calculated like below.

    Given
    :math:`old = \text{previous average}`,
    :math:`new = \text{new data from the data stream}`
    :math:`total = \text{total number of the collected data including new one}`

    , average is calculated like below
    :math:`average = \frac{(total-1) \times old}{total} + \frac{new}{total}`

    It will stack the error caused by inaccurate division.
    Increasing length of the data indefinitely and
    stacking error should not happen so ``AVERAGE`` logic should be updated.

    5. ``APPEND``
    New data will extend the previous data.
    The length of the data may increase, but each value will stay the same.
    # TODO: We might have to forbid ``APPEND`` at all in most cases
    in live data reduction.

    """

    REPLACE = "REPLACE"
    SKIP = "SKIP"
    STACK = "STACK"
    AVERAGE = "AVERAGE"
    APPEND = "APPEND"

    default = REPLACE


class CommunicationChannel:
    """
    Communication channel type.

    Default
    -------
    ``SQUEUE``

    Options
    -------
    1. SQUEUE
    2. MQUEUE
    3. Kafka Consumer
    4. Kafka Producer
    5. Bulletin Board
    """

    SQUEUE = "SQUEUE"  # Single process queue.
    MQUEUE = "MQUEUE"  # Multi process queue.
    KAFKA_CONSUMER = "KAFKA-CONSUMER"
    KAFKA_PRODUCER = "KAFKA-PRODUCER"
    BULLETIN_BOARD = "BULLETIN-BOARD"

    default = SQUEUE


class ContextScope:
    """
    Scope of application context.

    Default
    -------
    ``THREAD``

    Options
    -------
    1. ``THREAD`` - Default.
    Single/multi-thread in a single process.

    2. ``PROCESS``
    Single/multi-thread in single process or multiple processes.
    Not supported yet.

    3. ``CLUSTER``
    Single/multi-thread in single/multi-process in single/multi-machines.
    Not supported yet.

    """

    THREAD = "THREAD"
    PROCESS = "PROCESS"
    CLUSTER = "CLUSTER"

    default = THREAD
