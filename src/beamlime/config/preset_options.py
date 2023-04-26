# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import enum
from typing import Protocol, Union

from beamlime import __name__ as RESERVED_APP_NAME  # noqa F401

from .tools import find_home

if hasattr(enum, "StrEnum"):
    FlagType = enum.StrEnum
else:
    FlagType = enum.Flag

HOME_DIR = find_home()  # ~/.beamlime
DEFAULT_CONFIG_PATH = HOME_DIR.joinpath("default-config.yaml")
DEFAULT_LOG_DIR = HOME_DIR.joinpath("logs")
DEFAULT_CUSTOM_HANDLER_DIR = HOME_DIR.joinpath("custom-handlers")

DEFAULT_TIMEOUT = 60  # s
MAX_TIMEOUT = 600  # s
DEFAULT_WAIT_INTERVAL = 1  # s
MIN_WAIT_INTERVAL = 1e-2  # s


class PresetOptionProtocol(Protocol):
    @classmethod
    @property
    def DEFAULT(cls) -> Union[str, int]:
        pass


class NewDataPolicyOptions(FlagType):
    """

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

    @classmethod
    @property
    def DEFAULT(cls):
        return cls.REPLACE.value


class CommunicationChannelOptions(FlagType):
    """
    1. ``QUEUE``
    Use ``queue.QUEUE`` as a communication interface between two applications.
    #TODO: Currently, async is showing unexpected behaviour on the jupyter notebook.
    We might want to use multiprocess instead of async.

    2. ``KAFKA``
    #TODO: Not implemented yet.
    """

    TQUEUE = "SQUEUE"  # Single process queue.
    MQUEUE = "MQUEUE"  # Multi process queue.
    KAFKA_CONSUMER = "KAFKA-CONSUMER"  # KAFKA consumer.
    KAFKA_PRODUCER = "KAFKA-PRODUCER"  # KAFKA producer.

    @classmethod
    @property
    def DEFAULT(cls):
        return cls.QUEUE.value


class ParellelismMethodOptions(FlagType):
    """
    1. ``ASYNC``

    2. ``PROCESS``
    # TODO: Not implemented yet.

    3. ``CLUSTER``
    # TODO: Not implemented yet.

    """

    ASYNC = "ASYNC"
    PROCESS = "PROCESS"
    CLUSTER = "CLUSTER"

    @classmethod
    @property
    def DEFAULT(cls):
        return cls.ASYNC.value
