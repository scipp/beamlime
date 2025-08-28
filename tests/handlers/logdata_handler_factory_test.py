# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
import time
from dataclasses import dataclass

import pytest
import scipp as sc
from scipp.testing import assert_identical

from beamlime.core.message import Message, StreamId, StreamKind
from beamlime.handlers.accumulators import LogData
from beamlime.handlers.timeseries_handler import LogdataHandlerFactory


@pytest.fixture
def attribute_registry():
    return {
        "temperature_sensor": {"units": "K"},
        "pressure_sensor": {"units": "Pa"},
    }


@dataclass
class LogCapture:
    """Container for logger and its handler with captured messages."""

    logger: logging.Logger
    messages: list[str]


@pytest.fixture
def log_capture() -> LogCapture:
    """Fixture that provides a logger with a handler that captures log messages.

    Returns
    -------
    LogCapture object containing the logger and its captured messages
    """

    class LogCaptureHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.messages: list[str] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.messages.append(self.format(record))

    # Create and configure handler
    handler = LogCaptureHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)

    # Create logger
    logger = logging.getLogger('beamlime.test_logger')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.propagate = False  # Don't propagate to parent handlers

    yield LogCapture(logger=logger, messages=handler.messages)

    # Clean up
    logger.removeHandler(handler)


def test_logdata_handler_factory_initialization(attribute_registry):
    """Test that LogdataHandlerFactory initializes correctly"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        logger=None,
        attribute_registry=attribute_registry,
    )
    assert factory is not None


def test_make_handler_with_valid_source(attribute_registry):
    """Test creating a handler with a valid source name"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        attribute_registry=attribute_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="temperature_sensor")
    handler = factory.make_handler(key)

    # Just verify the handler is created
    assert handler is not None


def test_make_handler_with_missing_source(attribute_registry, log_capture: LogCapture):
    """Test creating a handler with a source name that doesn't have attributes"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        logger=log_capture.logger,
        attribute_registry=attribute_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="nonexistent_sensor")
    handler = factory.make_handler(key)

    assert handler is None
    # Check that a warning was logged
    assert any(
        "No attributes found for source name 'nonexistent_sensor'" in message
        for message in log_capture.messages
    )


def test_make_handler_with_invalid_attributes(
    attribute_registry, log_capture: LogCapture
):
    """Test creating a handler with invalid attributes for a source"""
    # Create a copy of the attribute registry with invalid attributes
    invalid_registry = attribute_registry.copy()
    invalid_registry["invalid_sensor"] = {"units": "abcde"}  # invalid unit string

    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        logger=log_capture.logger,
        attribute_registry=invalid_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="invalid_sensor")
    handler = factory.make_handler(key)

    assert handler is None
    # Check that a warning was logged about invalid attributes
    assert any(
        "Failed to create NXlog for source name 'invalid_sensor'" in message
        for message in log_capture.messages
    )


def test_full_handler_lifecycle(attribute_registry):
    """Test the full lifecycle of creating and using a handler"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        attribute_registry=attribute_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="temperature_sensor")
    handler = factory.make_handler(key)
    assert handler is not None

    # Create a message
    timestamp = int(time.time() * 1e9)  # Current time in nanoseconds
    message = Message(
        timestamp=timestamp,
        stream=key,
        value=LogData(time=1_000_000_000, value=273.15),
    )

    # Handle the message and check results
    results = handler.handle([message])
    assert len(results) == 1
    result = results[0].value

    # Check the DataArray
    assert isinstance(result, sc.DataArray)
    assert_identical(result.data, sc.array(dims=["time"], values=[273.15], unit="K"))

    # Check the time coordinate
    expected_time = sc.datetime("1970-01-01T00:00:01.000000", unit="ns")
    assert_identical(result.coords["time"][0], expected_time)


def test_handler_with_multiple_messages(attribute_registry):
    """Test handling multiple messages within the same update interval"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        attribute_registry=attribute_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="temperature_sensor")
    handler = factory.make_handler(key)

    base_timestamp = int(time.time() * 1e9)

    # Create messages
    message1 = Message(
        timestamp=base_timestamp,
        stream=key,
        value=LogData(time=1_000_000_000, value=273.15),
    )
    message2 = Message(
        timestamp=base_timestamp + 100_000_000,  # 100ms later, still within interval
        stream=key,
        value=LogData(time=2_000_000_000, value=293.15),
    )

    # Handle messages and check results
    results = handler.handle([message1, message2])
    assert len(results) == 1
    result = results[0].value

    # Check the DataArray has both values
    assert_identical(
        result.data, sc.array(dims=["time"], values=[273.15, 293.15], unit="K")
    )

    # Check the time coordinates are sorted
    expected_times = ["1970-01-01T00:00:01.000000", "1970-01-01T00:00:02.000000"]
    assert_identical(
        result.coords["time"],
        sc.datetimes(dims=["time"], values=expected_times, unit="ns"),
    )


def test_handler_across_update_intervals(attribute_registry):
    """Test handling messages across different update intervals"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        config_registry=FakeConfigRegistry(
            {'temperature_sensor': {'update_every': {'value': 0.5}}}
        ),
        attribute_registry=attribute_registry,
    )

    key = StreamId(kind=StreamKind.LOG, name="temperature_sensor")
    handler = factory.make_handler(key)

    base_timestamp = int(time.time() * 1e9)

    # First interval
    message1 = Message(
        timestamp=base_timestamp,
        stream=key,
        value=LogData(time=1_000_000_000, value=273.15),
    )

    results1 = handler.handle([message1])
    assert len(results1) == 1

    # Second interval (more than update_interval_seconds later)
    message2 = Message(
        timestamp=base_timestamp + int(1.5e9),  # 1.5 seconds later
        stream=key,
        value=LogData(time=2_000_000_000, value=293.15),
    )

    results2 = handler.handle([message2])
    assert len(results2) == 1

    # The full timeseries is returned, even across intervals
    result2 = results2[0].value
    assert_identical(
        result2.data, sc.array(dims=["time"], values=[273.15, 293.15], unit="K")
    )


def test_logdata_handler_preserves_source_name(attribute_registry):
    """Test that the generated messages preserve the source name"""
    factory = LogdataHandlerFactory(
        instrument="test_instrument",
        attribute_registry=attribute_registry,
    )

    source_name = "temperature_sensor"
    key = StreamId(kind=StreamKind.LOG, name=source_name)
    handler = factory.make_handler(key)

    message = Message(
        timestamp=int(time.time() * 1e9),
        stream=key,
        value=LogData(time=1_000_000_000, value=273.15),
    )

    results = handler.handle([message])
    assert len(results) == 1
    assert (
        results[0].stream.name
        == f'{source_name}/{fake_config_registry.service_name}/timeseries'
    )
