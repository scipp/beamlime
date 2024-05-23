# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# These fixtures cannot be found by pytest,
# if they are not defined in `conftest.py` under `tests` directory.
import json
from collections.abc import Generator
from typing import Literal

import pytest

from beamlime import Factory
from tests.applications.data import get_path


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--kafka-test", action="store_true", default=False)
    parser.addoption("--benchmark-test", action="store_true", default=False)
    parser.addoption("--full-benchmark-test", action="store_true", default=False)
    parser.addoption("--large-file-test", action="store_true", default=False)


@pytest.fixture(scope='session')
def kafka_test(request: pytest.FixtureRequest) -> Literal[True]:
    """
    Requires --kafka-test flag.
    """
    if not request.config.getoption('--kafka-test'):
        pytest.skip(
            "Skipping kafka required tests. "
            "Use ``--kafka-test`` option to run this test."
        )
    return True


@pytest.fixture(scope='session')
def benchmark_test(request: pytest.FixtureRequest) -> Literal[True]:
    """
    Requires --benchmark-test flag.
    """
    if not (
        request.config.getoption('--benchmark-test')
        or request.config.getoption('--full-benchmark-test')
    ):
        pytest.skip(
            "Skipping benchmark. Use ``--benchmark-test`` option to run this test."
        )

    return True


@pytest.fixture(scope='session')
def full_benchmark_test(request: pytest.FixtureRequest) -> Literal[True]:
    """
    Requires --full-benchmark-test flag.
    """
    if not request.config.getoption('--full-benchmark-test'):
        pytest.skip(
            "Skipping full benchmark. "
            "Use ``--full-benchmark-test`` option to run this test."
        )

    return True


@pytest.fixture(scope='session')
def large_file_test(request: pytest.FixtureRequest) -> Literal[True]:
    """
    Requires --large-file-test flag.
    """
    if not request.config.getoption('--large-file-test'):
        pytest.skip(
            "Skipping full benchmark. "
            "Use ``--large-file-test`` option to run this test."
        )

    return True


@pytest.fixture()
def local_logger() -> Generator[Literal[True], None, None]:
    """
    Keep a copy of logger names in logging.Logger.manager.loggerDict
    and remove newly added loggers at the end of the context.

    It will help a test not to interfere other tests.
    """
    from tests.logging.contexts import local_logger as _local_logger

    with _local_logger():
        yield True


@pytest.fixture()
def default_factory() -> Factory:
    """Returns a Factory that has all default providers of ``beamlime``."""
    from beamlime.logging.providers import log_providers

    return Factory(log_providers)


@pytest.fixture()
def ymir() -> dict:
    with open(get_path('ymir_detectors.json')) as f:
        data = json.load(f)
    return data


@pytest.fixture()
def loki(large_file_test: bool) -> dict:
    assert large_file_test
    with open(get_path('loki.json')) as f:
        return json.load(f)
