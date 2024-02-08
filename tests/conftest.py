# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# These fixtures cannot be found by pytest,
# if they are not defined in `conftest.py` under `tests` directory.
from typing import Generator, Literal

import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--kafka-test", action="store_true", default=False)
    parser.addoption("--benchmark-test", action="store_true", default=False)
    parser.addoption("--full-benchmark-test", action="store_true", default=False)


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


@pytest.fixture(scope='function')
def local_logger() -> Generator[Literal[True], None, None]:
    """
    Keep a copy of logger names in logging.Logger.manager.loggerDict
    and remove newly added loggers at the end of the context.

    It will help a test not to interfere other tests.
    """
    from tests.logging.contexts import local_logger as _local_logger

    with _local_logger():
        yield True
