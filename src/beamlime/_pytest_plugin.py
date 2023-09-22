# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addoption("--full-benchmark", action="store_true", default=False)
    parser.addoption("--kafka-test", action="store_true", default=False)


@pytest.fixture
def kafka_test(request: pytest.FixtureRequest) -> bool:
    """
    Requires --kafka-test flag.
    """
    if not request.config.getoption('--kafka-test'):
        pytest.skip(
            "Skipping kafka required tests. "
            "Use ``--kafka-test`` option to run this test."
        )
    return True


@pytest.fixture
def full_benchmark(request: pytest.FixtureRequest) -> bool:
    """
    Requires --full-benchmark flag.
    """
    if not request.config.getoption('--full-benchmark'):
        pytest.skip(
            "Skipping full benchmark. "
            "Use ``--full-benchmark`` option to run this test."
        )

    return True
