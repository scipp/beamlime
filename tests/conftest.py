# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# These fixtures cannot be found by pytest,
# if they are not defined in `conftest.py` under `tests` directory.
import json
import pathlib
from typing import Literal

import pooch
import pytest

_version = '0'


def _make_pooch():
    return pooch.create(
        path=pooch.os_cache('beamlime'),
        env='BEAMLIME_DATA_DIR',
        retry_if_failed=3,
        base_url='https://public.esss.dk/groups/scipp/beamlime/nexus_templates/',
        version=_version,
        registry={
            'loki.json': 'md5:29574acd34eb6479f14bd8d6c04aed64',
            # A version of ymir.json where we added two fake detectors and
            # removed a templating string - to make it like something we might
            # read in a real run start message
            'ymir_detectors.json': 'md5:d9a25d4375ae3d414d91bfe504baa844',
            'ymir.json': 'md5:5e913075094d97c5e9e9aca76fc32554',
            'ymir_detectors.nxs': 'md5:2e143cd839a84301b7459d5ab6df8454',
            # readme of the dataset
            'README.md': 'md5:d53ffcf9c1363af7043e5a1a2071d2bd',
        },
    )


_pooch = _make_pooch()
_pooch.fetch('README.md')


def get_path(name: str) -> pathlib.Path:
    """
    Return the path to a data file bundled with beamlime test helpers.

    This function only works with example data and cannot handle
    paths to custom files.
    """

    return pathlib.Path(_pooch.fetch(name))


def get_checksum(name: str) -> str:
    """
    Return the checksum of a data file bundled with beamlime test helpers.

    This function only works with example data and cannot handle
    paths to custom files.
    """
    return _pooch.registry[name]


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


@pytest.fixture
def ymir() -> dict:
    with open(get_path('ymir_detectors.json')) as f:
        data = json.load(f)
    return data


@pytest.fixture
def ymir_static_file() -> str:
    return str(get_path("ymir_detectors.nxs").as_posix())


@pytest.fixture
def loki(large_file_test: bool) -> dict:
    assert large_file_test
    with open(get_path('loki.json')) as f:
        return json.load(f)
