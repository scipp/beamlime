# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib

_version = '0'


def _make_pooch():
    import pooch

    return pooch.create(
        path=pooch.os_cache('beamlime'),
        env='BEAMLIME_DATA_DIR',
        retry_if_failed=3,
        base_url='https://public.esss.dk/groups/scipp/beamlime/benchmarks/',
        version=_version,
        registry={'benchmark_results.json': 'md5:f2a760909133c635f3109e43b298a8c1'},
    )


_pooch = _make_pooch()


def get_path(name: str) -> pathlib.Path:
    """
    Return the path to a data file bundled with ess nmx.

    This function only works with example data and cannot handle
    paths to custom files.
    """
    return pathlib.Path(_pooch.fetch(name))


def benchmark_results() -> pathlib.Path:
    return get_path('benchmark_results.json')
