# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pathlib

import pooch

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
            'ymir_detectors.json': 'md5:02bc6160081a96733c5056cfaa047fca',
            'ymir.json': 'md5:5e913075094d97c5e9e9aca76fc32554',
            # readme of the dataset
            'README.md': 'md5:778a0f290894182db5db0170b4f102fa',
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
