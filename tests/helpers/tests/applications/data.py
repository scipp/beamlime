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
            # A version of ymir.json where we added two fake detectors and
            # removed a templating string - to make it like something we might
            # read in a real run start message
            'ymir_detectors.json': 'md5:d9a25d4375ae3d414d91bfe504baa844',
            'ymir.json': 'md5:5e913075094d97c5e9e9aca76fc32554',
            'ymir_detectors.nxs': 'md5:2e143cd839a84301b7459d5ab6df8454',
            # readme of the dataset
            'README.md': 'md5:7e101b5c38dddc7d530e0594a2058950',
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
