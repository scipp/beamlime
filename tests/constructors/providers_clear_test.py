# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)


def test_provider_clear_all():
    import pytest

    from beamlime.constructors import ProviderNotFoundError, Providers

    _original_providers = {
        _type: _provider for _type, _provider in Providers._providers.items()
    }
    Providers.clear_all()
    Providers[int] = lambda: 99
    assert Providers[int]() == 99
    Providers.clear_all()
    with pytest.raises(ProviderNotFoundError):
        Providers[int]

    Providers._providers = _original_providers
