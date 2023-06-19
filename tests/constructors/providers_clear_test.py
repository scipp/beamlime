# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)


def test_binder_clear_all():
    import pytest

    from beamlime.constructors import ProviderNotFoundError, clean_binder

    with clean_binder() as binder:
        binder[int] = lambda: 99
        assert binder[int]() == 99
        binder.clear_all()
        with pytest.raises(ProviderNotFoundError):
            binder[int]
        assert len(binder) == 0
