# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Generic, NewType, TypeVar

import pytest

from beamlime.constructors import Factory, GenericProvider, ProviderNotFoundError

generic_factory = Factory()
Data = NewType("Data", object)
_T = TypeVar("_T")


class Buffer(GenericProvider, Generic[_T]):
    ...


@generic_factory.provider
def buffer_of(data_type: Data) -> Buffer:
    return Buffer()


def test_generic_factory():
    with pytest.raises(ProviderNotFoundError):
        generic_factory[Buffer]

    assert isinstance(generic_factory[Buffer[int]], Buffer)
