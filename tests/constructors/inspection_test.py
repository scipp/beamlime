# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Any

import pytest

from beamlime.constructors.inspectors import ProductSpec, issubproduct
from beamlime.constructors.providers import UnknownType


def test_nested_product_spec_not_allowed():
    _seed_spec = ProductSpec(int)
    _wrapped_again = ProductSpec(_seed_spec)
    assert _seed_spec is _wrapped_again


def test_new_type_underlying_type_retrieved():
    from typing import NewType

    seed_type = int
    new_type = NewType("new_type", seed_type)
    product_spec = ProductSpec(new_type)
    assert product_spec.product_type is new_type
    assert product_spec.returned_type is int


def test_supported_type_check():
    from typing import NewType, Union

    seed_type = int
    new_type = NewType("new_type", seed_type)
    standard_product_spec = ProductSpec(int)
    compatible_product_spec = ProductSpec(new_type)
    incompatible_product_spec = ProductSpec(str)
    wrong_type_product_spec = ProductSpec(Union[int, str])

    assert issubproduct(standard_product_spec, compatible_product_spec)
    assert not issubproduct(standard_product_spec, incompatible_product_spec)
    assert not issubproduct(standard_product_spec, wrong_type_product_spec)


@pytest.mark.parametrize("any_type", [None, Any, UnknownType])
def test_supported_type_check_any(any_type):
    any_product_spec = ProductSpec(any_type)
    for product_spec in (ProductSpec(int), ProductSpec(str)):
        assert issubproduct(product_spec, any_product_spec)
        assert not issubproduct(any_product_spec, product_spec)
