# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import pytest

from beamlime.constructors.inspectors import ProductSpec


def test_product_spec_compare():
    _left_spec = ProductSpec(int)
    _right_spec = ProductSpec(int)
    assert _left_spec == _right_spec


def test_product_spec_compare_false():
    _left_spec = ProductSpec(int)
    _right_spec = ProductSpec(float)
    assert _left_spec != _right_spec


def test_product_spec_compare_with_wrong_type_raises():
    _seed_spec = ProductSpec(int)
    with pytest.raises(NotImplementedError):
        assert _seed_spec != int


def test_nested_product_spec_not_allowed():
    _seed_spec = ProductSpec(int)
    _wrapped_again = ProductSpec(_seed_spec)
    assert _seed_spec is _wrapped_again


def test_new_type_underlying_type_retrieved():
    from typing import NewType

    new_type = NewType("new_type", int)
    product_spec = ProductSpec(new_type)
    assert product_spec.product_type is new_type
    assert product_spec.returned_type is int
