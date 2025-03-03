# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import pytest
import scipp as sc
from pydantic import ValidationError

from beamlime.config import models


def test_toa_range_defaults():
    toa = models.TOARange()
    assert not toa.enabled
    assert toa.low == 0.0
    assert toa.high == 72_000.0
    assert toa.unit == "us"
    assert toa.range_ns is None


def test_toa_range_custom():
    toa = models.TOARange(enabled=True, low=1.0, high=2.0, unit="ms")
    assert toa.enabled
    low, high = toa.range_ns
    assert sc.identical(low, sc.scalar(1_000_000.0, unit="ns"))
    assert sc.identical(high, sc.scalar(2_000_000.0, unit="ns"))


@pytest.mark.parametrize('unit', ['m', 'invalid'])
def test_toa_range_invalid_unit(unit):
    with pytest.raises(ValidationError):
        models.TOARange(unit=unit)


def test_weighting_method_values():
    assert models.WeightingMethod.PIXEL_NUMBER == "pixel_number"


def test_pixel_weighting_defaults():
    weight = models.PixelWeighting()
    assert not weight.enabled
    assert weight.method == models.WeightingMethod.PIXEL_NUMBER


def test_pixel_weighting_custom():
    weight = models.PixelWeighting(enabled=True)
    assert weight.enabled


def test_pixel_weighting_invalid_method():
    with pytest.raises(ValidationError):
        models.PixelWeighting(method="invalid")


def test_start_time_defaults():
    model = models.StartTime()
    assert model.value == 0.0
    assert model.unit == "ns"


def test_update_every_defaults():
    model = models.UpdateEvery()
    assert model.value == 1.0
    assert model.unit == "s"


@pytest.mark.parametrize(
    ("value", "unit", "expected_ns"),
    [
        (1, "ns", 1),
        (1, "us", 1000),
        (1, "ms", 1_000_000),
        (1, "s", 1_000_000_000),
    ],
)
def test_time_model_conversions(value, unit, expected_ns):
    model = models.TimeModel(value=value, unit=unit)
    assert model.value_ns == expected_ns


def test_update_every_validation():
    with pytest.raises(ValidationError):
        models.UpdateEvery(value=0.05)  # Below minimum of 0.1


def test_roi_axis_percentage_defaults():
    roi = models.ROIAxisRange()
    assert roi.low == 0.49
    assert roi.high == 0.51


@pytest.mark.parametrize(
    ("low", "high", "should_raise"),
    [
        (0.0, 0.5, False),
        (0.5, 0.999, False),
        (-0.01, 0.5, True),
        (0.5, 1.0, True),
        (0.6, 0.5, True),  # High must be greater than low
        (0.5, 0.5, True),  # High must be greater than low
    ],
)
def test_roi_axis_percentage_validation(low, high, should_raise):
    if should_raise:
        with pytest.raises(ValidationError):
            models.ROIAxisRange(low=low, high=high)
    else:
        roi = models.ROIAxisRange(low=low, high=high)
        assert roi.low == low
        assert roi.high == high


def test_roi_rectangle_defaults():
    roi = models.ROIRectangle()
    assert roi.x == models.ROIAxisRange()
    assert roi.y == models.ROIAxisRange()
