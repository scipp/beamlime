# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)

import scipp as sc
from ess.reduce.nexus.types import CalibratedBeamline, SampleRun
from scipp.testing import assert_identical

from beamlime.config.raw_detectors import bifrost


def test_workflow_produces_detector_with_consecutive_detector_number():
    wf = bifrost.reduction_workflow
    da = wf.compute(CalibratedBeamline[SampleRun])
    assert_identical(
        da.coords['detector_number'].transpose(da.dims),
        sc.arange('', 1, 13500 + 1, unit=None, dtype='int32').fold(
            dim='', sizes=da.sizes
        ),
    )
