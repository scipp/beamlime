# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Any, Generator, List, NewType, TypedDict

import numpy as np
import scipp as sc
from numpy.random import default_rng

# Derived Configuration
EventFrameRate = NewType("EventFrameRate", int)  # [events/frame]
ReferenceTimeZero = NewType("ReferenceTimeZero", int)  # [ns]

# Generated Data
DetectorCounts = NewType("DetectorCounts", sc.Variable)
TimeCoords = NewType("TimeCoords", dict[str, sc.Variable])
RandomPixelId = NewType("RandomPixelId", sc.Variable)
RandomEvent = NewType("RandomEvent", sc.DataArray)
RandomEvents = NewType("RandomEvents", List[sc.DataArray])


DetectorNumberCandidates = NewType("DetectorNumberCandidates", List[int])


class EV44(TypedDict):
    reference_time: list[float] | np.ndarray
    reference_time_index: list[int] | np.ndarray
    time_of_flight: list[float] | np.ndarray
    pixel_id: list[int] | np.ndarray


RandomEV44Generator = Generator[EV44, Any, Any]


def random_ev44_generator(
    detector_numbers: DetectorNumberCandidates,
) -> Generator[EV44, Any, Any]:
    """Randomly select detector numbers (pixel ids) and generate events per frame."""
    rng = default_rng(123)
    event_rate = 10_000
    frame_rate = 14
    ef_rate = int(event_rate / frame_rate)

    et_zero = ReferenceTimeZero(13620492**11)  # No reason
    while True:
        cur_event_number = int(
            ef_rate * (rng.integers(99, 101) / 100)
        )  # 1% of fluctuation
        yield EV44(
            reference_time=[et_zero],
            reference_time_index=[0],
            time_of_flight=rng.random((cur_event_number,)) * 800 + 200,  # No reason
            pixel_id=rng.choice(detector_numbers, cur_event_number),
        )
        et_zero += int(1e9 / frame_rate)  # Move to next frame
