# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Any, Generator, List, NewType, TypedDict

import numpy as np
from numpy.random import default_rng

# Configuration
EventRate = NewType("EventRate", int)  # [events/s]
FrameRate = NewType("FrameRate", int)  # [Hz]
NumFrames = NewType("NumFrames", int)  # [dimensionless]
DataFeedingSpeed = NewType("DataFeedingSpeed", float)  # [s/counts]

# Hard-coded Configuration
ReferenceTimeZero = NewType("ReferenceTimeZero", int)  # [ns]

# Arguments
DetectorName = NewType("DetectorName", str)
DetectorNumberCandidates = NewType("DetectorNumberCandidates", List[int])


class EV44(TypedDict):
    source_name: DetectorName
    reference_time: list[float] | np.ndarray
    reference_time_index: list[int] | np.ndarray
    time_of_flight: list[float] | np.ndarray
    pixel_id: list[int] | np.ndarray


RandomEV44Generator = Generator[EV44, Any, Any]


def random_ev44_generator(
    source_name: DetectorName,
    detector_numbers: DetectorNumberCandidates,
    event_rate: EventRate,
    frame_rate: FrameRate,
) -> Generator[EV44, Any, Any]:
    """Randomly select detector numbers (pixel ids) and generate events per frame."""
    rng = default_rng(123)
    ef_rate = int(event_rate / frame_rate)

    et_zero = ReferenceTimeZero(13620492**11)  # No reason
    while True:
        cur_event_number = int(
            ef_rate * (rng.integers(99, 101) / 100)
        )  # 1% of fluctuation
        yield EV44(
            source_name=source_name,
            reference_time=[et_zero],
            reference_time_index=[0],
            time_of_flight=rng.random((cur_event_number,)) * 800 + 200,  # No reason
            pixel_id=rng.choice(detector_numbers, cur_event_number),
        )
        et_zero += int(1e9 / frame_rate)  # Move to next frame
