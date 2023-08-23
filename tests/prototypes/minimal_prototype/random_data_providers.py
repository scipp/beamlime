# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import List, NewType

import scipp as sc
from numpy.random import Generator as RNG

from beamlime.constructors import ProviderGroup

# Configurable Types
FrameRate = NewType("FrameRate", int)  # [Hz]
EventRate = NewType("EventRate", int)  # [events/s]
NumPixels = NewType("NumPixels", int)  # [pixels/detector]
NumFrames = NewType("NumFrames", int)
RandomSeed = NewType("RandomSeed", int)

# Derived Configuration
EventFrameRate = NewType("EventFrameRate", int)  # [events/frame]

# Generated Data
DetectorCounts = NewType("DetectorCounts", sc.Variable)
TimeCoords = NewType("TimeCoords", dict[str, sc.Variable])
RandomPixelId = NewType("RandomPixelId", sc.Variable)
RandomEvent = NewType("RandomEvent", sc.DataArray)
RandomEvents = NewType("RandomEvents", List[RandomEvent])


def provide_rng(random_seed: RandomSeed) -> RNG:
    from numpy.random import default_rng

    return default_rng(random_seed)


def calculate_event_per_frame(
    frame_rate: FrameRate, event_rate: EventRate
) -> EventFrameRate:
    return EventFrameRate(int(event_rate / frame_rate))


def provide_time_coords(rng: RNG, ef_rate: EventFrameRate) -> TimeCoords:
    dummy_zeros = [zr for zr in range(int(13620492e11), int(13620492e11) + ef_rate)]
    et_zero = sc.datetimes(dims=["event"], values=dummy_zeros, unit='ns')
    et_offset = sc.array(dims=["event"], values=rng.random((ef_rate,)), unit='ms')

    return TimeCoords({"event_time_zero": et_zero, "event_time_offset": et_offset})


def provide_dummy_counts(ef_rate: EventFrameRate) -> DetectorCounts:
    return DetectorCounts(
        sc.ones(dims=["event"], shape=(ef_rate,), unit=sc.units.counts)
    )


def provide_random_events(
    rng: RNG,
    ef_rate: EventFrameRate,
    num_pixels: NumPixels,
    time_coords: TimeCoords,
    data: DetectorCounts,
    num_frames: NumFrames,
) -> RandomEvents:
    """
    Whole set of random events should be created in advance
    since randomly generating data can consume non-trivial amount of time
    and it should not interfere other async applications.
    """

    def random_pixel_id() -> RandomPixelId:
        return RandomPixelId(
            sc.array(
                dims=["event"],
                values=rng.integers(low=0, high=num_pixels, size=ef_rate),
                unit=sc.units.dimensionless,
            )
        )

    return RandomEvents(
        [
            RandomEvent(
                sc.DataArray(
                    data=data, coords={"pixel_id": random_pixel_id(), **time_coords}
                )
            )
            for _ in range(num_frames)
        ]
    )


random_data_providers = ProviderGroup(provide_rng, provide_random_events)
random_data_providers[RandomSeed] = lambda: 123
random_data_providers.cached_provider(EventFrameRate, calculate_event_per_frame)
random_data_providers.cached_provider(TimeCoords, provide_time_coords)
random_data_providers.cached_provider(DetectorCounts, provide_dummy_counts)


def provide_dummy_events() -> RandomEvents:
    num_pixels = NumPixels(10_000)
    event_frame_rate = EventFrameRate(1_000)
    num_frames = NumFrames(10)
    rng = provide_rng(RandomSeed(123))
    time_coords = provide_time_coords(rng, event_frame_rate)
    data = provide_dummy_counts(event_frame_rate)
    return provide_random_events(
        rng, event_frame_rate, num_pixels, time_coords, data, num_frames
    )
