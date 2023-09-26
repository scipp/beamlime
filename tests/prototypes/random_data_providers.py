# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any, Generator, List, NewType

import scipp as sc
from numpy.random import Generator as RNG

from beamlime.constructors import ProviderGroup

from .parameters import EventRate, FrameRate, NumFrames, NumPixels, RandomSeed

# Derived Configuration
EventFrameRate = NewType("EventFrameRate", int)  # [events/frame]

# Generated Data
DetectorCounts = NewType("DetectorCounts", sc.Variable)
TimeCoords = NewType("TimeCoords", dict[str, sc.Variable])
RandomPixelId = NewType("RandomPixelId", sc.Variable)
RandomEvent = NewType("RandomEvent", sc.DataArray)
RandomEvents = NewType("RandomEvents", List[sc.DataArray])


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
    et_offset = sc.array(
        dims=["event"], values=rng.random((ef_rate,)) * 800 + 200, unit='ns'
    )

    return TimeCoords({"event_time_zero": et_zero, "event_time_offset": et_offset})


def provide_dummy_counts(ef_rate: EventFrameRate) -> DetectorCounts:
    return DetectorCounts(sc.ones(sizes={"event": ef_rate}, unit='counts'))


def provide_random_pixel_id_generator(
    rng: RNG, ef_rate: EventFrameRate, num_pixels: NumPixels, num_frames: NumFrames
) -> Generator[RandomPixelId, Any, Any]:
    for _ in range(num_frames):
        yield RandomPixelId(
            sc.array(
                dims=["event"],
                values=rng.integers(low=0, high=num_pixels, size=ef_rate),
                unit=sc.units.dimensionless,
            )
        )


def provide_random_event_generator(
    pixel_id_generator: Generator[RandomPixelId, Any, Any],
    time_coords: TimeCoords,
    data: DetectorCounts,
) -> Generator[RandomEvent, Any, Any]:
    for pixel_id in pixel_id_generator:
        yield RandomEvent(
            sc.DataArray(data=data, coords={"pixel_id": pixel_id, **time_coords})
        )


def provide_random_events(
    random_event_generator: Generator[RandomEvent, Any, Any]
) -> RandomEvents:
    """
    Whole set of random events should be created at once in advance
    since randomly generating data can consume non-trivial amount of time
    and it should not interfere other async applications.
    """

    return RandomEvents([random_event for random_event in random_event_generator])


random_data_providers = ProviderGroup(
    provide_rng,
    provide_random_pixel_id_generator,
    provide_random_event_generator,
    provide_random_events,
)
random_data_providers.cached_provider(EventFrameRate, calculate_event_per_frame)
random_data_providers.cached_provider(TimeCoords, provide_time_coords)
random_data_providers.cached_provider(DetectorCounts, provide_dummy_counts)


def dump_random_dummy_events() -> RandomEvents:
    num_pixels = NumPixels(10_000)
    event_frame_rate = EventFrameRate(10_000)
    num_frames = NumFrames(10)
    rng = provide_rng(RandomSeed(123))
    time_coords = provide_time_coords(rng, event_frame_rate)
    data = provide_dummy_counts(event_frame_rate)
    pixel_id_generator = provide_random_pixel_id_generator(
        rng, event_frame_rate, num_pixels, num_frames
    )
    random_event_generator = provide_random_event_generator(
        pixel_id_generator, time_coords, data
    )
    return provide_random_events(random_event_generator)
