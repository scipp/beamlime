# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Generator

import pytest
import sciline as sl

from tests.benchmarks.runner import (
    BenchmarkResult,
    BenchmarkRunner,
    BenchmarkSession,
    BenchmarkTargetName,
    SingleRunReport,
    TimeMeasurement,
    create_benchmark_session_factory,
)
from tests.prototypes.parameters import (
    ChunkSize,
    EventRate,
    FrameRate,
    HistogramBinSize,
    NumFrames,
    NumPixels,
    PrototypeParameters,
    RandomSeed,
)
from tests.prototypes.random_data_providers import RandomEvents


def dump_random_events(
    *,
    num_pixels: NumPixels,
    event_rate: EventRate,
    frame_rate: FrameRate,
    num_frames: NumFrames,
    random_seed: RandomSeed,
    **_,
) -> RandomEvents:
    from tests.prototypes.random_data_providers import (
        calculate_event_per_frame,
        provide_dummy_counts,
        provide_random_event_generator,
        provide_random_events,
        provide_random_pixel_id_generator,
        provide_rng,
        provide_time_coords_generator,
    )

    event_frame_rate = calculate_event_per_frame(frame_rate, event_rate)
    rng = provide_rng(random_seed=random_seed)
    time_coords_generator = provide_time_coords_generator(
        rng, event_frame_rate, num_frames, frame_rate
    )
    data = provide_dummy_counts(event_frame_rate)
    pixel_id_generator = provide_random_pixel_id_generator(
        rng, event_frame_rate, num_pixels, num_frames
    )
    random_event_generator = provide_random_event_generator(
        pixel_id_generator, time_coords_generator, data
    )
    return provide_random_events(random_event_generator)


def build_pipeline(
    *,
    events: RandomEvents,
    num_pixels: NumPixels,
    histogram_bin_size: HistogramBinSize,
    chunk_size: ChunkSize,
    frame_rate: FrameRate,
    **_,
) -> sl.Pipeline:
    import scipp as sc

    from tests.prototypes.workflows import (
        ChunkID,
        Events,
        FirstPulseTime,
        provide_pipeline,
    )

    num_chunks = len(events) // chunk_size
    num_chunks += 1 if len(events) % chunk_size else 0

    pl = provide_pipeline(
        num_pixels=num_pixels,
        frame_rate=frame_rate,
        histogram_bin_size=histogram_bin_size,
    )
    pl[Events] = events
    pl[ChunkSize] = chunk_size
    pl[FirstPulseTime] = sc.scalar(0, unit='ms')
    pl.set_param_table(
        params=sl.ParamTable(
            ChunkID,
            columns={
                Events: [
                    Events(events[chunk_id::num_chunks])
                    for chunk_id in range(num_chunks)
                ]
            },
            index=[ChunkID(chunk_id) for chunk_id in range(num_chunks)],
        )
    )

    return pl


class OfflineWorkflowRunner(BenchmarkRunner):
    def __call__(self, *, workflow: sl.Pipeline, **params) -> SingleRunReport:
        import time

        from tests.prototypes.workflows import Visualized

        start = time.time()
        result = workflow.compute(Visualized)
        end = time.time()

        return SingleRunReport(
            callable_name=BenchmarkTargetName("offline_workflow"),
            benchmark_result=BenchmarkResult(
                time=TimeMeasurement(value=end - start, unit='s')
            ),
            arguments=params,
            output=result.sizes,
        )


@pytest.fixture(scope="session")
def offline_workflow_benchmark(
    benchmark_test: bool,
) -> Generator[BenchmarkSession, None, None]:
    """Create a benchmark session for the offline workflow."""
    assert benchmark_test

    benchmark_factory = create_benchmark_session_factory()
    with benchmark_factory.temporary_provider(BenchmarkRunner, OfflineWorkflowRunner):
        benchmark_session = benchmark_factory[BenchmarkSession]
        yield benchmark_session
        benchmark_session.save()


@pytest.fixture(params=[28, 140])
def chunk_size(request: pytest.FixtureRequest) -> ChunkSize:
    """Chunk size to benchmark."""
    return ChunkSize(request.param)


@pytest.fixture(params=[10_000, 100_000, 1_000_000, 10_000_000, 20_000_000])
def num_pixels_all_range(request: pytest.FixtureRequest) -> NumPixels:
    """Full range of num_pixels to benchmark."""
    return NumPixels(request.param)


@pytest.fixture(params=[10_000, 100_000, 1_000_000, 10_000_000, 100_000_000])
def event_rate_all_range(request: pytest.FixtureRequest) -> NumPixels:
    """Full range of event_rate to benchmark."""
    return NumPixels(request.param)


@pytest.fixture
def prototype_recipe_all_range(
    full_benchmark_test: bool,
    chunk_size: ChunkSize,
    num_pixels_all_range: NumPixels,
    event_rate_all_range: EventRate,
) -> PrototypeParameters:
    assert full_benchmark_test

    return PrototypeParameters(
        chunk_size=chunk_size,
        num_pixels=num_pixels_all_range,
        event_rate=event_rate_all_range,
    )


def test_offline_workflow_runner():
    from dataclasses import asdict

    params = asdict(PrototypeParameters())
    events = dump_random_events(**params)
    pl = build_pipeline(events=events, **params)
    runner = OfflineWorkflowRunner()
    result = runner(workflow=pl, **params)
    assert result.output == {'wavelength': params['histogram_bin_size']}


def test_offline_workflow_benchmark_all_range(
    offline_workflow_benchmark: BenchmarkSession,
    prototype_recipe_all_range: PrototypeParameters,
):
    from dataclasses import asdict

    import scipp as sc

    params = asdict(prototype_recipe_all_range)
    params.update({'scipp-version': sc.__version__})
    events = dump_random_events(**params)
    pl = build_pipeline(events=events, **params)

    with offline_workflow_benchmark.configure(iterations=3):
        offline_workflow_benchmark.run(workflow=pl, **params)
