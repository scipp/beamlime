# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from typing import Generator

import pytest

from beamlime.constructors import Factory

from .prototype_mini import (
    BaseApp,
    StopWatch,
    TargetCounts,
    VisualizationDaemon,
    run_prototype,
)


@pytest.fixture
def mini_factory() -> Generator[Factory, None, None]:
    from .prototype_mini import mini_prototype_factory

    yield mini_prototype_factory()


@pytest.fixture
def kafka_factory(kafka_test: bool) -> Generator[Factory, None, None]:
    from .prototype_kafka import KafkaTopicDeleted, kafka_prototype_factory

    assert kafka_test
    kafka = kafka_prototype_factory()
    yield kafka
    assert kafka[KafkaTopicDeleted]


def prototype_test_helper(prototype_factory: Factory, reference_app_tp: type[BaseApp]):
    from .parameters import ChunkSize, EventRate, NumFrames, NumPixels

    # No laps recorded.
    assert len(prototype_factory[StopWatch].lap_counts) == 0

    num_frames = 140
    chunk_size = 28
    run_prototype(
        prototype_factory,
        parameters={
            EventRate: 10**4,
            NumPixels: 10**4,
            NumFrames: num_frames,
            ChunkSize: chunk_size,
        },
    )
    stop_watch = prototype_factory[StopWatch]
    reference_app_name = prototype_factory[reference_app_tp].app_name
    assert prototype_factory[TargetCounts] == int(num_frames / chunk_size)
    assert stop_watch.lap_counts[reference_app_name] == prototype_factory[TargetCounts]


def test_mini_prototype(mini_factory: Factory):
    prototype_test_helper(mini_factory, VisualizationDaemon)


def test_kafka_prototype(kafka_factory: Factory):
    prototype_test_helper(kafka_factory, VisualizationDaemon)
