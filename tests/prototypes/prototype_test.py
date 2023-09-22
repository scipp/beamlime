# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.constructors import Factory


@pytest.fixture
def mini_factory():
    from .prototype_mini import mini_prototype_factory

    yield mini_prototype_factory()


@pytest.fixture
def kafka_factory(kafka_test: bool):
    from .prototype_kafka import KafkaTopicDeleted, kafka_prototype_factory

    assert kafka_test
    kafka = kafka_prototype_factory()
    yield kafka
    assert kafka[KafkaTopicDeleted]


def prototype_test_helper(prototype_factory: Factory):
    from .parameters import ChunkSize, EventRate, NumFrames, NumPixels
    from .prototype_mini import StopWatch, TargetCounts, run_prototype

    with pytest.raises(Warning):
        # No laps recorded.
        prototype_factory[StopWatch].laps_counts

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
    assert prototype_factory[TargetCounts] == int(num_frames / chunk_size)
    assert prototype_factory[StopWatch].laps_counts == prototype_factory[TargetCounts]


def test_mini_prototype(mini_factory: Factory):
    prototype_test_helper(mini_factory)


def test_kafka_prototype(kafka_factory: Factory):
    prototype_test_helper(kafka_factory)
