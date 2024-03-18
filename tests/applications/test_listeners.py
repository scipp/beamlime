# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

import pytest

from beamlime.applications.daemons import (
    Application,
    DetectorDataReceived,
    FakeListener,
    RunStart,
)

pytest_plugins = ('pytest_asyncio',)


class MockLogger(list):
    def info(self, m, *args, **kwargs):
        self.append(m)


@pytest.fixture
def num_frames() -> int:
    return 1


@pytest.fixture
def fake_listener(num_frames: int) -> FakeListener:
    from beamlime.applications.daemons import (
        DataFeedingSpeed,
        NexusTemplatePath,
        NumFrames,
    )
    from beamlime.logging import BeamlimeLogger

    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    simulator = FakeListener(
        nexus_template_path=NexusTemplatePath(path),
        speed=DataFeedingSpeed(1),
        num_frames=NumFrames(num_frames),
    )

    simulator.logger = BeamlimeLogger(MockLogger())
    return simulator


def test_kafka_simulator_contructor(
    fake_listener: FakeListener,
) -> None:
    assert len(fake_listener.nexus_container.detectors) == 2  # ymir has no detectors


async def test_kafka_listener(fake_listener: FakeListener, num_frames: int) -> None:
    generator = fake_listener.run()
    assert isinstance(await anext(generator), RunStart)
    for _ in range(num_frames * 2):
        assert isinstance(await anext(generator), DetectorDataReceived)
    assert isinstance(await anext(generator), Application.Stop)
