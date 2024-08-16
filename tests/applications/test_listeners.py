# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import time

import pytest

from beamlime.applications.daemons import (
    Application,
    DetectorDataReceived,
    FakeListener,
    RunStart,
)
from beamlime.applications.handlers import DataAssembler

pytest_plugins = ('pytest_asyncio',)


class MockLogger(list):
    def info(self, m, *args, **kwargs):
        self.append(m)


@pytest.fixture(params=[1, 2, 3])
def num_frames(request) -> int:
    return request.param


@pytest.fixture()
def fake_listener(num_frames: int, ymir) -> FakeListener:
    from beamlime.applications.daemons import (
        DataFeedingSpeed,
        EventRate,
        FrameRate,
        NumFrames,
    )

    return FakeListener(
        logger=MockLogger(),
        nexus_structure=ymir,
        nexus_template_file="",
        speed=DataFeedingSpeed(1),
        num_frames=NumFrames(num_frames),
        event_rate=EventRate(100),
        frame_rate=FrameRate(14),
    )


def test_fake_listener_constructor(fake_listener: FakeListener) -> None:
    # ymir_detectors has 2 hypothetical detectors
    assert len(fake_listener.random_event_generators) == 2


async def test_fake_listener(fake_listener: FakeListener, num_frames: int) -> None:
    generator = fake_listener.run()
    assert isinstance(await anext(generator), RunStart)
    for _ in range(num_frames * 2):
        assert isinstance(await anext(generator), DetectorDataReceived)
    assert isinstance(await anext(generator), Application.Stop)


async def test_data_assembler_returns_after_n_messages(fake_listener):
    handler = DataAssembler(logger=MockLogger(), merge_every_nth=2)
    gen = fake_listener.run()
    handler.set_run_start(await anext(gen))
    response = handler.assemble_detector_data(await anext(gen))
    assert response is None
    response = handler.assemble_detector_data(await anext(gen))
    assert response is not None


async def test_data_assembler_returns_after_s_seconds(fake_listener):
    handler = DataAssembler(
        logger=MockLogger(),
        merge_every_nth=3,  # higher than number of messages we push below
        max_seconds_between_messages=0.1,
    )
    gen = fake_listener.run()
    handler.set_run_start(await anext(gen))
    response = handler.assemble_detector_data(await anext(gen))
    assert response is None
    time.sleep(0.1)
    response = handler.assemble_detector_data(await anext(gen))
    assert response is not None
