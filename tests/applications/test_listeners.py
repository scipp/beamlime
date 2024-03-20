# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
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
        EventRate,
        FrameRate,
        NexusTemplatePath,
        NumFrames,
    )
    from tests.applications.data import get_path

    return FakeListener(
        logger=MockLogger(),
        nexus_template_path=NexusTemplatePath(get_path('ymir.json').as_posix()),
        speed=DataFeedingSpeed(1),
        num_frames=NumFrames(num_frames),
        event_rate=EventRate(100),
        frame_rate=FrameRate(14),
    )


def test_fake_listener_contructor(
    fake_listener: FakeListener,
) -> None:
    assert len(fake_listener.nexus_container.detectors) == 2  # ymir has no detectors


async def test_fake_listener(fake_listener: FakeListener, num_frames: int) -> None:
    generator = fake_listener.run()
    assert isinstance(await anext(generator), RunStart)
    for _ in range(num_frames * 2):
        assert isinstance(await anext(generator), DetectorDataReceived)
    assert isinstance(await anext(generator), Application.Stop)
