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
def fake_listener() -> FakeListener:
    from beamlime.applications.daemons import (
        NexusTemplatePath,
        DataFeedingSpeed,
        EventRate,
        FrameRate,
        NumFrames,
    )

    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    return FakeListener(
        logger=MockLogger(),
        nexus_template_path=NexusTemplatePath(path),
        speed=DataFeedingSpeed(1),
        num_frames=NumFrames(1),
        event_rate=EventRate(100),
        frame_rate=FrameRate(14),
    )


def test_fake_listener_constructor(
    fake_listener: FakeListener,
) -> None:
    assert len(fake_listener.nexus_container.detectors) == 0  # ymir has no detectors


async def test_fake_listener(fake_listener: FakeListener) -> None:
    generator = fake_listener.run()
    assert isinstance(await anext(generator), RunStart)
    assert isinstance(await anext(generator), DetectorDataReceived)
    assert isinstance(await anext(generator), Application.Stop)
