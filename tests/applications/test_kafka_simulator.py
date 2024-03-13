# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

import pytest

from beamlime.applications.daemons import (
    Application,
    DetectorDataReceived,
    ESSKafkaStreamSimulator,
    RunStart,
)

pytest_plugins = ('pytest_asyncio',)


class MockLogger(list):
    def info(self, m, *args, **kwargs):
        self.append(m)


@pytest.fixture
def kafka_stream_simulator() -> ESSKafkaStreamSimulator:
    from beamlime.applications.daemons import (
        DataFeedingSpeed,
        NexusTemplatePath,
        NumFrames,
    )
    from beamlime.logging import BeamlimeLogger

    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    simulator = ESSKafkaStreamSimulator(
        nexus_template_path=NexusTemplatePath(path),
        speed=DataFeedingSpeed(1),
        num_frames=NumFrames(1),
    )

    simulator.logger = BeamlimeLogger(MockLogger())
    return simulator


def test_kafka_simulator_contructor(
    kafka_stream_simulator: ESSKafkaStreamSimulator,
) -> None:
    assert (
        len(kafka_stream_simulator.nexus_container.detectors) == 0
    )  # ymir has no detectors


async def test_kafka_simulator(kafka_stream_simulator: ESSKafkaStreamSimulator) -> None:
    generator = kafka_stream_simulator.run()
    assert isinstance(await anext(generator), RunStart)
    assert isinstance(await anext(generator), DetectorDataReceived)
    assert isinstance(await anext(generator), Application.Stop)
