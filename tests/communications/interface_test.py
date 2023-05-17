# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from asyncio.runners import run as async_run
from typing import TypeVar
from unittest.mock import patch

import pytest

from beamlime.communication.interfaces import MultiProcessQueue, SingleProcessQueue

QueueType = TypeVar("QueueType", SingleProcessQueue, MultiProcessQueue)


@pytest.mark.parametrize(
    ["queue_constructor"], [(SingleProcessQueue,), (MultiProcessQueue,)]
)
def test_queue_single_data(queue_constructor: QueueType):
    sample_data = {"beam": "lime"}
    _queue = queue_constructor()
    async_run(_queue.put(sample_data))
    assert async_run(_queue.get()) == sample_data


@pytest.mark.parametrize(
    ["queue_constructor"], [(SingleProcessQueue,), (MultiProcessQueue,)]
)
def test_queue_get_empty_raises(queue_constructor: QueueType):
    from queue import Empty

    _queue = queue_constructor()
    with pytest.raises(Empty):
        async_run(_queue.get())


@pytest.mark.parametrize(
    ["queue_constructor"], [(SingleProcessQueue,), (MultiProcessQueue,)]
)
def test_queue_put_full_raises(queue_constructor: QueueType):
    from queue import Full

    _queue = queue_constructor(maxsize=1)
    async_run(_queue.put("a"))

    with pytest.raises(Full):
        async_run(_queue.put("b"))


@pytest.mark.parametrize(
    ["queue_constructor"], [(SingleProcessQueue,), (MultiProcessQueue,)]
)
def test_queue_multiple_data(queue_constructor: QueueType):
    _queue = queue_constructor()
    async_run(_queue.put("a"))
    async_run(_queue.put("b"))
    async_run(_queue.put("c"))

    assert async_run(_queue.get()) == "a"
    assert async_run(_queue.get()) == "b"

    async_run(_queue.put("d"))

    assert async_run(_queue.get()) == "c"
    assert async_run(_queue.get()) == "d"


@patch("confluent_kafka.Consumer")
def test_consumer(ck_consumer):
    from beamlime.communication.interfaces import KafkaConsumer
    from tests.test_helper import stub_consume

    consumer = KafkaConsumer({})
    ck_consumer.consume = stub_consume([1, 2, 3, 4])
    consumer._consumer = ck_consumer
    assert async_run(consumer.consume(chunk_size=1)) == (1,)
    assert async_run(consumer.consume(chunk_size=2)) == (
        2,
        3,
    )
    assert async_run(consumer.consume(chunk_size=3)) == (4,)


@patch("confluent_kafka.Consumer")
def test_consumer_empty_raises(ck_consumer):
    from queue import Empty

    from beamlime.communication.interfaces import KafkaConsumer
    from tests.test_helper import stub_consume

    consumer = KafkaConsumer({})
    ck_consumer.consume = stub_consume([1, 2, 3, 4])
    consumer._consumer = ck_consumer
    assert async_run(consumer.consume(chunk_size=4)) == (1, 2, 3, 4)
    with pytest.raises(Empty):
        async_run(consumer.consume(chunk_size=1))
