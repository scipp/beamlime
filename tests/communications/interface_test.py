# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from asyncio.runners import run as async_run
from queue import Empty, Full
from typing import TypeVar
from unittest.mock import patch

import pytest

from beamlime.communication.interfaces import (
    BullettinBoard,
    KafkaConsumer,
    KafkaProducer,
    MultiProcessQueue,
    SingleProcessQueue,
)
from tests.communications.mock_helpers import mock_kafka

QueueType = TypeVar("QueueType", SingleProcessQueue, MultiProcessQueue)


def test_bulletinboard_post():
    board = BullettinBoard()
    sample_message = {"command": "start"}
    async_run(board.post(sample_message))
    assert board._board == sample_message


def test_bulletinboard_post_over_maxsize_raises():
    board = BullettinBoard()
    sample_message = {"command": "start"}
    for _ in range(10):
        async_run(board.post({_: _}))

    with pytest.raises(Full):
        async_run(board.post(sample_message))


def test_bulletinboard_read():
    board = BullettinBoard()
    sample_message = {"command": "start"}
    async_run(board.post(sample_message))
    assert async_run(board.read()) == sample_message
    assert not async_run(board.read()) is sample_message


def test_bulletinboard_read_empty_raises():
    board = BullettinBoard()
    with pytest.raises(Empty):
        async_run(board.read())


def test_bulletinboard_clear():
    board = BullettinBoard()
    async_run(board.post({0: 1}))
    assert len(async_run(board.read())) == 1
    board.clear()
    with pytest.raises(Empty):
        async_run(board.read())


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
    _queue = queue_constructor()
    with pytest.raises(Empty):
        async_run(_queue.get())


@pytest.mark.parametrize(
    ["queue_constructor"], [(SingleProcessQueue,), (MultiProcessQueue,)]
)
def test_queue_put_full_raises(queue_constructor: QueueType):
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
@mock_kafka(initial_data=(1, 2, 3, 4))
def test_consumer(ck_consumer):
    consumer = KafkaConsumer(topic="", options={})
    consumer._consumer = ck_consumer
    assert async_run(consumer.consume(chunk_size=1)) == (1,)
    assert async_run(consumer.consume(chunk_size=2)) == (
        2,
        3,
    )
    assert async_run(consumer.consume(chunk_size=3)) == (4,)


@patch("confluent_kafka.Consumer")
@mock_kafka(initial_data=(1, 2, 3, 4))
def test_consumer_empty_raises(ck_consumer):
    consumer = KafkaConsumer(topic="", options={})

    consumer._consumer = ck_consumer
    assert async_run(consumer.consume(chunk_size=4)) == (1, 2, 3, 4)
    with pytest.raises(Empty):
        async_run(consumer.consume(chunk_size=1))


@patch("confluent_kafka.Consumer")
@patch("confluent_kafka.Producer")
@mock_kafka()
def test_producer(ck_producer, ck_consumer):
    producer = KafkaProducer(topic="", options={})
    producer._producer = ck_producer

    consumer = KafkaConsumer(topic="", options={})
    consumer._consumer = ck_consumer

    async_run(producer.produce("", value=0))
    async_run(producer.produce("", value=1))
    async_run(producer.produce("", value=2))

    assert async_run(consumer.consume(chunk_size=1)) == (0,)
    assert async_run(consumer.consume(chunk_size=2)) == (
        1,
        2,
    )


@patch("confluent_kafka.Producer")
@mock_kafka()
def test_producer_buffer_error_raises(ck_producer):
    from unittest.mock import Mock

    producer = KafkaProducer(topic="", options={})
    ck_producer.produce = Mock(ck_producer.produce)
    ck_producer.produce.side_effect = BufferError
    producer._producer = ck_producer

    with pytest.raises(Full):
        async_run(producer.produce("", value=1))


@patch("confluent_kafka.Consumer")
@patch("confluent_kafka.Producer")
@mock_kafka()
def test_producer_poll_fail_raises(ck_producer, ck_consumer):
    from unittest.mock import Mock

    producer = KafkaProducer(topic="", options={})
    producer._producer.produce = Mock(ck_producer.produce)
    # Expected to fail to fill buffer

    consumer = KafkaConsumer(topic="", options={})
    consumer._consumer = ck_consumer

    with pytest.raises(Full):
        async_run(producer.produce("", value=1))

    with pytest.raises(Empty):
        async_run(consumer.consume(chunk_size=1))
