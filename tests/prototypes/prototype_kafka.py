# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from queue import Empty
from typing import Callable, List, NewType, Optional

import scipp as sc
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, PartitionMetadata, TopicMetadata

from beamlime.constructors import Factory, ProviderGroup
from beamlime.core.schedulers import retry

from .parameters import ChunkSize, FrameRate, NumFrames
from .prototype_mini import BaseApp, BasePrototype, DataStreamListener
from .random_data_providers import RandomEvents
from .workflows import Events

KafkaBrokerAddress = NewType("KafkaBrokerAddress", str)
KafkaTopic = NewType("KafkaTopic", str)
KafkaBootstrapServer = NewType("KafkaBootstrapServer", str)
ConsumerContextManager = Callable[[], Consumer]


def provide_kafka_bootstrap_server(
    broker_address: Optional[KafkaBrokerAddress] = None,
) -> KafkaBootstrapServer:
    boostrap_server_addr = broker_address or 'localhost:9092'
    return KafkaBootstrapServer(boostrap_server_addr)


def provide_kafka_admin(broker_address: KafkaBootstrapServer) -> AdminClient:
    return AdminClient({'bootstrap.servers': broker_address})


def provide_kafka_producer(broker_address: KafkaBootstrapServer) -> Producer:
    return Producer({'bootstrap.servers': broker_address})


def provide_kafka_consumer_ctxt_manager(
    broker_address: KafkaBootstrapServer, kafka_topic_partition: TopicPartition
) -> ConsumerContextManager:
    from contextlib import contextmanager

    @contextmanager
    def consumer_manager():
        cs = Consumer(
            {
                'bootstrap.servers': broker_address,
                'group.id': "BEAMLIME",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }
        )
        cs.assign([kafka_topic_partition])
        yield cs
        cs.close()

    return consumer_manager


def kafka_topic_exists(topic: KafkaTopic, admin_cli: AdminClient) -> bool:
    return topic in admin_cli.list_topics().topics


TopicCreated = NewType("TopicCreated", bool)


def create_topic(admin: AdminClient, topic: KafkaTopic) -> TopicCreated:
    from confluent_kafka.admin import NewTopic

    if not admin.list_topics().topics.get(topic):
        import time

        admin.create_topics([NewTopic(topic)])
        time.sleep(0.1)

    return TopicCreated(True)


def retrieve_topic_partition(
    admin: AdminClient, topic: KafkaTopic, topic_created: TopicCreated
) -> TopicPartition:
    topic_meta: TopicMetadata

    if not (topic_meta := admin.list_topics().topics.get(topic)) or not topic_created:
        raise ValueError(f"There is no topic named {topic} in the broker")
    elif len(topic_meta.partitions) != 1:
        raise NotImplementedError("There should be exactly 1 partition for testing.")

    part_meta: PartitionMetadata
    _, part_meta = topic_meta.partitions.popitem()
    return TopicPartition(topic, part_meta.id, offset=0)


def provide_random_kafka_topic(admin_cli: AdminClient) -> KafkaTopic:
    import uuid

    def generate_ru_tp(prefix: str = "BEAMLIMETEST") -> KafkaTopic:
        return KafkaTopic('.'.join((prefix, uuid.uuid4().hex)))

    random_topic: KafkaTopic
    while kafka_topic_exists((random_topic := generate_ru_tp()), admin_cli):
        ...

    return random_topic


KafkaTopicDeleted = NewType("KafkaTopicDeleted", bool)


class TemporaryTopicNotDeleted(Exception):
    ...


def delete_topic(topic: KafkaTopic, admin_cli: AdminClient) -> KafkaTopicDeleted:
    from concurrent.futures import Future

    futures: dict[str, Future] = admin_cli.delete_topics([topic])

    @retry(TemporaryTopicNotDeleted, max_trials=10, interval=0.1)
    def wait_for_kafka_topic_deleted():
        if futures[topic].running():
            raise TemporaryTopicNotDeleted

    try:
        wait_for_kafka_topic_deleted()
        return KafkaTopicDeleted(True)
    except TemporaryTopicNotDeleted:
        return KafkaTopicDeleted(False)


RandomEventBuffers = NewType('RandomEventBuffers', list[bytes])


def provide_random_event_buffers(random_events: RandomEvents) -> RandomEventBuffers:
    from streaming_data_types.eventdata_ev44 import serialise_ev44

    return RandomEventBuffers(
        [
            serialise_ev44(
                source_name='LIME',
                message_id=i_event,
                reference_time=event.coords['event_time_zero'].values,
                reference_time_index=[0],
                time_of_flight=event.coords['event_time_offset'].values,
                pixel_id=event.coords['pixel_id'].values,
            )
            for i_event, event in enumerate(random_events)
        ]
    )


class KafkaStreamSimulatorBase(BaseApp, ABC):
    kafka_topic: KafkaTopic
    admin: AdminClient
    producer: Producer

    def produce_data(self, raw_data: bytes, max_size: int = 100_000) -> None:
        slice_steps = range((len(raw_data) + max_size - 1) // max_size)
        slices = [
            raw_data[i_slice * max_size : (i_slice + 1) * max_size]
            for i_slice in slice_steps
        ]
        for sliced in slices:
            try:
                self.producer.produce(self.kafka_topic, sliced)
            except Exception:
                self.producer.flush()
                self.producer.produce(self.kafka_topic, sliced)

    @abstractmethod
    def stream(self) -> None:
        ...

    async def run(self) -> None:
        import asyncio

        self.stream()
        await asyncio.sleep(0.5)
        self.producer.flush()
        self.info("Data streaming to kafka finished...")

    def __del__(self) -> None:
        if kafka_topic_exists(self.kafka_topic, self.admin):
            delete_topic(self.kafka_topic, self.admin)


class KafkaStreamSimulatorScippOnly(KafkaStreamSimulatorBase):
    random_events: RandomEvents

    def stream(self) -> None:
        import json

        from scipp.serialization import serialize

        for random_event in self.random_events:
            header, serialized_list = serialize(random_event)
            self.producer.produce(self.kafka_topic, f'header:{json.dumps(header)}')

            for data_buffer in serialized_list:
                self.produce_data(data_buffer)
                self.producer.produce(self.kafka_topic, 'finished')
                self.debug("Result: %s", self.producer.poll(1))


class KafkaStreamSimulator(KafkaStreamSimulatorBase):
    random_events: RandomEventBuffers

    def stream(self) -> None:
        for i_frame, random_event in enumerate(self.random_events):
            self.producer.produce(self.kafka_topic, "starts")
            self.produce_data(random_event)
            self.producer.produce(self.kafka_topic, 'finished')
            self.debug(
                "Produced %sth message divided into %s chunks.",
                i_frame + 1,
                self.producer.poll(1),
            )


class KafkaListenerBase(BaseApp, ABC):
    raw_data_pipe: List[Events]
    chunk_size: ChunkSize
    kafka_topic: KafkaTopic
    consumer_cxt: ConsumerContextManager
    num_frames: NumFrames
    frame_rate: FrameRate

    def merge_bytes(self, datalist: list[bytes]) -> bytes:
        from functools import reduce

        return reduce(lambda x, y: x + y, datalist)

    @retry(Empty, max_trials=10, interval=0.1)
    def _poll(self, consumer: Consumer) -> Message:
        if (msg := consumer.poll(timeout=0)) is None:
            raise Empty
        else:
            return msg

    def poll(self, consumer: Consumer) -> Message | None:
        try:
            return self._poll(consumer)
        except Empty:
            return None

    @abstractmethod
    def poll_one_data(self, consumer: Consumer) -> sc.DataArray | None:
        ...

    async def send_data_chunk(self, data_chunk: Events, i_frame: int) -> None:
        import asyncio

        self.debug("Sending %s th, %s pieces of data.", i_frame + 1, len(data_chunk))
        self.raw_data_pipe.append(Events(data_chunk))
        await asyncio.sleep(0)

    def start_stop_watch(self) -> None:
        self.stop_watch.start()

    async def run(self) -> None:
        with self.consumer_cxt() as consumer:
            self.start_stop_watch()
            data_chunk: Events = Events([])
            i_frame = 0
            while (event := self.poll_one_data(consumer)) is not None:
                data_chunk.append(event)
                if len(data_chunk) >= self.chunk_size and (i_frame := i_frame + 1):
                    await self.send_data_chunk(data_chunk, i_frame)
                    data_chunk = Events([])

            if data_chunk:
                await self.send_data_chunk(data_chunk, i_frame)

            self.info("Data streaming finished...")


class KafkaListenerScippOnly(KafkaListenerBase):
    def poll_one_data(self, consumer: Consumer) -> sc.DataArray | None:
        import json

        from scipp.serialization import deserialize

        header: dict = {}
        data_list: list[bytes] = []
        header_prefix = b'header'
        finished_prefix = b'finished'

        while msg := self.poll(consumer):
            raw_msg: bytes = msg.value()
            if raw_msg.startswith(header_prefix):
                header = json.loads(raw_msg.removeprefix(header_prefix))
            elif raw_msg.startswith(finished_prefix):
                da = deserialize(header, [self.merge_bytes(data_list)])
                if not isinstance(da, sc.DataArray):
                    raise TypeError('Expected sc.DataArray, but got ', type(da))
                return da
            else:
                data_list.append(raw_msg)

        return None


class KafkaListener(KafkaListenerBase):
    def deserialize(self, data_list: list[bytes]) -> sc.DataArray:
        from streaming_data_types.eventdata_ev44 import deserialise_ev44

        data = deserialise_ev44(self.merge_bytes(data_list))
        return sc.DataArray(
            data=sc.ones(dims=['event'], shape=(len(data.pixel_id),), unit='counts'),
            coords={
                'event_time_offset': sc.Variable(
                    dims=['event'], values=data.time_of_flight, unit='ms', dtype=float
                ),
                'event_time_zero': sc.Variable(
                    dims=['event'], values=data.reference_time, unit='ns'
                ),
                'pixel_id': sc.Variable(
                    dims=['event'], values=data.pixel_id, dtype='int'
                ),
            },
        )

    def poll_one_data(self, consumer: Consumer) -> sc.DataArray | None:
        data_list: list[bytes] = []
        header_prefix = b'starts'
        finished_prefix = b'finished'

        while msg := self.poll(consumer):
            raw_msg: bytes = msg.value()
            if raw_msg.startswith(header_prefix):
                ...
            elif raw_msg.startswith(finished_prefix):
                return self.deserialize(data_list)
            else:
                data_list.append(raw_msg)

        return None


class KafkaPrototype(BasePrototype):
    kafka_simulator: KafkaStreamSimulator

    def collect_sub_daemons(self) -> list[BaseApp]:
        return [self.kafka_simulator] + super().collect_sub_daemons()


def collect_kafka_providers() -> ProviderGroup:
    kafka_providers = ProviderGroup()
    kafka_providers.cached_provider(
        KafkaBootstrapServer, provide_kafka_bootstrap_server
    )
    kafka_providers.cached_provider(AdminClient, provide_kafka_admin)
    kafka_providers.cached_provider(KafkaTopic, provide_random_kafka_topic)
    kafka_providers[Producer] = provide_kafka_producer
    kafka_providers[ConsumerContextManager] = provide_kafka_consumer_ctxt_manager
    kafka_providers[TopicCreated] = create_topic
    kafka_providers[TopicPartition] = retrieve_topic_partitian
    kafka_providers[KafkaStreamSimulator] = KafkaStreamSimulator
    kafka_providers[KafkaPrototype] = KafkaPrototype
    kafka_providers[KafkaTopicDeleted] = delete_topic
    kafka_providers[RandomEventBuffers] = provide_random_event_buffers

    return kafka_providers


def kafka_prototype_factory() -> Factory:
    from .prototype_mini import Prototype, prototype_base_providers

    kafka_providers = collect_kafka_providers()
    base_providers = prototype_base_providers()
    base_providers[Prototype] = KafkaPrototype
    base_providers[DataStreamListener] = KafkaListener

    return Factory(base_providers, kafka_providers)


if __name__ == "__main__":
    import logging

    from beamlime.logging import BeamlimeLogger

    from .parameters import EventRate, NumPixels
    from .prototype_mini import run_prototype

    kafka_factory = kafka_prototype_factory()
    kafka_factory[BeamlimeLogger].setLevel(logging.DEBUG)

    run_prototype(
        kafka_factory,
        parameters={EventRate: 10**4, NumPixels: 10**4, NumFrames: 140},
    )
