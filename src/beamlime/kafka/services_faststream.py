import asyncio
import base64
import json
import threading
from math import prod

import numpy as np
import pydantic
from config_subscriber import ConfigSubscriber
from confluent_kafka import Producer
from faststream import FastStream
from faststream.confluent import KafkaBroker, KafkaMessage

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

npix = {'detector1': (64, 64), 'detector2': (128, 128)}

MAX_CHUNK_SIZE = 1024 * 1024


class ArrayMessage(pydantic.BaseModel):
    data: np.ndarray
    model_config = {"arbitrary_types_allowed": True}

    @pydantic.field_serializer('data')
    def serialize_array(self, v: np.ndarray) -> str:
        metadata = {
            'shape': v.shape,
            'dtype': str(v.dtype),
            'data': base64.b64encode(v.tobytes()).decode(),
        }
        return json.dumps(metadata)

    @pydantic.field_validator('data', mode='before')
    def parse_array(cls, v):
        if isinstance(v, str):
            metadata = json.loads(v)
            data = np.frombuffer(
                base64.b64decode(metadata['data']), dtype=np.dtype(metadata['dtype'])
            ).reshape(metadata['shape'])
            return data
        return v if isinstance(v, np.ndarray) else np.array(v)


config_subscriber = ConfigSubscriber("localhost:9092")
monitor_counts_producer = Producer({"bootstrap.servers": "localhost:9092"})
detector_counts_producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')


@broker.subscriber(
    "beamlime.detector.events", batch=True, max_records=4, polling_interval=0.5
)
async def histogram(messages: list[ArrayMessage], message: KafkaMessage):
    chunks = {}
    for raw, msg in zip(list(message.raw_message), messages, strict=True):
        chunks.setdefault(raw.key(), []).append(msg.data)

    for key, ids in chunks.items():
        norm = len(ids)
        ids = np.concatenate(ids)
        shape = npix[key.decode('utf-8')]
        counts = np.bincount(ids, minlength=prod(shape)).reshape(shape) / norm

        result = ArrayMessage(name='monitor1', data=counts).model_dump_json()
        detector_counts_producer.produce(
            "beamlime.detector.counts",
            key=key,
            value=result,
            callback=delivery_callback,
        )
    # Poll to handle delivery reports
    detector_counts_producer.poll(0)
    detector_counts_producer.flush()


@broker.subscriber(
    "beamlime.monitor.events", batch=True, max_records=4, polling_interval=0.5
)
async def histogram_monitor(
    messages: list[ArrayMessage],
    message: KafkaMessage,
):
    nbin = config_subscriber.get_config("monitor-bins") or 100
    bins = np.linspace(0, 71, nbin)
    chunks = {}
    for raw, msg in zip(list(message.raw_message), messages, strict=True):
        chunks.setdefault(raw.key(), []).append(msg.data)

    for key, counts in chunks.items():
        norm = len(counts)
        counts = np.concatenate(counts)
        hist, _ = np.histogram(counts, bins=bins)
        midpoints = (bins[1:] + bins[:-1]) / 2
        buffer = np.concatenate((hist / norm, midpoints))
        result = ArrayMessage(name='monitor1', data=buffer).model_dump_json()
        monitor_counts_producer.produce(
            "beamlime.monitor.counts",
            key=key,
            value=result,
            callback=delivery_callback,
        )
    # Poll to handle delivery reports
    monitor_counts_producer.poll(0)
    monitor_counts_producer.flush()


class FakeDetector:
    def __init__(self, *, npix: int = npix, nevent: int = 1_000, name: str):
        self._name = name
        self.npix = npix
        self.nevent = nevent
        self._rng = np.random.default_rng()
        self._reps = 200
        self._current = 0
        self._data = self._rng.integers(
            0, self.npix, self.nevent * self._reps, dtype=np.int32
        ).reshape(self._reps, self.nevent)

    def get_data(self) -> np.ndarray:
        self._current += 1
        return self._data[self._current % self._reps]

    async def publish_data(self):
        data = self.get_data()
        message = ArrayMessage(data=data)
        await broker.publish(message, topic="beamlime.detector.events", key=self._name)

    async def run(self):
        while True:
            await self.publish_data()
            await asyncio.sleep(0.1)


class FakeMonitor:
    def __init__(
        self, *, loc: float = 30, scale: float = 10, nevent: int = 1_000, name: str
    ):
        self._rng = np.random.default_rng()
        self._data = self._rng.normal(loc=loc, scale=scale, size=1_000_000)
        self._current = 0
        self._nevent = nevent
        self._name = name

    def get_data(self) -> np.ndarray:
        """Return a random sample of data."""
        self._current = (self._current + self._nevent) % len(self._data)
        return self._data[self._current - self._nevent : self._current]

    async def publish_data(self):
        data = self.get_data()
        message = ArrayMessage(data=data)
        await broker.publish(message, topic="beamlime.monitor.events", key=self._name)

    async def run(self):
        while True:
            await self.publish_data()
            await asyncio.sleep(0.1)


config_subscriber_thread = threading.Thread(target=config_subscriber.start)


@app.after_startup
async def startup():
    config_subscriber_thread.start()
    detector1 = FakeDetector(
        name='detector1', npix=prod(npix['detector1']), nevent=1000
    )
    detector2 = FakeDetector(
        name='detector2', npix=prod(npix['detector2']), nevent=2000
    )
    monitor1 = FakeMonitor(name='monitor1')
    monitor2 = FakeMonitor(name='monitor2', nevent=2000, loc=25)
    monitor3 = FakeMonitor(name='monitor3', nevent=5000, loc=10)
    await asyncio.sleep(1)
    await asyncio.gather(
        detector1.run(), detector2.run(), monitor1.run(), monitor2.run(), monitor3.run()
    )


@app.after_shutdown
async def shutdown():
    config_subscriber.stop()
    config_subscriber_thread.join()


async def main():
    app = FastStream(broker)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
