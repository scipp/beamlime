import asyncio
import json
import threading
from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
from config_subscriber import ConfigSubscriber
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

npix = 64**2

MAX_CHUNK_SIZE = 1024 * 1024


@dataclass
class ArrayMessage:
    shape: tuple[int, ...]
    dtype: str
    data: bytes

    @classmethod
    def from_array(cls, array: np.ndarray):
        return cls(array.shape, str(array.dtype), array.tobytes())

    def serialize(self) -> bytes:
        metadata = {'shape': self.shape, 'dtype': str(self.dtype)}
        header = json.dumps(metadata).encode('utf-8')
        header_size = len(header).to_bytes(4, 'big')
        return header_size + header + self.data

    @classmethod
    def deserialize(cls, msg: bytes) -> np.ndarray:
        header_size = int.from_bytes(msg[:4], 'big')
        header = json.loads(msg[4 : 4 + header_size].decode('utf-8'))
        data = msg[4 + header_size :]
        array = np.frombuffer(data, dtype=np.dtype(header['dtype']))
        return array.reshape(header['shape'])


@broker.subscriber("detector-events", batch=True, max_records=2, polling_interval=1)
@broker.publisher("detector-counts")
async def histogram(msg: list[bytes]) -> bytes:
    chunks = [np.frombuffer(chunk, dtype=np.int32) for chunk in msg]
    ids = np.concatenate(chunks)
    counts = np.bincount(ids, minlength=npix).reshape(64, 64)
    return ArrayMessage.from_array(counts).serialize()


config_subscriber = ConfigSubscriber("localhost:9092")


@broker.subscriber("monitor-events", batch=True, max_records=2, polling_interval=1)
@broker.publisher("monitor-counts")
async def histogram_monitor(msg: list[bytes]) -> bytes:
    nbin = config_subscriber.get_config("monitor-bins") or 100
    chunks = [np.frombuffer(chunk, dtype=np.float64) for chunk in msg]
    data = np.concatenate(chunks)
    bins = np.linspace(0, 71, nbin)
    hist, _ = np.histogram(data, bins=bins)
    midpoints = (bins[1:] + bins[:-1]) / 2
    return ArrayMessage.from_array(np.concatenate((hist, midpoints))).serialize()


# @broker.subscriber("detector-counts")
# async def plot(msg: bytes):
#    data = ArrayMessage.deserialize(msg)
#    plt.imshow(data, cmap='viridis')
#    plt.title(f'Total counts: {data.sum()/1e6:.1f} M')
#    plt.colorbar()
#    plt.savefig('plot.png')
#    plt.close()


class FakeDetector:
    def __init__(self, npix: int = npix, nevent: int = 1_000):
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
        await broker.publish(data.tobytes(), topic="detector-events")

    async def run(self):
        while True:
            await self.publish_data()
            await asyncio.sleep(0.5)


class FakeMonitor:
    def __init__(self):
        self._rng = np.random.default_rng()
        self._data = self._rng.normal(loc=30, scale=10, size=1_000_000)
        self._current = 0

    def get_data(self, size: int = 1000) -> np.ndarray:
        """Return a random sample of data."""
        self._current = (self._current + size) % len(self._data)
        return self._data[self._current - size : self._current]

    async def publish_data(self):
        data = self.get_data()
        await broker.publish(data.tobytes(), topic="monitor-events")

    async def run(self):
        while True:
            await self.publish_data()
            await asyncio.sleep(0.5)


@app.after_startup
async def test():
    thread = threading.Thread(target=config_subscriber.start).start()
    detector = FakeDetector(npix=npix, nevent=100)
    monitor = FakeMonitor()
    await asyncio.sleep(1)
    await asyncio.gather(detector.run(), monitor.run())


async def main():
    app = FastStream(broker)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
