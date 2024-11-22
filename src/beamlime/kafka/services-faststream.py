import asyncio

import matplotlib.pyplot as plt
import numpy as np
from faststream import FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

npix = 64**2

MAX_CHUNK_SIZE = 1024 * 1024


@broker.subscriber("detector-events", batch=True, max_records=1000, polling_interval=1)
@broker.publisher("detector-counts")
async def histogram(msg: list[bytes]) -> bytes:
    chunks = [np.frombuffer(chunk, dtype=np.int32) for chunk in msg]
    ids = np.concatenate(chunks)
    return np.bincount(ids, minlength=npix).tobytes()


@broker.subscriber("detector-counts")
async def plot(msg: bytes):
    data = np.frombuffer(msg, dtype=np.int64).reshape(64, 64)
    plt.imshow(data, cmap='viridis')
    plt.title(f'Total counts: {data.sum()/1e6:.1f} M')
    plt.colorbar()
    plt.savefig('plot.png')
    plt.close()


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
            # await asyncio.sleep(0.01)


@app.after_startup
async def test():
    detector = FakeDetector(npix=npix, nevent=100)
    await asyncio.sleep(1)
    await detector.run()


async def main():
    app = FastStream(broker)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
