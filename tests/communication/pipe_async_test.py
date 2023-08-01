# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import asyncio

from beamlime.communication.pipes import Pipe


async def run_workers(*workers):
    tasks = [asyncio.create_task(worker) for worker in workers]
    return await asyncio.gather(*tasks)


def test_pipe_single_reader_writer_async():
    pipe = Pipe()

    async def reader() -> list:
        result = []
        for _ in range(5):
            async with pipe.open_async_readable(timeout=0.01) as buffer:
                result.append(await buffer.read())  # Read only 1 from the chunk
            await asyncio.sleep(0.01)
        return result

    async def writer() -> None:
        for i in range(5):
            pipe.write(i)
            await asyncio.sleep(0.01)

    workers_coroutine = run_workers(writer(), reader())
    _, result = asyncio.run(workers_coroutine)
    assert result == list(range(5))


def test_pipe_multiple_readers_async():
    pipe = Pipe(*list(range(10)))

    async def reader(delay) -> list:
        await asyncio.sleep(delay)
        result = []
        for _ in range(5):
            async with pipe.open_async_readable() as buffer:
                result.append(await buffer.read())
            await asyncio.sleep(0.01)
        return result

    workers_coroutine = run_workers(reader(0), reader(0.005))
    reader1_result, reader2_result = asyncio.run(workers_coroutine)
    assert reader1_result == [i * 2 for i in range(5)]
    assert reader2_result == [i * 2 + 1 for i in range(5)]
