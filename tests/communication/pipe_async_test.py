# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import asyncio

from beamlime.communication.pipes import Empty, Pipe


def create_pipe_with_chunks(chunk_size, chunks_len):
    pipe = Pipe(chunk_size=chunk_size)
    chunks = [
        list(range(i * chunk_size, (i + 1) * chunk_size)) for i in range(chunks_len)
    ]
    for chunk in chunks:
        pipe.write_all(*chunk)
    return pipe, chunks


async def run_workers(*workers):
    tasks = [asyncio.create_task(worker) for worker in workers]
    return await asyncio.gather(*tasks)


def test_pipe_read_single_async():
    chunk_size = 5
    pipe, _ = create_pipe_with_chunks(chunk_size=chunk_size, chunks_len=2)

    async def worker(delay):
        await asyncio.sleep(delay)
        result = []
        try:
            while True:
                async with pipe.open_async_readable(timeout=0) as buffer:
                    result.append(await buffer.read())  # Read only 1 from the chunk
                await asyncio.sleep(0.01)
        except Empty:
            return result

    workers_coroutine = run_workers(worker(0), worker(0.005))
    worker1_result, worker2_result = asyncio.run(workers_coroutine)
    assert worker1_result == [i * 2 for i in range(5)]
    assert worker2_result == [i * 2 + 1 for i in range(5)]


def test_pipe_read_single_all_chunk_async():
    chunk_size = 5
    pipe, chunks = create_pipe_with_chunks(chunk_size=chunk_size, chunks_len=6)

    async def worker(delay):
        await asyncio.sleep(delay)
        result = []
        try:
            while True:
                async with pipe.open_async_readable(timeout=0) as buffer:
                    received_chunk = []
                    while (_data := await buffer.read()) is not Empty:
                        received_chunk.append(_data)
                    result.append(received_chunk)
                await asyncio.sleep(0.01)
        except Empty:
            return result

    workers_coroutine = run_workers(worker(0), worker(0.005))
    worker1_result, worker2_result = asyncio.run(workers_coroutine)
    assert worker1_result == [chunks[0], chunks[2], chunks[4]]
    assert worker2_result == [chunks[1], chunks[3], chunks[5]]


def test_pipe_read_all_async():
    chunk_size = 5
    pipe, chunks = create_pipe_with_chunks(chunk_size=chunk_size, chunks_len=6)

    async def worker(delay):
        await asyncio.sleep(delay)
        result = []
        try:
            while True:
                async with pipe.open_async_readable(timeout=0) as buffer:
                    received_chunk = []
                    async for _data in buffer.readall():
                        received_chunk.append(_data)
                    result.append(received_chunk)
                    await asyncio.sleep(0.01)
        except Empty:
            return result

    workers_coroutine = run_workers(worker(0), worker(0.005))
    worker1_result, worker2_result = asyncio.run(workers_coroutine)
    assert worker1_result == [chunks[0], chunks[2], chunks[4]]
    assert worker2_result == [chunks[1], chunks[3], chunks[5]]
