# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.communication.pipes import Pipe


def test_pipe_write_single():
    pipe = Pipe()
    sample_data_piece = [1, 2, 3]
    pipe.write(sample_data_piece)
    assert len(pipe) == 1
    assert pipe._data[0] == sample_data_piece
    assert pipe._data[0] is sample_data_piece


def test_pipe_write_all():
    pipe = Pipe()
    sample_data_1 = [1, 2, 3]
    sample_data_2 = [4, 5, 6]
    sample_data_chunk = [sample_data_1, sample_data_2]
    pipe.write_all(*sample_data_chunk)

    assert len(pipe) == len(sample_data_chunk)
    assert pipe._data[0] == sample_data_1
    assert pipe._data[1] == sample_data_2
    assert pipe._data[0] is sample_data_1
    assert pipe._data[1] is sample_data_2


def create_initialized_pipe(initial_data_len=3):
    pipe = Pipe()
    sample_data_chunk = [
        list(range(ith_piece * 3, (ith_piece + 1) * 3))
        for ith_piece in range(initial_data_len)
    ]
    pipe.write_all(*sample_data_chunk)
    return pipe, sample_data_chunk


def test_pipe_read_single_small_chunk():
    pipe, initial_data = create_initialized_pipe()
    data_ref = pipe._data[0]
    data_copy = list(data_ref)

    with pipe.open_readable() as buffer:
        assert len(buffer) == len(initial_data)
        result = buffer.read()
        assert len(buffer) == len(initial_data) - 1
        assert result == initial_data[0]
        assert result == data_copy
        assert result is not data_copy
        assert result is data_ref

    assert len(pipe) == len(initial_data) - 1


def test_pipe_read_bigger_chunk():
    initial_data_len = 6
    pipe, initial_data = create_initialized_pipe(initial_data_len)

    with pipe.open_readable() as buffer:
        assert len(buffer) == initial_data_len
        assert buffer.read() == initial_data[0]

    assert len(pipe) == initial_data_len - 1

    with pipe.open_readable() as buffer:
        assert len(buffer) == initial_data_len - 1
        assert buffer.read() == initial_data[1]
        assert buffer.read() == initial_data[2]

    assert len(pipe) == initial_data_len - 3


def test_pipe_read_timeout():
    import time
    from queue import Empty

    timeout = 0.5
    pipe, _ = create_initialized_pipe(initial_data_len=0)
    started = time.time()
    with pytest.raises(Empty):
        with pipe.open_readable(timeout=timeout, retry_interval=0.1) as _:
            ...

    consumed = time.time() - started
    assert consumed > timeout
    assert consumed - timeout < 0.01


def test_pipe_read_single_until_empty():
    from queue import Empty

    pipe, initial_data = create_initialized_pipe()

    with pipe.open_readable() as buffer:
        for data in initial_data:
            assert data == buffer.read()
        assert buffer.read() is Empty
        assert buffer.read() is Empty  # It should always return Empty if it is Empty.


def test_pipe_read_interrupted():
    from queue import Empty

    pipe, initial_data = create_initialized_pipe()
    with pytest.raises(Empty):
        with pipe.open_readable() as buffer:
            for data in initial_data:
                assert data == buffer.read()
            assert len(pipe) == 0
            raise Empty

    assert len(pipe) == len(initial_data)


def test_pipe_read_all_small_chunk():
    pipe, initial_data = create_initialized_pipe()

    with pipe.open_readable() as buffer:
        for i_data, buffer_data in enumerate(buffer.readall()):
            assert initial_data[i_data] == buffer_data

    assert len(pipe) == 0


def test_pipe_read_all_until_empty():
    from queue import Empty

    pipe, initial_data = create_initialized_pipe()

    with pipe.open_readable() as buffer:
        for i_data, buffer_data in enumerate(buffer.readall()):
            assert initial_data[i_data] == buffer_data
        assert buffer.read() is Empty
        assert buffer.read() is Empty  # It should always return Empty if it is Empty.
