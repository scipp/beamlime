# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from queue import Empty, Full
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Generator,
    Generic,
    Iterator,
    List,
    Type,
    TypeVar,
    Union,
)

BufferData = TypeVar("BufferData")


MAX_CHUNK_SIZE = 100
DEFAULT_CHUNK_SIZE = int(MAX_CHUNK_SIZE / 10)
MAX_BUFFER_SIZE = MAX_CHUNK_SIZE * 10


class BufferBase(Generic[BufferData]):
    """
    Buffer base class that carries a single chunk of data.
    """

    def __init__(
        self,
        *_initial_data: BufferData,
        max_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self._data: List[BufferData] = list(_initial_data)
        self.max_size: int = max_size

    @property
    def max_size(self) -> int:
        return self._max_size

    @max_size.setter
    def max_size(self, _max_size: int) -> None:
        if _max_size > MAX_BUFFER_SIZE:
            raise ValueError("Maximum chunk size is limited to ", MAX_CHUNK_SIZE)
        self._max_size = _max_size

    def __len__(self) -> int:
        return len(self._data)


class SyncBuffer(BufferBase[BufferData]):
    """Synchronous reading interfaces of a buffer."""

    def readall(self) -> Generator[BufferData, None, None]:
        """
        Returns an iterator that yields the first piece of data
        until the buffer is empty.
        """
        while self._data:
            yield self._data.pop(0)

    def read(self) -> Union[BufferData, Type[Empty]]:
        """
        Pop the first piece of data if there is any data available,
        else return ``queue.Empty``.
        """
        try:
            return self._data.pop(0)
        except IndexError:
            return Empty


class AsyncBuffer(BufferBase[BufferData]):
    """Asynchronous reading interfaces of a buffer."""

    async def readall(self) -> AsyncGenerator[BufferData, None]:
        """
        Returns an iterator that yields the first piece of data
        until the buffer is empty.
        """
        while self._data:
            yield self._data.pop(0)

    async def read(self) -> Union[BufferData, Type[Empty]]:
        """
        Pop the first piece of data if there is any data available,
        else return ``queue.Empty``.
        """
        try:
            return self._data.pop(0)
        except IndexError:
            return Empty


BufferType = Union[Type[SyncBuffer[BufferData]], Type[AsyncBuffer[BufferData]]]
Buffer = Union[SyncBuffer[BufferData], AsyncBuffer[BufferData]]


class Pipe(BufferBase[BufferData]):
    """
    Pipe object can create a buffer to read a data from the pipe,
    or write a chunk or a piece of data into the pipe.

    If there is an exception raised in the reading data context,
    it will restore all data from the chunk into the pipe.

    If the context reaches the end without any exception raised,
    consumed data will be discarded from the buffer
    and the rest will be restored into the pipe.
    """

    def __init__(
        self,
        *_initial_data: BufferData,
        max_size: int = MAX_BUFFER_SIZE,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        super().__init__(*_initial_data, max_size=max_size)
        self.chunk_size: int = chunk_size

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, _chunk_size: int) -> None:
        if _chunk_size > MAX_CHUNK_SIZE:
            raise ValueError("Maximum chunk size is limited to ", MAX_CHUNK_SIZE)
        self._chunk_size = _chunk_size

    @contextmanager
    def _open_buffer(
        self, buffer_type: BufferType[BufferData]
    ) -> Iterator[Buffer[BufferData]]:
        """
        Create a buffer of the ``buffer_type``.
        """
        try:
            from copy import deepcopy

            chunk, self._data = (
                self._data[: self.chunk_size],
                self._data[self.chunk_size :],
            )
            _buffer = buffer_type(*deepcopy(chunk), max_size=self.chunk_size)
            yield _buffer
        except Exception as err:
            self._data = chunk + self._data
            raise err
        else:
            consumed = max(0, len(chunk) - len(_buffer))
            self._data = chunk[consumed:] + self._data
        finally:
            ...

    @contextmanager
    def open_readable(
        self, timeout: int = 0, retry_interval: float = 0.1
    ) -> Iterator[BufferBase[BufferData]]:
        """
        Yield a buffer containing a copy of the first chunk of data.
        """
        from ..core.schedulers import retry

        max_trials = int(timeout / retry_interval) + 1

        @retry(Empty, max_trials=max_trials, interval=retry_interval)
        def wait_for_data() -> None:
            if not self._data:
                raise Empty("There is no data left in the pipe.")

        wait_for_data()
        try:
            with self._open_buffer(SyncBuffer[BufferData]) as _buffer:
                yield _buffer
        finally:
            ...

    @asynccontextmanager
    async def open_async_readable(
        self, timeout: int = 0, retry_interval: float = 0.1
    ) -> AsyncIterator[BufferBase[BufferData]]:
        """
        Yield an async buffer containing a copy of the first chunk of data.
        """

        from ..core.schedulers import async_retry

        max_trials = int(timeout / retry_interval) + 1

        @async_retry(Empty, max_trials=max_trials, interval=retry_interval)
        async def wait_for_data() -> None:
            if not self._data:
                raise Empty("There is no data left in the pipe.")

        await wait_for_data()

        try:
            with self._open_buffer(AsyncBuffer[BufferData]) as _buffer:
                yield _buffer
        finally:
            ...

    def write(self, data: BufferData) -> None:
        """
        Append a copy of a piece of data into the pipe.

        Raises
        ------
        queue.Full
            If the pipe will have more than ``self.max_size``
            of data pieces with the new piece appended.
        """
        from copy import copy

        if len(self._data) >= self.max_size:
            raise Full(f"Pipe is full. There are {self.max_size} pieces of data.")
        self._data.append(copy(data))

    def write_all(self, *data_chunks: BufferData) -> None:
        """
        Extend a chunk of copied data into the pipe.

        Raises
        ------
        queue.Full
            If the pipe will have more than ``self.max_size``
            of data pieces with the new chunk appended.
        """
        from copy import copy

        if len(self._data) + len(data_chunks) > self.max_size:
            raise Full(f"Pipe is full. There are {self.max_size} pieces of data.")
        self._data.extend([copy(data) for data in data_chunks])
