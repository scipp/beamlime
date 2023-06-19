# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from queue import Empty, Full
from typing import Any, Dict, Generic, Iterator, List, Type, TypeVar, Union

BufferData = TypeVar("BufferData")

MAX_CHUNK_SIZE = 100
DEFAULT_CHUNK_SIZE = int(MAX_CHUNK_SIZE / 10)
MAX_BUFFER_SIZE = MAX_CHUNK_SIZE * 10


class _Buffer(Generic[BufferData]):
    _data: List[BufferData]

    @property
    def max_size(self) -> int:
        return self._max_size

    @max_size.setter
    def max_size(self, _max_size) -> None:
        if _max_size > MAX_BUFFER_SIZE:
            raise ValueError("Maximum chunk size is limited to ", MAX_CHUNK_SIZE)
        self._max_size = _max_size

    def __len__(self):
        return len(self._data)


class ReadableBuffer(_Buffer, Generic[BufferData]):
    def __init__(
        self,
        _initial_data: List[BufferData],
        max_size: int = MAX_CHUNK_SIZE,
    ):
        if not isinstance(_initial_data, List):
            raise TypeError("Initial data of a readable buffer should be a list.")
        self._data = list(_initial_data)
        self.max_size = max_size

    def readall(self):
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


class AsyncReadableBuffer(ReadableBuffer, Generic[BufferData]):
    async def readall(self):
        """
        Returns an iterator that yields the first piece of data
        until the buffer is empty.
        """
        while self._data:
            yield await self.read()

    async def read(self) -> Union[BufferData, Type[Empty]]:
        """
        Pop the first piece of data if there is any data available,
        else return ``queue.Empty``.
        """
        try:
            return self._data.pop(0)
        except IndexError:
            return Empty


class PipeObject(_Buffer):
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
        *,
        data_type: Type[BufferData] = object,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_size: int = MAX_BUFFER_SIZE,
    ) -> None:
        self.data_type = data_type
        self.chunk_size: int = chunk_size
        self.max_size: int = max_size
        self._data = list()

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, _chunk_size) -> None:
        if _chunk_size > MAX_CHUNK_SIZE:
            raise ValueError("Maximum chunk size is limited to ", MAX_CHUNK_SIZE)
        self._chunk_size = _chunk_size

    @contextmanager
    def _open_readable(self, buffer_type):
        """
        Create a buffer of the ``buffer_type``.
        """
        try:
            from copy import deepcopy

            chunk, self._data = (
                self._data[: self.chunk_size],
                self._data[self.chunk_size :],
            )
            _buffer = buffer_type(deepcopy(chunk), max_size=self.chunk_size)
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
        self, timeout: int = 0, retrial_interval: float = 0.1
    ) -> Iterator[ReadableBuffer]:
        """
        Yield a buffer containing a copy of the first chunk of data.
        """
        from ..core.schedulers import retry

        if timeout:
            max_retrials = int(timeout / retrial_interval)

            @retry(
                Empty,
                max_trials=max_retrials + 1,
                interval=retrial_interval,
            )
            def wait_for_data():
                if not self._data:
                    raise Empty("There is no data left in the pipe.")

            wait_for_data()
        try:
            with self._open_readable(ReadableBuffer) as _buffer:
                yield _buffer
        finally:
            ...

    @asynccontextmanager
    async def open_async_readable(
        self, timeout: int = 0, retrial_interval: float = 0
    ) -> Iterator[AsyncReadableBuffer]:
        """
        Yield an async buffer containing a copy of the first chunk of data.
        """

        from ..core.schedulers import async_retry

        if timeout:
            max_retrials = int(timeout / retrial_interval)

            @async_retry(
                Empty,
                max_trials=max_retrials + 1,
                interval=retrial_interval,
            )
            async def wait_for_data():
                if not self._data:
                    raise Empty("There is no data left in the pipe.")

            await wait_for_data()

        try:
            with self._open_readable(AsyncReadableBuffer) as _buffer:
                yield _buffer
        finally:
            ...

    def write(self, data: BufferData):
        """
        Append a copy of a piece of data into the pipe.

        Raises
        ------
          queue.Full: If the pipe will have more than ``self.max_size``
                      of data pieces if the new piece is appended.
        """
        from copy import copy

        if len(self._data) >= self.max_size:
            raise Full(f"Pipe is full. There are {self.max_size} pieces of data.")
        self._data.append(copy(data))

    def write_all(self, data_chunk: List[BufferData]):
        """
        Extend a chunk of copied data into the pipe.

        Raises
        ------
          queue.Full: If the pipe will have more than ``self.max_size``
                      of data pieces if the new chunk is appended.
        """
        from copy import copy

        if len(self._data) + len(data_chunk) > self.max_size:
            raise Full(f"Pipe is full. There are {self.max_size} pieces of data.")
        self._data.extend([copy(data) for data in data_chunk])


class PipeMeta(type):
    def __instancecheck__(self, __instance: Any) -> bool:
        return isinstance(__instance, PipeObject)


class pipe(PipeObject, Generic[BufferData], metaclass=PipeMeta):
    _pipes: Dict[Type, PipeObject] = dict()

    def __new__(
        cls,
        *,
        data_type: Type[BufferData],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_size: int = MAX_BUFFER_SIZE,
    ):
        if data_type in cls._pipes:
            return cls._pipes[data_type]
        else:
            cls._pipes[data_type] = PipeObject(
                data_type=data_type, chunk_size=chunk_size, max_size=max_size
            )
            return cls._pipes[data_type]
