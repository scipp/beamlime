# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Commonly used serializer/deserialize pairs.
from abc import ABC, abstractstaticmethod
from typing import ByteString, TypeVar, Union

_InputType = TypeVar["_InputType"]
_OutputType = TypeVar["_OutputType"]


class SerializerPair(ABC):
    """
    Serializer/Deserializer pair abstract class.
    """

    @abstractstaticmethod
    def serializer(data: _InputType) -> _OutputType:
        ...

    @abstractstaticmethod
    def deserializer(data: _OutputType) -> _InputType:
        ...


class ScippSerializerPair(SerializerPair):
    """
    Scipp object serializer/deserializer pair.
    """

    from scipp import DataArray, DataGroup, Variable

    @staticmethod
    def serializer(data: Union[DataArray, DataGroup, Variable]) -> ByteString:
        from scipp.serialization import serialize

        header, frames = serialize(data)
        return {"SCIPP": True, "header": header, "frames": frames}

    @staticmethod
    def scipp_deserializer(data: ByteString) -> Union[DataArray, DataGroup, Variable]:
        from scipp.serialization import deserialize

        return deserialize(header=data["header"], frames=["frames"])
