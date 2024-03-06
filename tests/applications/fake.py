import json
import os
from dataclasses import dataclass
from typing import Union

from scippneutron.io.load_nexus import JSONGroup, json_nexus_group

from beamlime import DaemonInterface, MessageProtocol, MessageRouter

Path = Union[str, bytes, os.PathLike]


@dataclass
class RunStart(MessageProtocol):
    content: JSONGroup
    sender: type
    receiver: type


class FakeListener(DaemonInterface):
    messenger: MessageRouter

    def __init__(self, nexus_structure: dict):
        self._group = json_nexus_group(nexus_structure)

    @classmethod
    def from_file(cls, path: Path):
        '''Read nexus structure from json file'''
        with open(path) as f:
            nexus_structure = json.load(f)
        return cls(nexus_structure)

    async def run(self) -> None:
        self.info("Fake data streaming started...")

        await self.messenger.send_message_async(
            RunStart(
                content=self._group,
                sender=self.__class__,
                receiver=self.messenger.__class__,
            )
        )
        await self.messenger.send_message_async(
            self.messenger.StopRouting(
                content=None,
                sender=self.__class__,
                receiver=self.messenger.__class__,
            )
        )
        self.info("Fake data streaming finished...")
