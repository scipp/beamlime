from beamlime import DaemonInterface


class FakeListener(DaemonInterface):

    messenger: MessageRouter

    def __init__(self, run_start_message: dict):
        self.run_start_message = run_start_message

    async def run(self) -> None:
        self.info("Fake data streaming started...")

        await self.messenger.send_message_async(
            RunStart(
                content=self.run_start_message,
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
