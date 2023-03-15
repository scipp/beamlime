import asyncio
from queue import Empty, Queue
from random import SystemRandom

from beamlime.resources.generated import load_static_default_config

method_map = {"times_ten": lambda x: x * 10}


class TmpDataReductionInterface:
    queue = None

    def __init__(self, config: dict) -> None:
        self.config = config
        self.parse_config()

    def set_queue(self, queue: Queue):
        self.queue = queue

    def get_queue(self) -> Queue:
        return self.queue

    def parse_config(self):
        self.workflow_map = dict()
        for workflow in self.config["workflows"]:
            self.workflow_map[workflow["name"]] = workflow

        self.target_map = dict()
        for target in self.config["targets"]:
            self.target_map[target["name"]] = target

    @staticmethod
    async def retrieve_new_data(queue, timeout=1):
        waited = 0.1
        while waited < timeout:
            try:
                return queue.get(block=False)
            except Empty:
                await asyncio.sleep(0.1)
                waited += 0.1
        return None

    @staticmethod
    async def process(new_data, config, workflow_map, target_map):
        result_map = dict()
        for mapping in config["workflow-target-mapping"]:
            wf = mapping["workflow"]
            # func_str = workflow_map[wf]["process"]
            # ldic = locals()
            # exec(f"func = {func_str}", globals(), ldic)
            # func = ldic["func"]
            func = method_map[workflow_map[wf]["process"]]
            tg_indices = [target_map[t]["index"] for t in mapping["targets"]]
            tgs = [new_data[tg_idx] for tg_idx in tg_indices]
            result_map[wf] = func(*tgs)
        return result_map

    @staticmethod
    async def _run(queue, config, workflow_map, target_map):
        await asyncio.sleep(2)
        new_data = await TmpDataReductionInterface.retrieve_new_data(queue)
        while new_data is not None:
            await asyncio.sleep(0.1)
            result = await TmpDataReductionInterface.process(
                new_data, config, workflow_map, target_map
            )
            new_data = await TmpDataReductionInterface.retrieve_new_data(queue)
            print(f"\033[0;32m {result}")

    def create_task(self):
        return asyncio.create_task(
            self._run(
                queue=self.queue,
                config=self.config,
                workflow_map=self.workflow_map,
                target_map=self.target_map,
            )
        )


class DummyInterface:
    queue = None

    def __init__(self) -> None:
        pass

    def set_queue(self, queue: Queue):
        self.queue = queue

    def get_queue(self) -> Queue:
        return self.queue

    @staticmethod
    async def send_data(queue, data):
        queue.put(data)

    @staticmethod
    async def _run(queue):
        rd = SystemRandom()
        for _ in range(10):
            await asyncio.sleep(0.3)
            dummy_data = {"data": rd.random()}
            await DummyInterface.send_data(queue, dummy_data)
            print(f"\033[0;31m sent dummy data {dummy_data}")

    def create_task(self):
        return asyncio.create_task(self._run(queue=self.queue))


def build_instances(config: dict):
    itf_map = dict()
    for itf in config["data-stream"]["interfaces"]:
        itf_map[itf["name"]] = itf
        if itf["data-handler"] == "DATAREDUCTION":
            itf["instance"] = TmpDataReductionInterface(config["data-reduction"])
        elif itf["data-handler"] == "DUMMY":
            itf["instance"] = DummyInterface()
    return itf_map


def connect_instances(config: dict, itf_map: dict):
    for mapping in config["data-stream"]["interface-mapping"]:
        sender = itf_map[mapping["from"]]
        receiver = itf_map[mapping["to"]]
        if sender["output-channel"] != receiver["input-channel"]:
            raise ValueError(
                "`input-channel` of the `from` interface"
                " and the `output-channel` of the `to` interface"
                "should have the same option."
            )
        if sender["output-channel"] == "QUEUE":
            if (
                receiver["instance"].get_queue() is None
                and sender["instance"].get_queue() is None
            ):
                new_queue = Queue(maxsize=100)
                sender["instance"].set_queue(new_queue)
                receiver["instance"].set_queue(new_queue)
            elif (
                receiver["instance"].get_queue() is not None
                and sender["instance"].get_queue() is not None
            ) and receiver["instance"].get_queue() != sender["instance"].get_queue():
                raise RuntimeError("there's a problem in the mapping")
            elif receiver["instance"].get_queue() is not None:
                sender["instance"].set_queue(receiver["instance"].get_queue())
            elif sender["instance"].get_queue() is not None:
                receiver["instance"].set_queue(receiver["instance"].get_queue())


async def main(instances: list):
    tasks = [inst.create_task() for inst in instances]
    for task in tasks:
        await task


if __name__ == "__main__":
    config = load_static_default_config()

    itf_map = build_instances(config)
    connect_instances(config, itf_map)

    asyncio.run(main([inst["instance"] for inst in itf_map.values()]))
