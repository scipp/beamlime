import asyncio
from queue import Empty, Queue
from typing import Iterable, List, Union

import numpy as np

from beamlime.resources.generated import load_static_default_config


def average(data: dict):
    return np.average(np.array(data))


method_map = {"average": average}


def wrap_target_index(indices: Union[str, int, List]) -> List:
    """
    In the configuration file, the `index` of the target can be
    a string, an integer or a list[str, int].
    If the `index` is a single string or integer,
    it needs to be wrapped to be used in the `nested_data_get`.

    The check could be done in the `nested_data_get`
    but then it will check every items in the list since it is run recursively.
    """
    if isinstance(indices, List):
        for item in indices:
            if not isinstance(item, (str, int)):
                raise TypeError(
                    "Each index in `indices` should be either `str` or `int`."
                )
        return indices
    return [indices]


def nested_data_get(nested_obj: Iterable, *indices):
    """

    >>> nested_obj = {'a': {'b': [1,2,3]}}
    >>> nested_data_get(nested_obj, 'a', 'b', 0)
    1
    """
    idx = indices[0]
    try:
        child = nested_obj[idx]
    except TypeError:
        raise TypeError(
            f"Index {idx} with type {type(idx)}"
            f"doesn't match the key/index type of {type(nested_obj)}"
        )
    except KeyError:
        raise KeyError(f"{nested_obj} doesn't have the key {idx}")
    except IndexError:
        raise IndexError(f"{idx} is out of the range of {len(nested_obj)-1}")
    if len(indices) == 1:
        return child
    else:
        return nested_data_get(child, *indices[1:])


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
    def process(new_data, config, workflow_map, target_map):
        result_map = dict()
        for mapping in config["workflow-target-mapping"]:
            wf = mapping["workflow"]
            process_id = workflow_map[wf]["process"]
            func = method_map[process_id]
            tg_indices = [
                wrap_target_index(target_map[t]["index"]) for t in mapping["targets"]
            ]
            tgs = [nested_data_get(new_data, *tg_idx) for tg_idx in tg_indices]
            result_map[wf] = {"process": process_id, "result": func(*tgs)}
        return result_map

    @staticmethod
    async def _run(queue, config, workflow_map, target_map):
        await asyncio.sleep(1)
        new_data = await TmpDataReductionInterface.retrieve_new_data(queue)
        while new_data is not None:
            await asyncio.sleep(0.2)
            result = TmpDataReductionInterface.process(
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


def build_instances(config: dict):
    from importlib import import_module

    itf_map = dict()
    for itf in config["data-stream"]["interfaces"]:
        itf_map[itf["name"]] = itf
        handler_name = itf["data-handler"].split(".")
        dh_parent = ".".join(handler_name[:-1])
        dh_class = handler_name[-1]
        if len(dh_parent) > 0:
            parent_module = import_module(dh_parent)
        else:
            parent_module = import_module(__name__)

        handler = getattr(parent_module, dh_class)

        # This if statements will be replaced parsed arguments from configuration.
        if dh_class == "TmpDataReductionInterface":
            itf["instance"] = handler(config["data-reduction"])
        elif dh_class == "Fake2dDetectorImageFeeder":
            itf["instance"] = handler(num_frame=12)
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
