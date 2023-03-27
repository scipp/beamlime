import asyncio
from queue import Queue

from beamlime.resources.generated import load_static_default_config


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
