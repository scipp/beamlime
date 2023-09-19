from .parameters import EventRate, NumPixels
from .prototype_mini import (
    DataStreamListener,
    Prototype,
    StopWatch,
    TargetCounts,
    run_prototype,
)


def test_mini_prototype():
    from .prototype_mini import BasePrototype, DataStreamSimulator, base_factory

    factory = base_factory()

    run_prototype(
        factory,
        parameters={
            EventRate: 10**4,
            NumPixels: 10**5,
        },
        providers={Prototype: BasePrototype, DataStreamListener: DataStreamSimulator},
    )
    assert factory[StopWatch].laps_counts == factory[TargetCounts]
