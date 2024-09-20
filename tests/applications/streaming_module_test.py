# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime.applications._nexus_helpers import (
    StreamModuleKey,
    StreamModuleValue,
    collect_streaming_modules,
)


def test_collect_streaming_modules_event_data(ymir: dict) -> None:
    modules = collect_streaming_modules(ymir)
    expected_ev44_modules = {
        StreamModuleKey(
            "ev44", "hypothetical_detector", f"ymir_{i}"
        ): StreamModuleValue(
            (
                'entry',
                'instrument',
                f'hypothetical_detector_{i}',
                'ymir_detector_events',
            ),
            {
                "name": "ymir_detector_events",
                "type": "group",
                "children": [
                    {
                        "module": "ev44",
                        "config": {
                            "topic": "hypothetical_detector",
                            "source": f"ymir_{i}",
                        },
                    }
                ],
                "attributes": [{"name": "NX_class", "values": "NXevent_data"}],
            },
        )
        for i in range(2)
    }
    ev44_modules = {
        key: value for key, value in modules.items() if key.module_type == "ev44"
    }
    assert ev44_modules == expected_ev44_modules


def test_collect_streaming_modules_nxlogs(ymir: dict) -> None:
    modules = collect_streaming_modules(ymir)
    expected_f144_examples = {
        StreamModuleKey(
            "f144", "ymir_motion", "delay_source_chopper"
        ): StreamModuleValue(
            ('entry', 'instrument', 'mini_chopper', 'delay'),
            {
                "name": "delay",
                "type": "group",
                "attributes": [
                    {"name": "NX_class", "dtype": "string", "values": "NXlog"}
                ],
                "children": [
                    {
                        "module": "f144",
                        "config": {
                            "source": "delay_source_chopper",
                            "topic": "ymir_motion",
                            "dtype": "double",
                        },
                    }
                ],
            },
            dtype="double",
        ),
        StreamModuleKey(
            "f144", "ymir_motion", "YMIR-SEE:SE-LS336-004:KRDG0"
        ): StreamModuleValue(
            ('entry', 'instrument', 'lakeshore', 'temperatur_sensor_1'),
            {
                "name": "temperatur_sensor_1",
                "type": "group",
                "attributes": [
                    {"name": "NX_class", "dtype": "string", "values": "NXlog"}
                ],
                "children": [
                    {
                        "module": "f144",
                        "config": {
                            "source": "YMIR-SEE:SE-LS336-004:KRDG0",
                            "topic": "ymir_motion",
                            "dtype": "double",
                            "value_units": "K",
                        },
                        "attributes": [
                            {"name": "units", "dtype": "string", "values": "K"}
                        ],
                    }
                ],
            },
            dtype="double",
            value_units="K",
        ),
    }
    for expected_key, expected_value in expected_f144_examples.items():
        assert expected_key in modules
        assert modules[expected_key] == expected_value


def test_collect_streaming_modules_tdct(ymir: dict) -> None:
    modules = collect_streaming_modules(ymir)
    expected_tdct_keys = (
        StreamModuleKey("tdct", "ymir_motion", "chopper_source"),
        StreamModuleKey("tdct", "ymir_motion", "YMIR-TS:Ctrl-EVR-01:01-TS-I"),
    )
    # Not testing values since tdct parents are very long
    # Other modules are the only child of their parent
    # but tdct parents have many children
    for expected_key in expected_tdct_keys:
        assert expected_key in modules
