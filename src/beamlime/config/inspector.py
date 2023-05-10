# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Optional


def validate_config_path(config_path: str) -> Optional[bool]:
    import os.path

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration yaml file was not found in {config_path}."
        )
    return True


# Subset of configuration validity checking helpers
# Input arguments of these helpers should be the same as it is in the yaml file.
# For example, you should not apply ``list_to_dict`` to the list-like subset
# before you check the validity.
# And the input argument should be the highest level of configuration
# rather than multiple subset.


def validate_application_mapping(
    data_stream_config: dict, raise_error: bool = True
) -> Optional[bool]:
    # TODO: Update this to use in broker.
    from .tools import list_to_dict, wrap_item

    app_configs = list_to_dict(data_stream_config["applications"])
    app_mapping = list_to_dict(
        data_stream_config["applications-mapping"], key_field="from", value_field="to"
    )

    for sender_name, receiver_names in app_mapping.items():
        receiver_name_list = wrap_item(receiver_names, list)

        sender_config = app_configs[sender_name]
        receiver_configs = [
            app_configs[receiver_n] for receiver_n in receiver_name_list
        ]

        receiver_i_chn_set = set([cfg["input-channel"] for cfg in receiver_configs])
        if (len(receiver_i_chn_set) != 1) or (
            sender_config["output-channel"] != receiver_i_chn_set.pop()
        ):
            if raise_error:
                raise ValueError(
                    "`input-channel` of the `from` application"
                    " and the `output-channel` of the `to` application "
                    "should have the same option."
                )
            else:
                return False
    return True
