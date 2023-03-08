from string import Template

import yaml
from yaml import safe_load

from .loaders import (
    load_config_tpl,
    load_data_stream_interface_tpl,
    load_data_stream_mapping_tpl,
    load_kafka_tpl,
)


def represent_none(self, _):
    return self.represent_scalar("tag:yaml.org,2002:null", "")


yaml.add_representer(type(None), represent_none)


def build_default_setting():
    config = safe_load(load_config_tpl())
    ds_interface = Template(load_data_stream_interface_tpl())
    kafka_interface = safe_load(ds_interface.substitute(name="kafka"))
    kafka_interface["kafka"]["input-channel"]["type"] = "kafka"
    kafka_config = safe_load(load_kafka_tpl())
    kafka_config["topic"] = "test"
    kafka_interface["kafka"]["input-channel"]["config"] = kafka_config
    data_reduction = safe_load(ds_interface.substitute(name="data-reduction"))
    data_reduction["type"] = "byte"
    dashboard = safe_load(ds_interface.substitute(name="dashboard"))
    config["data-stream"]["interfaces"] = [kafka_interface, data_reduction, dashboard]
    ds_mapping = load_data_stream_mapping_tpl()
    kafka_dr = safe_load(ds_mapping)
    kafka_dr["from"] = "kafka"
    kafka_dr["to"] = "data-reduction"
    dr_dashboard = safe_load(ds_mapping)
    dr_dashboard["from"] = "data-reduction"
    dr_dashboard["to"] = "dashboard"
    config["data-stream"]["interface-mapping"] = ds_mapping
    return config


def save_default_yaml():
    warning_message = (
        "# THIS FILE IS AUTO-GENERATED.\n"
        "# Please don't update it manually.\n"
        "# Use `tox -e config-build` to generate a new one.\n\n"
    )
    default_config = build_default_setting()
    order = ["general", "dashboard", "data-stream", "data-reduction"]
    with open("default-setting.yaml", "w") as file:
        file.write(warning_message)
        for section in order:
            file.write(yaml.dump({section: default_config[section]}))
            file.write("\n")


if __name__ == "__main__":
    save_default_yaml()
