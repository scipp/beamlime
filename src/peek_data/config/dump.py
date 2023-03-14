import yaml

from ..resources.templates import (
    load_config_tpl,
    load_data_stream_interface_tpl,
    load_data_stream_mapping_tpl,
    load_kafka_tpl,
)


def represent_none(self, _):
    return self.represent_scalar("tag:yaml.org,2002:null", "")


yaml.add_representer(type(None), represent_none)


def build_default_setting():
    config = load_config_tpl()

    kafka_interface = load_data_stream_interface_tpl()
    kafka_interface["name"] = "kafka"
    kafka_interface["input-channel"]["type"] = "kafka"
    kafka_config = load_kafka_tpl()
    kafka_config["topic"] = "test"
    kafka_interface["input-channel"]["config"] = kafka_config

    data_reduction = load_data_stream_interface_tpl()
    data_reduction["name"] = "data-reduction"
    data_reduction["type"] = "internal-stream"

    dashboard = load_data_stream_interface_tpl()
    dashboard["name"] = "dashboard"
    dashboard["type"] = "internal-stream"

    config["data-stream"]["interfaces"] = [kafka_interface, data_reduction, dashboard]

    kafka_dr = load_data_stream_mapping_tpl()
    kafka_dr["from"] = "kafka"
    kafka_dr["to"] = "data-reduction"
    dr_dashboard = load_data_stream_mapping_tpl()
    dr_dashboard["from"] = "data-reduction"
    dr_dashboard["to"] = "dashboard"
    config["data-stream"]["interface-mapping"] = [kafka_dr, dr_dashboard]

    return config


def save_default_yaml(dir: str = "./", filename: str = "default-setting.yaml"):
    import os

    warning_message = (
        "# THIS FILE IS AUTO-GENERATED.\n"
        "# Please don't update it manually.\n"
        "# Use `tox -e config-build` to generate a new one.\n\n"
    )
    default_config = build_default_setting()
    order = ["general", "dashboard", "data-stream", "data-reduction"]
    with open(os.path.join(dir, filename), "w") as file:
        file.write(warning_message)
        for section in order:
            file.write(yaml.dump({section: default_config[section]}, sort_keys=False))
            file.write("\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="./")
    parser.add_argument("--name", default="default-setting.yaml")
    args = parser.parse_args()
    save_default_yaml(dir=args.directory, filename=args.name)
