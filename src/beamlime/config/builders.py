from ..resources.templates import (
    load_config_tpl,
    load_data_stream_interface_tpl,
    load_workflow_tpl,
)


def build_default_interface(name: str):
    tpl = load_data_stream_interface_tpl()
    tpl["name"] = name
    return tpl


def build_sample_workflow_config():
    tpl = load_workflow_tpl()
    tpl["name"] = "sample-workflow"
    tpl.pop("reference")
    tpl["preprocess"] = "pass"
    tpl["process"] = "lambda x: x"
    tpl["postprocess"] = "pass"
    tpl["new-input"] = "APPEND"
    tpl["new-result"] = "REPLACE"
    tpl["save"] = False
    return tpl


def build_default_config():
    import datetime

    tpl = load_config_tpl()
    # General
    tpl["general"]["user"] = "scipp"
    tpl["general"]["name"] = "default configuration"
    tpl["general"]["last-updated"] = str(datetime.datetime.now())
    # Data Stream
    tpl["data-stream"]["interfaces"] = [
        build_default_interface("interface-0"),
        build_default_interface("interface-1"),
    ]
    tpl["data-stream"]["interface-mapping"] = [
        {"from": "interface-0", "to": "interface-1"}
    ]
    # Data Reduction
    tpl["data-reduction"]["interface"] = "interface-1"
    tpl["data-reduction"]["workflow-target-mapping"] = [
        {"workflow": "sample-workflow", "targets": ["raw-data"]}
    ]
    tpl["data-reduction"]["targets"] = [{"name": "raw-data", "index": ""}]
    tpl["data-reduction"]["workflows"] = [build_sample_workflow_config()]

    return tpl
