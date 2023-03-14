import yaml

from ..config.builders import build_default_config, build_sample_workflow_config


def represent_none(self, _):
    return self.represent_scalar("tag:yaml.org,2002:null", "")


yaml.add_representer(type(None), represent_none)


def check_filename_extension(filename: str, expected_extensions: tuple) -> None:
    if not filename.endswith(expected_extensions):
        raise Warning(
            f"{filename} doesn't have an expected extension. "
            "The exported file may not open properly. "
            f"Try changing the extension to one of {expected_extensions}."
        )


def check_path_occupancy(file_path: str) -> None:
    import os

    if os.path.exists(file_path):
        raise FileExistsError(
            f"Failed to export {file_path}. " "File path already exists. "
        )


def export_yaml(
    yaml_obj: dict,
    filename: str = None,
    directory: str = "./",
    header: str = "",
    order: list = None,
    overwrite: bool = False,
) -> None:
    import os

    file_path = os.path.join(directory, filename)
    if not overwrite:
        check_path_occupancy(file_path)
    check_filename_extension(filename, ("yaml", "yml"))

    with open(file_path, "w") as file:
        file.write(header)
        if order is None:
            file.write(yaml.dump(yaml_obj, sort_keys=False))
        else:
            for isection, section in enumerate(order):
                file.write(yaml.dump({section: yaml_obj[section]}, sort_keys=False))
                if isection < len(order) - 1:
                    file.write("\n")


def export_default_yaml(
    filename: str = "default-setting.yaml",
    directory: str = "./",
    overwrite: bool = False,
):
    default_config = build_default_config()
    export_yaml(
        default_config,
        filename=filename,
        directory=directory,
        header=(
            "# THIS FILE IS AUTO-GENERATED.\n"
            "# Please don't update it manually.\n"
            "# Use `tox -e config-build` to generate a new one.\n\n"
        ),
        order=["general", "dashboard", "data-stream", "data-reduction"],
        overwrite=overwrite,
    )


def export_sample_workflow_yaml(
    filename: str = "sample-workflow.yaml",
    directory: str = "./",
    overwrite: bool = False,
):
    workflow_config = build_sample_workflow_config()
    export_yaml(
        workflow_config,
        filename=filename,
        directory=directory,
        header=(
            "# THIS FILE IS AUTO-GENERATED.\n"
            "# Please don't update it manually.\n"
            "# Use `tox -e config-build` to generate a new one.\n\n"
        ),
        overwrite=overwrite,
    )
