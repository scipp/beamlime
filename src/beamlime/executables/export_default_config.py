# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from ..resources.exporters import export_default_yaml

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="./")
    parser.add_argument("--name", default="default-setting.yaml")
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    export_default_yaml(
        directory=args.directory, filename=args.name, overwrite=args.overwrite
    )
