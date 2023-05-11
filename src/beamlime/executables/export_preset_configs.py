# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)


if __name__ == "__main__":
    import argparse

    from ..resources.exporters import export_default_yaml, export_fake_2d

    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="./")
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    export_fake_2d(directory=args.directory, overwrite=args.overwrite)
    export_default_yaml(directory=args.directory, overwrite=args.overwrite)
