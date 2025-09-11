# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Script to create a copy of a NeXus file with only geometry information."""

import argparse
import sys
from pathlib import Path

import h5py
import numpy as np


def _copy_attributes(src: h5py.Group, dst: h5py.Group) -> None:
    for key, value in src.attrs.items():
        dst.attrs[key] = value


def _copy_detector_fields(
    src_group: h5py.Group, dst_group: h5py.Group, use_pixel_shape: bool
) -> None:
    compression_fields: list[str] = [
        'detector_number',
        'x_pixel_offset',
        'y_pixel_offset',
        'z_pixel_offset',
    ]
    for field in compression_fields:
        if field in src_group:
            # Using compression makes a 10x size difference (tested on DREAM)
            data: np.ndarray = src_group[field][:]
            dst_group.create_dataset(
                field,
                data=data,
                compression='gzip',
                compression_opts=1,
                shuffle=True,
            )
            _copy_attributes(src_group[field], dst_group[field])
    src_group.copy('depends_on', dst_group)
    if use_pixel_shape and 'pixel_shape' in src_group:
        src_group.copy('pixel_shape', dst_group)


def _copy_monitor_fields(src_group: h5py.Group, dst_group: h5py.Group) -> None:
    src_group.copy('depends_on', dst_group)


def write_minimal_geometry(
    input_filename: Path, output_filename: Path, use_pixel_shape: bool = True
) -> None:
    """Create minimal geometry file with only detector positions and transformations."""
    with h5py.File(input_filename, 'r') as fin, h5py.File(output_filename, 'w') as fout:

        def ensure_parent_groups(name: str) -> None:
            parts: list[str] = name.split('/')
            current_path: str = ''
            for part in parts[:-1]:  # Skip the last part (current group name)
                if not part:
                    continue
                current_path = f"{current_path}/{part}"
                if current_path not in fout:
                    src_group: h5py.Group = fin[current_path]
                    dst_group: h5py.Group = fout.create_group(current_path)
                    _copy_attributes(src_group, dst_group)

        def visit_and_copy(name: str, obj: h5py.Group | h5py.Dataset) -> None:
            if isinstance(obj, h5py.Group):
                if 'NX_class' in obj.attrs:
                    nx_class: str | bytes = obj.attrs['NX_class']
                    if isinstance(nx_class, bytes):
                        nx_class = nx_class.decode()

                    if nx_class == 'NXdetector':
                        ensure_parent_groups(name)
                        dst_group: h5py.Group = fout.create_group(name)
                        _copy_attributes(obj, dst_group)
                        _copy_detector_fields(
                            obj, dst_group, use_pixel_shape=use_pixel_shape
                        )
                    elif nx_class == 'NXmonitor':
                        ensure_parent_groups(name)
                        dst_group: h5py.Group = fout.create_group(name)
                        _copy_attributes(obj, dst_group)
                        _copy_monitor_fields(obj, dst_group)
                    elif nx_class in ('NXsource', 'NXsample'):
                        ensure_parent_groups(name)
                        dst_group: h5py.Group = fout.create_group(name)
                        _copy_attributes(obj, dst_group)
                        obj.copy('depends_on', dst_group)
                    elif nx_class == 'NXtransformations':
                        ensure_parent_groups(name)
                        dst_group: h5py.Group = fout.create_group(name)
                        _copy_attributes(obj, dst_group)
                        # Copy all contents of the transformations group
                        for key in obj:
                            obj.copy(key, dst_group)

        # Copy root attributes
        _copy_attributes(fin, fout)
        fin.visititems(visit_and_copy)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            'Create a copy of a NeXus file, stripping non-geometry data such '
            'as event data.'
        )
    )
    parser.add_argument('input', type=Path, help='Input NeXus file')
    parser.add_argument('output', type=Path, help='Output NeXus file')
    parser.add_argument(
        '--no-pixel-shape',
        action='store_false',
        dest='use_shape',
        help='Do not keep pixel shape (which can be large) if present in input file',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite output file if it exists',
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f'Input file {args.input} does not exist', file=sys.stderr)  # noqa: T201
        return 1

    if args.output.exists() and not args.force:
        print(f'Output file {args.output} already exists', file=sys.stderr)  # noqa: T201
        return 1

    write_minimal_geometry(args.input, args.output, use_pixel_shape=args.use_shape)
    return 0


if __name__ == '__main__':
    sys.exit(main())
