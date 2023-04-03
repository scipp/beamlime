# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

from ..loaders import find_source


def _load_ascii(ascii_name: str) -> str:
    """
    Load an ascii art that is included in the module beamlime.resources.asciiarts.
    """
    filepath = find_source(ascii_name + ".ascii", module=__package__)
    with open(filepath) as file:
        return "".join(file.readlines())


load_title_ascii = partial(_load_ascii, ascii_name="title")
