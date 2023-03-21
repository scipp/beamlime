# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from beamlime.resources.loaders import find_source
import pickle


def _load_img(img_name):
    from beamlime.resources.images import __name__
    filepath = find_source(img_name + ".pickle", module=__name__)
    with open(filepath, "rb") as file:
        return pickle.load(file)


load_icon_img = partial(_load_img, img_name="icon")
