# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial

import numpy as np
from PIL import Image

from ..loaders import find_source


def load_image(filename: str, module: str = __package__) -> np.ndarray:
    """
    Load an image using ``PIL.Image`` and
    return the image as an numpy array with dtype ``np.float64``.
    """
    filepath = find_source(filename, module=module)
    with Image.open(filepath) as img:
        return np.asarray(img, dtype=np.float64)


def _load_png(img_name: str) -> np.ndarray:
    """
    Load an image that is included in the module beamlime.resources.images.
    """
    return load_image(img_name + ".png")


load_icon_img = partial(_load_png, img_name="icon")
