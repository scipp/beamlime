# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import numpy as np


def fake_2d_detector_img_generator(
    seed_img: np.ndarray,
    num_frame: int = 128,
    min_intensity: float = 0.5,
    signal_mu: float = 0.5,
    signal_err: float = 0.3,
    noise_mu: float = 0,
    noise_err: float = 0,
    random_seed: int = 0,
) -> np.ndarray:
    """
    Yield a 2d detector image based on the ``seed_img``.
    All the pixels that has more intensity (average of all channel)
    than ``min_intensity`` will contain ``signal`` and ``noise``.
    And all other pixels will only contain ``noise``.
    Both ``signal`` and ``noise`` are arbitrary number of normal distribution.
    """
    rng = np.random.default_rng(random_seed)
    width, height, channel = seed_img.shape
    intensities = sum(seed_img.reshape(width * height, channel).T)
    normalized = intensities / intensities.max()
    signal_mask = normalized > min_intensity
    for _ in range(num_frame):
        noise = rng.normal(noise_mu, noise_err, size=normalized.shape)
        signal = rng.normal(signal_mu, signal_err, size=normalized.shape) * signal_mask
        yield (noise + signal).reshape((width, height))
