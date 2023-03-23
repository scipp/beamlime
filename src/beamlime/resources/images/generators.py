# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from functools import partial

import numpy as np


def adds_up_to(number: float, frags: list = None, max_split: int = 5) -> np.ndarray:
    if number == 0 or len(frags) == max_split - 1:
        return np.array(frags + [number])
    new_frag = np.random.random() * number
    return adds_up_to(number - new_frag, frags + [new_frag])


def split_number(
    intensity: float, min_intensity: float, max_frame: int = 128, max_split: int = 5
) -> np.array:
    if intensity < min_intensity:
        return np.zeros(max_frame)

    margin = max(0.1, intensity - (max_split * min_intensity))
    fragments = adds_up_to(margin, frags=[], max_split=max_split)
    distributed_margin = fragments + min_intensity
    distributed_intensity = np.zeros(max_frame)
    distributed_intensity[: len(distributed_margin)] = distributed_margin
    np.random.shuffle(distributed_intensity)
    return distributed_intensity


def add_noise(frame: np.ndarray, noise_range=0.2) -> np.ndarray:
    """
    Add uniform random noise within the range [0, noise_range) on the image.
    """
    return frame + np.random.uniform(0, abs(noise_range), frame.shape)


def fake_2d_detector_img_generator(
    seed_img: np.ndarray, num_frame: int = 128, threshold=0.5, noise_range=0
):
    """
    Split the ``seed_img`` into mupltiple frames and yield each frame.
    All the pixels that has less intensity (average of all channel)
    than ``threshold`` is killed to be 0 before being split.
    Each frame contains some of the randomly split pixels of original image.
    Before applying the noise, each pixel has intensity > ``threshold`` or 0.
    Uniform random noise is applied on each clean-frame if ``noise_range`` is not 0.
    """
    max_split = min(num_frame, 5)

    width, height, channel = seed_img.shape
    intensities = sum(seed_img.reshape(width * height, channel).T)

    normalized = intensities / intensities.max()
    threshold_mask = normalized * (normalized > threshold)

    amplified = (normalized + (threshold * (max_split - 1))) * threshold_mask

    splitter = partial(
        split_number, min_intensity=threshold, max_frame=num_frame, max_split=max_split
    )

    split_frame = np.array(list(map(splitter, amplified)))
    clean_frames = split_frame.reshape((width, height, num_frame))

    if noise_range != 0:
        frames = add_noise(clean_frames, noise_range=noise_range)
    for frame in frames.T:
        yield frame
