# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Author    : Sunyoung Yoo (ESS)
# Please feel free to update the hard-coded numbers in this file
# and submit a merge request(PR).
from dataclasses import dataclass
from typing import Generator

import numpy as np
from matplotlib import pyplot as plt


@dataclass
class DrawingOptions:
    color: str
    min_label_anchor: tuple = (0, 0)
    max_label_anchor: tuple = (0, 0)


@dataclass
class ESSInstrumentRequirements:
    num_pixels: tuple
    event_rate: tuple
    drawing_options: DrawingOptions


def _shift_pos(original: tuple, shift: tuple) -> tuple:
    return tuple(np.array(original) + np.array(shift))


def _plot_bound_line(
    ax: plt.Axes, lower_bound: tuple, upper_bound: tuple, drawing_option: DrawingOptions
):
    ax.plot(
        (lower_bound[0], upper_bound[0]),
        (lower_bound[1], lower_bound[1]),
        "--",
        color=drawing_option.color,
    )
    ax.plot(*lower_bound, marker=5, color=drawing_option.color)
    ax.plot(*upper_bound, marker=4, color=drawing_option.color)


def _min_max_pair(values: tuple) -> tuple:
    return min(values), max(values)


@dataclass
class ESSInstruments:
    """ESS Instrument Requirements for benchmarking."""

    beer: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(200000, 400000),
        event_rate=(3e5, 2e6, 5e7),
        drawing_options=DrawingOptions(color="red", min_label_anchor=(1e4, 2e4)),
    )
    bifrost: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(5000,),
        event_rate=(1e6, 1e5),
        drawing_options=DrawingOptions(color="blue", min_label_anchor=(0, 0.5e3)),
    )
    cspec: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(400000, 750000),
        event_rate=(1e6, 1e7),
        drawing_options=DrawingOptions(color="magenta", min_label_anchor=(0, 3e4)),
    )
    dream: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(4000000, 12000000),
        event_rate=(1.3e6, 1e7, 7.5e7),
        drawing_options=DrawingOptions(
            color="black", min_label_anchor=(0, 1.5e5), max_label_anchor=(0, 4e5)
        ),
    )
    loki: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(750000, 1500000),
        event_rate=(1e7, 3.33e5, 1.92e6, 1.2e5, 7.5e5, 4.69e4),
        drawing_options=DrawingOptions(color="pink", min_label_anchor=(0, 5e4)),
    )
    magic: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(1440000, 2880000),
        event_rate=(1e6, 1e7),
        drawing_options=DrawingOptions(
            color="purple",
            min_label_anchor=(1.1e7, -1e5),
            max_label_anchor=(1.1e7, -2e5),
        ),
    )
    estia: ESSInstrumentRequirements = ESSInstrumentRequirements(
        num_pixels=(250000, 500000),
        event_rate=(4e6, 2e6, 8e5, 8e5),
        drawing_options=DrawingOptions(
            color="grey", min_label_anchor=(4e6, -2e4), max_label_anchor=(4e6, -3e4)
        ),
    )
    # TODO: NMX
    # TODO: ODIN

    def items(self) -> Generator[tuple[str, ESSInstrumentRequirements], None, None]:
        from dataclasses import fields

        for field in fields(self):
            yield field.name, getattr(self, field.name)

    def plot_boundaries(self, ax: plt.Axes):
        for inst_name, inst_req in self.items():
            do = inst_req.drawing_options
            min_er, max_er = _min_max_pair(inst_req.event_rate)
            min_np, max_np = _min_max_pair(inst_req.num_pixels)
            label_anchors = [do.min_label_anchor, do.max_label_anchor]

            for num_pixels, label_anchor in zip(set([min_np, max_np]), label_anchors):
                _plot_bound_line(ax, (min_er, num_pixels), (max_er, num_pixels), do)
                label_pos = _shift_pos((min_er, num_pixels), label_anchor)
                ax.annotate(inst_name.upper(), label_pos, size=10)

            # Fill boundaries with color.
            ax.fill_between([min_er, max_er], min_np, max_np, color=do.color, alpha=0.1)

    def configure_full_scale(self, ax: plt.Axes) -> None:
        ax.grid(True)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlim(7 * 10**3, 1.4 * 10**8)
        ax.set_ylim(4 * 10**3, 4.5 * 10**7)

    def show(self) -> None:
        _, (ax) = plt.subplots(1, 1, figsize=(9, 7))
        self.plot_boundaries(ax)
        self.configure_full_scale(ax)
        ax.set_title("ESS instrument requirements")
        ax.set_xlabel("Event rate [Hz]")
        ax.set_ylabel("Number of pixels")
