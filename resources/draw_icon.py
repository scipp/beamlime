# This script for drawing icon is one-time used, but it is kept here for reference.
# To run this script, you may need to install a python package ``drawsvg``.
import drawsvg as dvg

LIME_DARK = '#5E6100'
LIME_LIGHT = '#A3A800'


def draw_lime_splits(cx: int, cy: int, radius: int) -> dvg.Group:
    import numpy as np

    start = np.pi + (np.pi / 6)
    center = np.array([cx, cy])
    lime_splits = []
    for i in range(5):
        cur_theta = start + i * np.pi / 4
        x, y = center + (np.array([np.cos(cur_theta), np.sin(cur_theta)]) * radius)

        lime_splits.append(
            dvg.Line(
                cx, cy, x, y, stroke_width=6, stroke=LIME_LIGHT, stroke_linecap="round"
            )
        )
        arc_rotation_r = radius * 0.8
        start_angle_deg = start / np.pi * 180
        if i < 4:
            little_arc_x, little_arc_y = center + (
                np.array(
                    [np.cos(cur_theta + np.pi / 12), np.sin(cur_theta + np.pi / 12)]
                )
                * (radius * 0.8)
            )
            lime_splits.append(
                dvg.ArcLine(
                    np.cos(cur_theta + np.pi / 12) * arc_rotation_r + cx,
                    np.sin(cur_theta + np.pi / 12) * arc_rotation_r + cy,
                    radius * 0.18,
                    start_angle_deg - (i + 1) * 45,
                    start_angle_deg - i * 45,
                    stroke_width=5,
                    stroke=LIME_LIGHT,
                    stroke_linecap="round",
                    fill='none',
                )
            )
        if i > 0:
            little_arc_x, little_arc_y = center + (
                np.array(
                    [np.cos(cur_theta - np.pi / 12), np.sin(cur_theta - np.pi / 12)]
                )
                * (radius * 0.8)
            )
            lime_splits.append(
                dvg.ArcLine(
                    little_arc_x,
                    little_arc_y,
                    radius * 0.18,
                    (-i + 2) * 45,
                    (-i + 3) * 45,
                    stroke_width=5,
                    stroke=LIME_LIGHT,
                    stroke_linecap="round",
                    fill='none',
                )
            )

    return dvg.Group(lime_splits)


def draw_lime_arc(cx: int, cy: int, radius: int) -> dvg.Group:
    return dvg.Group(
        [
            dvg.ArcLine(
                cx,
                cy,
                radius,
                -30,
                150,
                stroke_width=8,
                stroke=LIME_LIGHT,
                stroke_linecap="round",
                fill='none',
            )
        ]
    )


if __name__ == '__main__':
    d = dvg.Drawing(128, 128, origin='top-left')
    lime_cx, lime_cy = 60, 76
    lime_radius = 48
    d.append(draw_lime_splits(cx=lime_cx, cy=lime_cy, radius=lime_radius))
    d.append(draw_lime_arc(cx=lime_cx, cy=lime_cy, radius=lime_radius))
    d.save_svg('favicon.svg')
