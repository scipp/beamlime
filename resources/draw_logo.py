# This script for drawing logo is one-time used, but it is kept here for reference.
# To run this script, you may need to install
# a python package ``drawsvg`` and a font ``Comic Neue``.
import drawsvg as dvg

LIME_DARK = '#5E6100'
LIME_LIGHT = '#A3A800'


def write_text(*, x: int, y: int, width: int = 256) -> dvg.Group:
    split_x = x + width * 0.57
    transparent_line_1 = dvg.Line(x, y, split_x, y, opacity=0)
    transparent_line_2 = dvg.Line(split_x, y, width, y, opacity=0)
    text_recipes = [
        ('beam', LIME_DARK, 'normal', transparent_line_1),
        ('lime', LIME_LIGHT, 'bold', transparent_line_2),
    ]
    texts = [
        dvg.Text(
            text,
            font_size=int((width / 4) * 0.958),
            path=path,
            text_anchor='start',
            font_family='Comic Neue',
            font_weight=weight,
            fill=color,
        )
        for text, color, weight, path in text_recipes
    ]
    return dvg.Group(texts)


def draw_beam(*, start_x: int = 0, y: int, width: int) -> dvg.Group:
    beam_beam = dvg.Line(
        start_x,
        y,
        start_x + width,
        y,
        stroke_width=3,
        stroke=LIME_DARK,
        stroke_linecap="round",
    )
    lime_beam = dvg.Line(
        (start_x + width) * 0.66,
        y,
        start_x + width,
        y,
        stroke_width=3,
        stroke=LIME_LIGHT,
        stroke_linecap="round",
    )
    return dvg.Group([beam_beam, lime_beam])


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
                cx, cy, x, y, stroke_width=4, stroke=LIME_LIGHT, stroke_linecap="round"
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
                    radius * 0.16,
                    start_angle_deg - (i + 1) * 45,
                    start_angle_deg - i * 45,
                    stroke_width=2,
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
                    radius * 0.16,
                    (-i + 2) * 45,
                    (-i + 3) * 45,
                    stroke_width=2,
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
                stroke_width=5,
                stroke=LIME_LIGHT,
                stroke_linecap="round",
                fill='none',
            )
        ]
    )


if __name__ == '__main__':
    total_width = 300
    total_height = int(total_width * 0.3)
    d = dvg.Drawing(total_width, total_height, origin='top-left')
    # Draw text
    text_width = int(total_width * 0.85)

    d.append(
        draw_beam(
            start_x=int(total_width * 0.07),
            y=int(total_height * 0.4),
            width=int(text_width * 0.95),
        )
    )
    d.append(
        write_text(
            x=int(total_width * 0.02), y=int(total_height * 0.82), width=text_width
        )
    )
    lime_radius = int(total_width * 0.075)
    lime_cx, lime_cy = int(total_width * 0.88), int(total_height * 0.4)
    d.append(draw_lime_splits(cx=lime_cx, cy=lime_cy, radius=lime_radius))
    d.append(draw_lime_arc(cx=lime_cx, cy=lime_cy, radius=lime_radius))
    d.save_svg('logo.svg')
