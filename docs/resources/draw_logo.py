from math import cos, pi, sin

import numpy as np
from manim import Arc, Create, Group, Line, Scene, Text, Write


def get_rotation_matrix(degree: int = 0):
    theta = degree / 180 * pi
    return [[cos(theta), -sin(theta)], [sin(theta), cos(theta)]]


def rotate_coordinate(indices, center: list = (0, 0), angle: int = 0) -> tuple:
    centered = [idx - c for idx, c in zip(indices[:2], center[:2], strict=True)]
    rotation_matrix = get_rotation_matrix(degree=angle)
    rotated_indices = [
        sum([elem * idx for elem, idx in zip(row, centered, strict=True)])
        for row in rotation_matrix
    ]
    return tuple(idx + c for idx, c in zip(rotated_indices, center[:2], strict=True))


def build_beam(
    start: np.array, width: float, height: float, thickness: float, color: str
) -> Group:
    fl_start = start + np.array([width * 0.075, height * 0.3, 0])
    fl_end = fl_start + np.array([width * 0.485, 0, 0])
    fl = Line(start=fl_start, end=fl_end, stroke_width=thickness)
    fl.set_color(color)
    sl_start = fl_end + np.array([width * 0.1, 0, 0])
    sl_end = sl_start + np.array([width * 0.35, 0, 0])
    sl = Line(start=sl_start, end=sl_end, stroke_width=thickness)
    sl.set_color(color)
    return Group(fl, sl)


def partial_radial_line(
    center: np.array,
    angle: int,
    radius: float,
    thickness: float,
    crop_ratio: float = 0.2,
) -> Line:
    l_start = center.copy()
    l_start[0] += radius * crop_ratio
    r_start = [*rotate_coordinate(indices=l_start, center=center, angle=angle), 0]
    l_end = center.copy()
    l_end[0] += radius
    new_end = [*rotate_coordinate(indices=l_end, center=center, angle=angle), 0]
    return Line(start=r_start, end=new_end, stroke_width=thickness)


def build_lime(center: np.array, radius: int, color: str) -> Group:
    thicknesses = [radius * 5] + [radius * 7] * 4 + [radius * 5]
    narrow_angle = 28
    wide_angle = (180 - narrow_angle * 2) / 3
    intervals = [0, narrow_angle] + [wide_angle] * 3 + [narrow_angle]
    angles = [-45 + sum(intervals[: i + 1]) for i in range(len(intervals))]

    lines = [
        partial_radial_line(center, angle, radius, thickness)
        for thickness, angle in zip(thicknesses, angles, strict=True)
    ]

    lines.append(
        Arc(
            radius=radius,
            start_angle=(angles[0] / 180) * pi - 0.036,
            angle=pi + 0.085,
            arc_center=center,
            stroke_width=7.5 * radius,
        )
    )
    for line in lines:
        line.set_color(color)

    return Group(*lines)


class LimeScene(Scene):
    def __init__(self):
        super().__init__()
        self.lime_color = "#78BE21"

    def build_objects(self, font_size=128):
        text = Text("beamlime", font="Magnolia Script", font_size=font_size)
        beam_lines = build_beam(
            start=text.get_left(),
            width=text.width,
            height=text.height,
            thickness=text.font_size * 0.1,
            color=self.lime_color,
        )
        lime_lines = build_lime(
            center=beam_lines.get_right(),
            radius=1 * (font_size / 128),
            color=self.lime_color,
        )
        return text, beam_lines, lime_lines


class CreateLogoVideo(LimeScene):
    def construct(self):
        text, beam_lines, lime_lines = self.build_objects(font_size=64)
        self.add_foreground_mobject(text)
        self.play(Write(text))
        for line in beam_lines:
            self.play(Create(line))
        self.play(*[Create(line) for line in lime_lines])


class CreateLogoImage(LimeScene):
    def construct(self):
        text, beam_lines, lime_lines = self.build_objects(font_size=128)
        self.add_foreground_mobject(text)
        self.add(text)
        self.add(*beam_lines)
        self.add(*lime_lines)


class CreateIconImage(LimeScene):
    def construct(self):
        lime_lines = build_lime(
            center=[-0.5, -0.5, 0],
            radius=5,
            color=self.lime_color,
        )
        self.add(*lime_lines)
