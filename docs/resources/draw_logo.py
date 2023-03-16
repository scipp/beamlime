from math import cos, pi, sin

from manim import Arc, Create, Line, Scene, Text, Write


def get_rotation_matrix(degree: int = 0):
    theta = degree / 180 * pi
    return [[cos(theta), -sin(theta)], [sin(theta), cos(theta)]]


def rotate_coordinate(indices, center: list = (0, 0), degree: int = 0) -> tuple:
    centered = [idx - c for idx, c in zip(indices[:2], center[:2])]
    rotation_matrix = get_rotation_matrix(degree=degree)
    rotated_indices = [
        sum([elem * idx for elem, idx in zip(row, centered)]) for row in rotation_matrix
    ]
    return [idx + c for idx, c in zip(rotated_indices, center[:2])]


def draw_lime_line(lime_center):
    lines = []
    narrow = 28
    wide = (180 - narrow * 2) / 3
    intervals = [0, narrow] + [wide] * 3 + [narrow]
    widths = [4] + [6] * 4 + [4]
    degrees = [-45 + sum(intervals[: i + 1]) for i in range(len(intervals))]
    for width, degree in zip(widths, degrees):
        lime_start = lime_center.copy()
        lime_start[0] += 0.15
        new_start = rotate_coordinate(
            indices=lime_start, center=lime_center, degree=degree
        ) + [1]
        lime_end = new_start.copy()
        lime_end[0] += 0.3
        new_end = rotate_coordinate(
            indices=lime_end, center=new_start, degree=degree
        ) + [1]
        lines.append(Line(start=new_start, end=new_end, stroke_width=width))
    return lines


class CreateLogo(Scene):
    def construct(self):
        drc = "#78BE21"

        text = Text("beamlime", font="Magnolia Script")
        # txtc = "#4eaa70"
        # text.set_color(txtc)
        firstline = Line(start=[-1.17, 0.16, 1], end=[0.1, 0.16, 1], stroke_width=8)
        firstline.set_color(drc)
        secondline = Line(start=[0.4, 0.16, 1], end=[1.42, 0.16, 1], stroke_width=8)
        secondline.set_color(drc)

        self.play(Write(text))
        self.play(Create(firstline))
        self.play(Create(secondline))
        lime_lines = draw_lime_line(lime_center=secondline.get_right())
        arc = Arc(
            radius=0.45,
            start_angle=-1 / 4 * pi - 0.036,
            angle=pi + 0.085,
            arc_center=secondline.get_right(),
            stroke_width=6.5,
        )
        for obj in lime_lines:
            obj.set_color(drc)
        arc.set_color(drc)
        lime = [Create(lime_line) for lime_line in lime_lines] + [Create(arc)]
        self.play(*lime)
