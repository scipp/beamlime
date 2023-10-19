# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, VerticalScroll
from textual.widget import Widget
from textual.widgets import (
    Button,
    Header,
    Input,
    Label,
    Placeholder,
    TabbedContent,
    TabPane,
)

from ..prototypes.parameters import BenchmarkParameters


class ResultBrowser(Container):
    DEFAULT_CSS = """
    Horizontal {
        height: auto;
    }

    Container {
        height: auto;
    }

    Placeholder {
        height: 5;
    }

    Container.result_browser {
        border: solid $accent;
    }
    """

    def compose(self) -> ComposeResult:
        from textual.widgets import Collapsible

        with Collapsible(collapsed=False, title='Browse Previous Results'):
            with Horizontal():
                yield Container(
                    Placeholder(label='result list'), classes='result_browser'
                )
                yield Container(Placeholder(label='preview'), classes='result_browser')


class ResultVisualization(Container):
    DEFAULT_CSS = """
    Horizontal {
        height: auto;
    }

    Container {
        height: auto;
    }

    Placeholder {
        width: 100%;
        height: 10;
    }
    """

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield Container(Placeholder(label='selected result list'))
            yield Container(Placeholder(label='plotting tool'))


class PreviousResult(VerticalScroll):
    DEFAULT_CSS = """
    ResultBrowser {
        height: auto;
    }

    ResultVisualization {
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        yield ResultBrowser()
        yield ResultVisualization()


class BenchmarkAliasInputBox(Container):
    DEFAULT_CSS = """
    BenchmarkAliasInputBox {
        min-width: 88;
        height: auto;
    }

    Label, FilenameSuffix {
        align: left middle;
        padding: 1;
    }

    Container {
        height: auto;
        width: auto;
    }
    """

    def compose(self) -> ComposeResult:
        from ..prototypes.benchmark_env import provide_now

        prefix_label = Label('result_')
        prefix_container = Container(prefix_label)
        suffix_label = Label(f'_{provide_now()}.json')
        suffix_container = Container(suffix_label)

        def update_suffix():
            suffix_label.update(f'_{provide_now()}.json')

        self.set_interval(1, callback=update_suffix)

        input_container = Container(
            Input(placeholder='Alias for a new benchmark result.')
        )
        input_container.styles.min_width = 44

        with Horizontal():
            yield prefix_container
            yield input_container
            yield suffix_container


class NewBenchmark(VerticalScroll):
    def _collect_benchmark_parameter(self) -> dict[str, type]:
        from typing import get_type_hints

        return get_type_hints(BenchmarkParameters)

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield BenchmarkAliasInputBox()
            yield Container(Button(label='Start'))


class MainMenuTabs(Widget):
    def compose(self) -> ComposeResult:
        with TabbedContent():
            with TabPane(title="Previous Results"):
                yield PreviousResult()
            with TabPane(title="New Benchmark"):
                yield NewBenchmark()


class BenchmarkApp(App):
    DEFAULT_CSS = """
    MainMenu {
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield MainMenuTabs()

    def on_mount(self) -> None:
        self.title = "Beamlime Benchmark Helper"
