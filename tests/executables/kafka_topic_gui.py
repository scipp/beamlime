# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from textual.app import App, ComposeResult, Screen
from textual.containers import Container, Horizontal, VerticalScroll
from textual.widgets import Button, Header, Input, Pretty, SelectionList, Static
from textual.widgets.selection_list import Selection


class KafkaTopicButtons(Horizontal):
    def compose(self) -> ComposeResult:
        yield Button("Refresh ↻", id='refresh')
        yield Button("Delete Selected 🗑️ ", id='delete')
        yield Button("Select All ✅ ", id='all')
        yield Button("Clear Selection 🧹", id='clear')
        yield Button("↵ Back to log in", id="back-to-login")


class WarningScreen(Screen):
    DEFAULT_CSS = """
    Horizontal {
        padding: 1;
        height: auto;
        width: auto;
    }

    VerticalScroll {
        height: auto;
    }
    """

    def __init__(self, *instructions: Static | Pretty, **option_callbacks) -> None:
        self.instructions = instructions
        self.option_callbacks = option_callbacks
        super().__init__()

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            for instruction in self.instructions:
                yield instruction
        with Horizontal():
            for option_callback in self.option_callbacks:
                yield Button(option_callback, id=option_callback)

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()

        if (b_id := event.button.id) is not None and (
            callback := self.option_callbacks.get(b_id)
        ) is not None:
            callback()

        self.app.pop_screen()


class KafkaTopicManager(Screen):
    DEFAULT_CSS = """
    KafkaTopicButtons {
        height: auto;
        padding: 1;
    }

    VerticalScroll {
        height: auto;
    }

    SelectionList {
        border: solid $accent;
    }
    """

    def __init__(self, kafka_server: str) -> None:
        from confluent_kafka.admin import AdminClient

        self.admin = AdminClient(conf={"bootstrap.servers": kafka_server})
        self.action_map = {
            'refresh': self.refresh_topics,
            'delete': self.delete_selected,
            'all': self.select_all,
            'clear': self.clear_all,
        }
        self.topics = SelectionList()
        self.topics.border_title = f"Topics at {kafka_server}"
        self.refresh_topics()

        super().__init__(name="Kafka Admin Control Screen", id=None)

    def refresh_topics(self):
        p_selected = self.topics.selected
        new_options = [
            Selection(topic, topic, topic in p_selected, id=topic)
            for topic in self.admin.list_topics().topics
            if not topic.startswith('_')
        ]

        self.topics.clear_options()
        self.topics.add_options(new_options)

    def delete_selected(self):
        if selected := self.topics.selected:

            def yes_callback():
                self.admin.delete_topics(selected)
                for selected_topic in selected:
                    self.topics.remove_option(selected_topic)

            self.app.push_screen(
                WarningScreen(
                    Static("Following topics will be deleted."),
                    Pretty(selected),
                    Static("Continue?"),
                    yes=yes_callback,
                    no=lambda: None,
                )
            )
        self.refresh_topics()

    def select_all(self):
        self.topics.select_all()

    def clear_all(self):
        self.topics.deselect_all()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield KafkaTopicButtons()
        with VerticalScroll():
            yield self.topics

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id in self.action_map:
            event.stop()
            self.action_map[event.button.id]()


class LoginFailScreen(Screen):
    DEFAULT_CSS = """
    Container, Horizontal {
        height: auto;
        padding: 1;
    }
    """

    def compose(self) -> ComposeResult:
        from textual.widgets import Static

        yield Container(
            Static(
                "Kafka client could not connect to the server. 🙅\n"
                "Please check if the server is available. 🤔"
            )
        )
        with Horizontal():
            yield Button("↵ Back to log in", id="back-to-login")
            yield Button("Exit ❌", id="exit")


class KafkaLogin(Screen):
    DEFAULT_CSS = """
    Horizontal, Container {
        padding: 1;
        height: auto;
    }

    Container.input_box {
        border: solid $accent;
        height: auto;
    }
    """

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield Container(Button("Connect to kafka"))
            yield Container(Button("Exit ❌", id="exit"))

        self.kafka_server = Input(value="localhost:9092", placeholder="localhost:9092")
        input_box = Container(self.kafka_server, classes="input_box")
        input_box.border_title = "Kafka Broker Server Address"
        yield input_box

    def on_button_pressed(self, _: Button.Pressed):
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable

        try:
            # Use ``kafka-python`` api to check connectivitiy to the broker.
            # For actual api calls, ``confluent-kafka`` is used.
            # This is because the error raised for disconnection to the broker
            # in ``confluent-kafka`` is not captured properly by python.
            # However, ``confluent-kafka`` is the most stable and
            # maintained kafka python api.
            KafkaAdminClient(bootstrap_servers=self.kafka_server.value)
            self.app.push_screen(KafkaTopicManager(self.kafka_server.value))
        except NoBrokersAvailable:
            self.app.push_screen(LoginFailScreen())


class KafkaTopicApp(App):
    """Kafka topic managing tool.

    It shows all topics that do not start with ``'_'``,
    which is a prefix for hidden topics.
    One or more topics can be selected to be deleted.
    """

    def on_mount(self) -> None:
        self.push_screen(KafkaLogin())
        self.title = "Kafka Admin Helper"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'exit':
            self.app.exit()
        elif event.button.id == 'back-to-login':
            self.pop_screen()


if __name__ == "__main__":
    app = KafkaTopicApp()
    app.run()
