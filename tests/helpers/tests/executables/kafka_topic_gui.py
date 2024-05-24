# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from textual import work
from textual.app import App, ComposeResult, Screen
from textual.containers import Container, Horizontal, VerticalScroll
from textual.message import Message
from textual.widgets import Button, Header, Input, Pretty, SelectionList, Static
from textual.widgets.selection_list import Selection


def back_to_log_in_button() -> Button:
    return Button("↵ Back to log in", id="back-to-login")


def exit_button() -> Button:
    return Button("Exit ❌", id="exit")


class KafkaTopicInput(Horizontal):
    DEFAULT_CSS = """
    Container {
        height: auto;
        padding: 1;
    }
    """

    class AddRequest(Message):
        def __init__(self, new_topic) -> None:
            super().__init__()
            self.new_topic = new_topic

    def compose(self) -> ComposeResult:
        self.topic_input = Input('', 'Enter a new topic name')
        yield Container(self.topic_input)
        yield Container(Button('Add Topic ➕', id='add-topic'))  # noqa: RUF001

    def on_button_pressed(self, event: Button.Pressed):
        event.stop()
        self.post_message(self.AddRequest(self.topic_input.value))
        self.topic_input.clear()
        self.topic_input.focus()


class KafkaTopicButtons(Horizontal):
    def compose(self) -> ComposeResult:
        yield Button("Refresh ↻", id='refresh')
        yield Button("Delete Selected 🗑️ ", id='delete')
        yield Button("Select All ✅ ", id='select-all')
        yield Button("Clear Selection 🧹", id='deselect-all')
        yield back_to_log_in_button()
        yield exit_button()


class WarningScreen(Screen):
    DEFAULT_CSS = """
    Horizontal {
        height: auto;
        width: auto;
        padding: 1;
    }

    VerticalScroll {
        height: auto;
        padding: 1;
    }
    """

    def __init__(self, *instructions: Static | Pretty, **option_callbacks) -> None:
        super().__init__()
        self.instructions = instructions
        self.option_callbacks = option_callbacks

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            yield from self.instructions
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


class WaitingScreen(Screen):
    def compose(self) -> ComposeResult:
        from textual.widgets import LoadingIndicator

        yield LoadingIndicator()
        yield exit_button()


class KafkaTopicManager(Screen):
    DEFAULT_CSS = """
    KafkaTopicButtons {
        height: auto;
        padding: 1;
    }

    VerticalScroll {
        height: auto;
        max-height: 10;
        padding: 1;
    }

    SelectionList {
        padding: 1;
        border: solid $accent;
    }

    KafkaTopicInput {
        height: auto;
        border: solid $accent;
    }
    """

    def __init__(self, kafka_server: str) -> None:
        from functools import partial

        from confluent_kafka.admin import AdminClient

        super().__init__(name="Kafka Topic Control Screen")

        self.admin = AdminClient(conf={"bootstrap.servers": kafka_server})
        self.action_map = {
            'refresh': self.refresh_topics,
            'delete': self.delete_selected,
            'select-all': partial(self.select_or_deselect_all, True),
            'deselect-all': partial(self.select_or_deselect_all, False),
        }
        self.topics = SelectionList()
        self.topic_container = VerticalScroll(self.topics)
        self.topic_container.border_title = f"Topics at {kafka_server}"

    @work(exclusive=True)
    async def refresh_topics(self) -> None:
        import asyncio

        from textual.widgets.option_list import OptionDoesNotExist

        def compose_selection(topic_id: str) -> Selection:
            try:
                return self.topics.get_option(topic_id)
            except OptionDoesNotExist:
                return Selection(topic_id, topic_id, False, id=topic_id)

        all_topics = [
            topic
            for topic in self.admin.list_topics().topics
            if not topic.startswith('_')
        ]

        all_topics.sort()
        self.topics.clear_options()
        self.topics.add_options([compose_selection(topic) for topic in all_topics])
        await asyncio.sleep(1)  # At maximum once per second.

    @work(exclusive=True)
    async def delete_confirmed(self, selected: list[str]) -> None:
        from concurrent.futures import wait

        self.app.push_screen(WaitingScreen())
        selected = self.topics.selected
        to_be_deleted = self.admin.delete_topics(selected)
        wait(to_be_deleted.values())
        self.app.pop_screen()
        self.refresh_topics()

    @work(exclusive=True)
    async def delete_selected(self) -> None:
        if selected := self.topics.selected:
            from functools import partial

            yes_call_back = partial(self.delete_confirmed, selected=selected)

            self.app.push_screen(
                WarningScreen(
                    Static("Following topics will be deleted."),
                    Pretty(selected),
                    Static("Continue?"),
                    yes=yes_call_back,
                    no=lambda: None,
                )
            )

    @work(exclusive=True)
    async def select_or_deselect_all(self, selected: bool) -> None:
        """Select or deselect all selections.

        Two methods are combined as one so that ``work`` decorator
        allows selection or deselection canceled if not necessary.

        Setting ``exclusive`` argument as ``True`` in ``work`` decorator
        means a call of this method will cancel all previous ones in the task queue.
        For example, if you called ``select_all`` and then called ``deselect_all``,
        ``select_all`` does not need to be done before ``deselect_all``,
        so ``select_all` can be canceled if still not done.
        """
        if selected:
            self.topics.select_all()
        else:
            self.topics.deselect_all()

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id in self.action_map:
            self.action_map[event.button.id]()
            event.stop()

    def on_kafka_topic_input_add_request(self, message: KafkaTopicInput.AddRequest):
        from confluent_kafka.admin import NewTopic

        self.admin.create_topics([NewTopic(message.new_topic)])
        self.refresh_topics()
        message.stop()

    def compose(self) -> ComposeResult:
        new_topic_input = KafkaTopicInput()
        new_topic_input.border_title = "New Topic"

        yield Header(show_clock=True)
        yield new_topic_input
        yield KafkaTopicButtons()
        yield self.topic_container
        self.refresh_topics()


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
            yield back_to_log_in_button()
            yield exit_button()


class KafkaLogin(Screen):
    DEFAULT_CSS = """
    Horizontal {
        height: auto;
        padding: 1;
    }

    Container.input_box {
        height: auto;
        border: solid $accent;
    }
    """

    def compose(self) -> ComposeResult:
        self.address_input = Input(
            value="localhost:9092", placeholder="Kafka server address"
        )
        self.input_box = Container(self.address_input, classes="input_box")
        self.input_box.border_title = "Kafka Broker Server Address"

        with Horizontal():
            yield Container(Button("Connect to kafka"))
            yield Container(exit_button())
        yield self.input_box

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
            KafkaAdminClient(bootstrap_servers=self.address_input.value)
            self.app.push_screen(KafkaTopicManager(self.address_input.value))
        except NoBrokersAvailable:
            self.app.push_screen(LoginFailScreen())


class KafkaTopicApp(App):
    """Kafka topic managing tool.

    It shows all topics that do not start with ``'_'``,
    which is a prefix for hidden topics.
    One or more topics can be selected to be deleted.
    """

    def on_mount(self) -> None:
        self.title = "Kafka Topic Helper"
        self.push_screen(KafkaLogin())

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == 'exit':
            self.exit()
        elif event.button.id == 'back-to-login':
            event.stop()
            self.pop_screen()


if __name__ == "__main__":
    app = KafkaTopicApp()
    app.run()
