import random
import threading
import time
from typing import Any, Callable

import panel as pn
import param


class FakeKafkaStore:
    """
    Simulates a Kafka-like message store with eventual consistency.

    Values are stored with a simulated network delay and can be updated
    from multiple sources (widgets or external publishers).
    """

    def __init__(self, delay: float = 0.5) -> None:
        self._store: dict[str, Any] = {}
        self._subscribers: dict[str, list[Callable[[Any], None]]] = {}
        self._delay = delay
        self._lock = threading.Lock()

    def publish(self, topic: str, value: Any) -> None:
        """Publish a value to a topic with simulated delay."""

        def delayed_publish():
            time.sleep(self._delay)
            with self._lock:
                self._store[topic] = value
                # Notify all subscribers
                for callback in self._subscribers.get(topic, []):
                    try:
                        callback(value)
                    except Exception as e:
                        print(f"Error in subscriber callback: {e}")

        thread = threading.Thread(target=delayed_publish, daemon=True)
        thread.start()

    def get(self, topic: str, default: Any = None) -> Any:
        """Get current value from topic."""
        with self._lock:
            return self._store.get(topic, default)

    def subscribe(self, topic: str, callback: Callable[[Any], None]) -> None:
        """Subscribe to topic updates."""
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)


class ReactiveWidget(param.Parameterized):
    """
    A widget that maintains eventual consistency with a Kafka-like store.

    The widget can both send updates to the store and receive updates from it,
    demonstrating bidirectional synchronization with eventual consistency.
    """

    value = param.Number(default=50, bounds=(0, 100))

    def __init__(self, store: FakeKafkaStore, topic: str, **params) -> None:
        super().__init__(**params)
        self._store = store
        self._topic = topic
        self._updating_from_store = False

        # Subscribe to store updates
        self._store.subscribe(topic, self._on_store_update)

        # Watch for parameter changes to publish to store
        self.param.watch(self._on_widget_change, 'value')

        # Initialize with current store value
        current_value = self._store.get(topic, self.value)
        if current_value != self.value:
            self.value = current_value

    def _on_store_update(self, new_value: Any) -> None:
        """Handle updates from the store."""
        if not self._updating_from_store and new_value != self.value:
            self._updating_from_store = True
            try:
                self.value = new_value
            finally:
                self._updating_from_store = False

    def _on_widget_change(self, event) -> None:
        """Handle widget value changes to publish to store."""
        if not self._updating_from_store:
            self._store.publish(self._topic, event.new)


class KafkaWidgetDashboard:
    """
    Dashboard demonstrating eventual consistency between Panel widgets and Kafka-like store.
    """

    def __init__(self) -> None:
        self._store = FakeKafkaStore(delay=0.3)
        self._topic = "demo_slider_value"

        # Create reactive widget
        self._reactive_widget = ReactiveWidget(self._store, self._topic)

        # Create Panel components
        self._slider = pn.Param(
            self._reactive_widget,
            parameters=['value'],
            widgets={'value': pn.widgets.IntSlider},
            name="Reactive Slider",
        )

        self._publish_button = pn.widgets.Button(
            name="Publish Random Value", button_type="primary"
        )
        self._publish_button.on_click(self._publish_random_value)

        self._current_value_indicator = pn.pane.Markdown(
            self._get_status_text(), sizing_mode="stretch_width"
        )

        # Update status periodically
        self._start_status_updates()

    def _publish_random_value(self, event) -> None:
        """Simulate external publisher setting a random value."""
        random_value = random.randint(0, 100)
        self._store.publish(self._topic, random_value)

    def _get_status_text(self) -> str:
        """Get current status information."""
        widget_value = self._reactive_widget.value
        store_value = self._store.get(self._topic, "Not set")

        return f"""
        ## Kafka Widget Demo
        
        **Widget Value:** {widget_value}  
        **Store Value:** {store_value}  
        **Synchronized:** {'✅' if widget_value == store_value else '⏳'}
        
        *Move the slider or click the button to see eventual consistency in action.*
        """

    def _update_status(self) -> None:
        """Update status display."""
        self._current_value_indicator.object = self._get_status_text()

    def _start_status_updates(self) -> None:
        """Start periodic status updates."""

        def update_loop():
            while True:
                time.sleep(0.1)
                try:
                    self._update_status()
                except Exception as e:
                    print(f"Error updating status: {e}")

        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()

    def create_dashboard(self) -> pn.Column:
        """Create the Panel dashboard."""
        return pn.Column(
            self._current_value_indicator,
            self._slider,
            self._publish_button,
            sizing_mode="stretch_width",
            margin=20,
        )


def main() -> None:
    """Run the dashboard application."""
    pn.extension()

    dashboard = KafkaWidgetDashboard()
    app = dashboard.create_dashboard()

    app.servable()
    app.show(port=5007)


if __name__ == "__main__":
    main()
