# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import signal
import threading

from producer import ArrayProducer, ProducerConfig


class ProducerManager:
    def __init__(self):
        self.producers: dict[str, ArrayProducer] = {}
        self.threads: dict[str, threading.Thread] = {}
        self._shutdown_event = threading.Event()
        self._setup_signal_handling()

    def _setup_signal_handling(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, initiating shutdown...")
        self.shutdown()

    def add_producer(self, config: ProducerConfig):
        producer = ArrayProducer(config=config)
        self.producers[config.topic_name] = producer
        thread = threading.Thread(
            target=producer.run,
            name=f"producer-{config.topic_name}",
        )
        self.threads[config.topic_name] = thread

    def start_all(self):
        for thread in self.threads.values():
            thread.start()

    def shutdown(self):
        print("Shutting down all producers...")
        for producer in self.producers.values():
            producer.stop()

        for topic, thread in self.threads.items():
            print(f"Waiting for {topic} producer to stop...")
            thread.join(timeout=2.0)
            if thread.is_alive():
                print(f"Warning: {topic} producer did not stop cleanly")

        self.producers.clear()
        self.threads.clear()
