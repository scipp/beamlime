# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import signal

from producer_manager import ProducerConfig, ProducerManager


def main():
    manager = ProducerManager()

    # Configure multiple producers
    configs = [
        ProducerConfig(topic_name=f"sensor_data_{i}", array_size=(10, 10), interval=1.0)
        for i in range(4)
    ]

    # Add all producers
    for config in configs:
        manager.add_producer(config)

    # Start all producers
    manager.start_all()

    # Keep main thread alive
    try:
        signal.pause()
    except KeyboardInterrupt:
        manager.shutdown()


if __name__ == "__main__":
    main()
