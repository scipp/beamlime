# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import time
from threading import Event, Thread

import numpy as np
from confluent_kafka import Consumer, KafkaError, Producer


class ArrayProducer:
    def __init__(self, kafka_host='localhost:9092'):
        self.producer = Producer(
            {'bootstrap.servers': kafka_host, 'client.id': 'array_producer'}
        )

        self.consumer = Consumer(
            {
                'bootstrap.servers': kafka_host,
                'group.id': 'array_control_group',
                'auto.offset.reset': 'latest',
            }
        )
        self.consumer.subscribe(['topic1_control'])

        self.array_size = (10, 10)
        self.running = Event()
        self.running.set()
        self.consumer_thread = Thread(target=self._listen_control, daemon=True)
        self.consumer_thread.start()

    def delivery_callback(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')
            self.last_delivery_successful = False
        else:
            print(f'Message delivered to {msg.topic()}')
            self.last_delivery_successful = True

    def _listen_control(self):
        while self.running.is_set():
            msg = self.consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f'Consumer error: {msg.error()}')
                continue

            try:
                command = json.loads(msg.value().decode('utf-8'))
                if 'size' in command:
                    self.array_size = tuple(command['size'])
                    print(f"Updated array size to {self.array_size}")
            except json.JSONDecodeError as e:
                print(f"Failed to parse control message: {e}")

    def generate_array(self):
        return np.random.rand(*self.array_size).tolist()

    def run(self, interval=1.0):
        while self.running.is_set():
            data = {'timestamp': time.time(), 'data': self.generate_array()}
            try:
                self.last_delivery_successful = False
                self.producer.produce(
                    'topic1',
                    value=json.dumps(data).encode('utf-8'),
                    callback=self.delivery_callback,
                )
                # Poll for callbacks
                self.producer.poll(timeout=1.0)

                if not self.last_delivery_successful:
                    print("Failed to deliver message - no Kafka connection?")
            except Exception as e:
                print(f"Production error: {e}")

            if not self.running.is_set():
                break
            time.sleep(interval)

    def stop(self):
        print("Initiating shutdown...")
        self.running.clear()
        print("Waiting for consumer thread...")
        self.consumer_thread.join(timeout=1.0)
        print("Closing consumer...")
        self.consumer.close()
        print("Flushing producer...")
        remaining = self.producer.flush(timeout=2.0)
        if remaining > 0:
            print(f"Failed to flush {remaining} messages")
        print("Shutdown complete")


if __name__ == '__main__':
    producer = ArrayProducer()
    try:
        producer.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
        producer.stop()
