"""A fake confluent_kafka Producer for tests: callbacks only fire inside poll() and flush()."""

from confluent_kafka import KafkaException


class FakeProducer:
    """Mimics confluent_kafka.Producer: callbacks only fire inside poll() and flush()."""

    def __init__(self, mode="ack"):
        self.mode = mode  # ack | fail | silent
        self.sent = []  # every message accepted by produce()
        self.fail_ids: set[str] = set()  # event ids that get a delivery error
        self.reject_ids: set[str] = set()  # event ids produce() refuses outright
        self.buffer_full_times = 0
        self.always_buffer_full = False
        self._queue = []

    def produce(self, topic, key, value, headers, timestamp, on_delivery):
        if self.always_buffer_full:
            raise BufferError("queue full")
        if self.buffer_full_times:
            self.buffer_full_times -= 1
            raise BufferError("queue full")
        if key in self.reject_ids:
            raise KafkaException("message too large")
        message = {
            "topic": topic,
            "key": key,
            "value": value,
            "headers": dict(headers),
            "timestamp": timestamp,
        }
        self.sent.append(message)
        self._queue.append((key, on_delivery))

    def poll(self, timeout=0):
        self._deliver()

    def flush(self, timeout=None):
        self._deliver()
        return len(self._queue)

    def _deliver(self):
        remaining = []
        for key, callback in self._queue:
            if self.mode == "silent":
                remaining.append((key, callback))
            elif self.mode == "fail" or key in self.fail_ids:
                callback("broker transport failure", None)
            else:
                callback(None, None)
        self._queue = remaining
