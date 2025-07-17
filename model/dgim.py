from collections import deque
import time

class DGIM:
    def __init__(self, window_size_seconds=300):
        self.window_size = window_size_seconds
        self.buckets = deque()
        self.current_time = int(time.time())

    def add_bit(self, bit, timestamp):
        # Elimina los buckets fuera de ventana
        self._expire_old_buckets(timestamp)
        if bit == 1:
            self.buckets.appendleft((timestamp, 1))

    def _expire_old_buckets(self, current_timestamp):
        # Remueve buckets fuera de la ventana
        while self.buckets and (current_timestamp - self.buckets[-1][0]) > self.window_size:
            self.buckets.pop()

    def estimate(self):
        if not self.buckets:
            return 0

        estimate = 0
        last_timestamp = self.buckets[-1][0]

        for i, (timestamp, size) in enumerate(self.buckets):
            if i == len(self.buckets) - 1:
                estimate += size / 2
            else:
                estimate += size
        return int(estimate)

    def estimate_from_data(self, event_data):
        """
        event_data: defaultdict con claves (species, role) y valores = lista de eventos ["birth", "predator attack", ...]
        """
        current_time = int(time.time())

        for (species, role), events in event_data.items():
            for i, event in enumerate(events):
                timestamp = current_time - (len(events) - i)  # Simula orden temporal inverso
                bit = 1 if event == "predator attack" else 0
                self.add_bit(bit, timestamp)

        return self.estimate()
