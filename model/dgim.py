from collections import deque
import time

class DGIM:
    def __init__(self, event_data, window_str):
        self.window_size = self._parse_window(window_str)
        self.buckets = deque()
        self.current_time = int(time.time())
        self._process_event_data(event_data)

    def _parse_window(self, window_str):
        """
        Acepta solo '1min', '2min' o '5min' y retorna su equivalente en segundos.
        """
        if window_str == "1min":
            return 60
        elif window_str == "2min":
            return 120
        elif window_str == "5min":
            return 300
        else:
            raise ValueError(f"Ventana no válida: '{window_str}'. Usa '1min', '2min' o '5min'.")

    def _process_event_data(self, event_data):
        """
        Convierte eventos en bits y los agrega automáticamente.
        """
        current_time = int(time.time())

        for (species, role), events in event_data.items():
            for i, event in enumerate(events):
                timestamp = current_time - (len(events) - i)
                bit = 1 if event == "predator attack" else 0
                self.add_bit(bit, timestamp)

    def add_bit(self, bit, timestamp):
        self._expire_old_buckets(timestamp)
        if bit == 1:
            self.buckets.appendleft((timestamp, 1))

    def _expire_old_buckets(self, current_timestamp):
        while self.buckets and (current_timestamp - self.buckets[-1][0]) > self.window_size:
            self.buckets.pop()

    def estimate(self):
        if not self.buckets:
            return 0

        estimate = 0
        for i, (timestamp, size) in enumerate(self.buckets):
            if i == len(self.buckets) - 1:
                estimate += size / 2
            else:
                estimate += size
        return int(estimate)