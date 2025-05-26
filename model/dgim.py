from collections import defaultdict, deque

class DGIMBucket:
    """Bucket para el algoritmo DGIM"""

    def __init__(self, timestamp, size=1):
        self.timestamp = timestamp
        self.size = size

class DGIM:
    """Algoritmo DGIM para contar ataques de depredadores en ventana deslizante"""

    def __init__(self, window_size_seconds=3600):  # 1 hora por defecto
        self.window_size = window_size_seconds
        self.buckets = []
        self.current_time = 0

    def add_bit(self, bit, timestamp):
        """Agrega un bit (1 para ataque, 0 para no ataque) con timestamp"""
        self.current_time = timestamp
        self._remove_old_buckets()

        if bit == 1:
            # Crear nuevo bucket de tamaño 1
            new_bucket = DGIMBucket(timestamp, 1)
            self.buckets.append(new_bucket)
            self._merge_buckets()

    def _remove_old_buckets(self):
        """Elimina buckets fuera de la ventana de tiempo"""
        cutoff_time = self.current_time - self.window_size
        self.buckets = [b for b in self.buckets if b.timestamp > cutoff_time]

    def _merge_buckets(self):
        """Fusiona buckets del mismo tamaño cuando hay más de 2"""
        size_counts = defaultdict(list)

        # Agrupar buckets por tamaño
        for bucket in self.buckets:
            size_counts[bucket.size].append(bucket)

        # Fusionar cuando hay más de 2 buckets del mismo tamaño
        new_buckets = []
        for size, buckets in size_counts.items():
            if len(buckets) > 2:
                # Mantener los 2 más recientes y fusionar los más antiguos
                buckets.sort(key=lambda x: x.timestamp, reverse=True)
                new_buckets.extend(buckets[:2])

                # Fusionar buckets más antiguos en pares
                old_buckets = buckets[2:]
                while len(old_buckets) >= 2:
                    b1, b2 = old_buckets.pop(), old_buckets.pop()
                    merged = DGIMBucket(max(b1.timestamp, b2.timestamp), size * 2)
                    size_counts[size * 2].append(merged)

                # Si queda un bucket impar, agregarlo
                if old_buckets:
                    new_buckets.append(old_buckets[0])
            else:
                new_buckets.extend(buckets)

        self.buckets = new_buckets

    def estimate_count(self):
        """Estima el número de ataques en la ventana actual"""
        if not self.buckets:
            return 0

        # Sumar todos los buckets excepto el más antiguo (que se cuenta a medias)
        total = sum(bucket.size for bucket in self.buckets[:-1])
        if self.buckets:
            total += self.buckets[-1].size // 2

        return total