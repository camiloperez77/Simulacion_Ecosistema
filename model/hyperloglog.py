import math
import mmh3

class HyperLogLog:
    """HyperLogLog para estimar el número de especies distintas"""

    def __init__(self, precision=12):
        self.precision = precision
        self.m = 2 ** precision  # Número de buckets
        self.buckets = [0] * self.m
        self.alpha = self._get_alpha()

    def _get_alpha(self):
        """Constante de corrección basada en el número de buckets"""
        if self.m >= 128:
            return 0.7213 / (1 + 1.079 / self.m)
        elif self.m >= 64:
            return 0.709
        elif self.m >= 32:
            return 0.697
        else:
            return 0.5

    def add_species(self, species):
        """Agrega una especie al estimador"""
        # Hash de la especie
        hash_val = mmh3.hash(species) & 0x7FFFFFFF

        # Usar los primeros 'precision' bits para el bucket
        bucket = hash_val >> (32 - self.precision)

        # Contar los ceros a la izquierda en los bits restantes
        remaining_bits = hash_val & ((1 << (32 - self.precision)) - 1)
        leading_zeros = self._count_leading_zeros(remaining_bits, 32 - self.precision) + 1

        # Actualizar el bucket con el máximo
        self.buckets[bucket] = max(self.buckets[bucket], leading_zeros)

    def _count_leading_zeros(self, num, max_bits):
        """Cuenta los ceros a la izquierda en una representación binaria"""
        if num == 0:
            return max_bits
        count = 0
        for i in range(max_bits - 1, -1, -1):
            if (num >> i) & 1:
                break
            count += 1
        return count

    def estimate_cardinality(self):
        """Estima el número de especies distintas"""
        raw_estimate = self.alpha * (self.m ** 2) / sum(2 ** (-x) for x in self.buckets)

        # Corrección para estimaciones pequeñas
        if raw_estimate <= 2.5 * self.m:
            zeros = self.buckets.count(0)
            if zeros != 0:
                return self.m * math.log(self.m / zeros)

        # Corrección para estimaciones grandes
        if raw_estimate <= (1.0 / 30.0) * (2 ** 32):
            return raw_estimate
        else:
            return -2 ** 32 * math.log(1 - raw_estimate / (2 ** 32))