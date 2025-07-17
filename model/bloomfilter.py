import math
from collections import defaultdict

import mmh3

class BloomFilter:

    def __init__(self, expected_elements, false_positive_rate):  # Constructor
        self.size = self.optimal_size(expected_elements, false_positive_rate)
        self.hash_count = self.optimal_hash(self.size, expected_elements)
        self.bit_array = [0] * self.size

    def optimal_size(self, m, p):     # Calcula el tamaño óptimo del filtro
        return int(-m * math.log(p) / (math.log(2) ** 2))

    def optimal_hash(self, n, m):     # Calcula el número óptimo de funciones hash
        return int((n / m) * math.log(2))

    def hash(self, item, seed):    # Función hash usando mmh3
        return mmh3.hash(item, seed) % self.size

    def add(self, item):    # Agrega un item al filtro.
        for i in range(self.hash_count):
            index = self.hash(item, i)
            self.bit_array[index] = 1

    def contains(self, item):   # Verifica si un item probablemente existe en el conjunto.
        for i in range(self.hash_count):
            index = self.hash(item, i)
            if self.bit_array[index] == 0:
                return False
        return True

    def bloom_key(self, species: str, role: str, event: str) -> str:
        return f"{species}_{role}_{event}"