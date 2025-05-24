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


# data = defaultdict(list, {
#     ('butterfly', 'queen'): ['death', 'predator attack'],
#     ('ant', 'scout'): ['death', 'predator attack'],
#     ('spider', 'soldier'): ['death', 'death'],
#     ('bee', 'soldier'): ['birth'],
#     ('spider', 'queen'): ['death', 'birth', 'death'],
#     ('bee', 'scout'): ['predator attack'],
#     ('spider', 'worker'): ['death', 'predator attack', 'predator attack'],
#     ('ant', 'soldier'): ['birth', 'death', 'birth', 'birth'],
#     ('spider', 'scout'): ['predator attack', 'predator attack']
# })
#
# n = int(sum(len(events) for events in data.values()))
# print(f"Número total de eventos (n): {n}")
# bloom = BloomFilter(n, 0.03)  # Corregido: BloomFilter en lugar de bloomfilter
#
# # Añadir datos corregido
# for (species, role), events in data.items():
#     for event in events:  # Corregido el bucle y variable
#         bloom.add(f"{species}_{role}_{event}")  # event es un string
#
# # Ejemplo de uso (necesitarías definir estas variables)
# specie = "spider"
# rol = "queen"
# event = "birth"
# bf = bloom.bloom_key(specie, rol, event)
# print(bf)
#
# if bloom.contains(bf):
#     print(f"'{bf}' es posible que esté en el conjunto.")  # Corregido el mensaje
# else:
#     print(f"'{bf}' definitivamente no está en el conjunto.")


# # Example usage:
#
# # Initialize Bloom Filter with size 10 and 3 hash functions
# bf = BloomFilter(10, 0.03)
#
# # Add some email addresses
# emails = ['test@example.com', 'hello@world.com', 'foo@bar.com']
# for email in emails:
#     bf.add(email)  # Add email addresses to the Bloom filter
#
# # Check if an email address is in the Bloom Filter
# test_emails = ['test@example.com', 'not_in_set@example.com']
# for email in test_emails:
#     if bf.contains(email):
#         print(f"'{email}' is possibly in the set.")  # Email might be in the set
#     else:
#         print(f"'{email}' is definitely not in the set.")  # Email is not in the set