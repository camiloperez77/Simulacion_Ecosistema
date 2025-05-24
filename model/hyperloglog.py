import hashlib
import math

class HyperLogLog:
    def __init__(self, b=12):
        self.b = b
        self.m = 2 ** b
        self.registers = [0] * self.m

    def _hash(self, element):
        hash_object = hashlib.sha256(str(element).encode())
        hash_hex = hash_object.hexdigest()
        hash_binary = bin(int(hash_hex, 16))[2:].zfill(256)
        return hash_binary

    def _R(self, x):
        return len(x) - len(x.lstrip('0')) + 1

    def add(self, element):
        hash_value = self._hash(element)
        index = int(hash_value[:self.b], 2)
        tail = self._R(hash_value[self.b:])
        self.registers[index] = max(self.registers[index], tail)

    def estimate(self):
        alpha_m = 0.7213 / (1 + 1.079 / self.m)
        Z = 1 / sum([2 ** -reg for reg in self.registers])
        E = alpha_m * self.m ** 2 * Z

        # Corrección de sesgo
        if E <= 2.5 * self.m:
            V = self.registers.count(0)
            if V > 0:
                E = self.m * math.log(self.m / V)
        elif E > (1 / 30.0) * (2 ** 32):
            E = -2 ** 32 * math.log(1 - E / 2 ** 32)

        return round(E)