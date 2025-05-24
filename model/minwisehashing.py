import random
import mmh3

class MinWiseHashing:
    """MinWise Hashing para muestreo representativo de la población"""

    def __init__(self, num_hashes=128):
        self.num_hashes = num_hashes
        self.min_values = [float('inf')] * num_hashes
        self.samples = []
        self.hash_functions = [lambda x, i=i: mmh3.hash(x, i) for i in range(num_hashes)]

    def add_insect(self, insect_data):
        """Agrega un insecto al muestreo MinWise"""
        # Crear una representación única del insecto
        insect_key = f"{insect_data['species']}_{insect_data['role']}_{insect_data.get('age', 0)}"

        for i, hash_func in enumerate(self.hash_functions):
            hash_val = hash_func(insect_key) & 0x7FFFFFFF  # Asegurar valor positivo
            if hash_val < self.min_values[i]:
                self.min_values[i] = hash_val
                # Mantener muestra asociada con este hash mínimo
                if len(self.samples) <= i:
                    self.samples.extend([None] * (i - len(self.samples) + 1))
                self.samples[i] = insect_data.copy()

    def get_representative_sample(self, sample_size=50):
        """Obtiene una muestra representativa de la población"""
        valid_samples = [s for s in self.samples if s is not None]
        return random.sample(valid_samples, min(sample_size, len(valid_samples)))

    def estimate_jaccard_similarity(self, other_minwise):
        """Estima la similitud de Jaccard con otro conjunto MinWise"""
        matches = sum(1 for i in range(self.num_hashes)
                      if self.min_values[i] == other_minwise.min_values[i])
        return matches / self.num_hashes