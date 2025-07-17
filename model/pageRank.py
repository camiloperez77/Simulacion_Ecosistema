from collections import defaultdict


class PageRank:
    def __init__(self):
        self.species_data = defaultdict(lambda: {
            'total_impact': 0,
            'total_density': 0,
            'count': 0
        })

    def add_event(self, species: str, ecological_impact: int, population_density: int):
        data = self.species_data[species]
        data['total_impact'] += abs(ecological_impact)
        data['total_density'] += population_density
        data['count'] += 1

    def calculate_rank(self):
        if not self.species_data:
            return {}

        ranks = {}
        for species, data in self.species_data.items():
            avg_impact = data['total_impact'] / data['count']
            avg_density = data['total_density'] / data['count']
            ranks[species] = (avg_impact / 50) * (1000 / avg_density)

        # Normalizar a porcentajes (suma = 1)
        total = sum(ranks.values())
        return {species: (rank/total) for species, rank in ranks.items()}