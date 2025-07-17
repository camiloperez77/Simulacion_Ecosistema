import networkx as nx
import random
from geopy.distance import geodesic
from matplotlib import pyplot as plt


def construir_grafo_desde_eventos(eventos, threshold_km=155000):
    G = nx.Graph()
    ubicaciones = {}

    for evento in eventos:
        habitat = evento['location']['habitat']
        coords = (
            evento['location']['coordinates']['latitude'],
            evento['location']['coordinates']['longitude']
        )
        if habitat not in ubicaciones:
            ubicaciones[habitat] = coords
            G.add_node(habitat, pos=coords)

    habitats = list(ubicaciones.keys())

    for i in range(len(habitats)):
        for j in range(i + 1, len(habitats)):
            h1, h2 = habitats[i], habitats[j]
            coord1, coord2 = ubicaciones[h1], ubicaciones[h2]
            dist = geodesic(coord1, coord2).km
            if dist <= threshold_km:
                G.add_edge(h1, h2, weight=dist)

    return G

def random_walk_habitat(G, start_habitat, steps=5):
    if start_habitat not in G:
        raise ValueError(f"Hábitat {start_habitat} no existe en el grafo.")

    path = [start_habitat]
    current = start_habitat

    for _ in range(steps):
        vecinos = list(G.neighbors(current))
        if not vecinos:
            break
        current = random.choice(vecinos)
        path.append(current)

    return path
def visualizar_camino(G, camino):
    """
    Dibuja el grafo de hábitats y resalta el camino recorrido en rojo.
    """
    pos = nx.get_node_attributes(G, 'pos')

    plt.figure(figsize=(8, 5))
    nx.draw(G, pos, with_labels=True,
            node_color='lightblue', edge_color='gray',
            node_size=2000, font_size=12)

    camino_edges = [(camino[i], camino[i+1]) for i in range(len(camino)-1)]
    nx.draw_networkx_edges(G, pos, edgelist=camino_edges, edge_color='red', width=3)

    plt.title(" → ".join(camino), fontsize=10, color='green')
    plt.axis('off')
    plt.tight_layout()
    plt.show()
