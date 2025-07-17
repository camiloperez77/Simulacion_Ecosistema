import socket
import pickle
from pprint import pprint

from numpy.matrixlib.defmatrix import matrix
from tabulate import tabulate

from bloomfilter import BloomFilter
from model.MarkovChainAnalysis import MarkovChainAnalysis
from model.dgim import DGIM
from model.hyperloglog import HyperLogLog
from model.mapreduce import MapReduce
from model.minwisehashing import MinWiseHashing
from model.pageRank import PageRank
from model.random_walk_utils import construir_grafo_desde_eventos
from collections import Counter

from model.transition_matrix import Matrix_Transition

SOCKET_PATH = "/tmp/insect_query_socket"

def send_query(query):
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(SOCKET_PATH)
        sock.sendall(pickle.dumps(query))

        response = sock.recv(1024000)  # Buffer grande para recibir muchos datos
        result = pickle.loads(response)

        sock.close()
        return result
    except Exception as e:
        return {"status": "error", "message": f"Error de conexión: {e}"}

def print_stats():
    query = {"type": "stats"}
    result = send_query(query)
    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    stats = result["data"]

    print("\n===== ESTADÍSTICAS DEL ECOSISTEMA DE INSECTOS =====")
    print(f"Total de insectos registrados: {stats['total_insects']}")

    print("\nDistribución por especie:")
    species_data = [[species, count] for species, count in stats["by_species"].items()]
    print(tabulate(species_data, headers=["Especie", "Cantidad"], tablefmt="heavy_outline"))

    print("\nDistribución por rol:")
    role_data = [[role, count] for role, count in stats["by_role"].items()]
    print(tabulate(role_data, headers=["Rol", "Cantidad"], tablefmt="heavy_outline"))

    print("\nDistribución por hábitat:")
    habitat_data = [[habitat, count] for habitat, count in stats["by_habitat"].items()]
    print(tabulate(habitat_data, headers=["Hábitat", "Cantidad"], tablefmt="heavy_outline"))

    print("\nEventos por ventanas de tiempo:")
    print("Últimos 1 minuto:")
    minute_data = []
    for (species, role), count in stats["time_windows"]["1min"].items():
        minute_data.append([species, role, count])
    print(tabulate(minute_data, headers=["Especie", "Rol", "Cantidad"], tablefmt="heavy_outline"))

    print("\nTendencias de eventos (últimos 5 minutos):")
    event_trends = []
    for event, species_counts in stats["trends"]["events"]["5min"].items():
        for species, count in species_counts.items():
            event_trends.append([event, species, count])
    print(tabulate(event_trends, headers=["Evento", "Especie", "Cantidad"], tablefmt="heavy_outline"))

def query_species(species, limit=10):
    query = {"type": "species", "params": {"species": species, "limit": limit}}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    insects = result["data"]
    print(f"\n===== INSECTOS DE ESPECIE: {species.upper()} =====")

    for i, insect in enumerate(insects, 1):
        print(f"\n{i}. ID: {insect['_id']}")
        print(f"   Especie: {insect['insect']['species']}")
        print(f"   Rol: {insect['insect']['role']}")
        print(f"   Edad: {insect['insect']['age']}")
        print(f"   Evento: {insect['event']}")
        print(f"   Tiempo: {insect['eventTime']}")
        print(f"   Hábitat: {insect['location']['habitat']}")
        print(
            f"   Coordenadas: {insect['location']['coordinates']['latitude']}, {insect['location']['coordinates']['longitude']}")


def query_habitat_event(habitat, event, limit=10):
    query = {"type": "habitat_event", "params": {"habitat": habitat, "event": event, "limit": limit}}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    insects = result["data"]
    print(f"\n===== INSECTOS EN {habitat.upper()} CON EVENTO {event.upper()} =====")

    for i, insect in enumerate(insects, 1):
        print(f"\n{i}. ID: {insect['_id']}")
        print(f"   Especie: {insect['insect']['species']}")
        print(f"   Rol: {insect['insect']['role']}")
        print(f"   Edad: {insect['insect']['age']}")
        print(f"   Tiempo: {insect['eventTime']}")

def query_bloom_filter(window, specie, rol, even):
    query = {"type": "bloom_filter", "params": {"window": window}}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    data = result["data"]
    print(data)
    n = int(sum(len(events) for events in data.values()))
    print(f"Número total de eventos (n): {n}")
    bloom = BloomFilter(n, 0.03)

    # Añadir datos corregido
    for (species, role), events in data.items():
        for event in events:
            bloom.add(f"{species}_{role}_{event}")

    bf = bloom.bloom_key(specie, rol, even)
    print(bf)

    if bloom.contains(bf):
        print(f"'{bf}' es posible que esté en el conjunto.")  # Corregido el mensaje
    else:
        print(f"'{bf}' definitivamente no está en el conjunto.")

def estimate_unique_species(window):
    query = {"type": "cantidad", "params": {"window": window}}
    result = send_query(query)
    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    species_dict = result["data"]

    if not species_dict:
        print("No se encontraron datos para esa ventana de tiempo.")
        return

    hll = HyperLogLog()


    for species in species_dict.keys():
        hll.add(species)


    estimated_unique = hll.estimate()


    print(f"\n===== ESTIMACIÓN DE ESPECIES ÚNICAS EN VENTANA '{window}' =====")
    print(f"Estimación de especies distintas: {estimated_unique}")

def query_minwise(window, specie, rol, even):
    query = {"type": "minwise", "params": {"window": window}}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    data = result["data"]
    if not data:
        print(" No hay datos en la ventana seleccionada.")
        return

    print(f" Datos recuperados para ventana '{window}':")
    print(f"Número total de combinaciones: {sum(len(events) for events in data.values())}")

    # Crear MinWiseHashing con datos de la ventana
    minwise_actual = MinWiseHashing()
    for (species, role), eventos in data.items():
        for event in eventos:
            insect = {
                "species": species,
                "role": role,
                "age": 0
            }
            minwise_actual.add_insect(insect)

    # Crear MinWiseHashing para el evento ingresado
    insecto_nuevo = {
        "species": specie,
        "role": rol,
        "age": 0
    }
    minwise_nuevo = MinWiseHashing()
    minwise_nuevo.add_insect(insecto_nuevo)

    # Calcular similitud Jaccard
    similitud = minwise_actual.estimate_jaccard_similarity(minwise_nuevo)
    print(f"\n Similitud Jaccard estimada: {similitud:.2f}")

    # decisión según umbral de similitud
    UMBRAL = 0.5

    if similitud >= UMBRAL:
        print("El evento es MUY similar a eventos recientes. Podría ser redundante.")
    else:
        print("El evento parece ser nuevo (baja similitud con registros recientes).")


    muestra = minwise_actual.get_representative_sample(sample_size=3)
    print("\n Muestra representativa:")
    for i, m in enumerate(muestra, 1):
        print(f"{i}. {m}")

def query_dgim_filter(window):
    # window: string "1min", "2min", "5min"
    query = {"type": "dgim_filter", "params": {"window": window}}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    data = result["data"]
    print(data)
    window_seconds = {
        "1min": 60,
        "2min": 120,
        "5min": 300,
        "1hour": 3600
    }.get(window, 300)

    dgim = DGIM(window_size_seconds=window_seconds)
    estimated = dgim.estimate_from_data(data)
    print(f"Estimación de ataques de depredador en ventana '{window}': {estimated}")

def query_random_walk(window, start, steps):
    query = {
        "type": "random_walk",
        "params": {"window": window, "start": start, "steps": steps}
    }
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
    else:
        camino = result["data"]
        print("\n===== RANDOM WALK DE HÁBITATS =====")
        print(" → ".join(camino))

def query_random_walk_analisis(window, start, steps, repeticiones):
    query = {
        "type": "random_walk",
        "params": {"window": window, "start": start, "steps": steps}
    }

    frecuencias = Counter()

    for _ in range(repeticiones):
        result = send_query(query)
        if result["status"] == "ok":
            camino = result["data"]
            frecuencias.update(camino)
        else:
            print(f" Error: {result.get('message', 'Desconocido')}")
            return

    total_visitas = sum(frecuencias.values())

    print("\n===== ANÁLISIS DE DINÁMICA DE HÁBITATS (Random Walk) =====")
    print(f"Total de caminatas simuladas: {repeticiones}")
    print(f"Pasos por caminata: {steps}")
    print(f"Hábitat inicial: {start}\n")
    print("Frecuencia de aparición de hábitats:")

    for habitat, count in frecuencias.most_common():
        porcentaje = (count / total_visitas) * 100
        print(f" - {habitat}: {count} veces ({porcentaje:.2f}%)")

    print("\n Esto permite identificar los hábitats con mayor tránsito potencial.")

def query_pagerank(window):
    result = send_query({"type": "eco_density", "params": {"window": window}})

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    species = result["data"]


    if not species:
        print("No se encontraron datos para esa ventana de tiempo.")
        return

    page_rank = PageRank()

    # Añadir todos tus eventos (ejemplo)
    for insect in species:
        page_rank.add_event(
            species=insect['species'],
            ecological_impact=insect['ecologicalImpact'],
            population_density=insect['populationDensity']
        )

    result = page_rank.calculate_rank()

    print("Importancia ecológica de las especies:")
    for specie, weight in sorted(result.items(), key=lambda x: -x[1]):
        print(f"- {specie}: {weight:.4f}")

def query_mapreduce(map, reduc):
    query = {"type": "mapreduce"}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    data = result["data"]
    # print(data)
    map_red = MapReduce()
    final_result = map_red.master_controller(data, int(map), int(reduc))
    for key, count in final_result.items():
        print(f"{key}: {count}")


def query_markov():
    query = {"type": "markov"}
    result = send_query(query)

    if result["status"] != "ok":
        print(f"Error: {result.get('message', 'Desconocido')}")
        return

    data = result["data"]

    mark = Matrix_Transition()
    markov_result = mark.analyze_transitions(data, output_format='markov_chain')
    print("\nResultado como Cadena de Markov:")
    pprint(markov_result)

    # Ejemplo con formato de matriz de transición
    states, matrix = mark.analyze_transitions(data, output_format='transition_matrix')
    print("\nEstados:", states)
    print("\nMatriz de transición:")
    print(matrix)

    mc_analysis = MarkovChainAnalysis(matrix)
    results = mc_analysis.analyze_dtmc()
    print("Recurrent States:", results['recurrent_states'])
    print("Transient States:", results['transient_states'])
    print("Periodic States:", results['periodic_states'])
    print("Aperiodic States:", results['aperiodic_states'])
    print("Ergodic States:", results['ergodic_states'])


def show_menu():
    print("\n===== CLIENTE DE CONSULTA DE INSECTOS =====")
    print("1. Ver estadísticas generales")
    print("2. Consultar por especie")
    print("3. Consultar por hábitat y evento")
    print("4. Aplicar Bloom Filter")
    print("5. Minwise")
    print("6. HyperLogLog")
    print("7. DGIM")
    print("8. RandomWalk")
    print("9. Análisis Random Walk")
    print("10. PageRank")
    print("11. MapReduce")
    print("12. Markov Chain")
    print("0. Salir")



def main():
    while True:
        show_menu()
        choice = input("\nSelecciona una opción (0-11): ")

        if choice == "0":
            break
        elif choice == "1":
            print_stats()
        elif choice == "2":
            species = input("Introduce la especie (ant, bee, butterfly, spider): ")
            limit = int(input("Número máximo de resultados: "))
            query_species(species, limit)
        elif choice == "3":
            habitat = input("Introduce el hábitat (forest, field, garden, house): ")
            event = input("Introduce el evento (birth, death, predator attack): ")
            limit = int(input("Número máximo de resultados: "))
            query_habitat_event(habitat, event, limit)
        elif choice == "4":
            window = input("Ventana de tiempo (1min, 2min, 5min): ")
            specie = input("Introduce especie (ant, bee, butterfly, spider): ")
            rol = input("Introduce rol (worker, queen, soldier, scout): ")
            even = input("Introduce evento (birth, death, predator attack): ")
            query_bloom_filter(window, specie, rol, even)
        elif choice == "5":
            window = input("Ventana de tiempo (1min, 2min, 5min): ")
            specie = input("Introduce especie (ant, bee, butterfly, spider): ")
            rol = input("Introduce rol (worker, queen, soldier, scout): ")
            even = input("Introduce evento (birth, death, predator attack): ")
            query_minwise(window, specie, rol, even)
        elif choice == "6":
            window = input("Ventana de tiempo (1min, 2min, 5min): ")
            estimate_unique_species(window)
        elif choice == "7":
            window = "5min"
            query_dgim_filter(window)
        elif choice == "8":
            window = int(input("Ventana en segundos (ej. 300): "))
            start = input("Hábitat de inicio (forest, garden, house...): ")
            steps = int(input("Cuántos pasos simular: "))
            query_random_walk(window, start, steps)
        elif choice == "9":
            window = int(input("Ventana en segundos (ej. 600): "))
            start = input("Hábitat inicial (forest, garden, house...): ")
            steps = int(input("Pasos por caminata: "))
            repeticiones = int(input("Cuántas caminatas simular: "))
            query_random_walk_analisis(window, start, steps, repeticiones)
        elif choice == "10":
            window = input("Ventana de tiempo (1min, 2min, 5min): ")
            query_pagerank(window)
        elif choice == "11":
            map= input("Introduce número WORKERS de Mapeo: ")
            reduc = input("Introduce número WORKERS de Reducción: ")
            query_mapreduce(map, reduc)
        elif choice == "12":
            query_markov()
        else:
            print("Opción no válida. Inténtalo de nuevo.")


if __name__ == "__main__":
    print("Conectando al servidor de consultas...")
    try:
        # Verificar que el servidor esté activo
        test_query = {"type": "stats"}
        result = send_query(test_query)
        if result["status"] == "ok":
            print("Conexión exitosa al servidor de consultas.")
            main()
        else:
            print(f"Error al conectar con el servidor: {result.get('message', 'Desconocido')}")
    except Exception as e:
        print(f"Error: {e}")
        print("Asegúrate de que el servidor esté en ejecución.")