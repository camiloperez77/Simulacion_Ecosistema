import socket
import pickle
from tabulate import tabulate

from model.bloomfilter import BloomFilter

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


def show_menu():
    print("\n===== CLIENTE DE CONSULTA DE INSECTOS =====")
    print("1. Ver estadísticas generales")
    print("2. Consultar por especie")
    print("3. Consultar por hábitat y evento")
    print("4. Aplicar Bloom Filter")
    print("0. Salir")


def main():
    while True:
        show_menu()
        choice = input("\nSelecciona una opción (0-4): ")

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