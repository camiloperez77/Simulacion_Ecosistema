from confluent_kafka import Consumer
import json
import time
import threading
import pickle
import socket
import os
from datetime import datetime, timedelta
from collections import defaultdict

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'insect-consumer-group',
    'auto.offset.reset': 'earliest'
}


# Estructura de datos para almacenar los insectos
class InsectDataStore:
    def __init__(self):
        self.insects_by_id = {}
        self.insects_by_species = defaultdict(dict)
        self.insects_by_role = defaultdict(dict)
        self.insects_by_habitat = defaultdict(dict)
        self.insects_by_event = defaultdict(dict)

        # Para ventanas de tiempo
        self.time_windows = {
            '1min': defaultdict(int),
            '5min': defaultdict(int),
            '15min': defaultdict(int),
            '1hour': defaultdict(int)
        }

        # Para guardar datos de insectos por ventana de tiempo
        self.time_windows_data = {
            '1min': defaultdict(list),
            '2min': defaultdict(list),
            '5min': defaultdict(list),
        }

        # Datos para análisis de tendencias
        self.event_trends = {window: defaultdict(lambda: defaultdict(int)) for window in self.time_windows.keys()}
        self.species_trends = {window: defaultdict(int) for window in self.time_windows.keys()}

        # Lock para escritura segura en la estructura de datos
        self.lock = threading.RLock()

    def add_insect(self, insect_data):
        """Añadir un insecto al almacén de datos con seguridad para concurrencia"""
        with self.lock:
            insect_id = insect_data["_id"]
            species = insect_data["insect"]["species"]
            role = insect_data["insect"]["role"]
            habitat = insect_data["location"]["habitat"]
            event = insect_data["event"]
            event_time = datetime.strptime(insect_data["eventTime"].split()[0], "%Y-%m-%dT%H:%M:%S")

            # Actualizar tablas hash principales
            self.insects_by_id[insect_id] = insect_data
            self.insects_by_species[species][insect_id] = insect_data
            self.insects_by_role[role][insect_id] = insect_data
            self.insects_by_habitat[habitat][insect_id] = insect_data
            self.insects_by_event[event][insect_id] = insect_data

            # Actualizar ventanas de tiempo
            self._update_time_windows(species, role, event, event_time, habitat)

    def _update_time_windows(self, species, role, event, event_time, habitat):
        """Actualiza los contadores de las ventanas de tiempo"""
        now = datetime.now()

        # Solo procesar eventos dentro de la última hora
        if now - event_time > timedelta(hours=1):
            return

        # Actualizar contadores para cada ventana de tiempo
        if now - event_time <= timedelta(minutes=1):
            self.time_windows['1min'][(species, role)] += 1
            self.event_trends['1min'][event][species] += 1
            self.species_trends['1min'][species] += 1
            self.time_windows_data['1min'][(species, role)].append(event)

        if now - event_time <= timedelta(minutes=2):
            self.time_windows_data['2min'][(species, role)].append(event)


        if now - event_time <= timedelta(minutes=5):
            self.time_windows['5min'][(species, role)] += 1
            self.event_trends['5min'][event][species] += 1
            self.species_trends['5min'][species] += 1
            self.time_windows_data['5min'][(species, role)].append(event)

        if now - event_time <= timedelta(minutes=15):
            self.time_windows['15min'][(species, role)] += 1
            self.event_trends['15min'][event][species] += 1
            self.species_trends['15min'][species] += 1

        if now - event_time <= timedelta(hours=1):
            self.time_windows['1hour'][(species, role)] += 1
            self.event_trends['1hour'][event][species] += 1
            self.species_trends['1hour'][species] += 1

    def clean_old_data(self, max_age_hours=2):
        """Elimina datos más antiguos que el límite especificado"""
        with self.lock:
            now = datetime.now()
            to_remove = []

            for insect_id, data in self.insects_by_id.items():
                event_time = datetime.strptime(data["eventTime"].split()[0], "%Y-%m-%dT%H:%M:%S")
                if now - event_time > timedelta(hours=max_age_hours):
                    to_remove.append(insect_id)

            # Eliminar registros antiguos
            for insect_id in to_remove:
                data = self.insects_by_id[insect_id]
                species = data["insect"]["species"]
                role = data["insect"]["role"]
                habitat = data["location"]["habitat"]
                event = data["event"]

                del self.insects_by_id[insect_id]
                if insect_id in self.insects_by_species[species]:
                    del self.insects_by_species[species][insect_id]
                if insect_id in self.insects_by_role[role]:
                    del self.insects_by_role[role][insect_id]
                if insect_id in self.insects_by_habitat[habitat]:
                    del self.insects_by_habitat[habitat][insect_id]
                if insect_id in self.insects_by_event[event]:
                    del self.insects_by_event[event][insect_id]

            return len(to_remove)

    # Métodos de consulta
    def get_stats(self):
        """Obtiene estadísticas generales"""
        with self.lock:
            stats = {
                "total_insects": len(self.insects_by_id),
                "by_species": {species: len(insects) for species, insects in self.insects_by_species.items()},
                "by_role": {role: len(insects) for role, insects in self.insects_by_role.items()},
                "by_habitat": {habitat: len(insects) for habitat, insects in self.insects_by_habitat.items()},
                "by_event": {event: len(insects) for event, insects in self.insects_by_event.items()},
                "time_windows": {
                    window: dict(counts) for window, counts in self.time_windows.items()
                },
                "trends": {
                    "events": {window: dict(events) for window, events in self.event_trends.items()},
                    "species": {window: dict(species) for window, species in self.species_trends.items()}
                }
            }
            return stats

    def query_by_species(self, species, limit=10):
        """Consulta insectos por especie"""
        with self.lock:
            if species in self.insects_by_species:
                insects = list(self.insects_by_species[species].values())
                return insects[:limit] if limit else insects
            return []

    def query_by_habitat_and_event(self, habitat, event, limit=10):
        """Consulta insectos por hábitat y evento"""
        with self.lock:
            results = []
            if habitat in self.insects_by_habitat:
                for insect_data in self.insects_by_habitat[habitat].values():
                    if insect_data["event"] == event:
                        results.append(insect_data)
                        if limit and len(results) >= limit:
                            break
            return results

    def get_insects_in_time_window(self, window):
        with self.lock:
            if window not in self.time_windows_data:
                raise ValueError("Ventana no válida. Usar: '1min', '2min', '5min'")
            else:
                return self.time_windows_data[window]

# Crear el almacén de datos
data_store = InsectDataStore()

# Socket para comunicación entre procesos
SOCKET_PATH = "/tmp/insect_query_socket"

# Asegurarse de que el socket no exista previamente
try:
    os.unlink(SOCKET_PATH)
except OSError:
    if os.path.exists(SOCKET_PATH):
        raise

# Función para gestionar consultas remotas
def handle_query_client(conn, data_store):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break

            query = pickle.loads(data)
            response = {"status": "error", "message": "Query not recognized"}

            if query["type"] == "stats":
                response = {"status": "ok", "data": data_store.get_stats()}
            elif query["type"] == "species":
                species = query["params"]["species"]
                limit = query["params"].get("limit", 10)
                insects = data_store.query_by_species(species, limit)
                response = {"status": "ok", "data": insects}
            elif query["type"] == "habitat_event":
                habitat = query["params"]["habitat"]
                event = query["params"]["event"]
                limit = query["params"].get("limit", 10)
                insects = data_store.query_by_habitat_and_event(habitat, event, limit)
                response = {"status": "ok", "data": insects}
            elif query["type"] == "bloom_filter":
                window = query["params"]["window"]
                data = data_store.get_insects_in_time_window(window)
                response = {"status": "ok", "data": data}

            conn.sendall(pickle.dumps(response))
    except Exception as e:
        print(f"Error en manejo de cliente: {e}")
    finally:
        conn.close()


# Función para el servidor de consultas
def query_server(data_store):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(SOCKET_PATH)
    sock.listen(5)
    print(f"🔌 Servidor de consultas iniciado en {SOCKET_PATH}")

    try:
        while True:
            conn, addr = sock.accept()
            thread = threading.Thread(target=handle_query_client, args=(conn, data_store))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        print("🛑 Cerrando servidor de consultas...")
    finally:
        sock.close()
        try:
            os.unlink(SOCKET_PATH)
        except OSError:
            pass


# Función para procesar los mensajes de Kafka
def process_kafka_messages(data_store):
    consumer = Consumer(conf)
    consumer.subscribe(['insect-events'])

    # Contador para control de flujo
    message_count = 0
    last_cleanup_time = time.time()
    cleanup_interval = 120  # Segundos entre limpiezas

    try:
        while True:
            msg = consumer.poll(0.1)  # Poll más rápido para mensajes de alto volumen

            # Limpiar datos viejos periódicamente
            if time.time() - last_cleanup_time > cleanup_interval:
                removed = data_store.clean_old_data()
                print(f"🧹 Limpieza realizada: {removed} registros antiguos eliminados")
                last_cleanup_time = time.time()

            if msg is None:
                continue

            if msg.error():
                print(f"Error de consumidor: {msg.error()}")
                continue

            try:
                message_count += 1
                data = json.loads(msg.value().decode('utf-8'))

                # Añadir al almacén de datos
                data_store.add_insect(data)

                # Solo mostrar cada 1000 mensajes para no saturar la terminal
                if message_count % 10 == 0:
                    print(
                        f"🔄 Procesados {message_count} mensajes. Último: {data['insect']['species']} ({data['event']})")

                    # Mostrar algunas estadísticas de las ventanas de tiempo
                    stats = data_store.get_stats()
                    print(f"📊 Últimos minuto: {sum(stats['time_windows']['1min'].values())} eventos")
                    # print(f"📊 Últimos 5 minutos: {sum(stats['time_windows']['5min'].values())} eventos")

            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrupción por el usuario. Cerrando consumer...")
    finally:
        consumer.close()


# Iniciar hilos para procesamiento paralelo
if __name__ == "__main__":
    # Hilo para el servidor de consultas
    query_thread = threading.Thread(target=query_server, args=(data_store,))
    query_thread.daemon = True
    query_thread.start()

    # Hilo para procesamiento Kafka en el hilo principal
    process_kafka_messages(data_store)