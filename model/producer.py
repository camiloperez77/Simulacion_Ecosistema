from confluent_kafka import Producer
import json, random, time, uuid
from datetime import datetime
from faker import Faker

fake = Faker()

species = ["ant", "bee", "butterfly", "spider"]
roles = ["worker", "queen", "soldier", "scout"]
events = ["birth", "death", "predator attack"]
habitats = ["forest", "field", "garden", "house"]

def generate_insect():
    return {
        "_id": str(uuid.uuid4()),
        "insect": {
            "species": random.choice(species),
            "role": random.choice(roles),
            "age": random.randint(1, 10)
        },
        "event": random.choice(events),
        "eventTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S Z"),
        "location": {
            "habitat": random.choice(habitats),
            "coordinates": {
                "latitude": float(fake.latitude()),   # conversión a float
                "longitude": float(fake.longitude())  # conversión a float
            }
        }
    }

conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambiado a localhost
    'client.id': 'insect-simulator'
}

producer = Producer(conf)

try:
    while True:
        data = generate_insect()

        try:
            json_str = json.dumps(data)
        except TypeError as e:
            print("❌ Error serializando a JSON:", e)
            print("🔎 Datos problemáticos:", data)
            continue  # Salta este mensaje y sigue con el siguiente

        producer.produce('insect-events', value=json_str.encode('utf-8'))
        print("✅ Evento enviado:", data)
        producer.poll(0)  # Libera mensajes encolados
        time.sleep(random.uniform(2, 3))

except KeyboardInterrupt:
    print("🛑 Interrupción por el usuario. Cerrando producer...")

finally:
    producer.flush()  # Asegura envío de mensajes pendientes
