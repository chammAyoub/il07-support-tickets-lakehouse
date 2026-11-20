
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False
    ).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

fichier = "data/tickets.jsonl"

nombre_tickets = 26872

print("Début de l'envoi des tickets vers Kafka...")

with open(fichier, "r", encoding="utf-8") as f:
    for i, ligne in enumerate(f):
        if i >= nombre_tickets:
            break

        ticket = json.loads(ligne)

        producer.send(
            "tickets",
            key=ticket["ticket_id"],
            value=ticket
        ).get(timeout=10)

        print(f"Ticket envoyé : {ticket['ticket_id']}")
        time.sleep(0.01)

producer.flush()
producer.close()

print("Envoi terminé avec succès !")
