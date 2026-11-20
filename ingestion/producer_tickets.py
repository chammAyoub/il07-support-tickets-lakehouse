
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# Tickets de test
tickets = [
    {
        "ticket_id": "T001",
        "texte": "Je ne peux pas me connecter à mon compte",
        "categorie": "account",
        "priorite": "haute",
        "statut": "ouvert"
    },
    {
        "ticket_id": "T002",
        "texte": "Je souhaite demander un remboursement",
        "categorie": "refund",
        "priorite": "moyenne",
        "statut": "ouvert"
    },
    {
        "ticket_id": "T003",
        "texte": "Mon paiement a été refusé",
        "categorie": "payment",
        "priorite": "haute",
        "statut": "ouvert"
    }
]

# Envoi des tickets dans Kafka
for ticket in tickets:
    ticket["date_creation"] = datetime.now(timezone.utc).isoformat()

    producer.send(
        "tickets",
        key=ticket["ticket_id"],
        value=ticket
    ).get(timeout=10)

    print(f"Ticket envoyé : {ticket['ticket_id']}")
    time.sleep(2)

producer.flush()
producer.close()

print("Tous les tickets ont été envoyés avec succès !")
