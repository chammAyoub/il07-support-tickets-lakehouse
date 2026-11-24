
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False
    ).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)


updates = [
    {
        "ticket_id": "T001",
        "ancien_statut": "ouvert",
        "nouveau_statut": "en_cours"
    },
    {
        "ticket_id": "T002",
        "ancien_statut": "ouvert",
        "nouveau_statut": "resolu"
    },
    {
        "ticket_id": "T003",
        "ancien_statut": "ouvert",
        "nouveau_statut": "en_cours"
    }
]

for update in updates:
    update["date_mise_a_jour"] = datetime.now(
        timezone.utc
    ).isoformat()

    producer.send(
        "mises_a_jour_tickets",
        key=update["ticket_id"],
        value=update
    ).get(timeout=10)

    print(
        f"Ticket {update['ticket_id']} : "
        f"{update['ancien_statut']} -> "
        f"{update['nouveau_statut']}"
    )

    time.sleep(2)

producer.flush()
producer.close()

print("Toutes les mises à jour ont été envoyées !")
