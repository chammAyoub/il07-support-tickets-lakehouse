
from datasets import load_dataset
from datetime import datetime, timezone
import json
import os

print("Chargement du dataset Bitext...")

dataset = load_dataset(
    "bitext/Bitext-customer-support-llm-chatbot-training-dataset",
    split="train"
)

# Dossier de sortie
os.makedirs("data", exist_ok=True)

output_file = "data/tickets.jsonl"

with open(output_file, "w", encoding="utf-8") as f:
    for i, row in enumerate(dataset):
        ticket = {
            "ticket_id": f"T{i+1:06d}",
            "texte": row["instruction"],
            "categorie": row["category"],
            "intent": row["intent"],
            "priorite": "moyenne",
            "statut": "ouvert",
            "date_creation": datetime.now(
                timezone.utc
            ).isoformat()
        }

        f.write(
            json.dumps(ticket, ensure_ascii=False) + "\n"
        )

print(f"{len(dataset)} tickets préparés !")
print(f"Fichier créé : {output_file}")
