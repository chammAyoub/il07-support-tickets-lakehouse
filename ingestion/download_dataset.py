
from datasets import load_dataset

print("Téléchargement du dataset Bitext...")

dataset = load_dataset(
    "bitext/Bitext-customer-support-llm-chatbot-training-dataset"
)

print("Dataset chargé avec succès !")
print(dataset)

# Afficher un exemple
print("Premier ticket :")
print(dataset["train"][0])
