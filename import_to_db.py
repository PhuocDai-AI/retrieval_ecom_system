from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
import json
import uuid
from typing import List
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="./apps/.env")
QDRANT_API_KEY = os.getenv('QDRANT_API_KEY')
if not QDRANT_API_KEY:
    print("QDRANT_API_KEY not provided, using default value.")

# Qdrant connection configuration
QDRANT_HOST = "localhost"  # Replace with your Qdrant host
QDRANT_PORT = 6333  # Default Qdrant port
COLLECTION_NAME = "e_com"
VECTOR_SIZE = 640  # Adjust based on your vector size
BATCH_SIZE = 100  # Number of samples per batch

def load_data_from_json(file_path="test_updated.json") -> List[dict]:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}.")
        return []

def initialize_qdrant_client() -> QdrantClient:
    client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT,
        api_key=QDRANT_API_KEY,
        https=False
    )
    return client

def create_collection(client: QdrantClient) -> None:
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=VECTOR_SIZE,
            distance=Distance.COSINE
        )
    )
    print(f"Collection {COLLECTION_NAME} created successfully.")

def prepare_points(data: List[dict]) -> List[PointStruct]:
    points = []
    for item in data:
        # Ensure all required fields are present
        required_fields = ["id", "name", "price", "original_price", "description", 
                         "short_url", "discount_percentage", "image_base64", "vector"]
        if not all(field in item for field in required_fields):
            print(f"Skipping item with missing fields: {item.get('id', 'unknown')}")
            continue
            
        payload = {
            "id": item["id"],
            "name": item["name"],
            "price": item["price"],
            "original_price": item["original_price"],
            "description": item["description"],
            "short_url": item["short_url"],
            "discount_percentage": item["discount_percentage"],
            "image_base64": item["image_base64"]
        }
        point = PointStruct(
            id=str(uuid.uuid4()),  # Generate unique UUID for point ID
            vector=item["vector"],
            payload=payload
        )
        points.append(point)
    return points

def import_data_to_qdrant() -> None:
    # Load data from JSON file
    data = load_data_from_json()
    if not data:
        print("No data to import.")
        return

    # Initialize Qdrant client
    client = initialize_qdrant_client()
    
    # Create collection if it doesn't exist
    create_collection(client)
    
    # Prepare points
    points = prepare_points(data)
    if not points:
        print("No valid points to import.")
        return
    
    # Upsert points in batches
    total_points = len(points)
    for i in range(0, total_points, BATCH_SIZE):
        batch = points[i:i + BATCH_SIZE]
        client.upsert(
            collection_name=COLLECTION_NAME,
            points=batch
        )
        print(f"Successfully imported batch {i // BATCH_SIZE + 1} with {len(batch)} points.")
    
    print(f"Completed: Imported {total_points} points to Qdrant.")

if __name__ == "__main__":
    import_data_to_qdrant()