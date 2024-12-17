import os
import sys
import json
import re
import time
import uuid
from urllib.parse import urlparse
from openai import OpenAI
import qdrant_client
from qdrant_client.http import models as rest
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import glob

# Load environment variables
load_dotenv()

# ------------------------------
# Configuration and Setup
# ------------------------------

openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OpenAI API key not found in environment variables.")

client = OpenAI(api_key=openai_api_key)

GPT_MODEL = 'gpt-4'
EMBEDDING_MODEL = "text-embedding-3-large"

qdrant = qdrant_client.QdrantClient(host='localhost', port=6333)  # Adjust if needed

# This directory structure follows the assumption from previous code:
# script_dir/data/datasets/
script_dir = os.path.dirname(os.path.abspath(__file__))
output_folder = os.path.join(script_dir, 'output')

# ------------------------------
# Utility Functions
# ------------------------------

def sanitize_filename(filename):
    """Sanitize filename by removing invalid chars."""
    return re.sub(r'[<>:"/\\|?*]', '', filename).strip()

def get_embedding(text, retries=3, delay=5):
    """Generate embeddings for text."""
    for attempt in range(1, retries + 1):
        try:
            response = client.embeddings.create(model=EMBEDDING_MODEL, input=text)
            embedding = response.data[0].embedding
            return embedding
        except Exception as e:
            print(f"Attempt {attempt} - Error generating embedding: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    print("Max retries reached. Skipping text.")
    return None

def determine_last_completed_month():
    """Determine the last completed month in YYYY-MM format."""
    today = datetime.utcnow()
    # Move to first of current month, then go back one month
    first_of_current = today.replace(day=1)
    last_month = first_of_current - timedelta(days=1)
    return last_month.strftime("%Y-%m")

def recreate_collection(collection_name, vector_size):
    """Delete if exists and recreate the collection."""
    # Check if collection exists
    try:
        existing = qdrant.get_collection(collection_name)
        if existing:
            # Delete existing
            qdrant.delete_collection(collection_name)
    except:
        # Collection does not exist or something else happened
        pass

    qdrant.recreate_collection(
        collection_name=collection_name,
        vectors_config=rest.VectorParams(
            distance=rest.Distance.COSINE,
            size=vector_size,
        )
    )
    print(f"Collection '{collection_name}' recreated with vector size {vector_size}.")

# ------------------------------
# Main Processing
# ------------------------------

def main():
    # Parse month argument
    if len(sys.argv) > 1:
        month_str = sys.argv[1]
        # Validate format YYYY-MM
        try:
            datetime.strptime(month_str, "%Y-%m")
        except ValueError:
            print("Invalid month format. Use YYYY-MM.")
            sys.exit(1)
    else:
        month_str = determine_last_completed_month()
        print(f"No month provided. Using last completed month: {month_str}")

    # Month directory
    month_dir = os.path.join(output_folder, month_str)
    if not os.path.isdir(month_dir):
        print(f"Month directory not found: {month_dir}")
        sys.exit(1)

    # Gather all .md files in all subdirectories of month_dir
    # Structure: datasets/<month>/<subdir>/*.md
    subdirs = [d for d in os.listdir(month_dir) 
               if os.path.isdir(os.path.join(month_dir, d))]

    if not subdirs:
        print(f"No subdirectories found under {month_dir}. Nothing to process.")
        sys.exit(0)

    # Before processing, determine vector size using a sample embedding
    sample_embedding = get_embedding("Sample text to determine vector size.")
    if not sample_embedding:
        print("Failed to get a sample embedding for vector size determination.")
        sys.exit(1)
    vector_size = len(sample_embedding)

    # Process each subdirectory as a separate collection
    for subdir in subdirs:
        subdir_path = os.path.join(month_dir, subdir)
        md_files = glob.glob(os.path.join(subdir_path, '**', '*.md'), recursive=True)
        if not md_files:
            print(f"No .md files found in {subdir_path}. Skipping.")
            continue

        # Collection name derived from subdirectory name
        collection_name = f"{month_str}_{subdir}"
        # Recreate collection
        recreate_collection(collection_name, vector_size)

        points_to_upsert = []
        for md_file in md_files:
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
            except Exception as e:
                print(f"Error reading {md_file}: {e}")
                continue

            # Create embedding
            embedding = get_embedding(content)
            if not embedding:
                print(f"Failed to generate embedding for file {md_file}. Skipping.")
                continue

            # Payload: we store at least the filename and month
            payload = {
                "filename": os.path.basename(md_file),
                "filepath": md_file,
                "month": month_str,
                "subdirectory": subdir,
                "content": content
            }

            point_id = str(uuid.uuid4())
            point = rest.PointStruct(id=point_id, vector=embedding, payload=payload)
            points_to_upsert.append(point)

        # Upsert all points in a single request if possible
        if points_to_upsert:
            try:
                qdrant.upsert(collection_name=collection_name, points=points_to_upsert)
                print(f"Upserted {len(points_to_upsert)} documents into '{collection_name}'.")
            except Exception as e:
                print(f"Error upserting points into '{collection_name}': {e}")

    print("Ingestion completed.")

if __name__ == '__main__':
    main()
