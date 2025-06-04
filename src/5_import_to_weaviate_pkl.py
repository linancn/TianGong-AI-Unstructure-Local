from weaviate.classes.config import Configure, Property, DataType, Tokenization
from weaviate.classes.init import Auth
import logging
import os
import pickle
import weaviate
import uuid

from tools.text_to_weaviate import merge_pickle_list, fix_utf8, split_chunks

logging.basicConfig(
    filename="weaviate_aibook.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)


http_host = os.environ["WEAVIATE_HTTP_HOST"]
http_port = os.environ["WEAVIATE_HTTP_PORT"]
grpc_host = os.environ["WEAVIATE_GRPC_HOST"]
grpc_port = os.environ["WEAVIATE_GRPC_PORT"]

client = weaviate.connect_to_custom(
    http_host=http_host,  # Hostname for the HTTP API connection
    http_port=http_port,  # Default is 80, WCD uses 443
    http_secure=False,  # Whether to use https (secure) for the HTTP API connection
    grpc_host=grpc_host,  # Hostname for the gRPC API connection
    grpc_port=grpc_port,  # Default is 50051, WCD uses 443
    grpc_secure=False,  # Whether to use a secure channel for the gRPC API connection
    # auth_credentials=Auth.api_key(weaviate_api_key),  # API key for authentication
)

def split_chunks(doc_id, text_list, source):
    chunks = []
    for index, chunk_text in enumerate(text_list):
        doc_chunk_id = f"{doc_id}_{index}"
        chunks.append({
            "doc_chunk_id": doc_chunk_id,
            "content": chunk_text,
            "source": source,
        })
    return chunks

collection = client.collections.get(name="aibook")

# Set the root directory for pickle files
pickle_root = "temp"

# Recursively walk through all directories and find pickle files
for root, _, files in os.walk(pickle_root):
    for file in files:
        if file.endswith(".pkl"):
            pickle_path = os.path.join(root, file)
            # Calculate the relative path from pickle_root as source
            relative_path = os.path.splitext(os.path.relpath(pickle_path, pickle_root))[0]
            
            # Generate a document ID based on the pickle file
            doc_id = str(uuid.uuid4())
            
            try:
                with open(pickle_path, "rb") as f:
                    text_list = pickle.load(f)
                    text_list = [item for item in text_list if item is not None]
                    text_list = [item["text"] for item in text_list]

                data = merge_pickle_list(text_list)
                data = fix_utf8(data)
                chunks = split_chunks(doc_id, text_list=data, source=relative_path)

            except Exception as e:
                logging.error(f"Error reading pickle file {pickle_path}: {e}")
                continue

            # 分批插入，每批最多200条
            batch_size = 200
            for i in range(0, len(chunks), batch_size):
                batch = chunks[i:i+batch_size]
                collection.data.insert_many(batch)
                logging.info(f"Inserted batch {i//batch_size + 1} for {pickle_path}")

client.close()