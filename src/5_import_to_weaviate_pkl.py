from weaviate.classes.config import Configure, Property, DataType, Tokenization
from weaviate.classes.init import Auth
import logging
import os
import pickle
import weaviate
import uuid

from tools.text_to_weaviate import merge_pickle_list, fix_utf8, split_chunks, num_tokens_from_string

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

def split_text_by_tokens(text, max_tokens=480):
    if num_tokens_from_string(text) <= max_tokens:
        return [text]
    
    chunks = []
    words = text.split()
    current_chunk = []
    current_tokens = 0
    
    for word in words:
        word_tokens = num_tokens_from_string(word + " ")
        if current_tokens + word_tokens > max_tokens and current_chunk:
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]
            current_tokens = word_tokens
        else:
            current_chunk.append(word)
            current_tokens += word_tokens
    
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

def split_chunks_with_token_limit(doc_id, text_list, source, max_tokens=480):
    all_chunks = []
    chunk_index = 0
    
    for chunk_text in text_list:
        if num_tokens_from_string(chunk_text) > max_tokens:
            sub_chunks = split_text_by_tokens(chunk_text, max_tokens)
            for sub_chunk in sub_chunks:
                doc_chunk_id = f"{doc_id}_{chunk_index}"
                all_chunks.append({
                    "doc_chunk_id": doc_chunk_id,
                    "content": sub_chunk,
                    "source": source,
                })
                chunk_index += 1
        else:
            doc_chunk_id = f"{doc_id}_{chunk_index}"
            all_chunks.append({
                "doc_chunk_id": doc_chunk_id,
                "content": chunk_text,
                "source": source,
            })
            chunk_index += 1
    
    return all_chunks

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
                chunks = split_chunks_with_token_limit(doc_id, text_list=data, source=relative_path, max_tokens=480)

            except Exception as e:
                logging.error(f"Error reading pickle file {pickle_path}: {e}")
                continue

            batch_size = 200
            for i in range(0, len(chunks), batch_size):
                batch = chunks[i:i+batch_size]
                collection.data.insert_many(batch)
                logging.info(f"Inserted batch {i//batch_size + 1}/{(len(chunks)-1)//batch_size + 1} for {pickle_path}")

client.close()