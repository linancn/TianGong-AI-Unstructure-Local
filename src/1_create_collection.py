import os
from dotenv import load_dotenv

import weaviate
from weaviate.classes.config import Configure, DataType, Property, Tokenization
from weaviate.classes.init import Auth

load_dotenv()

http_host = os.environ["WEAVIATE_HTTP_HOST"]
http_port = os.environ["WEAVIATE_HTTP_PORT"]
grpc_host = os.environ["WEAVIATE_GRPC_HOST"]
grpc_port = os.environ["WEAVIATE_GRPC_PORT"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

client = weaviate.connect_to_custom(
    http_host=http_host,
    http_port=http_port,
    http_secure=False,
    grpc_host=grpc_host,
    grpc_port=grpc_port,
    grpc_secure=False,
    auth_credentials=Auth.api_key(weaviate_api_key),
)

if not client.collections.exists(name="aibook"):
    collection = client.collections.create(
        name="aibook",
        properties=[
            Property(
                name="content",
                data_type=DataType.TEXT,
                vectorize_property_name=True,
                tokenization=Tokenization.GSE,
            ),
            Property(
                name="source",
                data_type=DataType.TEXT,
                tokenization=Tokenization.GSE,
            ),
            Property(
                name="doc_chunk_id",
                data_type=DataType.TEXT,
            ),
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_transformers(
                name="content", source_properties=["content"]
            ),
        ],
    )

print(f"Collection created: {collection}")


client.close()
