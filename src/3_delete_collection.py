import os
from dotenv import load_dotenv

import weaviate
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
    # auth_credentials=Auth.api_key(weaviate_api_key),
)

if client.collections.exists(name="aibook"):
    client.collections.delete(name="aibook")
    print(f"Collection deleted")
# client.collections.delete_all()

client.close()
