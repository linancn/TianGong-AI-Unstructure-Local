import os
import weaviate
from collections import defaultdict
from weaviate.classes.query import MetadataQuery, Filter
from weaviate.classes.init import Auth

# Best practice: store your credentials in environment variables
http_host = os.environ["WEAVIATE_HTTP_HOST"]
http_port = os.environ["WEAVIATE_HTTP_PORT"]
grpc_host = os.environ["WEAVIATE_GRPC_HOST"]
grpc_port = os.environ["WEAVIATE_GRPC_PORT"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

client = weaviate.connect_to_custom(
    http_host=http_host,  # Hostname for the HTTP API connection
    http_port=http_port,  # Default is 80, WCD uses 443
    http_secure=False,  # Whether to use https (secure) for the HTTP API connection
    grpc_host=grpc_host,  # Hostname for the gRPC API connection
    grpc_port=grpc_port,  # Default is 50051, WCD uses 443
    grpc_secure=False,  # Whether to use a secure channel for the gRPC API connection
    auth_credentials=Auth.api_key(weaviate_api_key),  # API key for authentication
)
collection = client.collections.get("audit")


def hybrid_search_with_context(query, topK=3, extK=0):
    """
    Perform hybrid search and retrieve adjacent chunks if needed.
    
    Args:
        query (str): The search query
        topK (int): Number of top results to return (default: 3)
        extK (int): Number of chunks to extend context in both directions (default: 0)
    
    Returns:
        list: List of documents with context-extended content
    """
    # Perform hybrid search
    hybrid_results = collection.query.hybrid(
        query=query,
        target_vector="content",
        query_properties=["content"],
        alpha=0.3,
        return_metadata=MetadataQuery(score=True, explain_score=True),
        limit=topK,
    )
    
    # Return simplified results if no context extension required
    if extK <= 0:
        return [{"content": obj.properties["content"], 
                 "source": obj.properties.get("source", "")} 
                for obj in hybrid_results.objects]
    
    # For context extension, prepare data structures
    doc_chunks = defaultdict(list)
    doc_sources = {}
    added_chunks = set()
    chunks_to_fetch = []

    # Process search results
    for result in hybrid_results.objects:
        properties = result.properties
        content = properties["content"]
        doc_chunk_id = properties["doc_chunk_id"]
        doc_uuid, chunk_id_str = doc_chunk_id.split("_")
        chunk_id = int(chunk_id_str)
        
        # Track document source
        if "source" in properties:
            doc_sources[doc_uuid] = properties["source"]

        # Add current chunk
        doc_chunks[doc_uuid].append((chunk_id, content))
        added_chunks.add((doc_uuid, chunk_id))

        # Get total chunk count for this document
        total_chunk_count = collection.aggregate.over_all(
            total_count=True,
            filters=Filter.by_property("doc_chunk_id").like(f"{doc_uuid}*"),
        ).total_count
        
        # Identify adjacent chunks to fetch
        for i in range(1, extK + 1):
            # Previous chunks
            target_chunk = chunk_id - i
            if target_chunk >= 0 and (doc_uuid, target_chunk) not in added_chunks:
                chunks_to_fetch.append((doc_uuid, target_chunk))
                added_chunks.add((doc_uuid, target_chunk))
            
            # Next chunks
            target_chunk = chunk_id + i
            if target_chunk < total_chunk_count and (doc_uuid, target_chunk) not in added_chunks:
                chunks_to_fetch.append((doc_uuid, target_chunk))
                added_chunks.add((doc_uuid, target_chunk))
    
    # Batch fetch all adjacent chunks
    for doc_uuid, chunk_id in chunks_to_fetch:
        response = collection.query.fetch_objects(
            filters=Filter.by_property("doc_chunk_id").equal(f"{doc_uuid}_{chunk_id}"),
        )
        if response.objects:
            obj = response.objects[0]
            content = obj.properties["content"]
            if "source" in obj.properties and doc_uuid not in doc_sources:
                doc_sources[doc_uuid] = obj.properties["source"]
            doc_chunks[doc_uuid].append((chunk_id, content))
    
    # Process results into final format
    docs_list = []
    for doc_uuid, chunks in doc_chunks.items():
        # Sort chunks by chunk_id to ensure correct order
        chunks.sort(key=lambda x: x[0])
        # Combine chunks into a single text
        combined_content = "".join(chunk_content for _, chunk_content in chunks)
        source = doc_sources.get(doc_uuid, "")
        docs_list.append({"content": combined_content, "source": source})
    
    return docs_list

# Example usage of the new combined function
search_results = hybrid_search_with_context(
    query="税务人员在核定应纳税额时应回避的人员", 
    topK=3,
    extK=1
)

print(search_results)

client.close()