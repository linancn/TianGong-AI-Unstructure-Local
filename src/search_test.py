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
collection = client.collections.get("tiangong")


## colletion total chunk count
# response = collection.aggregate.over_all(total_count=True)
# print(response.total_count)

hybrid_results = collection.query.hybrid(
    query="税务人员在核定应纳税额时应回避的人员",
    target_vector="content",
    query_properties=["content"],
    alpha=0.3,
    return_metadata=MetadataQuery(score=True, explain_score=True),
    limit=3,
)
print(hybrid_results.objects)

def get_adjacent_chunks_from_weaviate(
    original_search_results, k=1, k_before=None, k_after=None
):
    if k_before is None:
        k_before = k
    if k_after is None:
        k_after = k

    # 用于存储每个文档的 chunk
    doc_chunks = defaultdict(list)
    added_chunks = set()

    # 解析 original_search_results 中每个 chunk
    for result in original_search_results:
        content = result.properties["content"]
        doc_chunk_id = result.properties["doc_chunk_id"]
        doc_uuid, chunk_id_str = doc_chunk_id.split("_")
        chunk_id = int(chunk_id_str)

        # 添加当前 chunk 到文档的 chunk 列表中，如果 (doc_uuid, chunk_id) 不存在重复
        if (doc_uuid, chunk_id) not in added_chunks:
            doc_chunks[doc_uuid].append((chunk_id, content))
            added_chunks.add((doc_uuid, chunk_id))

        # 检索前 k 个相邻 chunk
        if k_before:
            for i in range(1, k_before + 1):
                if chunk_id - i >= 0 and (doc_uuid, chunk_id - i) not in added_chunks:
                    before_response = collection.query.fetch_objects(
                        filters=Filter.by_property("doc_chunk_id").equal(
                            f"{doc_uuid}_{chunk_id - i}"
                        ),
                    )
                    if before_response.objects:
                        before_chunk = before_response.objects[0].properties["content"]
                        doc_chunks[doc_uuid].append((chunk_id - i, before_chunk))
                        added_chunks.add((doc_uuid, chunk_id - i))
        
        # 检索后 k 个相邻 chunk
        total_chunk_count = collection.aggregate.over_all(
            total_count=True,
            filters=Filter.by_property("doc_chunk_id").like(f"{doc_uuid}*"),
        ).total_count
        if k_after:
            for i in range(1, k_after + 1):
                if (
                    chunk_id + i <= total_chunk_count
                    and (doc_uuid, chunk_id + i) not in added_chunks
                ):
                    after_response = collection.query.fetch_objects(
                        filters=Filter.by_property("doc_chunk_id").equal(
                            f"{doc_uuid}_{chunk_id + i}"
                        ),
                    )
                    if after_response.objects:
                        after_chunk = after_response.objects[0].properties["content"]
                        doc_chunks[doc_uuid].append((chunk_id + i, after_chunk))
                        added_chunks.add((doc_uuid, chunk_id + i))

    # 对每个 doc 的 chunk 列表按 chunk_id 进行排序，并确保 chunk_id 和 content 保持配对
    for doc_uuid in doc_chunks:
        doc_chunks[doc_uuid].sort(key=lambda x: x[0])

    return doc_chunks


aa = get_adjacent_chunks_from_weaviate(hybrid_results.objects, k=4)

print(aa)   


# collection = client.collections.get("Audit")
# response = collection.query.bm25(
#     query="税务人员",
#     limit=3
# )

# for o in response.objects:
#     print(o.properties)


# for o in hybrid_results.objects:
#     print(o.properties)
#     print(o.metadata.score, o.metadata.explain_score)


# vector_results = collection.query.near_text(
#     query="税务人员在核定应纳税额时应回避的人员",
#     limit=10,
#     return_metadata=MetadataQuery(distance=True)
# )


# for o in vector_results.objects:
#     print(o.properties)
#     print(o.metadata.distance)


client.close()
