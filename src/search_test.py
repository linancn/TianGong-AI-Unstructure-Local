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
    # 用于存储每个文档的 source（优先取最早出现结果的 source）
    doc_sources = {}
    added_chunks = set()

    # 解析 original_search_results 中每个 chunk
    for result in original_search_results:
        properties = result.properties
        content = properties["content"]
        doc_chunk_id = properties["doc_chunk_id"]
        doc_uuid, chunk_id_str = doc_chunk_id.split("_")
        chunk_id = int(chunk_id_str)
        
        if doc_uuid not in doc_sources and "source" in properties:
            doc_sources[doc_uuid] = properties["source"]

        # 添加当前 chunk 到文档的 chunk 列表中
        if (doc_uuid, chunk_id) not in added_chunks:
            doc_chunks[doc_uuid].append((chunk_id, content))
            added_chunks.add((doc_uuid, chunk_id))

        # 检索前 k 个相邻 chunk
        if k_before:
            for i in range(1, k_before + 1):
                target_chunk = chunk_id - i
                if target_chunk >= 0 and (doc_uuid, target_chunk) not in added_chunks:
                    before_response = collection.query.fetch_objects(
                        filters=Filter.by_property("doc_chunk_id").equal(
                            f"{doc_uuid}_{target_chunk}"
                        ),
                    )
                    if before_response.objects:
                        before_obj = before_response.objects[0]
                        before_content = before_obj.properties["content"]
                        # 如未记录 source，则从相邻 chunk 中获取 source（假设同一 doc 的 source 一致）
                        if doc_uuid not in doc_sources and "source" in before_obj.properties:
                            doc_sources[doc_uuid] = before_obj.properties["source"]
                        doc_chunks[doc_uuid].append((target_chunk, before_content))
                        added_chunks.add((doc_uuid, target_chunk))
        
        # 检索后 k 个相邻 chunk
        total_chunk_count = collection.aggregate.over_all(
            total_count=True,
            filters=Filter.by_property("doc_chunk_id").like(f"{doc_uuid}*"),
        ).total_count
        if k_after:
            for i in range(1, k_after + 1):
                target_chunk = chunk_id + i
                if target_chunk <= total_chunk_count and (doc_uuid, target_chunk) not in added_chunks:
                    after_response = collection.query.fetch_objects(
                        filters=Filter.by_property("doc_chunk_id").equal(
                            f"{doc_uuid}_{target_chunk}"
                        ),
                    )
                    if after_response.objects:
                        after_obj = after_response.objects[0]
                        after_content = after_obj.properties["content"]
                        if doc_uuid not in doc_sources and "source" in after_obj.properties:
                            doc_sources[doc_uuid] = after_obj.properties["source"]
                        doc_chunks[doc_uuid].append((target_chunk, after_content))
                        added_chunks.add((doc_uuid, target_chunk))

    # 对每个 doc 的 chunk 列表按 chunk_id 进行排序
    for doc_uuid in doc_chunks:
        doc_chunks[doc_uuid].sort(key=lambda x: x[0])

    # 合并同一 doc 的 chunk content，并构造返回的列表
    docs_list = []
    for doc_uuid, chunks in doc_chunks.items():
        # 这里按顺序拼接 chunk 的内容，可根据需要添加空格或换行符
        combined_content = "".join(chunk_content for _, chunk_content in chunks)
        source = doc_sources.get(doc_uuid, "")
        docs_list.append({"content": combined_content, "source": source})
    
    return docs_list


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
