from tools.embedding import get_embedding
import qdrant_client
import re

qdrant = qdrant_client.QdrantClient(host="localhost", port=6333)

def query_qdrant(query, collection_name, top_k=4):
    try:
        embedded_query = get_embedding(query)
        if not embedded_query:
            print("Failed to generate embedding for query.")
            return []
        else:
            print(f"Embedding generated for query: type={type(embedded_query)}, length={len(embedded_query)}")
    except Exception as e:
        print(f"Error during embedding generation: {e}")
        return []

    try:
        query_results = qdrant.search(
            collection_name=collection_name,
            query_vector=embedded_query,
            limit=top_k,
            with_payload=True,
        )

        # If there's a last_updated_date field, sort by it descending
        sorted_results = sorted(
            query_results,
            key=lambda x: x.payload.get('last_updated_date', 0),
            reverse=True
        )
        return sorted_results
    except Exception as e:
        print(f"Error querying Qdrant: {e}")
        return []

def query_docs(context_variables, collection_name, query):
    if not collection_name:
        collection_name = "2024-11_Safety"
    print(f"Searching collection '{collection_name}' with query: {query}")
    query_results = query_qdrant(query, collection_name=collection_name)
    
    # Dump raw query results for debugging
    print("Raw query results:")
    try:
        serialized_results = [article.payload for article in query_results]
        print(serialized_results)
    except Exception as e:
        print(f"Error serializing query results: {e}")

    if not query_results:
        print("No results")
        return {"response": "No results found.", "results": []}

    # Branch processing based on collection name
    if collection_name == "SFPublicData":
        # Process as docs
        return process_as_docs(query_results)
    else:
        # Process as content or other type
        return process_as_content(query_results)

def process_as_docs(query_results):
    output = []
    response = "Top results:\n\n"

    for i, article in enumerate(query_results, 1):
        payload = article.payload
        title = payload.get("title", "No Title")
        description = payload.get("description", "")
        endpoint = payload.get("endpoint", "No Endpoint")
        columns = payload.get("columns", {})
        
        column_names = list(columns.keys())
        columns_formatted = ", ".join(column_names) if column_names else "No Columns Available"
        truncated_description = re.sub(
            r"\s+", " ", (description[:100] + "...") if len(description) > 100 else description
        )

        response += (
            f"{i}. **Title:** {title}\n"
            f"   **Description:** {truncated_description}\n"
            f"   **Endpoint:** {endpoint}\n"
            f"   **Columns:** {columns_formatted}\n\n"
        )

        output.append((title, description, endpoint, columns_formatted, column_names))

    return {"response": response, "results": output}

def process_as_content(query_results):
    output = []
    response = "Top text matches:\n\n"

    for i, result in enumerate(query_results, 1):
        payload = result.payload
        content = payload.get("content", "No Content")
        truncated_content = re.sub(
            r"\s+", " ", (content[:100] + "...") if len(content) > 100 else content
        )

        response += (
            f"{i}. **Content:** {truncated_content}\n\n"
        )

        output.append({"content": content})

    return {"response": response, "results": output}
