from tools.embedding import get_embedding
import qdrant_client
import re

qdrant = qdrant_client.QdrantClient(host="localhost", port=6333)
collection_name = "SFPublicData"

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
        # Perform the search without attempting to sort
        query_results = qdrant.search(
            collection_name=collection_name,
            query_vector=embedded_query,
            limit=top_k,
            with_payload=True,
        )

        # Sort the results by 'last_updated_date' in descending order
        sorted_results = sorted(
            query_results,
            key=lambda x: x.payload.get('last_updated_date', 0),
            reverse=True  # Set to True for descending order
        )

        return sorted_results
    except Exception as e:
        print(f"Error querying Qdrant: {e}")
        return []

def query_docs(context_variables, query):
    print(f"Searching knowledge base with query: {query}")
    query_results = query_qdrant(query, collection_name=collection_name)
    
    # Dump raw query results for debugging
    print("Raw query results:")
    try:
        serialized_results = [article.payload for article in query_results]
    except Exception as e:
        print(f"Error serializing query results: {e}")
    
    output = []

    for i, article in enumerate(query_results):
        payload = article.payload
        title = payload.get("title", "No Title")
        description = payload.get("description", "")
        endpoint = payload.get("endpoint", "No Endpoint")
        columns = payload.get("columns", {})
        
        # Extract column names
        column_names = list(columns.keys())
        columns_formatted = ", ".join(column_names) if column_names else "No Columns Available"

        output.append((title, description, endpoint, columns_formatted, column_names))

    if output:
        response = "Top results:\n\n"
        for i, (title, description, endpoint, columns, column_names) in enumerate(output, 1):
            truncated_description = re.sub(
                r"\s+", " ", (description[:100] + "...") if len(description) > 100 else description
            )
            response += (
                f"{i}. **Title:** {title}\n"
                f"   **Description:** {truncated_description}\n"
                f"   **Endpoint:** {endpoint}\n"
                f"   **Columns:** {columns}\n\n"
            )
        print(f"Found {len(output)} relevant articles")
        return {"response": response, "results": output}
    else:
        print("No results")
        return {"response": "No results found.", "results": []}
