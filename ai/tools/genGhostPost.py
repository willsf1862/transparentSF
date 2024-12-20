import requests
import json

node_service_url = "http://localhost:3000"

def generate_ghost_post(context_variables: dict, content, title=None):
    doc_title = title or context_variables.get("doc_title", "My Analysis Chart")

    # Ensure mobiledoc is a JSON string
    if isinstance(content, dict):
        try:
            content = json.dumps(content)  # Convert dictionary to a JSON string
        except (TypeError, ValueError) as e:
            print(f"Error encoding content: {e}")
            return f"Error: Invalid content format: {e}"

    # Validate that content is a string
    if not isinstance(content, str):
        print("Error: content is not a string.")
        return "Error: content must be a string."

    # Send POST request to Node.js service
    try:
        response = requests.post(
            f"{node_service_url}/create-post",
            json={
                "title": doc_title,
                "content": content  # Properly escaped JSON string
            }
        )
        response.raise_for_status()
        print("Post created successfully:", response.json()["post"]["url"])
        return response.json()["post"]["url"]
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return "Error: " + str(e)

if __name__ == "__main__":
    context_variables = {
        "doc_title": "Surprising Crime Statistics for November 2024"
    }
    content = "<p><img src='../static/chart_89b60b30.png' alt='Incident Count by Category for November 2024 compared to historical averages.' /><br>In a surprising development, November 2024 saw a significant reduction in crime rates across San Francisco. This shift marks a pivotal moment for the city with several key data points highlighting notable decreases in various crime categories.</p>"



    title = "Surprising Crime Statistics for November 2024"
    generate_ghost_post(context_variables, content, title)
