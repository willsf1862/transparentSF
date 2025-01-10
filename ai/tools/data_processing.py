import datetime
from urllib.parse import urlparse

def format_columns(columns):
    """Format the columns information into a readable string for embedding."""
    if not columns:
        return ""
    formatted = "Columns Information:\n"
    for col in columns:
        formatted += f"- **{col['name']}** ({col['dataTypeName']}): {col['description']}\n"
    return formatted

def serialize_columns(columns):
    """Serialize the columns into a structured dictionary for payload."""
    if not columns:
        return {}
    serialized = {}
    for col in columns:
        serialized[col['name']] = {
            "fieldName": col.get('fieldName', ''),
            "dataTypeName": col.get('dataTypeName', ''),
            "description": col.get('description', ''),
            "position": col.get('position', ''),
            "renderTypeName": col.get('renderTypeName', ''),
            "tableColumnId": col.get('tableColumnId', '')
        }
    return serialized

def extract_endpoint(url):
    """Extract the Socrata endpoint from the given URL."""
    parsed_url = urlparse(url)
    endpoint = parsed_url.path
    if parsed_url.query:
        endpoint += f"?{parsed_url.query}"
    return endpoint

def convert_to_timestamp(date_str):
    """Convert ISO date string to Unix timestamp."""
    if not date_str:
        return 0
    try:
        dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return int(dt.timestamp())
    except ValueError:
        return 0 