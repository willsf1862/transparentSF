import os
import re
import json
import qdrant_client
from qdrant_client.http import models as rest
from openai import OpenAI
import openai
from urllib.parse import urljoin
from swarm import Swarm, Agent
import time
from tools.anomaly_detection import anomaly_detection
import pandas as pd
from dotenv import load_dotenv
import logging
from tools.data_fetcher import set_dataset
from tools.vector_query import query_docs  # Add this import
from tools.generateTimeSeries import generate_timeseries_chart_html

# Import FastAPI and related modules
from fastapi import FastAPI, Request, Cookie, Depends
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uuid

# ------------------------------
# Configuration and Setup
# ------------------------------

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Serve static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to capture all levels of log messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Initialize connections
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OpenAI API key not found in environment variables.")

# Initialize OpenAI client for direct API calls
openai.api_key = openai_api_key

# Initialize the Swarm client
swarm_client = Swarm()
client = OpenAI()

qdrant = qdrant_client.QdrantClient(host="localhost", port=6333)

# Set embedding model
EMBEDDING_MODEL = "text-embedding-3-large"
#AGENT_MODEL = "gpt-3.5-turbo-16k"
AGENT_MODEL = "gpt-4"

# Set Qdrant collection
collection_name = "SFPublicData"

# Session management (simple in-memory store)
sessions = {}
# Load and combine the data
data_folder = './data'  # Replace with the actual path

# Initialize context_variables with an empty DataFrame
combined_df = {"dataset": pd.DataFrame()}

# Pass the dataset as a context variable
context_variables = {"dataset": combined_df}
# Your functions remain the same, but ensure they use the correct client

def set_dataset_in_context(context_variables, dataset):
    """
    Sets the dataset in the context variables.
    """
    context_variables["dataset"] = dataset

# Modify the load_data function to set the dataset in the context variables
def load_data(data_folder, context_variables):
    all_data = []
    # ...existing code...
    if all_data:
        df = pd.DataFrame(all_data)
    else:
        df = pd.DataFrame()  # Return empty DataFrame if no valid data

    # Set the dataset in the context variables
    set_dataset_in_context(context_variables, df)
    return df

# Ensure the dataset is set in the context variables when initializing
combined_df = {"dataset": load_data(data_folder, context_variables)}

def get_dataset(context_variables, *args, **kwargs):
    """
    Returns the dataset for analysis.
    """
    dataset = context_variables.get("dataset")
    if dataset is not None and not dataset.empty:
        return dataset
    else:
        return {'error': 'Dataset is not available or is empty.'}
        
def get_columns(context_variables, *args, **kwargs):
    """
    Returns the list of columns in the dataset.
    """
    dataset = context_variables.get("dataset")
    if dataset is not None and not dataset.empty:
        return {"columns": dataset.columns.tolist()}
    else:
        return {"error": "Dataset is not available or is empty."}

def get_data_summary(context_variables,*args, **kwargs):
    """
    Returns a statistical summary of the dataset.
    """
    dataset = context_variables.get("dataset")
    if dataset is not None:
        summary = dataset.describe(include='all').to_dict()
        return {"summary": summary}
    else:
        return {"error": "Dataset is not available."}     

def process_and_print_streaming_response(response):
    content = ""
    last_sender = ""

    for chunk in response:
        if "sender" in chunk:
            last_sender = chunk["sender"]

        if "content" in chunk and chunk["content"] is not None:
            if not content and last_sender:
                print(f"\033[94m{last_sender}:\033[0m", end=" ", flush=True)
                last_sender = ""
            print(chunk["content"], end="", flush=True)
            content += chunk["content"]

        if "tool_calls" in chunk and chunk["tool_calls"] is not None:
            for tool_call in chunk["tool_calls"]:
                f = tool_call["function"]
                name = f["name"]
                if not name:
                    continue
                print(f"\033[94m{last_sender}: \033[95m{name}\033[0m()")

        if "delim" in chunk and chunk["delim"] == "end" and content:
            print()  # End of response message
            content = ""

        if "response" in chunk:
            return chunk["response"]

def pretty_print_messages(messages) -> None:
    for message in messages:
        if message["role"] != "assistant":
            continue

        # print agent name in blue
        print(f"\033[94m{message['sender']}\033[0m:", end=" ")

        # print response, if any
        if message["content"]:
            print(message["content"])

        # print tool calls in purple, if any
        tool_calls = message.get("tool_calls") or []
        if len(tool_calls) > 1:
            print()
        for tool_call in tool_calls:
            f = tool_call["function"]
            name, args = f["name"], f["arguments"]
            arg_str = json.dumps(json.loads(args)).replace(":", "=")
            print(f"\033[95m{name}\033[0m({arg_str[1:-1]})")

def transfer_to_anomaly_agent(context_variables, *args, **kwargs):
    """
    Transfers the conversation to the Anomaly Finder Agent.
    """
    return anomaly_finder_agent

def transfer_to_data_agent(context_variables, *args, **kwargs):
    """
    Transfers the conversation to the Data Agent.
    """
    return data_agent


function_mapping = {
    'transfer_to_anomaly_agent': transfer_to_anomaly_agent,
    'transfer_to_data_agent': transfer_to_data_agent,
    'get_dataset': get_dataset,
    'get_columns': get_columns,
    'get_data_summary': get_data_summary,
    'anomaly_detection': anomaly_detection,
    'query_docs': query_docs,
    'set_dataset': set_dataset,
    'generate_category_chart_html': generate_timeseries_chart_html,
}


# Define the anomaly finder agent
anomaly_finder_agent = Agent(
    model=AGENT_MODEL,
    name="Analyst",
     instructions="""
    You are an expert in anomaly detection. Your primary job is to identify anomalies in the data provided.  When the conversation is transferred to you, get the get_data_summary and share it with the user. This will help them understand the data better.

    Use the get_dataset function (without any parameters) to access the dataset.

    Use the get_columns function (without any parameters) to get the list of columns.

    Use the get_data_summary function (without any parameters) to get a statistical summary of the data.
    Use the transfer_to_data_agent function to transfer the conversation back to the Dataset finder.  Only do this is if the user is done with the analysis and wants to find a new dataset.

    Use the anomaly_detection function to perform anomaly detection on the dataset. When calling this function, ensure you correctly pass values for the following parameters:group_field: Specify the column name by which you want to group the data. Use the result of get_columns to decide which column is suitable for grouping (e.g., 'Category'). filter_conditions: Pass in a list of conditions to filter the data. Use this to narrow down the dataset for specific analysis. The format is a list of dictionaries with 'field', 'operator', and 'value' keys.

    Example:

    python
    Copy code
    filter_conditions = [
        {'field': 'Date', 'operator': '>', 'value': '2022-01-01'},
        {'field': 'Region', 'operator': '==', 'value': 'San Francisco'}
    ]
    min_diff: Specify the minimum difference threshold to flag anomalies. This value helps determine how significant a difference must be to be considered an anomaly. If not specified, it defaults to 2. Numeric values only.

    recent_period and comparison_period: Pass these as dictionaries with 'start' and 'end' keys to define the date ranges for recent and comparison periods.

    Dates can be provided as strings in 'YYYY-MM-DD' format or as datetime.date objects. Make sure all the fields you use are actually in the dataset.
    The function needs both a date column name and a numeric column for analysis. 
    Always check the columns before you call anomaly_detection to be sure that the columns you send into the function are present in the dataset.
    Example using strings:

 
    recent_period = {'start': '2022-01-01', 'end': '2022-12-31'}
    comparison_period = {'start': '2021-01-01', 'end': '2021-12-31'}
    Example using datetime.date objects:

  
    recent_period = {'start': date(2022, 1, 1), 'end': date(2022, 12, 31)}
    comparison_period = {'start': date(2021, 1, 1), 'end': date(2021, 12, 31)}
    Ensure the dates are valid and correctly formatted. If you need specific date ranges, consider asking the user for the required dates.

    Complete function call example:

    anomaly_detection(
        group_field='Category',
        filter_conditions=[
            {'field': 'Date', 'operator': '>', 'value': '2022-01-01'}
        ],
        min_diff=2,
        recent_period={
            'start': '2024-09-01',
            'end': '2024-09-30'
        },
        comparison_period={
            'start': '2023-08-01',
            'end': '2023-08-31'
        },
        date_field='DATE',
        numeric_field='COUNT'
    )
    You must provide a recent_period and comparison_period. If the user doesn't provide them, then use the values in the example above. 
    """,
    functions=[get_dataset, get_columns, get_data_summary, anomaly_detection, transfer_to_data_agent, generate_timeseries_chart_html],
    context_variables=context_variables,
    debug=False,
)

# Define the Data Finder Agent with improved instructions
data_agent = Agent(
    model=AGENT_MODEL,
    name="Dataset Finder",
    instructions="""
    You are an expert in San Francisco Open Data and SoQL queries. Your role is to assist the user in finding the right dataset for their needs, help them specify the parameters necessary to generate a good query, and then retrieve the data before transferring the conversation to the Anomaly Finder agent.

    **Your Objectives:**

    1. **Understand the User's Data Needs:**
    - Engage with the user to clarify their requirements.
    - Ask specific questions to gather necessary parameters such as date ranges, fields of interest, filters, and grouping preferences.

    2. **Find the Appropriate Dataset:**
    - Use the `query_docs` function to search for relevant datasets in the knowledge base based on the user's needs.
    - Present the user with a shortlist of the most relevant datasets, including titles, descriptions, endpoints, and available columns.
    - Assist the user in selecting the most suitable dataset.

    3. **Gather Necessary Query Parameters:**
    - Once a dataset is selected, discuss with the user to determine:
        - Specific columns they are interested in.  You can show column names or fieldNames to the user, but only use fieldNames when making the query.
        - Exact date ranges (start and end dates in 'YYYY-MM-DD' format).
        - Any filters or conditions (e.g., categories, regions, statuses).
        - Grouping and aggregation requirements.

    4. **Generate a Complete SoQL Query:**
    - Construct a SoQL query that incorporates all the parameters provided by the user.  Remember to use column functions like `date_trunc_y()` or `date_trunc_ym()` for date grouping.
    - Ensure the query includes:
        - A `SELECT` clause with the desired columns.
        - A `WHERE` clause with exact dates and specified conditions.
        - `GROUP BY`, `ORDER BY`, and `LIMIT` clauses as needed.
    - Validate that all columns used in the query exist in the dataset's schema.
    - Make sure the query is properly URL-encoded when needed.

    5. **Set the Dataset:**
    - Use the `set_dataset` function to retrieve the data and store it in the context variables.
        - The `set_dataset` function requires two parameters:
        - `endpoint`: The 9-character dataset identifier plus the .json extension (e.g., 'wg3w-h783.json').  NOT THE ENTIRE URL.
        - `query`: The complete SoQL query string.
    - Confirm that the data has been successfully retrieved.

    6. **Transfer to the Anomaly Finder Agent:**
    - After successfully setting the dataset, use the `transfer_to_anomaly_agent` function to hand over the conversation for further analysis.

    **Important Guidelines:**

    - **Function Usage:**
      - Use `query_docs(query)` to search for datasets. The `query` parameter is a string describing the data the user is interested in.
      - Use `set_dataset(endpoint, query)` to set the dataset after the user selects one. The `endpoint` is the dataset identifier (e.g., 'abcd-1234.json'), and `query` is the SoQL query string.

    - **User Interaction:**
    - Always communicate clearly and professionally.
    - If you need additional information, ask the user specific questions.
    - Do not overwhelm the user with too many questions at once; prioritize based on what is essential to proceed.

    - **SoQL Query Construction:**
    - Be aware that SoQL differs from SQL in several key ways:
        - SoQL does **not** support the `EXTRACT()` function; use functions like `date_trunc_y()` or date_trunc_ym() for year and month
        - There is no `FROM` statement; the dataset is specified via the endpoint.
        - Use parentheses in `WHERE` clauses to ensure correct logical grouping when combining `AND` and `OR`.
    - Never leave placeholders or blanks in the query; ensure all parameters are filled with exact values.
    - Double-check that all fields and functions used are supported by SoQL and exist in the dataset.

    - **Data Freshness and Relevance:**
    - Prefer datasets that are up-to-date and most relevant to the user's query.
    - Inform the user if the data they are requesting is outdated or has limitations.

    **Example Workflow:**

    1. **Understanding User Needs:**
  
    2. **Finding the Appropriate Dataset:**

    3. **Gathering Query Parameters:**

    4. **Generating the SoQL Query:**

    - *Agent* constructs the query:
        ```soql
        SELECT incident_date, incident_type, severity
        WHERE incident_date >= '2023-08-01' AND incident_date <= '2023-09-30'
        AND (severity = 'High' OR severity = 'Critical')
        ORDER BY incident_date DESC
        ```
    - *Agent* ensures the query is URL-encoded if necessary.

    5. **Setting the Dataset:**

    - *Agent* uses:
        ```python
        set_dataset(
        endpoint='abcd-1234.json',
        query="SELECT incident_date, incident_type, severity WHERE incident_date >= '2023-08-01' AND incident_date <= '2023-09-30' AND (severity = 'High' OR severity = 'Critical') ORDER BY incident_date DESC"
        )
        ```
    - *Agent* confirms the data has been retrieved.

    6. **Transferring to Anomaly Finder Agent:**

    - *Agent* uses `transfer_to_anomaly_agent()`.

    **Remember:**

    - Always validate user inputs for correctness and completeness before constructing the query.
    - Keep the conversation user-focused, aiming to make the process smooth and efficient.
    - Enpoints are 9 characters long and are not the full URL. They are unique identifiers for datasets.
    """,
    functions=[transfer_to_anomaly_agent, query_docs, set_dataset],
    context_variables=context_variables,
    debug=False,
)

# ------------------------------------
# Web Routes and Session Management
# ------------------------------------

def get_session(session_id: str = Cookie(None)):
    if session_id and session_id in sessions:
        return sessions[session_id]
    else:
        # Initialize new session
        new_session_id = str(uuid.uuid4())
        sessions[new_session_id] = {
            "messages": [],
            "agent": data_agent  # Start with data_agent
        }
        return sessions[new_session_id]

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


async def generate_response(user_input, session_data):
    messages = session_data["messages"]
    agent = session_data["agent"]
    context_variables = session_data.get("context_variables") or {}

    # Append user message
    messages.append({"role": "user", "content": user_input})

    # Run the agent
    response_generator = swarm_client.run(
        agent=agent,
        messages=messages,
        context_variables=context_variables,
        stream=True,
        debug=False,
    )

    # Initialize assistant message
    assistant_message = {"role": "assistant", "content": "", "sender": agent.name}

    # Buffer for incomplete tool call arguments
    incomplete_tool_call = None
    current_function_name = None

    for chunk in response_generator:
        # Handle content
        if "content" in chunk and chunk["content"] is not None:
            content_piece = chunk["content"]
            assistant_message["content"] += content_piece

            # Send content as a JSON message to the client
            message = {
                "type": "content",
                "sender": assistant_message["sender"],
                "content": content_piece
            }
            yield json.dumps(message) + "\n"

        # Handle tool calls
        if "tool_calls" in chunk and chunk["tool_calls"] is not None:
            for tool_call in chunk["tool_calls"]:
                function_info = tool_call.get("function")
                if not function_info:
                    continue  # Skip if function info is missing

                # Update current_function_name if 'name' is provided
                if function_info.get("name"):
                    current_function_name = function_info["name"]

                # If we don't have a function name yet, skip processing
                if not current_function_name:
                    continue  # Cannot process without a function name

                arguments_fragment = function_info.get("arguments", "")

                # Initialize or append to incomplete tool call
                if incomplete_tool_call is None or incomplete_tool_call["function_name"] != current_function_name:
                    incomplete_tool_call = {
                        "type": "tool_call",
                        "sender": assistant_message["sender"],
                        "function_name": current_function_name,
                        "arguments": ""
                    }

                incomplete_tool_call["arguments"] += arguments_fragment

                # Try parsing the arguments
                try:
                    arguments_json = json.loads(incomplete_tool_call["arguments"])
                    incomplete_tool_call["arguments"] = arguments_json

                    # Send the complete tool call to the client
                    message = json.dumps(incomplete_tool_call) + "\n"
                    yield message

                    # Log the tool call
                    print(f"Sent tool_call message: {incomplete_tool_call}")
                    logger.debug(f"Processing tool call: {current_function_name}, Arguments fragment: {arguments_fragment}")

                    # Process the function call
                    function_to_call = function_mapping.get(current_function_name)
                    if function_to_call:
                        # Call the function with the arguments
                        function_args = incomplete_tool_call["arguments"]
                        if not isinstance(function_args, dict):
                            function_args = {}
                        result = function_to_call(context_variables, **function_args)

                        # If the function returns an Agent, update session_data["agent"]
                        if isinstance(result, Agent):
                            session_data["agent"] = result
                            assistant_message["sender"] = session_data["agent"].name
                            print(f"Agent transferred to {session_data['agent'].name}")

                        # Ensure the context variables are updated in the session data
                        session_data["context_variables"] = context_variables

                    # Reset the incomplete tool call and current function name
                    incomplete_tool_call = None
                    current_function_name = None
                except json.JSONDecodeError:
                    # Incomplete JSON, wait for more data
                    pass

        # Handle end of message
        if "delim" in chunk and chunk["delim"] == "end":
            # Assistant message is complete
            messages.append(assistant_message)
            # Reset assistant message
            assistant_message = {"role": "assistant", "content": "", "sender": session_data["agent"].name}
@app.post("/api/chat")
async def chat(request: Request, session_id: str = Cookie(None)):
    data = await request.json()
    user_input = data.get("query")

    # Retrieve or create session data
    if session_id is None or session_id not in sessions:
        # Create new session
        session_id = str(uuid.uuid4())
        sessions[session_id] = {
            "messages": [],
            "agent": data_agent  # Start with data_agent
        }

    session_data = sessions[session_id]

    # Create StreamingResponse
    response = StreamingResponse(
        generate_response(user_input, session_data),
        media_type="text/plain"
    )

    # Set the session_id as a cookie
    response.set_cookie(key="session_id", value=session_id)

    return response

# ------------------------------------
# Run the App with Uvicorn
# ------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("webSwarm:app", host="0.0.0.0", port=8000, reload=True)
