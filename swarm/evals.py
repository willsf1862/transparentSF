import os
import json
from datetime import datetime
from swarm import Swarm
from webSingle import analyst_agent
import pytest
from dotenv import load_dotenv
load_dotenv()

client = Swarm()
agent = analyst_agent

# Create logs subfolder if it doesn't exist
log_folder = 'logs'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Generate a unique log filename for the session
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"{log_folder}/session_{timestamp}.log"

def run_and_get_tool_calls(agent, initial_query, max_turns=5):
    messages = []
    collected_tool_calls = []

    # Start the conversation with the initial user query
    user_message = {"role": "user", "content": initial_query}
    messages.append(user_message)

    # Log the initial user query
    with open(log_filename, 'a') as log_file:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "message": user_message,
        }
        json.dump(log_entry, log_file, indent=2)
        log_file.write("\n\n")

    for turn in range(max_turns):
        # Keep track of the number of messages before the assistant's response
        previous_message_count = len(messages)

        # Assistant responds
        response = client.run(
            agent=agent,
            messages=messages,
            execute_tools=False,
        )

        # Get the updated list of messages from the assistant
        response_messages = response.messages

        # Extract new messages added by the assistant
        new_messages = response_messages[previous_message_count:]

        # If no new messages, end the loop
        if not new_messages:
            break

        # Process and log each new message
        for msg in new_messages:
            messages.append(msg)  # Add to the conversation history

            # Log the message
            with open(log_filename, 'a') as log_file:
                log_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "message": msg,
                }
                json.dump(log_entry, log_file, indent=2)
                log_file.write("\n\n")

            # Collect tool calls if any
            latest_tool_calls = msg.get("tool_calls", [])
            if latest_tool_calls:
                collected_tool_calls.extend(latest_tool_calls)

        # Check if the assistant expects further user input
        assistant_last_message = new_messages[-1]
        if assistant_last_message.get("role") == "assistant":
            content = assistant_last_message.get("content", "")
            if content and content.strip().endswith("?"):
                # Optionally, you can simulate a user response or break
                break  # For testing, we'll end the loop here
            elif not content and not assistant_last_message.get("tool_calls"):
                # No content or tool calls, end the loop
                break

    return collected_tool_calls

@pytest.mark.parametrize(
    "query",
    [
        "Please search for a dataset about Police Misconduct and then set the dataset to select everything from that endpoint for the period of Septmeber 2022 to October 2024.",
        "Please search for a dataset about Business Registrations and then set the dataset to select everything from that endpoint for the period of Septmeber 2022 to October 2024.",
        "Chart out quantity over time, but use the right column names.",
    ],
)
def test_sets_data_when_asked(query):
    tool_calls = run_and_get_tool_calls(analyst_agent, query)
    print(f"Tool Calls for query '{query}': {tool_calls}")
    assert any(call["function"]["name"] == "set_dataset" for call in tool_calls)


@pytest.mark.parametrize(
    "query",
    [
        "Who's the president of the United States?",
        "What is the time right now?",
        "Hi!",
    ],
)
def test_does_not_call_set_dataset_when_not_asked(query):
    tool_calls = run_and_get_tool_calls(analyst_agent, query)

    assert len(tool_calls) == 0
