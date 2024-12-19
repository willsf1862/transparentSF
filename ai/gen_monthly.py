from tools.genGhostPost import generate_ghost_post
from dotenv import load_dotenv
import os
import dotenv


load_dotenv()

# Example data for the chart
data  = {
        "version": "0.3.1",
        "atoms": [],
        "cards": [
            ["html", {
                "html": "this is a test"
            }]
        ],
        "markups": [],
        "sections": [
            [10, 0],
            [1, "p", [[0, [], 0, "Chart description goes here"]]]
        ]
    }

# Example Ghost configuration
ghost_config = {
    'url': os.getenv('GHOST_URL'),
    'api_key': os.getenv('GHOST_ADMIN_API_KEY')
}

# Generate the Ghost post
generate_ghost_post(data, ghost_config,"http://localhost:3000")
