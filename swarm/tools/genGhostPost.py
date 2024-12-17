from ghost import GhostAdminAPI
import plotly.graph_objects as go
import json
import base64
from io import BytesIO

def generate_chart_html(data, chart_title):
    """Generate a Plotly chart"""
    # This is a simple example - modify according to your data structure
    fig = go.Figure(data=[
        go.Scatter(x=data['dates'], y=data['values'], mode='lines+markers')
    ])
    
    fig.update_layout(
        title=chart_title,
        xaxis_title="Date",
        yaxis_title="Value",
        showlegend=False
    )
    
    return fig.to_html(include_plotlyjs=True, full_html=True)

def generate_ghost_post(data, ghost_config):
    """
    Generate a Ghost blog post with a chart
    
    Args:
        data (dict): Data for the chart (should contain 'dates' and 'values')
        ghost_config (dict): Ghost configuration with 'url', 'api_key'
    """
    # Initialize Ghost API client
    api = GhostAdminAPI(
        url=ghost_config['url'],
        key=ghost_config['api_key'],
        version="v5"
    )
    
    try:
        # Generate chart title
        chart_title = "My Analysis Chart"
        
        # Upload image to Ghost
        # image_response = api.images.upload(
        #     file_object=BytesIO(image_bytes),
        #     file_name='chart.png'
        # )
        
        # Create mobiledoc structure for the post
        mobiledoc = {
            "version": "0.3.1",
            "atoms": [],
            "cards": [
                ["image", {
                    "src": "https://example.com/placeholder-chart.png",
                    "caption": chart_title
                }]
            ],
            "markups": [],
            "sections": [
                [10, 0],  # Reference to the image card
                [1, "p", [
                    [0, [], 0, "Chart description goes here"]
                ]]
            ]
        }
        
        # Create the post
        post = api.posts.create({
            "title": chart_title,
            "mobiledoc": json.dumps(mobiledoc),
            "status": "draft"
        })
        
        print(f"Post created successfully: {post['url']}")
        
    except Exception as e:
        print(f"Error generating post: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Example data structure
    sample_data = {
        "dates": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "values": [10, 15, 12]
    }
    
    ghost_config = {
        "url": "https://your-ghost-blog.com",
        "api_key": "your_admin_api_key"
    }
    
    generate_ghost_post(sample_data, ghost_config)