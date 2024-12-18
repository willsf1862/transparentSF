from bs4 import BeautifulSoup
import plotly.io as pio
import json
import os
import logging
import datetime
import uuid

def extract_chart_from_html(html_file_path: str, chart_index: int = 0) -> str:
    """
    Extract a specific chart from an HTML file and save it as PNG.
    
    Args:
        html_file_path: Path to the HTML file containing the charts
        chart_index: Index of the chart to extract (0-based)
    
    Returns:
        str: Path to the saved PNG file
    """
    try:
        # Read the HTML file
        with open(html_file_path, 'r') as file:
            html_content = file.read()

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all chart divs
        chart_divs = soup.find_all('div', {'class': 'plotly-graph-div'})
        
        if not chart_divs:
            raise ValueError("No charts found in the HTML file")
            
        if chart_index >= len(chart_divs):
            raise ValueError(f"Chart index {chart_index} is out of range. Found {len(chart_divs)} charts.")
        
        # Get the specified chart
        chart_div = chart_divs[chart_index]
        
        # Extract the chart data
        chart_data = json.loads(chart_div['data-plotly'])
        
        # Create a new plotly figure
        fig = pio.from_json(json.dumps(chart_data))
        
        # Generate filename
        script_dir = os.path.dirname(os.path.abspath(__file__))
        static_dir = os.path.join(script_dir, '..', 'static')
        os.makedirs(static_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex
        image_filename = f"extracted_chart_{timestamp}_{unique_id}.png"
        image_path = os.path.join(static_dir, image_filename)
        
        # Save as PNG
        fig.write_image(image_path, engine="kaleido")
        
        return image_path
        
    except Exception as e:
        logging.error(f"Error extracting chart: {str(e)}")
        raise
