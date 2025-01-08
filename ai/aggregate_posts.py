from pathlib import Path
import logging
from tools.genGhostPost import generate_ghost_post
import sys
import markdown2
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('post_aggregation.log')
    ]
)

logger = logging.getLogger(__name__)

def read_markdown_file(file_path):
    """Read and return the contents of a markdown file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

def aggregate_location_posts(location_dir):
    """
    Aggregate all markdown files in a location directory into a single post.
    Returns a tuple of (title, content)
    """
    if not location_dir.is_dir():
        logger.error(f"Directory not found: {location_dir}")
        return None, None

    # Determine location type and name from directory
    location_name = location_dir.name
    is_citywide = location_name == 'citywide'
    title = "SF Citywide Analysis" if is_citywide else f"SF District {location_name.split('_')[1]} Analysis"

    # Add CSS styles for the content
    css_styles = """
    <style>
        .table {
            width: 100%;
            margin-bottom: 1rem;
            border-collapse: collapse;
        }
        .table-bordered {
            border: 1px solid #dee2e6;
        }
        .table th,
        .table td {
            padding: 0.75rem;
            border: 1px solid #dee2e6;
        }
        .code-block {
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 4px;
            overflow-x: auto;
        }
        img {
            max-width: 100%;
            height: auto;
            margin: 1rem 0;
        }
        ul, ol {
            padding-left: 2rem;
            margin-bottom: 1rem;
        }
        blockquote {
            border-left: 4px solid #dee2e6;
            padding-left: 1rem;
            margin-left: 0;
            color: #6c757d;
        }
    </style>
    """

    # Initialize content with CSS and header
    content = f"{css_styles}<h1>{title}</h1>\n\n"

    # Category headers mapping
    category_headers = {
        "public_safety": "Public Safety",
        "economy_and_community": "Economy and Community",
        "health_and_social_services": "Health and Social Services",
        "housing": "Housing"
    }

    # Process each markdown file in sorted order
    for category in category_headers.keys():
        md_file = location_dir / f"{category}_post.md"
        if md_file.exists():
            logger.info(f"Processing {md_file}")
            file_content = read_markdown_file(md_file)
            if file_content:
                # Add category header
                content += f"<h2>{category_headers[category]}</h2>\n\n"
                # Convert markdown content to HTML (basic conversion)
                html_content = convert_md_to_html(file_content)
                content += html_content + "\n\n"

    return title, content

def convert_md_to_html(markdown_content):
    """
    Convert markdown content to HTML using markdown2 library.
    Includes support for tables, fenced-code-blocks, header-ids, etc.
    """
    # Configure markdown2 with extras for enhanced conversion
    extras = [
        "fenced-code-blocks",  # Support ```code blocks```
        "tables",              # Support markdown tables
        "header-ids",          # Add IDs to headers
        "break-on-newline",    # Convert newlines to <br>
        "cuddled-lists",       # Allow lists without preceding newline
        "markdown-in-html",    # Parse markdown inside HTML blocks
        "code-friendly",       # Disable _ and __ for code
        "numbering",           # Support numbered lists
        "smarty-pants",        # Smart quotes, dashes, etc.
        "target-blank-links"   # Add target="_blank" to external links
    ]

    # Define link patterns separately
    link_patterns = [
        # Convert URLs to links
        (re.compile(r'((([A-Za-z]{3,9}:(?:\/\/)?)(?:[\-;:&=\+\$,\w]+@)?[A-Za-z0-9\.\-]+|(?:www\.|[\-;:&=\+\$,\w]+@)[A-Za-z0-9\.\-]+)((?:\/[\+~%\/\.\w\-_]*)?\??(?:[\-\+=&;%@\.\w_]*)#?(?:[\.\!\/\\\w]*))?)'),
         r'\1'),
        # Convert data.sfgov.org URLs
        (re.compile(r'(https?://data\.sfgov\.org/\S+)'),
         r'\1'),
        # Convert Socrata API URLs
        (re.compile(r'(https?://[^/]+/resource/[^/]+\.json\S*)'),
         r'\1')
    ]
    
    try:
        # Convert markdown to HTML with link patterns
        html_content = markdown2.markdown(
            markdown_content,
            extras=extras,
            link_patterns=link_patterns
        )
        
        # Additional processing for image tags to ensure proper styling
        html_content = html_content.replace(
            '<img src="',
            '<img style="max-width: 100%; height: auto;" src="'
        )
        
        # Ensure lists are properly spaced
        html_content = html_content.replace('</ul>\n<ul>', '\n')
        html_content = html_content.replace('</ol>\n<ol>', '\n')
        
        # Add CSS classes for better styling
        html_content = html_content.replace('<table>', '<table class="table table-bordered">')
        html_content = html_content.replace(
            '<pre><code>',
            '<pre class="code-block"><code>'
        )
        
        return html_content
        
    except Exception as e:
        logger.error(f"Error converting markdown to HTML: {e}")
        # Fallback to basic conversion if markdown2 fails
        return f"<p>{markdown_content}</p>"

def process_all_locations():
    """Process all location directories and generate Ghost posts."""
    posts_dir = Path("generated_posts")
    if not posts_dir.exists():
        logger.error("Generated posts directory not found")
        return

    # Process each location directory
    for location_dir in posts_dir.iterdir():
        if location_dir.is_dir():
            logger.info(f"Processing location: {location_dir}")
            title, content = aggregate_location_posts(location_dir)
            
            if title and content:
                logger.info(f"Generating Ghost post for {title}")
                try:
                    # Create Ghost post
                    post_url = generate_ghost_post(
                        context_variables={},
                        content=content,
                        title=title
                    )
                    logger.info(f"Successfully created post: {post_url}")
                except Exception as e:
                    logger.error(f"Error creating Ghost post for {title}: {e}")

if __name__ == "__main__":
    logger.info("Starting post aggregation process")
    process_all_locations()
    logger.info("Completed post aggregation process") 