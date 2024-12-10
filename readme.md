
# TransparentSF

A data analysis and visualization platform for San Francisco city data, focusing on crime statistics and governmental transparency.

## Overview

TransparentSF is a web-based application that provides interactive visualizations and analysis of San Francisco city data. It includes features for:

- Crime statistics visualization
- District-wise data analysis
- Anomaly detection in incident reports
- Automated report generation
- Interactive maps showing district-level statistics

## Features

### 1. Interactive Data Visualization
- Crime trend analysis with interactive charts
- District-wise comparison of incidents
- Temporal analysis of crime patterns
- Color-coded maps for geographical insights

### 2. Automated Analysis
- Anomaly detection in crime patterns
- Statistical analysis of incident reports
- Automated report generation
- Ghost blog integration for publishing insights

### 3. Official Dashboard
- Dedicated pages for city officials
- District supervisor-specific analytics
- Mayoral dashboard
- Comparative analysis across districts

## Technology Stack

- **Frontend**: HTML, CSS, JavaScript  
- **Backend**: Node.js, Python  
- **Data Visualization**: Plotly.js, Mapbox  
- **APIs**:  
  - OpenAI API for analysis  
  - Ghost CMS API for content publishing  
  - Mapbox API for geographical visualization

## Installation

**Prerequisites:**  
- Node.js (with npm)  
- Python3 with `pip`  
- Docker (for Qdrant)  

**Steps:**

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/transparentSF.git
   cd transparentSF
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

3. **Install Python dependencies:**
   ```bash
   cd swarm
   pip install -r requirements.txt
   cd ..
   ```

4. **Set up environment variables:**  
   Create a `.env` file in the project root with the following variables:
   ```env
   OPENAI_API_KEY=your_openai_api_key
   GHOST_URL=your_ghost_cms_url
   GHOST_ADMIN_API_KEY=your_ghost_admin_api_key
   MAPBOX_ACCESS_TOKEN=your_mapbox_token
   ```

## Usage

1. **Start the Development Server:**
   ```bash
   npm start
   ```
   Access the application at [http://localhost:3000](http://localhost:3000)

## Project Structure

- `/manual`: Core analysis and generation scripts
- `/swarm`: Python-based data processing and analysis
- `/templates`: HTML templates for report generation
- `/output`: Generated reports and visualizations

## Data Pipeline Steps

1. **Prerequisites:**  
   Ensure all dependencies and the `.env` file are set.

2. **Run Qdrant Server:**  
   Before running any data-processing scripts, make sure Qdrant is up:
   ```bash
   docker run -p 6333:6333 qdrant/qdrant
   ```

3. **Fetch Dataset URLs:**
   ```bash
   cd swarm
   python fetch_dataset_urls.py
   cd ..
   ```
   - Uses Selenium to scrape dataset URLs from [data.sfgov.org](https://data.sfgov.org)
   - Saves all URLs to `data/dataset_urls.json`
   - Handles pagination and removes duplicates

4. **Fetch Metadata:**
   ```bash
   cd swarm
   python fetch_metadata.py
   cd ..
   ```
   - Processes each URL in `dataset_urls.json`
   - Fetches metadata using the Socrata API
   - Saves individual JSON files in `datasets/`
   - Includes column info, descriptions, and update dates

5. **Prepare Data for Vector Database:**
   ```bash
   cd swarm
   python prep_data.py
   cd ..
   ```
   - Processes all JSON files in `datasets/`
   - Generates embeddings for each dataset
   - Stores processed data in Qdrant for searching

## Contributing

1. Fork the repository  
2. Create your feature branch:  
   ```bash
   git checkout -b feature/AmazingFeature
   ```
3. Commit your changes:  
   ```bash
   git commit -m 'Add some AmazingFeature'
   ```
4. Push to the branch:  
   ```bash
   git push origin feature/AmazingFeature
   ```
5. Open a Pull Request

## License

This project is licensed under the ISC License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- San Francisco Police Department for providing the incident report dataset
- OpenAI for AI capabilities
- Mapbox for geographical visualization support

---

**You’re all set!** If anything is unclear, ask more specific questions.