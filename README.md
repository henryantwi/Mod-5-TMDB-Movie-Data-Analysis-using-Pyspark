# 🎬 TMDB Movie Data Analysis Pipeline

A comprehensive data engineering project that extracts, transforms, and analyzes movie data from The Movie Database (TMDB) API using **PySpark**.

![Python](https://img.shields.io/badge/Python-3.14+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-4.1+-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Project Structure](#-project-structure)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Pipeline Architecture](#-pipeline-architecture)
- [KPI Analysis](#-kpi-analysis)
- [Visualizations](#-visualizations)
- [Output Files](#-output-files)
- [Contributing](#-contributing)

## 🎯 Overview

This project implements a complete **ETL (Extract, Transform, Load)** pipeline for movie data analysis. It fetches movie information from the TMDB API, cleans and transforms the data using PySpark, performs KPI analysis, and generates insightful visualizations.

### What You'll Learn

- How to build a production-ready ETL pipeline
- Working with REST APIs (with retry mechanisms)
- Data cleaning and transformation with PySpark
- KPI calculations and business analytics
- Data visualization with Matplotlib

## ✨ Features

### Data Extraction
- 🔄 **Retry Mechanism**: Automatic retries for rate limits and HTTP errors
- 💾 **Data Caching**: Cache raw data to avoid repeated API calls
- 📝 **Comprehensive Logging**: All operations logged to file and console

### Data Transformation
- 🧹 **Data Cleaning**: Handle missing values, invalid data, and duplicates
- 🔄 **Type Conversion**: Proper data types for all columns
- 💱 **Currency Normalization**: Convert budget/revenue to millions USD
- 📊 **JSON Processing**: Extract nested data from complex columns

### Analysis & Visualization
- 📈 **KPI Rankings**: Top movies by revenue, profit, ROI, rating, and popularity
- 🎭 **Franchise Analysis**: Compare franchise vs standalone movie performance
- 🎬 **Director Analysis**: Find most successful directors
- 📊 **5 Visualization Charts**: Professional annotated charts

## 📁 Project Structure

```
tmdb-movie-analysis/
├── 📂 etl/                          # ETL Pipeline Modules
│   ├── __init__.py                  # Package initialization
│   ├── extract.py                   # TMDB API data extraction
│   ├── transform.py                 # Data cleaning & transformation
│   └── load.py                      # Spark DataFrame operations
│
├── 📂 analysis/                     # Analysis Modules
│   ├── __init__.py                  # Package initialization
│   ├── kpi.py                       # KPI calculations & rankings
│   └── visualization.py             # Chart generation
│
├── 📂 utils/                        # Utility Modules
│   ├── __init__.py                  # Package initialization
│   ├── config.py                    # Configuration management
│   └── logger.py                    # Logging setup
│
├── 📂 data/                         # Data Storage (git-ignored)
│   ├── raw/                         # Raw API responses
│   └── processed/                   # Cleaned data (Parquet/CSV)
│
├── 📂 output/                       # Output Files (git-ignored)
│   └── visualizations/              # Generated charts
│
├── 📂 logs/                         # Log Files (git-ignored)
│
├── main.py                          # Pipeline entry point
├── requirements.txt                 # Python dependencies
├── pyproject.toml                   # Project configuration
├── .env.example                     # Environment template
├── .gitignore                       # Git ignore rules
└── README.md                        # This file
```

## 🚀 Installation

### Prerequisites

- Python 3.14 or higher
- Java 8 or 11 (required for PySpark)
- [uv](https://docs.astral.sh/uv/) package manager (recommended)

### Step-by-Step Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd tmdb-movie-analysis
   ```

2. **Create and activate virtual environment**
   ```bash
   # Using uv (recommended)
   uv venv
   
   # Activate on Windows
   .\.venv\Scripts\Activate.ps1
   
   # Activate on Linux/Mac
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   uv add -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit .env and add your TMDB API key
   # TMDB_API_KEY=your_api_key_here
   ```

## ⚙️ Configuration

### Getting a TMDB API Key

1. Create an account at [TMDB](https://www.themoviedb.org/)
2. Go to Settings → API
3. Request an API key (choose "Developer")
4. Copy your API key to the `.env` file

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `TMDB_API_KEY` | Your TMDB API key | Yes |

## 💻 Usage

### Basic Usage

Run the complete pipeline:

```bash
python main.py
```

### Command Line Options

```bash
# Use cached data (skip API extraction)
python main.py --skip-extract

# Skip visualization generation
python main.py --skip-visualization

# Choose output format
python main.py --output-format csv      # CSV only
python main.py --output-format parquet  # Parquet only (default)
python main.py --output-format all      # All formats

# Combine options
python main.py --skip-extract --output-format all
```

### Help

```bash
python main.py --help
```

## 🔧 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TMDB Movie Data Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌─────────┐ │
│  │ EXTRACT  │───▶│  TRANSFORM  │───▶│   LOAD   │───▶│ ANALYZE │ │
│  │          │    │             │    │          │    │         │ │
│  │ TMDB API │    │  PySpark    │    │ DataFrame│    │  KPIs   │ │
│  │ + Retry  │    │  Cleaning   │    │ + Save   │    │ + Viz   │ │
│  └──────────┘    └─────────────┘    └──────────┘    └─────────┘ │
│       │                │                 │               │       │
│       ▼                ▼                 ▼               ▼       │
│  ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌─────────┐ │
│  │ Raw JSON │    │  Cleaned    │    │ Parquet/ │    │  Charts │ │
│  │  Files   │    │    Data     │    │ CSV/JSON │    │  (PNG)  │ │
│  └──────────┘    └─────────────┘    └──────────┘    └─────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Extract**: Fetch movie data from TMDB API with retry mechanism
2. **Transform**: Clean, process JSON columns, handle missing values
3. **Load**: Create PySpark DataFrame and save to multiple formats
4. **Analyze**: Calculate KPIs and generate visualizations

## 📊 KPI Analysis

### Movie Rankings

| Metric | Description |
|--------|-------------|
| Highest Revenue | Top movies by box office earnings |
| Highest Budget | Most expensive productions |
| Highest Profit | Revenue - Budget |
| Highest ROI | Revenue / Budget (min $10M budget) |
| Most Voted | Movies with most user votes |
| Highest Rated | Best ratings (min 10 votes) |
| Most Popular | Highest popularity score |

### Advanced Analysis

- **Franchise vs Standalone**: Compare performance metrics
- **Top Franchises**: Most successful movie franchises
- **Top Directors**: Directors by total revenue and ratings
- **Genre Statistics**: Performance by genre
- **Yearly Trends**: Box office trends over time

### Search Queries

```python
# Search 1: Sci-Fi Action movies with Bruce Willis (sorted by rating)
# Search 2: Uma Thurman + Quentin Tarantino movies (sorted by runtime)
```

## 📈 Visualizations

The pipeline generates 5 professional charts:

1. **Revenue vs Budget** (`revenue_vs_budget.png`)
   - Scatter plot showing the relationship between movie budgets and revenues
   - Point size indicates popularity
   - Break-even line for reference

2. **ROI by Genre** (`roi_by_genre.png`)
   - Horizontal bar chart of mean ROI per genre
   - Color-coded by performance (green=high, red=low)

3. **Popularity vs Rating** (`popularity_vs_rating.png`)
   - Scatter plot revealing quality vs popularity relationship
   - Color intensity shows vote count

4. **Yearly Trends** (`yearly_trends.png`)
   - Line chart showing revenue, budget, and rating trends over time
   - Dual y-axis for financial and rating metrics

5. **Franchise vs Standalone** (`franchise_vs_standalone.png`)
   - Grouped bar chart comparing key metrics
   - Revenue, budget, popularity, and rating comparison

## 📂 Output Files

### Data Files

| File | Format | Location |
|------|--------|----------|
| `all_movies_raw.json` | JSON | `data/raw/` |
| `movies_processed/` | Parquet | `data/processed/` |
| `movies_processed.csv/` | CSV | `data/processed/` |
| `movies_processed.json/` | JSON | `data/processed/` |

### Visualization Files

All charts are saved as PNG files in `output/visualizations/`:

- `revenue_vs_budget.png`
- `roi_by_genre.png`
- `popularity_vs_rating.png`
- `yearly_trends.png`
- `franchise_vs_standalone.png`

### Log Files

Detailed logs are saved in `logs/` with timestamps:
- `tmdb_pipeline_YYYYMMDD_HHMMSS.log`

## 🐳 Docker Support

You can run the entire pipeline in a Docker container to ensure a consistent environment.

### Prerequisites for Docker
- Docker Desktop installed and running

### Running with Docker Compose (Recommended)

1. **Build and Run**
   ```bash
   docker-compose up --build
   ```

2. **Run in Background**
   ```bash
   docker-compose up -d --build
   ```

3. **View Logs**
   ```bash
   docker-compose logs -f
   ```

4. **Stop Containers**
   ```bash
   docker-compose down
   ```

The pipeline will run, save data to your local `data/` folder, and generate charts in `output/` just like running locally.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [TMDB](https://www.themoviedb.org/) for providing the movie data API
- [PySpark](https://spark.apache.org/docs/latest/api/python/) for distributed data processing
- [Matplotlib](https://matplotlib.org/) for visualization capabilities

---

**Made with ❤️ for Data Engineering**