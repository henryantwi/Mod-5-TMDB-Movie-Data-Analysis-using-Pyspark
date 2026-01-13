# TMDB Movie Data Analysis - User Guide

## Table of Contents
1. [Project Overview](#project-overview)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [Setup Instructions](#setup-instructions)
5. [Running the Analysis](#running-the-analysis)
6. [Using Jupyter Notebooks](#using-jupyter-notebooks)
7. [Troubleshooting](#troubleshooting)
8. [Output Files](#output-files)

---

## Project Overview

This project analyzes TMDB (The Movie Database) movie data using **PySpark** for distributed data processing and **Matplotlib** for visualizations. The analysis includes:

- Data fetching from TMDB API
- Data processing and transformation using PySpark
- Statistical analysis and insights
- Rich visualizations including:
  - Revenue vs. Budget Trends
  - ROI Distribution by Genre
  - Popularity vs. Rating
  - Yearly Trends in Box Office Performance
  - Franchise vs. Standalone Movie Comparison

---

## Prerequisites

Before starting, ensure you have:

- **Docker** and **Docker Compose** installed
  - [Install Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac/Linux)
- **TMDB API Key** (for fetching data)
  - Get your free API key at [TMDB API](https://www.themoviedb.org/settings/api)
- Basic familiarity with command line/terminal

---

## Project Structure

```
Mod-5-TMDB-Movie-Data-Analysis-using-Pyspark/
├── data/
│   ├── raw/              # Raw JSON data from TMDB API
│   ├── processed/        # Processed Parquet files
│   └── figures/          # Generated visualizations (PNG files)
├── notebooks/
│   └── main.ipynb        # Jupyter notebook for interactive analysis
├── src/
│   ├── fetch_data.py     # Fetches data from TMDB API
│   ├── process_data.py   # Processes and transforms raw data
│   ├── analysis.py       # Analysis functions and statistics
│   ├── visualize.py      # Visualization generation
│   └── logger.py         # Logging configuration
├── docker-compose.yml    # Docker orchestration configuration
├── Dockerfile            # Docker image definition
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables (create this!)
└── USER_GUIDE.md         # This file
```

---

## Setup Instructions

### Step 1: Clone the Repository

```bash
cd "C:\Users\HenryNanaAntwi\Development\Data Engineering Stuff\DE05 Big Data"
cd Mod-5-TMDB-Movie-Data-Analysis-using-Pyspark
```

### Step 2: Create Environment File

Create a `.env` file in the project root directory with your TMDB API credentials:

```bash
# Windows PowerShell
New-Item -Path .env -ItemType File

# Or manually create .env file and add:
TMDB_API_KEY=your_api_key_here
TMDB_READ_ACCESS_TOKEN=your_read_access_token_here
```

**Example `.env` file:**
```env
TMDB_API_KEY=1234567890abcdef1234567890abcdef
TMDB_READ_ACCESS_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

> **Note:** Never commit your `.env` file to version control!

### Step 3: Build Docker Container

Build the Docker image with all dependencies:

```bash
docker-compose build
```

This will:
- Pull the official Apache Spark image
- Install Python packages (PySpark, pandas, matplotlib, etc.)
- Set up Jupyter Lab
- Configure the environment

**Expected output:**
```
[+] Building 45.2s (12/12) FINISHED
 => [internal] load build definition from Dockerfile
 => => transferring dockerfile: 1.23kB
 ...
 => => naming to docker.io/library/mod-5-tmdb-movie-data-analysis-using-pyspark-jupyter
```

---

## Running the Analysis

### Method 1: Run All Scripts in Sequence (Recommended for First Time)

#### Step 1: Start the Container

```bash
docker-compose up -d
```

This starts the Jupyter container in detached mode.

#### Step 2: Enter the Container Shell

```bash
docker exec -it jupyter bash
```

You should see a prompt like:
```
spark@<container-id>:~/work$
```

#### Step 3: Fetch Data from TMDB API

```bash
cd /home/spark
python src/fetch_data.py
```

**Expected output:**
```
2026-01-13 10:30:15 - INFO - Starting data fetching from TMDB API...
2026-01-13 10:30:20 - INFO - Fetched 500 popular movies
2026-01-13 10:30:25 - INFO - Saved to /home/spark/data/raw/movies.json
```

This creates `data/raw/movies.json` with movie data.

#### Step 4: Process Raw Data

```bash
python src/process_data.py
```

**Expected output:**
```
2026-01-13 10:31:00 - INFO - Loading data from /home/spark/data/raw/movies.json
2026-01-13 10:31:05 - INFO - Starting processing...
2026-01-13 10:31:15 - INFO - Writing parsed data to /home/spark/data/processed/movies.parquet
2026-01-13 10:31:20 - INFO - Processing complete.
```

This creates `data/processed/movies.parquet` with cleaned data.

#### Step 5: Run Analysis

```bash
python src/analysis.py
```

**Expected output:**
```
2026-01-13 10:32:00 - INFO - Loading data from /home/spark/data/processed/movies.parquet
2026-01-13 10:32:05 - INFO - Loaded 500 movies.
--- Financials ---
        title  revenue_musd
0  Avengers...      858.373
...
```

This performs statistical analysis and generates basic plots.

#### Step 6: Generate Visualizations

```bash
python src/visualize.py
```

**Expected output:**
```
2026-01-13 10:33:00 - INFO - Starting TMDB Movie Visualization Script
2026-01-13 10:33:05 - INFO - Loaded 500 movies for visualization.
======================================================================
2026-01-13 10:33:10 - INFO - Generating Revenue vs. Budget plot...
2026-01-13 10:33:12 - INFO - Saved: data/figures/01_revenue_vs_budget.png
2026-01-13 10:33:14 - INFO - Generating ROI Distribution by Genre plot...
2026-01-13 10:33:16 - INFO - Saved: data/figures/02_roi_by_genre.png
...
2026-01-13 10:33:30 - INFO - All visualizations generated successfully!
```

This creates all visualization PNG files in `data/figures/`.

#### Step 7: Exit Container

```bash
exit
```

### Method 2: Run Individual Scripts from Host

You can also run scripts directly without entering the container:

```bash
# Fetch data
docker exec jupyter python /home/spark/src/fetch_data.py

# Process data
docker exec jupyter python /home/spark/src/process_data.py

# Run analysis
docker exec jupyter python /home/spark/src/analysis.py

# Generate visualizations
docker exec jupyter python /home/spark/src/visualize.py
```

### Method 3: Run Complete Pipeline Script

Create a pipeline script to run everything in sequence:

```bash
# Inside container or using docker exec
python -c "
import subprocess
import sys

scripts = [
    'src/fetch_data.py',
    'src/process_data.py',
    'src/analysis.py',
    'src/visualize.py'
]

for script in scripts:
    print(f'\\n{'='*70}')
    print(f'Running {script}...')
    print('='*70)
    result = subprocess.run([sys.executable, script], cwd='/home/spark')
    if result.returncode != 0:
        print(f'Error running {script}')
        break
"
```

---

## Using Jupyter Notebooks

### Starting Jupyter Lab

Jupyter Lab starts automatically when you run `docker-compose up`. Access it at:

```
http://localhost:8888
```

**Token:** `spark` (configured in docker-compose.yml)

### Running the Analysis Notebook

1. Open your browser and navigate to `http://localhost:8888`
2. Enter token: `spark`
3. Open `notebooks/main.ipynb`
4. Run cells sequentially using `Shift + Enter`

### Creating New Notebooks

1. Click "New" → "Notebook" → "Python 3"
2. Import PySpark:
   ```python
   from pyspark.sql import SparkSession
   
   spark = SparkSession.builder \
       .appName("MyAnalysis") \
       .master("local[*]") \
       .getOrCreate()
   
   df = spark.read.parquet("/home/spark/data/processed/movies.parquet")
   df.show(5)
   ```

---

## Troubleshooting

### Issue 1: Container Won't Start

**Problem:** `docker-compose up` fails

**Solutions:**
```bash
# Check Docker is running
docker --version

# Remove old containers
docker-compose down
docker-compose up --build

# Check logs
docker-compose logs -f jupyter
```

### Issue 2: Permission Errors

**Problem:** "Permission denied" when writing files

**Solution:**
```bash
# Fix data directory permissions (from host)
# Windows (PowerShell as Administrator)
icacls "data" /grant Everyone:(OI)(CI)F /T

# Linux/Mac
chmod -R 777 data
```

### Issue 3: TMDB API Errors

**Problem:** "Invalid API key" or rate limiting

**Solutions:**
- Verify your API key in `.env` file
- Check TMDB API status: https://status.themoviedb.org/
- Wait a few minutes if rate limited (40 requests per 10 seconds limit)

### Issue 4: Port 8888 Already in Use

**Problem:** Jupyter can't start on port 8888

**Solution:**
```yaml
# Edit docker-compose.yml, change ports:
ports:
  - "9999:8888"  # Use port 9999 on host instead

# Then access at http://localhost:9999
```

### Issue 5: Module Import Errors

**Problem:** "ModuleNotFoundError: No module named 'src'"

**Solution:**
```bash
# Inside container, set PYTHONPATH
export PYTHONPATH=/home/spark:$PYTHONPATH

# Or run from correct directory
cd /home/spark
python src/visualize.py
```

### Issue 6: Visualization Files Not Created

**Problem:** No PNG files in `data/figures/`

**Solutions:**
```bash
# Check if processed data exists
ls -la /home/spark/data/processed/

# Manually create figures directory
mkdir -p /home/spark/data/figures

# Check logs for specific errors
python src/visualize.py
```

---

## Output Files

### Data Files

| File | Description | Format |
|------|-------------|--------|
| `data/raw/movies.json` | Raw movie data from TMDB API | JSON |
| `data/processed/movies.parquet` | Cleaned and transformed data | Parquet |

### Visualization Files

All visualizations are saved in `data/figures/`:

| File | Description |
|------|-------------|
| `01_revenue_vs_budget.png` | Scatter plot showing relationship between budget and revenue with break-even line |
| `02_roi_by_genre.png` | Horizontal bar chart showing average ROI by genre |
| `03_popularity_vs_rating.png` | Scatter plot correlating popularity scores with ratings |
| `04_yearly_trends.png` | Line chart showing box office trends over years |
| `05_franchise_vs_standalone.png` | Comparison charts between franchise and standalone movies |
| `05b_top_franchises.png` | Top 10 franchises by total revenue |

---

## Advanced Usage

### Running with Custom Spark Configuration

```bash
docker exec jupyter spark-submit \
  --master local[4] \
  --driver-memory 2g \
  --executor-memory 2g \
  /home/spark/src/visualize.py
```

### Extracting Files from Container

```bash
# Copy figures to host
docker cp jupyter:/home/spark/data/figures ./output_figures

# Copy processed data
docker cp jupyter:/home/spark/data/processed/movies.parquet ./movies.parquet
```

### Stopping and Cleaning Up

```bash
# Stop container
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Remove all data
docker-compose down -v
rm -rf data/
```

---

## Quick Reference Commands

### Essential Docker Commands

```bash
# Build and start
docker-compose up --build -d

# Enter container shell
docker exec -it jupyter bash

# View logs
docker-compose logs -f jupyter

# Stop container
docker-compose down

# Restart container
docker-compose restart
```

### Essential Python Scripts

```bash
# From inside container (/home/spark)
python src/fetch_data.py      # Fetch data
python src/process_data.py    # Process data
python src/analysis.py        # Run analysis
python src/visualize.py       # Generate plots
```

---

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review Docker logs: `docker-compose logs -f`
3. Check PySpark logs in the container: `/home/spark/.local/share/jupyter/runtime/`

---

## License

This project is for educational purposes as part of the DE05 Big Data module.

---

**Happy Analyzing!**
