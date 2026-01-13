# TMDB Movie Data Analysis  (PySpark Edition)

## What This Project Is About

I updated this project to transition from a single-node Pandas approach to a scalable **PySpark** architecture. The goal remains to understand movie success drivers, but the underlying tech stack is now designed to handle "Big Data" scale.

**Techniques Used:**
*   **PySpark** for distributed data processing (ETL) and aggregation.
*   **Pandas & Seaborn** for final lightweight visualization.
*   **Robust ETL**: Retry mechanics for APIs, structured logging, and Parquet storage.
*   **Orchestration**: A main Jupyter Notebook orchestrates the entire flow.

## Architecture

1.  **Extract**: `src/fetch_data.py` uses `requests` and `tenacity` to reliably fetch data from TMDB API and save raw JSON.
2.  **Transform**: `src/process_data.py` uses **PySpark** to load nested JSON, flatten structures (arrays/structs), clean types, and save optimized **Parquet** files.
3.  **Load/Analyze**: `src/analysis.py` uses **PySpark** transformations to aggregate stats (Revenue, ROI, Franchises) efficiently.
4.  **Visualize**: Results are collected to Pandas for clean plotting in the notebook.

## Project Structure

```
Movie-Data-Analysis/
├── data/
│   ├── raw/                    # Raw JSON from TMDB API
│   └── processed/              # Parquet files (PySpark output)
├── notebooks/
│   └── main.ipynb              # MAIN ENTRY POINT (Orchestrator)
├── src/
│   ├── fetch_data.py           # Robust API Fetcher
│   ├── process_data.py         # PySpark ETL logic
│   ├── analysis.py             # PySpark Analysis Logic
│   └── logger.py               # Centralized logging
├── requirements.txt
└── README.md
```

## Getting Started

### Prerequisites

1.  **Python 3.10+** (Managed via `uv` or `pip`).
2.  **Java JDK 17+** (Required for PySpark).
3.  **Windows Users**: You MUST have `winutils.exe` and `hadoop.dll` configured in `%HADOOP_HOME%/bin`.

### Installation

1.  **Install Dependencies**:
    ```bash
    uv sync  # or pip install -r requirements.txt
    ```

2.  **Set up API Key**:
    - Create a `.env` file:
      ```
      TMDB_API_KEY=your_api_key_here
      ```

### Running the Analysis

The entire pipeline is orchestrated in **Jupyter**:

1.  Start Jupyter Lab:
    ```bash
    jupyter lab
    ```
2.  Open `notebooks/main.ipynb`.
3.  Run all cells to execute the pipeline step-by-step.

### Running Modules Individually

You can also run specific modules as scripts:

```bash
# Fetch Data
python -m src.fetch_data

# Process Data (PySpark Local)
python -m src.process_data

# Run Analysis (PySpark Local)
python -m src.analysis
```

## Comparisons: Pandas vs. PySpark

| Feature | Pandas Implementation | PySpark Implementation |
| :--- | :--- | :--- |
| **Execution** | Eager (In-Memory) | Lazy (DAG Execution) |
| **Handling Nested JSON** | Python loops / slow apply() | Native `transform`, `array_join` functions |
| **Storage** | CSV (Slow, untyped) | Parquet (Fast, columnar, typed) |
| **Scaling** | Limited by RAM | Distributed Cluster Capable |

---
*Updated Jan 2026 for Big Data Engineering Module.*
