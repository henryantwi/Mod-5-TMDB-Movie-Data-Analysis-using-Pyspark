# TMDB Movie Data Analysis - Final Report

## Executive Summary

This report presents the findings from a comprehensive analysis of movie data from The Movie Database (TMDB) using PySpark for distributed data processing. The project implements a complete ETL pipeline and generates actionable insights into movie industry trends, financial performance, and audience reception patterns.

---

## 1. Methodology

### 1.1 Data Pipeline Architecture

The analysis follows a robust ETL (Extract, Transform, Load) architecture:

1. **Extract**: Movie data is fetched from the TMDB API using a custom `TMDBFetcher` class with built-in retry logic for rate limiting and error handling.

2. **Transform**: Raw JSON data is processed using PySpark to:
   - Flatten nested structures (cast, crew, genres, collections)
   - Convert monetary values to millions USD for readability
   - Compute derived metrics (Profit, ROI)
   - Parse and standardize date formats

3. **Load**: Processed data is stored in Parquet format for efficient columnar storage and fast analytical queries.

### 1.2 Technology Stack

| Component | Technology |
|-----------|------------|
| Data Processing | PySpark 4.x |
| Data Storage | Parquet |
| Visualization | Matplotlib, Seaborn |
| Containerization | Docker |
| API Integration | TMDB REST API |

### 1.3 Key Metrics Computed

- **Revenue (M$)**: Total box office earnings in millions USD
- **Budget (M$)**: Production budget in millions USD
- **Profit**: Revenue - Budget
- **ROI (Return on Investment)**: Revenue / Budget
- **Vote Average**: Audience rating (0-10 scale)
- **Vote Count**: Number of user votes
- **Popularity**: TMDB popularity score

---

## 2. Key Insights

### 2.1 Financial Performance KPIs

The analysis computed the following key performance indicators:

| KPI | Description | Filter Criteria |
|-----|-------------|-----------------|
| Highest Revenue | Top-grossing movies | None |
| Highest Budget | Most expensive productions | None |
| Highest Profit | Most profitable movies | None |
| Lowest Profit (Flops) | Biggest financial losses | None |
| Highest ROI | Best return on investment | Budget ≥ $10M |
| Lowest ROI | Worst return on investment | Budget ≥ $10M |
| Most Voted | Highest audience engagement | None |
| Highest Rated | Best audience reception | Vote Count ≥ 10 |
| Lowest Rated | Worst audience reception | Vote Count ≥ 10 |
| Most Popular | Highest TMDB popularity | None |

### 2.2 Genre Analysis

- Genres were exploded from pipe-delimited strings for individual analysis
- Average ROI and revenue calculated per genre
- Certain genres consistently outperform others in profitability

### 2.3 Franchise vs. Standalone Comparison

Key finding: **Franchise movies tend to generate higher average revenue and operate with larger budgets compared to standalone films.**

This suggests:
- Studios invest more heavily in franchise properties
- Brand recognition drives box office performance
- Franchise films benefit from established audiences

### 2.4 Yearly Trends

- Box office revenue and production budgets tracked over time
- Allows identification of industry growth patterns
- Reveals economic impact on film industry

### 2.5 Director Analysis

- Top directors ranked by cumulative box office revenue
- Correlation between director track record and film success

---

## 3. Challenges Encountered

### 3.1 Technical Challenges

| Challenge | Solution |
|-----------|----------|
| **PySpark 4.x on Windows** | Implemented workaround for missing `UnixStreamServer` in `socketserver` module by patching with `TCPServer` |
| **Nested JSON Structure** | Used PySpark's `explode` and `split` functions to flatten arrays and delimited strings |
| **Module Import Paths** | Configured dynamic path resolution for both local and Docker environments |
| **Rate Limiting (TMDB API)** | Implemented retry logic with exponential backoff in `TMDBFetcher` |

### 3.2 Data Quality Challenges

| Issue | Mitigation |
|-------|------------|
| Missing budget/revenue data | Applied filters (Budget ≥ $10M) for ROI calculations to avoid division errors and outliers |
| Low vote counts skewing ratings | Required minimum 10 votes for rating-based rankings |
| Inconsistent date formats | Used Spark's date parsing with LEGACY time parser policy |

### 3.3 Environment Challenges

- Cross-platform compatibility (Windows vs. Linux containers)
- Spark session configuration for local vs. cluster mode
- Memory management for large datasets

---

## 4. Visualizations Generated

The analysis produces the following visualizations:

1. **Revenue vs. Budget (Scatter Plot)**
   - Reveals relationship between production investment and returns
   - Break-even line identifies profitable vs. unprofitable films

2. **ROI by Genre (Bar Chart)**
   - Compares average return on investment across genres
   - Identifies most financially efficient genres

3. **Popularity vs. Rating (Scatter Plot)**
   - Examines correlation between audience engagement and quality ratings

4. **Yearly Box Office Trends (Line Chart)**
   - Tracks industry revenue and budget evolution over time

5. **Franchise vs. Standalone Comparison (Grouped Bar Chart)**
   - Side-by-side comparison of average revenue and budget

---

## 5. Conclusions

### 5.1 Key Takeaways

1. **High budget does not guarantee high profit** - ROI analysis reveals that mid-budget films can be more profitable than blockbusters.

2. **Franchise power is real** - Movies belonging to established franchises consistently outperform standalone films in revenue.

3. **Genre matters** - Certain genres demonstrate significantly higher ROI, making them safer investments.

4. **Audience engagement ≠ Quality** - Popularity scores do not always correlate with high ratings.

### 5.2 Recommendations for Future Analysis

- Incorporate streaming data for modern release patterns
- Add sentiment analysis from reviews
- Expand dataset to include international box office
- Time-series forecasting for revenue predictions

### 5.3 Technical Recommendations

- Consider Apache Spark on Databricks or AWS EMR for larger datasets
- Implement incremental data loading for continuous updates
- Add data validation checks in the ETL pipeline

---