import os
import socketserver
# --- PySpark 4.x on Windows Workaround ---
if os.name == 'nt' and not hasattr(socketserver, 'UnixStreamServer'):
    socketserver.UnixStreamServer = socketserver.TCPServer
# -----------------------------------------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, desc, asc, year, count, sum, mean, median, 
    when, lit, to_date
)
from pathlib import Path
from typing import Optional, List, Union

# Add src to path if running directly
# Handles both local development and Docker container environments
import sys
try:
    from src.logger import setup_logger
except ImportError:
    # Running as standalone script - add parent directory to path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    # Also add Docker path if running in container
    sys.path.insert(0, "/home/spark")
    from logger import setup_logger

logger = setup_logger(__name__)

class MovieAnalyzer:
    """
    Analyzes movie data using PySpark.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df: Optional[DataFrame] = None

    def load_data(self, path: str):
        """Loads processed Parquet data."""
        logger.info(f"Loading data from {path}")
        if not Path(path).exists():
             logger.error(f"Input file {path} does not exist.")
             raise FileNotFoundError(f"{path} not found.")
             
        self.df = self.spark.read.parquet(path)
        # Add Year column for analysis
        if "release_date" in self.df.columns:
            self.df = self.df.withColumn("release_year", year(col("release_date")))
        
        # Cache since we'll run many queries
        self.df.cache()
        logger.info(f"Loaded {self.df.count()} movies.")

    def rank_movies(self, metric: str, ascending: bool = False, top_n: int = 5, 
                   filter_col: str = None, filter_val: Union[int, float] = None):
        """
        Ranks movies based on a specific metric.

        Args:
            metric (str): Column name to rank by (e.g., 'revenue_musd', 'roi', 'profit').
            ascending (bool, optional): Sort in ascending order if True. Defaults to False.
            top_n (int, optional): Number of top results to return. Defaults to 5.
            filter_col (str, optional): Column name to filter on before ranking. Defaults to None.
            filter_val (Union[int, float], optional): Minimum value for filter_col. Defaults to None.

        Returns:
            pd.DataFrame: Pandas DataFrame with 'title' and metric columns, sorted and limited to top_n.
        """
        data = self.df
        
        if filter_col:
            data = data.filter(col(filter_col) >= filter_val)
            
        sort_col = asc(metric) if ascending else desc(metric)
        
        ranked = data.select("title", metric) \
                     .orderBy(sort_col) \
                     .limit(top_n)
        
        return ranked.toPandas()

    def get_financial_stats(self):
        """Returns top movies by various financial metrics."""
        stats = {}
        
        # highest revenue
        stats['top_revenue'] = self.rank_movies('revenue_musd')
        # highest budget
        stats['top_budget'] = self.rank_movies('budget_musd')
        # highest profit
        stats['top_profit'] = self.rank_movies('profit')
        # lowest profit (flops)
        stats['flops'] = self.rank_movies('profit', ascending=True)
        # highest ROI (budget > 10M)
        stats['top_roi'] = self.rank_movies('roi', filter_col='budget_musd', filter_val=10)
        # Lowest ROI (budget > 10M)
        stats['flops_roi'] = self.rank_movies('roi', ascending=True, filter_col='budget_musd', filter_val=10)
        # Most Voted Movies
        stats['most_voted'] = self.rank_movies('vote_count')
        # Highest Rated Movies (votes >= 10)
        stats['highest_rated'] = self.rank_movies('vote_average', filter_col='vote_count', filter_val=10)
        # Lowest Rated Movies (votes >= 10)
        stats['lowest_rated'] = self.rank_movies('vote_average', ascending=True, filter_col='vote_count', filter_val=10)
        # Most Popular Movies
        stats['most_popular'] = self.rank_movies('popularity')
        
        return stats

    def get_specific_movies(self):
        """Runs specific filtering queries (Bruce Willis, Uma Thurman)."""
        # Sci-Fi Action with Bruce Willis
        bruce = self.df.filter(
            col("genres").contains("Science Fiction") & 
            col("genres").contains("Action") & 
            col("cast").contains("Bruce Willis")
        ).select("title", "vote_average", "release_date") \
         .orderBy(desc("vote_average"))
        
        # Uma Thurman & Quentin Tarantino
        uma_qt = self.df.filter(
            col("cast").contains("Uma Thurman") & 
            col("director").contains("Quentin Tarantino")
        ).select("title", "runtime", "release_date") \
         .orderBy(asc("runtime"))
         
        return {
            "bruce_willis_scifi": bruce.toPandas(),
            "uma_qt_collab": uma_qt.toPandas()
        }

    def get_genre_stats(self):
        """Returns statistics by genre."""
        # Clean method since genres are pipe-delimited strings
        from pyspark.sql.functions import split, explode
        
        # Explode genres: Movie A "Action|SciFi" -> Row 1 Action, Row 2 SciFi
        genre_df = self.df.withColumn("genre", explode(split(col("genres"), "\|")))
        
        # Aggregate
        return genre_df.groupBy("genre").agg(
            count("title").alias("count"),
            mean("roi").alias("avg_roi"),
            mean("revenue_musd").alias("avg_revenue"),
            mean("vote_average").alias("avg_rating")
        ).orderBy(desc("avg_revenue")).toPandas()
        
    def get_yearly_trends(self):
        """Returns yearly aggregation of revenue and budget."""
        return self.df.filter(col("release_year").isNotNull()) \
            .groupBy("release_year") \
            .agg(
                sum("revenue_musd").alias("total_revenue"),
                sum("budget_musd").alias("total_budget"),
                mean("vote_average").alias("avg_rating")
            ) \
            .orderBy("release_year") \
            .toPandas()

    def get_all_movies_for_plot(self):
        """Returns relevant columns for scatter plots (Revenue vs Budget, Popularity vs Rating)."""
        return self.df.select(
            "title", "budget_musd", "revenue_musd", "roi", 
            "popularity", "vote_average", "vote_count"
        ).toPandas()

    def analyze_franchises(self):
        """Analyzes franchise vs standalone movies."""
        # Create flag
        df_Flagged = self.df.withColumn("is_franchise", 
             when(col("belongs_to_collection").isNotNull(), "Franchise")
            .otherwise("Standalone")
        )
        
        # Aggregation
        # Note: median is approx in Spark SQL (requires percentiles)
        # Here we use mean for simplicity or approxQuantile for median
        stats = df_Flagged.groupBy("is_franchise").agg(
            mean("revenue_musd").alias("avg_revenue"),
            mean("budget_musd").alias("avg_budget"),
            mean("popularity").alias("avg_popularity"),
            mean("vote_average").alias("avg_rating")
        ).toPandas().set_index("is_franchise")
        
        # Top Franchises
        franchise_rank = self.df.filter(col("belongs_to_collection").isNotNull()) \
            .groupBy("belongs_to_collection") \
            .agg(
                count("title").alias("movie_count"),
                sum("revenue_musd").alias("total_revenue"),
                mean("vote_average").alias("avg_rating")
            ) \
            .orderBy(desc("total_revenue")) \
            .limit(5)
            
        return stats, franchise_rank.toPandas()

    def analyze_directors(self):
        """Top Directors by Revenue."""
        return self.df.filter(col("director") != "") \
            .groupBy("director") \
            .agg(
                count("title").alias("movie_count"),
                sum("revenue_musd").alias("total_revenue"),
                mean("vote_average").alias("avg_rating")
            ) \
            .orderBy(desc("total_revenue")) \
            .limit(5) \
            .toPandas()

    def generate_plots(self, output_dir: str):
        """Generates and saves plots to the output directory."""
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        sns.set_theme(style="whitegrid")
        plt.rcParams['figure.figsize'] = (12, 6)
        
        logger.info(f"Generating plots in {out_path}...")
        
        # 1. Revenue vs Budget
        df_all = self.get_all_movies_for_plot()
        plt.figure()
        sns.scatterplot(data=df_all, x="budget_musd", y="revenue_musd", 
                        size="vote_count", hue="vote_average", palette="viridis", sizes=(20, 200))
        plt.plot([0, df_all['budget_musd'].max()], [0, df_all['budget_musd'].max()], 'r--', label='Break-even')
        plt.title("Revenue vs. Budget")
        plt.xlabel("Budget (M$)")
        plt.ylabel("Revenue (M$)")
        plt.savefig(out_path / "revenue_vs_budget.png")
        plt.close()
        
        # 2. ROI by Genre
        df_genre = self.get_genre_stats().sort_values("avg_roi", ascending=False)
        plt.figure()
        sns.barplot(data=df_genre, x="avg_roi", y="genre", hue="genre", palette="magma", legend=False)
        plt.title("Average ROI by Genre")
        plt.xlabel("Average ROI")
        plt.tight_layout()
        plt.savefig(out_path / "roi_by_genre.png")
        plt.close()
        
        # 3. Popularity vs Rating
        plt.figure()
        sns.scatterplot(data=df_all, x="vote_average", y="popularity", alpha=0.7)
        plt.title("Popularity vs. Rating")
        plt.xlabel("Vote Average")
        plt.ylabel("Popularity")
        plt.savefig(out_path / "popularity_vs_rating.png")
        plt.close()
        
        # 4. Yearly Trends
        df_yearly = self.get_yearly_trends()
        plt.figure()
        sns.lineplot(data=df_yearly, x="release_year", y="total_revenue", marker="o", label="Revenue")
        sns.lineplot(data=df_yearly, x="release_year", y="total_budget", marker="o", label="Budget")
        plt.title("Yearly Box Office Trends (M$)")
        plt.xlabel("Year")
        plt.ylabel("Amount (M$)")
        plt.savefig(out_path / "yearly_trends.png")
        plt.close()
        
        # 5. Franchise vs Standalone
        stats, _ = self.analyze_franchises()
        stats_reset = stats.reset_index()

        # Melt the dataframe to have Revenue and Budget in one column
        stats_melted = stats_reset.melt(
            id_vars="is_franchise", 
            value_vars=["avg_revenue", "avg_budget"], 
            var_name="Metric", 
            value_name="Amount (M$)"
        )

        # Rename the metrics for better legend readability
        stats_melted["Metric"] = stats_melted["Metric"].replace({
            "avg_revenue": "Revenue", 
            "avg_budget": "Budget"
        })

        plt.figure()
        # Create grouped bar chart
        sns.barplot(data=stats_melted, x="Metric", y="Amount (M$)", hue="is_franchise", palette="muted")

        plt.title("Franchise vs. Standalone: Revenue and Budget Comparison")
        plt.ylabel("Amount (M$)")
        plt.xlabel("Metric")
        plt.legend(title="Franchise Type")
        plt.savefig(out_path / "franchise_vs_standalone.png")
        plt.close()
        
        logger.info("Plots saved successfully.")

def main():
    """Local test run."""
    # Determine Spark Master URL and Data Dir
    master_url = os.getenv("SPARK_MASTER", "local[*]")
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    
    spark = SparkSession.builder \
        .appName("TMDB_Analysis") \
        .master(master_url) \
        .getOrCreate()
        
    analyzer = MovieAnalyzer(spark)
    
    try:
        analyzer.load_data(str(data_dir / "processed" / "movies.parquet"))
        
        logger.info("--- Financials ---")
        print(analyzer.get_financial_stats()['top_revenue'])
        
        logger.info("--- Specific Queries ---")
        print(analyzer.get_specific_movies()['bruce_willis_scifi'])
        
        logger.info("--- Franchise Stats ---")
        stats, top_fr = analyzer.analyze_franchises()
        print(stats)
        
        # Generate Plots
        analyzer.generate_plots(data_dir / "figures")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
