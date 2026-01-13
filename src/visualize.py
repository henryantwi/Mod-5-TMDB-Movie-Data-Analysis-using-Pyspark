import os
import socketserver
# --- PySpark 4.x on Windows Workaround ---
if os.name == 'nt' and not hasattr(socketserver, 'UnixStreamServer'):
    socketserver.UnixStreamServer = socketserver.TCPServer
# -----------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, split, explode, mean, sum, count, 
    when, desc
)
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for saving files
import seaborn as sns
import sys

# Add src to path if running directly
try:
    from src.logger import setup_logger
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    sys.path.insert(0, "/home/spark")
    from logger import setup_logger

logger = setup_logger(__name__)


class MovieVisualizer:
    """
    Generates and saves visualizations for TMDB movie data analysis using PySpark and Matplotlib.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df = None
        
    def load_data(self, path: str):
        """Loads processed Parquet data."""
        logger.info(f"Loading data from {path}")
        if not Path(path).exists():
            logger.error(f"Input file {path} does not exist.")
            raise FileNotFoundError(f"{path} not found.")
            
        self.df = self.spark.read.parquet(path)
        
        # Add release_year column if not present
        if "release_year" not in self.df.columns and "release_date" in self.df.columns:
            self.df = self.df.withColumn("release_year", year(col("release_date")))
        
        self.df.cache()
        logger.info(f"Loaded {self.df.count()} movies for visualization.")
    
    def setup_plot_style(self):
        """Configure matplotlib and seaborn styling."""
        sns.set_theme(style="whitegrid", palette="muted")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
        
    def plot_revenue_vs_budget(self, output_path: Path):
        """
        Visualization 1: Revenue vs. Budget Trends
        Shows the relationship between movie budgets and revenues with a break-even line.
        """
        logger.info("Generating Revenue vs. Budget plot...")
        
        # Get data and convert to pandas
        df_plot = self.df.select(
            "title", "budget_musd", "revenue_musd", 
            "vote_average", "vote_count"
        ).filter(
            (col("budget_musd").isNotNull()) & 
            (col("revenue_musd").isNotNull()) &
            (col("budget_musd") > 0) &
            (col("revenue_musd") > 0)
        ).toPandas()
        
        # Create scatter plot
        fig, ax = plt.subplots(figsize=(14, 8))
        scatter = ax.scatter(
            df_plot['budget_musd'], 
            df_plot['revenue_musd'],
            c=df_plot['vote_average'], 
            s=df_plot['vote_count'] / 100,
            cmap='viridis', 
            alpha=0.6,
            edgecolors='black',
            linewidth=0.5
        )
        
        # Add break-even line
        max_val = max(df_plot['budget_musd'].max(), df_plot['revenue_musd'].max())
        ax.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Break-even Line', alpha=0.8)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Vote Average', rotation=270, labelpad=20)
        
        ax.set_xlabel('Budget (Million USD)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax.set_title('Revenue vs. Budget Trends\n(Size = Vote Count, Color = Rating)', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path / "01_revenue_vs_budget.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '01_revenue_vs_budget.png'}")
    
    def plot_roi_by_genre(self, output_path: Path):
        """
        Visualization 2: ROI Distribution by Genre
        Shows average Return on Investment for each genre.
        """
        logger.info("Generating ROI Distribution by Genre plot...")
        
        # Explode genres and calculate average ROI
        genre_df = self.df.filter(
            (col("genres").isNotNull()) & 
            (col("roi").isNotNull()) &
            (col("roi") > 0)
        ).withColumn("genre", explode(split(col("genres"), "\\|")))
        
        genre_stats = genre_df.groupBy("genre").agg(
            mean("roi").alias("avg_roi"),
            count("title").alias("movie_count")
        ).filter(col("movie_count") >= 10).orderBy(desc("avg_roi")).toPandas()
        
        # Create horizontal bar plot
        fig, ax = plt.subplots(figsize=(12, 10))
        colors = sns.color_palette("rocket_r", len(genre_stats))
        
        bars = ax.barh(genre_stats['genre'], genre_stats['avg_roi'], color=colors, edgecolor='black', linewidth=0.7)
        
        # Add value labels
        for i, (value, count) in enumerate(zip(genre_stats['avg_roi'], genre_stats['movie_count'])):
            ax.text(value + 0.05, i, f'{value:.2f}x ({int(count)} movies)', 
                   va='center', fontsize=9, fontweight='bold')
        
        ax.set_xlabel('Average ROI (Return on Investment)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Genre', fontsize=12, fontweight='bold')
        ax.set_title('ROI Distribution by Genre\n(Minimum 10 movies per genre)', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)
        ax.invert_yaxis()
        
        plt.tight_layout()
        plt.savefig(output_path / "02_roi_by_genre.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '02_roi_by_genre.png'}")
    
    def plot_popularity_vs_rating(self, output_path: Path):
        """
        Visualization 3: Popularity vs. Rating
        Shows the relationship between movie popularity and ratings.
        """
        logger.info("Generating Popularity vs. Rating plot...")
        
        # Get data
        df_plot = self.df.select(
            "title", "popularity", "vote_average", "vote_count"
        ).filter(
            (col("popularity").isNotNull()) & 
            (col("vote_average").isNotNull()) &
            (col("vote_count") >= 100)  # Filter for movies with sufficient votes
        ).toPandas()
        
        # Create scatter plot with density
        fig, ax = plt.subplots(figsize=(14, 8))
        
        scatter = ax.scatter(
            df_plot['vote_average'], 
            df_plot['popularity'],
            c=df_plot['vote_count'],
            cmap='plasma',
            s=50,
            alpha=0.5,
            edgecolors='black',
            linewidth=0.3
        )
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Vote Count', rotation=270, labelpad=20)
        
        # Add trend line
        z = np.polyfit(df_plot['vote_average'], df_plot['popularity'], 1)
        p = np.poly1d(z)
        ax.plot(df_plot['vote_average'].sort_values(), 
               p(df_plot['vote_average'].sort_values()), 
               "r--", linewidth=2, alpha=0.8, label=f'Trend line')
        
        ax.set_xlabel('Vote Average (Rating)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Popularity Score', fontsize=12, fontweight='bold')
        ax.set_title('Popularity vs. Rating\n(Movies with 100+ votes)', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path / "03_popularity_vs_rating.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '03_popularity_vs_rating.png'}")
    
    def plot_yearly_trends(self, output_path: Path):
        """
        Visualization 4: Yearly Trends in Box Office Performance
        Shows total revenue and budget trends over the years.
        """
        logger.info("Generating Yearly Trends plot...")
        
        # Calculate yearly aggregates
        yearly_stats = self.df.filter(
            col("release_year").isNotNull()
        ).groupBy("release_year").agg(
            sum("revenue_musd").alias("total_revenue"),
            sum("budget_musd").alias("total_budget"),
            mean("vote_average").alias("avg_rating"),
            count("title").alias("movie_count")
        ).orderBy("release_year").toPandas()
        
        # Create multi-axis plot
        fig, ax1 = plt.subplots(figsize=(16, 8))
        
        # Revenue and Budget on primary axis
        ax1.plot(yearly_stats['release_year'], yearly_stats['total_revenue'], 
                marker='o', linewidth=2.5, markersize=6, label='Total Revenue', 
                color='green', alpha=0.8)
        ax1.plot(yearly_stats['release_year'], yearly_stats['total_budget'], 
                marker='s', linewidth=2.5, markersize=6, label='Total Budget', 
                color='red', alpha=0.8)
        
        ax1.set_xlabel('Release Year', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Amount (Million USD)', fontsize=12, fontweight='bold', color='black')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc='upper left', fontsize=10)
        
        # Average Rating on secondary axis
        ax2 = ax1.twinx()
        ax2.plot(yearly_stats['release_year'], yearly_stats['avg_rating'], 
                marker='D', linewidth=2, markersize=5, label='Avg Rating', 
                color='blue', alpha=0.7, linestyle='--')
        ax2.set_ylabel('Average Rating', fontsize=12, fontweight='bold', color='blue')
        ax2.tick_params(axis='y', labelcolor='blue')
        ax2.legend(loc='upper right', fontsize=10)
        ax2.set_ylim([0, 10])
        
        plt.title('Yearly Trends in Box Office Performance\n(Revenue, Budget, and Average Rating)', 
                 fontsize=14, fontweight='bold', pad=20)
        
        fig.tight_layout()
        plt.savefig(output_path / "04_yearly_trends.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '04_yearly_trends.png'}")
    
    def plot_franchise_vs_standalone(self, output_path: Path):
        """
        Visualization 5: Comparison of Franchise vs. Standalone Success
        Compares financial and popularity metrics between franchise and standalone movies.
        """
        logger.info("Generating Franchise vs. Standalone comparison plot...")
        
        # Create franchise flag
        df_comparison = self.df.withColumn("movie_type", 
            when(col("belongs_to_collection").isNotNull(), "Franchise")
            .otherwise("Standalone")
        )
        
        # Calculate statistics
        comparison_stats = df_comparison.groupBy("movie_type").agg(
            mean("revenue_musd").alias("avg_revenue"),
            mean("budget_musd").alias("avg_budget"),
            mean("roi").alias("avg_roi"),
            mean("popularity").alias("avg_popularity"),
            mean("vote_average").alias("avg_rating"),
            count("title").alias("movie_count")
        ).toPandas()
        
        # Create subplot with multiple comparisons
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Franchise vs. Standalone Movie Success Comparison', 
                    fontsize=16, fontweight='bold', y=0.995)
        
        metrics = [
            ('avg_revenue', 'Average Revenue (Million USD)', axes[0, 0]),
            ('avg_budget', 'Average Budget (Million USD)', axes[0, 1]),
            ('avg_roi', 'Average ROI', axes[1, 0]),
            ('avg_popularity', 'Average Popularity Score', axes[1, 1])
        ]
        
        colors = ['#2ecc71', '#e74c3c']
        
        for metric, title, ax in metrics:
            bars = ax.bar(comparison_stats['movie_type'], comparison_stats[metric], 
                         color=colors, edgecolor='black', linewidth=1.5, alpha=0.8, width=0.6)
            
            # Add value labels
            for bar, value, count in zip(bars, comparison_stats[metric], comparison_stats['movie_count']):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{value:.2f}\n({int(count)} movies)',
                       ha='center', va='bottom', fontsize=11, fontweight='bold')
            
            ax.set_ylabel(title, fontsize=11, fontweight='bold')
            ax.set_xlabel('Movie Type', fontsize=11, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            ax.set_title(title, fontsize=12, fontweight='bold', pad=10)
        
        plt.tight_layout()
        plt.savefig(output_path / "05_franchise_vs_standalone.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '05_franchise_vs_standalone.png'}")
        
        # Also create a detailed franchise ranking
        self._plot_top_franchises(output_path)
    
    def _plot_top_franchises(self, output_path: Path):
        """Helper: Plot top franchises by total revenue."""
        logger.info("Generating Top Franchises plot...")
        
        top_franchises = self.df.filter(
            col("belongs_to_collection").isNotNull()
        ).groupBy("belongs_to_collection").agg(
            sum("revenue_musd").alias("total_revenue"),
            count("title").alias("movie_count"),
            mean("vote_average").alias("avg_rating")
        ).orderBy(desc("total_revenue")).limit(10).toPandas()
        
        fig, ax = plt.subplots(figsize=(12, 10))
        colors = sns.color_palette("coolwarm", len(top_franchises))
        
        bars = ax.barh(top_franchises['belongs_to_collection'], 
                      top_franchises['total_revenue'], 
                      color=colors, edgecolor='black', linewidth=0.7)
        
        # Add labels
        for i, (revenue, count, rating) in enumerate(zip(
            top_franchises['total_revenue'], 
            top_franchises['movie_count'],
            top_franchises['avg_rating']
        )):
            ax.text(revenue + 50, i, 
                   f'${revenue:.0f}M ({int(count)} movies, {rating:.1f}★)', 
                   va='center', fontsize=9, fontweight='bold')
        
        ax.set_xlabel('Total Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Franchise', fontsize=12, fontweight='bold')
        ax.set_title('Top 10 Movie Franchises by Total Revenue', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path / "05b_top_franchises.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved: {output_path / '05b_top_franchises.png'}")
    
    def generate_all_visualizations(self, output_dir: str):
        """
        Generate all visualizations and save them to the output directory.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Starting visualization generation in {output_path}...")
        logger.info("=" * 70)
        
        self.setup_plot_style()
        
        try:
            # Import numpy here (needed for trend line)
            import numpy as np
            globals()['np'] = np
            
            # Generate all plots
            self.plot_revenue_vs_budget(output_path)
            self.plot_roi_by_genre(output_path)
            self.plot_popularity_vs_rating(output_path)
            self.plot_yearly_trends(output_path)
            self.plot_franchise_vs_standalone(output_path)
            
            logger.info("=" * 70)
            logger.info(f"All visualizations generated successfully!")
            logger.info(f"Output directory: {output_path.absolute()}")
            
        except Exception as e:
            logger.error(f"Error generating visualizations: {e}")
            raise


def main():
    """Main execution function."""
    # Determine Spark Master URL and Data Dir
    master_url = os.getenv("SPARK_MASTER", "local[*]")
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    
    logger.info("Starting TMDB Movie Visualization Script")
    logger.info(f"Spark Master: {master_url}")
    logger.info(f"Data Directory: {data_dir}")
    
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("TMDB_Visualizations") \
        .master(master_url) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        visualizer = MovieVisualizer(spark)
        
        # Load processed data
        input_path = str(data_dir / "processed" / "movies.parquet")
        visualizer.load_data(input_path)
        
        # Generate all visualizations
        output_dir = str(data_dir / "figures")
        visualizer.generate_all_visualizations(output_dir)
        
    except Exception as e:
        logger.error(f"Visualization script failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
