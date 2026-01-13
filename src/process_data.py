import os
import sys
import socketserver
# --- PySpark 4.x on Windows Workaround ---
if os.name == 'nt' and not hasattr(socketserver, 'UnixStreamServer'):
    socketserver.UnixStreamServer = socketserver.TCPServer
# -----------------------------------------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, array_join, transform, expr, 
    to_date, filter as array_filter, element_at, 
    size, concat_ws, slice as array_slice
)
from pyspark.sql.types import DoubleType, IntegerType
from pathlib import Path

# Add src to path if running directly
# Handles both local development and Docker container environments
try:
    from src.logger import setup_logger
except ImportError:
    # Running as standalone script - add parent directory to path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    # Also add Docker path if running in container
    sys.path.insert(0, "/home/spark")
    from logger import setup_logger

logger = setup_logger(__name__)

def process_data(input_path: str, output_path: str, spark: SparkSession):
    """
    Reads raw Movie JSON data, cleans it, and writes to Parquet.
    """
    # 1. Load Data
    logger.info(f"Loading data from {input_path}")
    if not Path(input_path).exists():
         logger.error(f"Input file {input_path} does not exist.")
         raise FileNotFoundError(f"{input_path} not found.")

    # multiline=True is required because json.dump writes a single large JSON array, not JSON Lines
    df = spark.read.option("multiline", "true").json(input_path)

    logger.info("Starting processing...")

    # 2. Drop irrelevant columns
    drop_cols = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(*drop_cols)

    # 3. Extract properties from Structs/Arrays using Higher Order Functions
    
    # Genres: Array<Struct<id, name>> -> "Action|Adventure"
    if "genres" in df.columns:
        df = df.withColumn("genres", array_join(transform("genres", lambda x: x["name"]), "|"))

    # Belongs to Collection: Struct<...name...> -> "Toy Story Collection"
    if "belongs_to_collection" in df.columns:
        df = df.withColumn("belongs_to_collection", col("belongs_to_collection.name"))

    # Production Countries/Companies/Languages: Array<Struct<name>> -> "Name1|Name2"
    for c in ["production_countries", "production_companies", "spoken_languages"]:
        if c in df.columns:
             df = df.withColumn(c, array_join(transform(c, lambda x: x["name"]), "|"))

    # 4. Type Conversions & Cleaning Numeric Columns
    # Convert '0' budgets/revenues to NULL to avoid skewing averages (and for 0 division checks)
    for c in ["budget", "revenue"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
            df = df.withColumn(c, when(col(c) == 0, None).otherwise(col(c)))

    # Convert others to proper types
    if "popularity" in df.columns:
        df = df.withColumn("popularity", col("popularity").cast(DoubleType()))
    if "date" not in df.columns and "release_date" in df.columns: # Sometimes 'date' field conflicts
        df = df.withColumn("release_date", to_date(col("release_date")))

    # 5. Calculations (Budget/Revenue in Millions)
    if "budget" in df.columns:
        df = df.withColumn("budget_musd", col("budget") / 1_000_000)
    if "revenue" in df.columns:
        df = df.withColumn("revenue_musd", col("revenue") / 1_000_000)

    # ROI & Profit
    # Profit = Revenue - Budget
    # ROI = Revenue / Budget (if Budget > 0)
    if "budget_musd" in df.columns and "revenue_musd" in df.columns:
        df = df.withColumn("profit", col("revenue_musd") - col("budget_musd"))
        df = df.withColumn("roi", 
            when(col("budget_musd") > 0, col("revenue_musd") / col("budget_musd"))
            .otherwise(0)
        )
        
    # Drop original raw columns
    df = df.drop("budget", "revenue")

    # 6. Filter Data
    # Keep only 'Released' movies
    if "status" in df.columns:
        df = df.filter(col("status") == "Released").drop("status")

    # Deduplicate by ID
    df = df.dropDuplicates(["id"])
    
    # Drop where ID or Title is null
    df = df.na.drop(subset=["id", "title"])

    # 7. Extract Credits Info (Director, Cast, Sizes)
    if "credits" in df.columns:
        # Cast Size / Crew Size
        df = df.withColumn("cast_size", size(col("credits.cast")))
        df = df.withColumn("crew_size", size(col("credits.crew")))

        # Director: Find first crew member where job == 'Director'
        # filter(crew, c -> c.job == 'Director')[0].name
        df = df.withColumn("director", 
            element_at(
                transform(
                    array_filter(col("credits.crew"), lambda c: c["job"] == "Director"),
                    lambda x: x["name"]
                ), 
                1
            )
        )

        # Top 5 Cast Members
        # slice(cast, 1, 5) -> transform to name -> join with |
        df = df.withColumn("cast", 
            array_join(
                transform(
                    array_slice(col("credits.cast"), 1, 5),
                    lambda x: x["name"]
                ), 
                "|"
            )
        )
        
        # Drop the huge credits struct
        df = df.drop("credits")

    # 8. Handling Nulls in Text Columns
    for c in ["overview", "tagline"]:
        if c in df.columns:
           df = df.withColumn(c, when((col(c) == "") | (col(c) == "No Data"), None).otherwise(col(c)))

    # 9. Coalesce & Write
    # Coalesce to 1 to produce a single output file (useful for small datasets like this assignment)
    # Mode 'overwrite' replaces existing data
    logger.info(f"Writing parsed data to {output_path}...")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Processing complete.")

if __name__ == "__main__":
    # Determine Spark Master URL from environment or default to local
    master_url = os.getenv("SPARK_MASTER", "local[*]")
    
    spark = SparkSession.builder \
        .appName("TMDB_Process_Data") \
        .master(master_url) \
        .getOrCreate()
    
    # Use DATA_DIR env var if set, otherwise determine based on environment
    # Docker: /home/spark/data, Local: ./data relative to script location
    data_dir_env = os.getenv("DATA_DIR")
    if data_dir_env:
        data_dir = Path(data_dir_env)
    elif Path("/home/spark/data").exists():
        # Running in Docker container
        data_dir = Path("/home/spark/data")
    else:
        # Running locally - use 'data' folder relative to project root
        data_dir = Path(__file__).parent.parent / "data"
    
    raw_path = str(data_dir / "raw" / "movies.json")
    out_path = str(data_dir / "processed" / "movies.parquet")
    
    try:
        process_data(raw_path, out_path, spark)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
    finally:
        spark.stop()

