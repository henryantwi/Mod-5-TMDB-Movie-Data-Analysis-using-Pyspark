import os
import json
import requests
from pathlib import Path
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import local logger setup
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

class TMDBFetcher:
    """
    A robust fetcher for TMDB API interactions with retry logic and logging.
    """
    
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the fetcher.
        
        Args:
            api_key (str, optional): TMDB API Key. If None, loads from environment.
        """
        load_dotenv()
        self.api_key = api_key or os.getenv('TMDB_API_KEY')
        self.logger = setup_logger(__name__)
        
        if not self.api_key:
            self.logger.error("TMDB_API_KEY not found in environment variables.")
            raise ValueError("TMDB_API_KEY is required.")
            
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException,)),
        reraise=True
    )
    def fetch_movie_details(self, movie_id: int) -> dict[str, Any] | None:
        """
        Fetches details for a specific movie ID with retries.
        
        Args:
            movie_id (int): The TMDB movie ID.
            
        Returns:
            dict[str, Any] | None: The JSON response dictionary, or None if 404.
        """
        url = f"{self.BASE_URL}/movie/{movie_id}"
        params = {
            "api_key": self.api_key,
            "language": "en-US",
            "append_to_response": "credits"
        }
        
        try:
            self.logger.info(f"Fetching details for movie ID: {movie_id}")
            response = self.session.get(url, params=params)
            
            # Handle 404 specifically (don't retry if it doesn't exist)
            if response.status_code == 404:
                self.logger.warning(f"Movie ID {movie_id} not found (404).")
                return None
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP error occurred for movie {movie_id}: {http_err}")
            raise # Let tenacity handle retryable errors (500s, 429s)
        except Exception as err:
            self.logger.error(f"An unexpected error occurred for movie {movie_id}: {err}")
            raise

    def fetch_specific_movies(self, movie_ids: List[int], max_workers: int = 10) -> List[Dict[str, Any]]:
        """
        Fetches data for a list of movie IDs concurrently.
        
        Args:
            movie_ids (List[int]): List of movie IDs.
            max_workers (int): Maximum number of concurrent threads (default: 10).
            
        Returns:
            List[dict[str, Any]]: List of movie data dictionaries.
        """ 
        # Filter out invalid movie IDs (e.g., 0)
        valid_movie_ids = [mid for mid in movie_ids if mid != 0]
        if len(valid_movie_ids) < len(movie_ids):
            self.logger.info(f"Skipping {len(movie_ids) - len(valid_movie_ids)} invalid movie ID(s) (e.g., ID 0).")
        
        movies = []
        total = len(valid_movie_ids)
        completed = 0
        
        def fetch_single_movie(movie_id: int) -> Optional[Dict[str, Any]]:
            """Helper function to fetch a single movie and handle errors."""
            try:
                return self.fetch_movie_details(movie_id)
            except Exception as e:
                self.logger.error(f"Failed to fetch movie {movie_id} after retries. Skipping. Error: {e}")
                return None
        
        self.logger.info(f"Starting concurrent fetch of {total} movies with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all fetch tasks
            future_to_movie_id = {
                executor.submit(fetch_single_movie, movie_id): movie_id 
                for movie_id in valid_movie_ids
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_movie_id):
                movie_id = future_to_movie_id[future]
                completed += 1
                
                try:
                    data = future.result()
                    if data:
                        movies.append(data)
                    self.logger.info(f"Progress: {completed}/{total} completed (Movie ID: {movie_id}).")
                except Exception as e:
                    self.logger.error(f"Unexpected error processing result for movie {movie_id}: {e}")
        
        self.logger.info(f"Fetch complete. Successfully retrieved {len(movies)} out of {total} movies.")
        return movies

    @staticmethod
    def save_raw_data(data: list[dict[str, Any]], filename: Path) -> None:
        """
        Saves the fetched data to a JSON file.
        
        Args:
            data (list[dict[str, Any]]): Data to save.
            filename (Path): Destination path.
        """
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with path.open('w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"Saved {len(data)} movies to {path}") # Keep print for user feedback in addition to log

def main():
    """Main execution entry point."""
    # List of IDs from assignment
    movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457, 351286, 109445, 321612, 260513]
    
    fetcher = TMDBFetcher()
    movies_data = fetcher.fetch_specific_movies(movie_ids)
    
    # Use DATA_DIR env var if set, otherwise default to local 'data' folder
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    output_path = data_dir / "raw" / "movies.json"
    
    TMDBFetcher.save_raw_data(movies_data, output_path)

if __name__ == "__main__":
    main()
