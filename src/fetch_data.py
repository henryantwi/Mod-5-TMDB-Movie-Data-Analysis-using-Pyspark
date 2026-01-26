import os
import json
import requests
from pathlib import Path
from typing import List, Dict, Optional, Any
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

    def fetch_specific_movies(self, movie_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Fetches data for a list of movie IDs.
        
        Args:
            movie_ids (List[int]): List of movie IDs.
            
        Returns:
            List[dict[str, Any]]: List of movie data dictionaries.
        """ 
        movies = []
        total = len(movie_ids)
        
        for i, movie_id in enumerate(movie_ids):
            # Skip ID 0 as it's often a placeholder/error in datasets
            if movie_id == 0:
                self.logger.info("Skipping movie ID 0 as it's invalid.")
                continue
                
            try:
                data = self.fetch_movie_details(movie_id)
                if data:
                    movies.append(data)
                self.logger.info(f"Progress: {i+1}/{total} completed.")
            except Exception as e:
                self.logger.error(f"Failed to fetch movie {movie_id} after retries. Skipping. Error: {e}")
                
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
