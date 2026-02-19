from src.bronze_ingestion import fetch_raw_breweries
from src.silver_transformation import separate_in_location
from src.gold_analytics import count_breweries_per_type_location
from utils.utils import find_root


root = find_root()

fetch_raw_breweries(root)

separate_in_location(root)

count_breweries_per_type_location(root)



