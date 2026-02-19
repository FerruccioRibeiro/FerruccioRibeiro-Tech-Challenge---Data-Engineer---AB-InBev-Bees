from pathlib import Path
import logging

#SRC Functions
from src.utils.file_ops import read_files, save_file


logger = logging.getLogger(__name__)


def count_breweries_per_type_location(path):
    """
    Function that read all silver files. remove unecessary columns and group dataframe to show quantity of breweries
    """
    path = Path(path)
    df = read_files(path / 'data/silver')
    logger.info(f'[Gold Analytics] Dropping unnecessary columns: {list(df.columns.difference(["name", "brewery_type", "city", "state_province", "country"], sort=False))}')
    df = df[['brewery_type', 'city', 'state_province', 'country']].reset_index()
    logger.info('[Gold Analytics] Aggregating data by type, country, and state')
    df_group = df.groupby(['brewery_type', 'country', 'state_province']).agg(count_breweries=('index', 'count')).reset_index()
    save_file(path=path, folder= 'data/gold', df=df_group)