import unicodedata
import logging
from pathlib import Path
from pandera.errors import SchemaErrors
import numpy as np

#SRC Functions
from src.utils.file_ops import  read_files, save_file
from src.schemas import brewery_silver_schema, VALID_BREWERY_TYPES

logger = logging.getLogger(__name__)

def remove_accents(text):
    """
    Function to remove accents of the words
    """
    if not isinstance(text, str):
        return text
    nfkd_form = unicodedata.normalize('NFKD', text)
    return "".join([char for char in nfkd_form if not unicodedata.combining(char)])

def clean_geographical_data(df):
    """
    Clean latitude and longitude out of range
    """
    invalid_lat = (df['latitude'] > 90) | (df['latitude'] < -90)
    invalid_lon = (df['longitude'] > 180) | (df['longitude'] < -180)
    
    invalid_total = df[invalid_lat | invalid_lon].shape[0]
    
    if invalid_total > 0:
        logger.warning(f'[Silver Transformation] Data Quality Alert | {invalid_total} records with corrupted coordinates converted to NULL')
        
        df.loc[invalid_lat, 'latitude'] = np.nan
        df.loc[invalid_lon, 'longitude'] = np.nan
        
    return df


def separate_in_location(path):
    """
    This functions is responsible for cleaning.
    First, read all files in bronze layer
    Second, filter for valid types of breweries in api doc
    Third, take out all duplicated ids
    Fourth, remove corrupted characters
    Fifth, validate schema
    Sixth, put null in the wrong coordinates
    """
    path = Path(path)
    df = read_files(path / 'data/bronze')

    all_rows = len(df)

    df_invalid = df[~df['brewery_type'].isin(VALID_BREWERY_TYPES)]
    if not df_invalid.empty:
        rows_filtered = len(df_invalid)
        logger.warning(f'[Silver Transformation] Non-standard types found in {rows_filtered} out of {all_rows} records: {df_invalid["brewery_type"].unique()}')
    df = df[df['brewery_type'].isin(VALID_BREWERY_TYPES)]

    total_duplicated = df.duplicated(subset=['id']).sum()
    if total_duplicated > 0:
        logger.warning(f'[Silver Transformation] Dropping {len(total_duplicated)} records with duplicate IDs')
    df = df.drop_duplicates(subset=['id'])

    df = df.replace('\ufffd', '', regex=True)

    try:
        brewery_silver_schema.validate(df, lazy=True)
        logger.info('[Silver Transformation] Data Quality Check: Silver schema successfully validated!')
    except SchemaErrors as err:
        logger.error(f'[Silver Transformation] Data Quality Violation in Silver layer: {err}')


    df = clean_geographical_data(df)

    for (country, state_province), group in df.groupby(['country', 'state_province']):
        save_file(path=path / 'data/silver', folder=f"{remove_accents(country)}/{remove_accents(state_province)}", df=group)

