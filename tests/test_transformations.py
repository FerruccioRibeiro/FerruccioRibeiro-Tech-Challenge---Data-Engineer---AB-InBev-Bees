import pytest
import pandas as pd
import pandera as pa

#SRC Functions
from src.silver_transformation import clean_geographical_data, remove_accents
from src.schemas import brewery_silver_schema


def test_clean_geographical_data_convert_invalids_too_nan():
    df_input = pd.DataFrame({
        'id': ['brewery-1', 'brewery-2'],
        'latitude': [120.5, -23.5], 
        'longitude': [-46.6, 500.0]
    })
    df_result = clean_geographical_data(df_input)
    assert pd.isna(df_result.loc[0, 'latitude'])
    assert pd.isna(df_result.loc[1, 'longitude'])
    assert df_result.loc[1, 'latitude'] == -23.5

def test_reject_wrong_brewerie_type():
    df_invalid = pd.DataFrame({
        'id': ['123'],
        'name': ['Test'],
        'brewery_type': ['non-existent type'],
        'city': ['BH'],
        'state_province': ['MG'],
        'latitude': [None],
        'longitude': [None]
    })
    with pytest.raises(pa.errors.SchemaError):
        brewery_silver_schema.validate(df_invalid)

def test_remove_accents():
    assert remove_accents('São Paulo') == 'Sao Paulo'
    assert remove_accents('Munique (München)') == 'Munique (Munchen)'

def test_duplicated_ids():
    df_input = pd.DataFrame({'id': ['abc', 'abc'], 'state': ['MG', 'MG']})
    df_cleaned = df_input.drop_duplicates(subset=['id'])
    assert len(df_cleaned) == 1