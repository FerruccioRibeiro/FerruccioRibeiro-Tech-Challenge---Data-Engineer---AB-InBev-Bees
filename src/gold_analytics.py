import pandas as pd
from utils.utils import read_files, save_file


def count_breweries_per_type_location(path):
    df = read_files(path / "data/silver")
    print("Agrupando por tipo, pais e estado")
    df_group = df.groupby(['brewery_type', 'country', 'state_province']).agg(count_breweries=("index", "count")).reset_index()
    save_file(path=path, folder= "data/gold", df=df_group)