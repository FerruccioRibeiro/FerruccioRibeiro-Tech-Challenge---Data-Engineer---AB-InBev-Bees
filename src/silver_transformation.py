import pandas as pd
import unicodedata
from utils.utils import  read_files, save_file

def remover_acentos(texto):
    if not isinstance(texto, str):
        return texto
    # Normaliza para decompor caracteres (Ex: 'ã' vira 'a' + '~')
    nfkd_form = unicodedata.normalize('NFKD', texto)
    # Filtra mantendo apenas o que não for marca de acentuação
    return "".join([char for char in nfkd_form if not unicodedata.combining(char)])


def separate_in_location(path):
    df = read_files(path / "data/bronze")
    
    print(f"Eliminando colunas desnecessarias {list(df.columns.difference(['name', 'brewery_type', 'city', 'state_province', 'country'], sort=False))}")
    df = df[['name', 'brewery_type', 'city', 'state_province', 'country']].reset_index()

    for (pais, estado), group in df.groupby(['country', 'state_province']):
        df_filtrado = df[(df['country'] == pais) & (df['state_province'] == estado)]
        save_file(path=path / "data/silver", folder=f"{pais}/{estado}", df=df_filtrado)

