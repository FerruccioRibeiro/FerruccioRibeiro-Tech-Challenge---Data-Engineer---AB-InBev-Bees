from pathlib import Path
from pandas import read_json, read_parquet, concat

def find_root():
    caminho_script = Path(__file__).resolve()
    return caminho_script.parent.parent

def create_folder(path, folder):
    path_data = path / folder
    path_data.mkdir(parents=True, exist_ok=True)
    return path_data

def read_files(path):
    lista_dfs = []

    if 'bronze' in str(path):
        for file in path.rglob('*.json'):
            print(f"Lendo o arquivo {file}")
            temp_df = read_json(file)
            lista_dfs.append(temp_df)

    elif 'silver' in str(path):
        for file in path.rglob('*.parquet'):
            print(f"Lendo o arquivo {file}")
            temp_df = read_parquet(file)
            lista_dfs.append(temp_df)

    df_final = concat(lista_dfs, ignore_index=True)

    return df_final

def save_file(path, folder, df):
    caminho = create_folder(path=path, folder=folder)
    print(f"Salvando o arquivo {caminho}")
    df.to_parquet(f'{caminho}/dados_final.parquet', index=False)