import os
import pandas as pd
from pathlib import Path
import logging


logger = logging.getLogger(__name__)

def get_base_path():
    """
    Get the base path
    """
    env_path = os.getenv('DATA_PATH')
    if env_path:
        return Path(env_path)

def create_folder(path, folder):
    """
    Create folder
    """
    path_data = Path(path) / folder
    os.makedirs(path_data, exist_ok=True)
    return path_data

def read_files(path):
    """
    Read all the files in folders
    """
    lista_dfs = []
    path = Path(path)

    if 'bronze' in str(path):
        for file in path.rglob('*.json'):

            logger.info(f'[File Ops] Reading file: {file}')
            temp_df = pd.read_json(file)
            lista_dfs.append(temp_df)

    elif 'silver' in str(path):
        for file in path.rglob('*.parquet'):
            logger.info(f'[File Ops] Reading file: {file}')
            temp_df = pd.read_parquet(file)
            lista_dfs.append(temp_df)

    if not lista_dfs:
        logger.warning(f'[File Ops] No files found at path: {path}')
        return pd.DataFrame()

    df_final = pd.concat(lista_dfs, ignore_index=True)

    return df_final

def save_file(path, folder, df):
    """
    Save files in the correct path
    """
    path = create_folder(path=path, folder=folder)
    logger.info(f'[File Ops] Saving file to: {path}')
    df.to_parquet(f'{path}/dados_final.parquet', index=False)