import json
import requests
from constances import BASE_URL
from utils.utils import create_folder



def save_raw_page(path, page, n_page):
    caminho_arquivo = path / f'brewerys_page{n_page}.json'
    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        json.dump(page, f, ensure_ascii=False, indent=4)
    print(f"Dados salvos com sucesso em: {caminho_arquivo}")


def get_endpoint(endpoint, params = None):
    headers = {
        "Accept": "application/json"
    }

    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status() # Lança erro se a API falhar (404, 500, etc)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro na extração: {e}")
        return []


def fetch_raw_breweries(path):
    params = {"page": 1, "per_page": 200}
    caminho = create_folder(path=path, folder="data/bronze")

    while True:
        print(f"Buscando pagina {params["page"]} na api")
        page = get_endpoint("breweries", params=params)
        
        if not page:
            print(f"A pagina {params['page']} retorna vazio")
            break
        
        save_raw_page(caminho, page=page, n_page=params['page'])
        params["page"] += 1