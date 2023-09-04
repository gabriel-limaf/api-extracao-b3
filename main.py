# usar no terminal para ativar a API uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# uvicorn main:app --reload
# abrir local para testar a API http://localhost:8000/docs


import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
from fastapi import FastAPI
from typing import List


app = FastAPI()


def process_tickers(txt_path, tickers: List[str]):
    processed_data = []
    with open(txt_path, "r", encoding='utf-8') as arquivo:
        # Lê cada linha do arquivo
        for linha in arquivo:
            # Remove caracteres de nova linha (\n) no final da linha
            linha = linha.strip()
            if linha[12:24].strip() in tickers and (linha[10:12].strip() == "02" or "08"):
                data_pregao = datetime.strptime(linha[2:10], "%Y%m%d").date()
                codigo_bdi = linha[10:12].strip()  # pegar regra do bdi para colocar em condicional (pegando o 02 e 08)
                ticker = linha[12:24].strip()
                preco_abertura = float(linha[56:69]) / 100
                preco_maximo = float(linha[69:82]) / 100
                preco_minimo = float(linha[82:95]) / 100
                preco_medio = float(linha[95:108]) / 100
                preco_fechamento = float(linha[108:121]) / 100
                qnt_negociada = int(linha[152:170])
                vol_negociado = float(linha[170:188]) / 100
                processed_data.append({"ticker": ticker, "data_pregao": data_pregao, "codigo_bdi": codigo_bdi,
                                       "preco_abertura": preco_abertura, "preco_maximo": preco_maximo,
                                       "preco_minimo": preco_minimo, "preco_medio": preco_medio,
                                       "preco_fechamento": preco_fechamento, "qnt_negociada": qnt_negociada,
                                       "vol_negociado": vol_negociado})
    return processed_data


def download_arquivo(url, destination):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print("Download concluído.")


def unzip(destination):
    with ZipFile(destination, 'r') as zip_file:
        # extrair todos os arquivos
        print('Extraindo...')
        zip_file.extractall()
        print('Concluido!')


@app.post("/stockprices/filter")
def process_tickers_endpoint(tickers: List[str]):
    yesterday = datetime.now() - timedelta(days=1)
    data_consulta = yesterday.strftime('%d'.zfill(2) + '%m'.zfill(2) + '%Y')
    file_url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{data_consulta}.ZIP'
    destination = f'COTAHIST_D{data_consulta}.ZIP'
    txt_path = f'COTAHIST_D{data_consulta}.TXT'
    download_arquivo(file_url, destination), unzip(destination)
    processed_data = process_tickers(txt_path, tickers)
    return processed_data
