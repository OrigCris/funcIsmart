"""Extração de dados da API Symplicity e upload para o Blob Storage."""
import datetime
import json
import logging
import os
from io import BytesIO
from time import sleep

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings


API_TOKEN = "5apbZ9Zubu5bbduQz4T+VyQUtQ1qlBWpqhvZxXas8Zi18E2j4+h0Ap4xRZDaeqHEIPUJz/QoI0cdH++mmrkawAS2NRnnQi+4qLcQWKPlwi4I0tMIzItOUxNcqyfAgYjZoqrh9a9XVmU="
BLOB_CONN_STR = os.getenv("BLOB_CONN_STR")
CONTAINER_NAME = "01-raw"
RAW_BASE_PATH = "raw_data/symplicity"

ENDPOINTS = {
    "students": "https://ismart-csm.symplicity.com/api/public/v1/students",
    "jobs": "https://ismart-csm.symplicity.com/api/public/v1/jobs",
    "employability": "https://ismart-csm.symplicity.com/api/public/v1/reports/f6abf76899a2c2ca6153ecab7e383c33/data?run=&format=",
    "events": "https://ismart-csm.symplicity.com/api/public/v1/reports/20ad4a56a9f7af07c1c3ad18044f7fc7/data?run=&format=",
}


def fetch_paginated_models(url: str, headers: dict) -> list:
    page, all_data = 1, []
    while True:
        params = {"page": page}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            logging.error(f"Erro {resp.status_code} na página {page}")
            break
        js = resp.json()
        models = js.get("models", [])
        if not models:
            break
        all_data.extend(models)
        total = js.get("total", len(models))
        logging.info(f"Página {page} carregada ({len(models)} registros). Total até agora: {len(all_data)}/{total}")
        if len(all_data) >= total:
            break
        page += 1
        sleep(0.3)
    return all_data


def fetch_tabular_report(url: str, headers: dict) -> list:
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        logging.error(f"Erro {resp.status_code} no endpoint tabular.")
        return []
    data = json.loads(resp.content.decode("utf-8"))
    if not data or not isinstance(data, list) or len(data) < 2:
        return []
    header, *rows = data
    df = pd.DataFrame(rows, columns=header)
    return df.to_dict(orient="records")


def upload_to_blob(connection: str, container: str, blob_path: str, data: list):
    blob_service = BlobServiceClient.from_connection_string(connection)
    blob_name = f"{blob_path}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    blob_client.upload_blob(
        json_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info(f"Upload concluído: {blob_name} ({len(data)} registros)")
    return blob_name


def run() -> None:
    """Executa a extração de todos os endpoints do Symplicity e faz upload no Blob."""
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info(f"Função executada às {utc_timestamp}")

    headers = {"Authorization": f"Token {API_TOKEN}", "Accept": "application/json"}
    resultados = {}

    for nome, url in ENDPOINTS.items():
        logging.info(f"\nIniciando extração: {nome.upper()}")
        try:
            if "reports" in url:
                data = fetch_tabular_report(url, headers)
            else:
                data = fetch_paginated_models(url, headers)

            if data:
                caminho = f"{RAW_BASE_PATH}/{nome}/{nome}"
                blob = upload_to_blob(BLOB_CONN_STR, CONTAINER_NAME, caminho, data)
                resultados[nome] = {"status": "Sucesso", "registros": len(data), "arquivo": blob}
            else:
                resultados[nome] = {"status": "Sem dados"}
        except Exception as e:
            logging.exception(f"Erro no endpoint {nome}: {e}")
            resultados[nome] = {"status": "Erro", "mensagem": str(e)}

    logging.info("Resumo final das execuções:")
    for nome, res in resultados.items():
        logging.info(f"🔹 {nome.upper()} → {res}")

    logging.info("Python timer trigger function executed.")
