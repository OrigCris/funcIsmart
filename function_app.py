import logging, json, datetime, requests
import os
import azure.functions as func
from letrus_job import job
from evolucional_job import job_evo
from khan_job import job_khan
from pathlib import Path
from sql_to_supabase import job as supabase_job
from azure.storage.blob import BlobServiceClient, ContentSettings
from time import sleep
import pandas as pd
from io import BytesIO

app = func.FunctionApp()

@app.blob_trigger(
    arg_name="inputblob",
    path="plataformas/{name}",
    connection="BLOB_PLAT_CONN_STR",
    source="EventGrid"
)
def plataformas_job(inputblob: func.InputStream):
    blob_name = inputblob.name
    file_name = blob_name.split("/")[-1]

    logging.info(f"Blob recebido: {blob_name}")

    try:
        # =========================================================
        # EVOLUCIONAL
        # Ex.: plataformas/evolucional/raw/base_bruta_evo.xlsx
        # =========================================================
        if blob_name.startswith("plataformas/evolucional/raw/"):
            if not file_name.startswith("base_bruta_evo"):
                logging.info(f"Arquivo ignorado no fluxo evolucional: {file_name}")
                return

            logging.info(f"Arquivo válido do evolucional detectado: {blob_name}")

            file_bytes = inputblob.read()
            base_bruta = pd.read_excel(BytesIO(file_bytes))

            job_evo.processar_iol_evolucional(base_bruta, file_name)
            logging.info(f"Processamento evolucional concluído: {file_name}")
            return

        # =========================================================
        # KHAN
        # Ex.: plataformas/khan/raw/khan_progresso_bruta_2026-03-15.csv
        # =========================================================
        if blob_name.startswith("plataformas/khan/raw/"):
            if not file_name.startswith("khan_progresso_bruta_"):
                logging.info(f"Arquivo ignorado no fluxo khan: {file_name}")
                return

            logging.info(f"Arquivo válido do khan detectado: {blob_name}")

            file_bytes = inputblob.read()

            try:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="utf-8")
            except UnicodeDecodeError:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="latin1")

            job_khan.processar_iol_khan_progresso(
                base_bruta=base_bruta,
                file_name=file_name
            )
            logging.info(f"Processamento khan concluído: {file_name}")
            return

        # =========================================================
        # OUTROS ARQUIVOS
        # =========================================================
        logging.info(f"Blob ignorado fora dos diretórios mapeados: {blob_name}")

    except Exception as e:
        logging.exception(f"Erro ao processar {file_name}: {str(e)}")
        raise

@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def letrus_job(myTimer: func.TimerRequest) -> None:
    job.process_aux()
    job.process_silver()

@app.timer_trigger(schedule="0 0 */2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def sql_to_supabase(myTimer: func.TimerRequest) -> None:
    logging.info("⏰ Iniciando execução diária de múltiplas migrações para o supabase")
    base_path = Path(__file__).parent
    config_path = base_path / "sql_to_supabase" /"config" / "mappings.json"
    with open(config_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)
        for item in mappings:
            table_name = item["table_name"]
            query_file = base_path / item["query_file"]
            
            with open(query_file, "r", encoding="utf-8") as q:
                query = q.read()
            
            logging.info(f"🚀 Processando tabela '{table_name}'")
            try:
                # Limpa tabela
                supabase_job.clear_supabase_table(table_name)
                # Executa query e envia dados
                supabase_job.insert_enriched_data(table_name, query)

                logging.info(f"✅ Concluído: {table_name}") 
            except Exception as e:
                logging.error(f"❌ Erro na tabela '{table_name}': {str(e)}")

# ======================================
# 🔧 CONFIGURAÇÕES
# ======================================
API_TOKEN = "5apbZ9Zubu5bbduQz4T+VyQUtQ1qlBWpqhvZxXas8Zi18E2j4+h0Ap4xRZDaeqHEIPUJz/QoI0cdH++mmrkawAS2NRnnQi+4qLcQWKPlwi4I0tMIzItOUxNcqyfAgYjZoqrh9a9XVmU="
BLOB_CONN_STR = os.getenv('BLOB_CONN_STR')
CONTAINER_NAME = "01-raw"
RAW_BASE_PATH = "raw_data/symplicity"

ENDPOINTS = {
    "students": "https://ismart-csm.symplicity.com/api/public/v1/students",
    "jobs": "https://ismart-csm.symplicity.com/api/public/v1/jobs",
    "employability": "https://ismart-csm.symplicity.com/api/public/v1/reports/f6abf76899a2c2ca6153ecab7e383c33/data?run=&format=",
    "events": "https://ismart-csm.symplicity.com/api/public/v1/reports/20ad4a56a9f7af07c1c3ad18044f7fc7/data?run=&format="
}

# ======================================
# 🧩 FUNÇÕES AUXILIARES
# ======================================
def fetch_paginated_models(url: str, headers: dict) -> list:
    """Faz chamadas paginadas a endpoints do tipo /students ou /jobs."""
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
    """Faz chamadas para endpoints tipo /reports/... que retornam lista de listas."""
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
    """Faz upload de JSON para o Blob Storage."""
    blob_service = BlobServiceClient.from_connection_string(connection)
    blob_name = f"{blob_path}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    blob_client.upload_blob(json_bytes, overwrite=True,
                            content_settings=ContentSettings(content_type="application/json"))
    logging.info(f"Upload concluído: {blob_name} ({len(data)} registros)")
    return blob_name

@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def extractSymplicity(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    """Executa todos os dias às 07:00 UTC"""
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


    logging.info('Python timer trigger function executed.')