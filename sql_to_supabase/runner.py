"""Orquestra a migração SQL Server -> Supabase a partir do mappings.json."""
import json
import logging
from datetime import datetime
from pathlib import Path

from shared_job_helpers import notify_slack
from sql_to_supabase import job as supabase_job

_BASE_PATH = Path(__file__).resolve().parent
_CONFIG_PATH = _BASE_PATH / "config" / "mappings.json"


def _load_mappings() -> list:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_query(query_file: str) -> str:
    # `query_file` no mappings.json é relativo à raiz do projeto.
    project_root = _BASE_PATH.parent
    full_path = project_root / query_file
    with open(full_path, "r", encoding="utf-8") as q:
        return q.read()


def _formatar_alerta_falhas(falhas: list, total: int, inicio: datetime) -> str:
    linhas = [
        ":x: *Falha na migração SQL Server -> Supabase*",
        f"Início: {inicio.strftime('%d/%m/%Y %H:%M:%S')}",
        f"Tabelas processadas: {total}  |  Falhas: {len(falhas)}",
        "",
        "*Detalhes:*",
    ]
    for tabela, erro in falhas:
        # Trunca o erro pra não estourar o limite de 40k chars do Slack
        erro_curto = (erro[:300] + "...") if len(erro) > 300 else erro
        linhas.append(f"• `{tabela}` — {erro_curto}")
    return "\n".join(linhas)


def run() -> None:
    """Itera o mappings.json, limpa cada tabela no Supabase e reinsere os dados.

    Falhas em uma tabela não interrompem as demais; ao final, se houver
    qualquer falha, dispara uma notificação no Slack.
    """
    inicio = datetime.now()
    mappings = _load_mappings()
    falhas: list[tuple[str, str]] = []

    for item in mappings:
        table_name = item["table_name"]
        query = _read_query(item["query_file"])

        logging.info(f"🚀 Processando tabela '{table_name}'")
        try:
            supabase_job.clear_supabase_table(table_name)
            supabase_job.insert_enriched_data(table_name, query)
            logging.info(f"✅ Concluído: {table_name}")
        except Exception as e:
            logging.error(f"❌ Erro na tabela '{table_name}': {str(e)}")
            falhas.append((table_name, str(e)))

    if falhas:
        mensagem = _formatar_alerta_falhas(falhas, total=len(mappings), inicio=inicio)
        notify_slack(mensagem)
