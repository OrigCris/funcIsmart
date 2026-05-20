import json
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging, os, pyodbc, requests
from supabase import create_client, Client
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

KEY_VAULT_NAME = os.environ["KEY_VAULT_NAME"]


# Mapeamento de tipos Python (do pyodbc cursor.description) para tipos Postgres
# usado pela função RPC `ensure_columns` do Supabase.
_PY_TO_PG_TYPE = {
    bool: "boolean",
    int: "bigint",
    float: "double precision",
    Decimal: "numeric",
    datetime: "timestamp",
    date: "date",
    time: "time",
    bytes: "bytea",
    bytearray: "bytea",
    UUID: "uuid",
    str: "text",
}


def _map_pyodbc_type_to_pg(py_type) -> str:
    """Mapeia um tipo Python (vindo de cursor.description) para o tipo Postgres."""
    if py_type is None:
        return "text"
    return _PY_TO_PG_TYPE.get(py_type, "text")


def serialize_value(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()  # exemplo: '2025-11-12' ou '2025-11-12T06:00:00'
    return v

def get_secret_from_keyvault(secret_name: str) -> str:
    """Get secret from Azure Key Vault"""
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=KEY_VAULT_NAME, credential=credential)
        return client.get_secret(secret_name).value
    except Exception as e:
        logging.error(f"Error retrieving secret from Key Vault: {str(e)}")
        raise


def get_database_connection():
    """Get database connection using SQL Server authentication"""
    logging.info("Attempting to establish database connection")

    try:
        # Get database credentials from Key Vault
        server = 'ismart-sql-server.database.windows.net'
        database = 'dev-ismart-sql-db'
        username = 'ismart'
        password = 'th!juyep8iFr'
        driver = "ODBC Driver 18 for SQL Server"

        # Build connection string with SQL authentication
        CONNECTION_STRING = (
            f'Driver={{{driver}}};'
            f'Server={server};'
            f'Database={database};'
            f'UID={username};'
            f'PWD={password};'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;'
            'Login Timeout=15;'
        )

        logging.info(f"Using connection string: {CONNECTION_STRING}")

        # Establish connection
        conn = pyodbc.connect(CONNECTION_STRING)

        logging.info("Database connection established successfully")
        return conn
    except Exception as e:
        logging.error(f"Failed to establish database connection: {str(e)}")
        raise

def get_supabase_client() -> Client:
    """Get authenticated Supabase client"""
    url = 'https://fqtdvwmkmqgeiyudicur.supabase.co'
    key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZxdGR2d21rbXFnZWl5dWRpY3VyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1Njg1Njc0NiwiZXhwIjoyMDcyNDMyNzQ2fQ.OjJXDIrBUZvZyUBzmt34Kd1ESCSNCi31-k-SbZIgUHo'
    supabase = create_client(url, key)
    return supabase


def send_to_supabase(records: list, table_name: str):
    """Insert or upsert records into Supabase"""
    if not records:
        logging.warning("⚠️ No records to send to Supabase")
        return

    try:
        batch_size = 10000
        supabase = get_supabase_client()

        total = len(records)
        logging.info(f"📤 Enviando {total} registros para '{table_name}' em lotes de {batch_size}...")

        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            try:
                logging.info(f"➡️ Lote {i}–{i+len(batch)-1} ({len(batch)} registros)")
                response = supabase.table(table_name).insert(batch).execute()

                if not response.data:
                    logging.warning("⚠️ Supabase retornou resposta sem data para este lote")
            except Exception as e:
                logging.error(f"❌ Erro ao enviar lote {i}–{i+len(batch)-1} para '{table_name}': {e}")
                raise

        logging.info(f"✅ Todos os {total} registros foram enviados para '{table_name}'.")

    except Exception as e:
        logging.error(f"❌ Error sending data to Supabase: {str(e)}")
        raise

def ensure_supabase_columns(table_name: str, cursor_description) -> None:
    """Garante que toda coluna retornada pela query exista no Supabase.

    Compara as colunas vindas do `cursor.description` do SQL Server com as
    colunas existentes na tabela do Supabase (descobertas via SELECT * LIMIT 1).
    Para cada coluna faltante, chama a RPC `ensure_columns` para criá-la.
    """
    if not cursor_description:
        return

    source_columns = [
        (col[0], _map_pyodbc_type_to_pg(col[1])) for col in cursor_description
    ]

    try:
        supabase = get_supabase_client()
        sample = supabase.table(table_name).select("*").limit(1).execute()

        if sample.data:
            existing = set(sample.data[0].keys())
        else:
            # Tabela vazia (caso comum logo após `clear_supabase_table`): mandamos
            # toda a lista de colunas; a RPC `ensure_columns` usa `if not exists`
            # então é idempotente para colunas já existentes.
            existing = set()

        missing = [
            {"name": name, "type": pg_type}
            for name, pg_type in source_columns
            if name not in existing
        ]

        if not missing:
            return

        logging.info(
            f"🧬 Sincronizando schema de '{table_name}': adicionando "
            f"{len(missing)} coluna(s) -> {[c['name'] for c in missing]}"
        )

        supabase.rpc(
            "ensure_columns",
            {"p_table": table_name, "p_columns": missing},
        ).execute()

        logging.info(f"✅ Schema de '{table_name}' sincronizado.")

    except Exception as e:
        logging.error(f"❌ Erro ao sincronizar schema de '{table_name}': {str(e)}")
        raise


def clear_supabase_table(table_name: str):
    """Apaga todos os registros da tabela no Supabase de forma genérica e segura."""
    try:
        supabase = get_supabase_client()
        logging.info(f"🧹 Limpando tabela '{table_name}' no Supabase...")

        # Busca uma amostra para descobrir uma coluna válida
        sample = supabase.table(table_name).select("*").limit(1).execute()

        if not sample.data:
            logging.info(f"ℹ️ Tabela '{table_name}' já está vazia.")
            return

        # Pega o nome da primeira coluna
        first_column = list(sample.data[0].keys())[0]
        first_value = sample.data[0][first_column]

        # Decide a forma de filtro de acordo com o tipo do valor
        if isinstance(first_value, (int, float)):
            filter_expr = supabase.table(table_name).delete().gt(first_column, -999999999)
        elif isinstance(first_value, str):
            filter_expr = supabase.table(table_name).delete().neq(first_column, "")
        else:
            filter_expr = supabase.table(table_name).delete().neq(first_column, None)

        # Executa a deleção
        response = filter_expr.execute()

        count = len(response.data) if response.data else 0
        logging.info(f"✅ Tabela '{table_name}' limpa com sucesso ({count} registros removidos).")

    except Exception as e:
        logging.error(f"❌ Erro ao limpar tabela '{table_name}': {str(e)}")
        raise

def insert_enriched_data(table_name: str, query: str) -> None:
    conn = None
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(query)

        description = cursor.description
        columns = [column[0] for column in description]
        rows = cursor.fetchall()

        results = [
            {col: serialize_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]

        logging.info(f"📊 {len(results)} registros obtidos de '{table_name}'")

        ensure_supabase_columns(table_name, description)

        send_to_supabase(results, table_name)

    except Exception as e:
        logging.error(f"❌ Erro em '{table_name}': {str(e)}")
        raise
    finally:
        if conn:
            conn.close()