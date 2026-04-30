import logging
import os
import re
from datetime import datetime, timedelta
from typing import Iterable, Optional

import pandas as pd
import pyodbc


def get_connection_sqlserver():
    try:
        logging.info("Conectando ao SQL Server...")

        server = os.getenv("SQL_SERVER_HOST", "ismart-sql-server.database.windows.net")
        database = os.getenv("SQL_SERVER_DB", "dev-ismart-sql-db")
        username = os.getenv("SQL_SERVER_USER", "ismart")
        password = os.getenv("SQL_SERVER_PASSWORD", "th!juyep8iFr")
        driver = os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 18 for SQL Server")

        conn_str = (
            f"Driver={{{driver}}};"
            f"Server={server};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
            "Login Timeout=15;"
        )

        conn = pyodbc.connect(conn_str)
        logging.info("Conexao com SQL estabelecida.")
        return conn

    except Exception as exc:
        logging.exception("Erro ao conectar ao SQL Server: %s", str(exc))
        raise


def extrair_data_do_nome_arquivo(file_name: str) -> datetime:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", file_name)
    if not match:
        raise ValueError(
            f"Nao foi possivel extrair a data do nome do arquivo: {file_name}"
        )

    return datetime.strptime(match.group(1), "%Y-%m-%d")


def domingo_anterior(data_ref: datetime) -> datetime:
    dias_desde_domingo = (data_ref.weekday() + 1) % 7
    return (data_ref - timedelta(days=dias_desde_domingo)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def calcular_id_tempo(data_ref: datetime) -> int:
    if data_ref.day <= 3:
        ultimo_dia_mes_anterior = data_ref.replace(day=1) - timedelta(days=1)
        return int(ultimo_dia_mes_anterior.strftime("%Y%m"))
    return int(data_ref.strftime("%Y%m"))


def normalizar_texto(
    serie: pd.Series,
    *,
    lowercase: bool = False,
    empty_values: Optional[Iterable[str]] = None,
) -> pd.Series:
    valores_vazios = {"nan", "None", ""}
    if lowercase:
        valores_vazios.add("none")
    if empty_values:
        valores_vazios.update(empty_values)

    normalizada = serie.astype(str).str.strip()
    if lowercase:
        normalizada = normalizada.str.lower()

    return normalizada.replace({valor: None for valor in valores_vazios})


def normalizar_numerico(
    serie: pd.Series,
    *,
    replacements: Optional[dict[str, Optional[str]]] = None,
) -> pd.Series:
    valores = normalizar_texto(serie)
    if replacements:
        valores = valores.replace(replacements)

    return pd.to_numeric(
        valores.str.replace("%", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def validar_colunas_obrigatorias(
    df: pd.DataFrame,
    colunas_obrigatorias: Iterable[str],
    *,
    origem: str,
):
    faltantes = [coluna for coluna in colunas_obrigatorias if coluna not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas obrigatorias ausentes em {origem}: {faltantes}")


def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    query = """
    WITH cte AS (
        SELECT
            CAST(ra AS VARCHAR(100)) AS ra,
            id_matricula,
            id_tempo,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(ra AS VARCHAR(100))
                ORDER BY id_tempo DESC, id_matricula DESC
            ) AS rn
        FROM ismart_matricula
    )
    SELECT ra, id_matricula
    FROM cte
    WHERE rn = 1
    """

    matriculas = pd.read_sql(query, conn)
    matriculas["ra"] = matriculas["ra"].astype(str).str.strip()
    return matriculas


def remover_linhas_sem_identificador(
    df: pd.DataFrame,
    *,
    colunas_identificadoras: Iterable[str] = ("ra", "id_matricula"),
    contexto_log: str = "do DataFrame final",
) -> pd.DataFrame:
    colunas_presentes = [coluna for coluna in colunas_identificadoras if coluna in df.columns]
    if not colunas_presentes:
        logging.warning(
            "Nenhuma coluna identificadora encontrada para remover linhas invalidas em %s.",
            contexto_log,
        )
        return df.copy()

    mascara_validos = df[colunas_presentes].notna().any(axis=1)
    removidas = int((~mascara_validos).sum())

    if removidas:
        logging.info(
            "Linhas sem identificador valido removidas em %s: %s",
            contexto_log,
            removidas,
        )

    return df.loc[mascara_validos].copy()
