import os
import re
import logging
from io import BytesIO
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import pandas as pd
import pyodbc

BLOB_PLAT_CONN_STR = os.getenv('BLOB_PLAT_CONN_STR')


def _get_connection_sqlserver():
    try:
        logging.info("🟣 Conectando ao SQL Server...")

        server = 'ismart-sql-server.database.windows.net'
        database = 'dev-ismart-sql-db'
        username = 'ismart'
        password = 'th!juyep8iFr'
        driver = "ODBC Driver 18 for SQL Server"

        conn_str = (
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

        conn = pyodbc.connect(conn_str)
        logging.info("✅ Conexão com SQL estabelecida.")
        return conn

    except Exception as e:
        logging.exception("Erro ao conectar ao SQL Server: %s", str(e))
        raise


def extrair_data_do_nome_arquivo(file_name: str) -> datetime:
    """
    Extrai a data do nome do arquivo no padrão YYYY-MM-DD.
    Ex.:
    - base_bruta_evo_2026-03-17.xlsx
    - evolucional_2026-03-17.xlsx
    """
    match = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
    if not match:
        raise ValueError(
            f"Não foi possível extrair a data do nome do arquivo: {file_name}"
        )

    return datetime.strptime(match.group(1), "%Y-%m-%d")


def domingo_anterior(data_ref: datetime) -> datetime:
    # Ex.: se data_ref = 17/03/2026 (terça), retorna 15/03/2026
    dias_desde_domingo = (data_ref.weekday() + 1) % 7
    return (data_ref - timedelta(days=dias_desde_domingo)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def calcular_id_tempo(data_ref: datetime) -> int:
    if data_ref.day <= 3:
        ultimo_dia_mes_anterior = data_ref.replace(day=1) - timedelta(days=1)
        return int(ultimo_dia_mes_anterior.strftime("%Y%m"))
    return int(data_ref.strftime("%Y%m"))


def normalizar_numerico(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(
        serie.astype(str)
        .str.strip()
        .replace(
            {
                "Sem seleção": "0",
                "nan": None,
                "None": None,
                "": None,
            }
        )
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )


def aplicar_regra_1(base_bruta: pd.DataFrame) -> pd.DataFrame:
    """
    Regra 1 do arquivo enviado:
    - cria coluna Seleção
    - se 'Progresso até a meta no módulo 1' == 'Sem seleção' => 0, senão 1
    - depois substitui 'Sem seleção' por 0 na base
    """
    base = base_bruta.copy()

    base["Seleção"] = base["Progresso até a meta no módulo 1"].apply(
        lambda x: 0 if str(x).strip() == "Sem seleção" else 1
    )

    base.replace({"Sem seleção": 0}, inplace=True)
    return base


def aplicar_regra_2(base_bruta: pd.DataFrame, conn, data_arquivo: datetime) -> pd.DataFrame:
    """
    Usa como data de corte o último domingo com base na data do arquivo.
    Tudo que estiver após esse domingo é considerado futuro e deve ficar como NULL.
    """
    base = base_bruta.copy()

    query = """
    SELECT Data, Ciclo, Atividade
    FROM ismart_aux_calendario_evo
    """
    calendario = pd.read_sql(query, conn)

    if calendario.empty:
        logging.warning("Calendário vazio em ismart_aux_calendario_evo.")
        return base

    calendario["Data"] = pd.to_datetime(calendario["Data"]).dt.normalize()
    calendario = calendario.sort_values("Data").reset_index(drop=True)

    data_ref = pd.Timestamp(domingo_anterior(data_arquivo))
    calendario_valido = calendario[calendario["Data"] <= data_ref]

    if calendario_valido.empty:
        logging.warning(
            "Nenhuma linha válida no calendário até o último domingo (%s).",
            data_ref.date()
        )
        return base

    dict_colunas_modulos = {
        "Módulo 1": ["Progresso até a meta no módulo 1", "Progresso geral no módulo 1"],
        "Módulo 2": ["Progresso até a meta no módulo 2", "Progresso geral no módulo 2"],
        "Módulo 3": ["Progresso até a meta no módulo 3", "Progresso geral no módulo 3"],
        "Desafio": ["Desafio"],
    }

    # Tudo após o último domingo da data do arquivo é futuro => null
    calendario_futuro = calendario[calendario["Data"] > data_ref]

    for _, row in calendario_futuro.iterrows():
        ciclo = row["Ciclo"]
        atividade = row["Atividade"]

        if atividade in dict_colunas_modulos:
            for col in dict_colunas_modulos[atividade]:
                base.loc[base["Ciclo"] == ciclo, col] = None

    return base


def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    """
    Para cada RA, pega o id_matricula vinculado ao maior id_tempo.
    """
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


def montar_dataframe_final(base_bruta: pd.DataFrame, conn, file_name: str) -> pd.DataFrame:
    colunas_esperadas = [
        "RA",
        "Ciclo",
        "Progresso até a meta no módulo 1",
        "Progresso até a meta no módulo 2",
        "Progresso até a meta no módulo 3",
        "Progresso geral no módulo 1",
        "Progresso geral no módulo 2",
        "Progresso geral no módulo 3",
        "Desafio",
    ]

    faltantes = [c for c in colunas_esperadas if c not in base_bruta.columns]
    if faltantes:
        raise ValueError(f"Colunas obrigatórias ausentes no Excel: {faltantes}")

    data_arquivo = extrair_data_do_nome_arquivo(file_name)

    base = aplicar_regra_1(base_bruta)
    base = aplicar_regra_2(base, conn, data_arquivo)

    base["RA"] = base["RA"].astype(str).str.strip()

    matriculas = buscar_matriculas_mais_recentes(conn)

    base = base.merge(
        matriculas,
        how="left",
        left_on="RA",
        right_on="ra",
    )

    semana = domingo_anterior(data_arquivo)
    id_tempo = calcular_id_tempo(semana)

    desafio_num = normalizar_numerico(base["Desafio"])

    final = pd.DataFrame(
        {
            "id_matricula": pd.to_numeric(base["id_matricula"], errors="coerce"),
            "id_tempo": id_tempo,
            "ra": base["RA"],
            "semana": semana,
            "progresso_modulo_1": normalizar_numerico(base["Progresso geral no módulo 1"]),
            "progresso_modulo_2": normalizar_numerico(base["Progresso geral no módulo 2"]),
            "progresso_modulo_3": normalizar_numerico(base["Progresso geral no módulo 3"]),
            "desafio": desafio_num,
            "atividade": base["Ciclo"],
            "desafio_ajustado": desafio_num.apply(
                lambda x: None if pd.isna(x) else (1 if x > 0 else 0)
            ),
            "selecionou_carreira": pd.to_numeric(
                base["Seleção"], errors="coerce"
            ).fillna(0).astype(int),
        }
    )

    return final


def gravar_iol_evolucional(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar.")
        return

    cursor = conn.cursor()

    semana_ref = df_final["semana"].iloc[0]

    delete_sql = """
    DELETE FROM iol_evolucional
    WHERE semana = ?
    """

    insert_sql = """
    INSERT INTO iol_evolucional (
        id_matricula,
        id_tempo,
        ra,
        semana,
        progresso_modulo_1,
        progresso_modulo_2,
        progresso_modulo_3,
        desafio,
        atividade,
        desafio_ajustado,
        selecionou_carreira
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    linhas_inseridas = 0

    for _, row in df_final.iterrows():
        cursor.execute(
            insert_sql,
            None if pd.isna(row["id_matricula"]) else int(row["id_matricula"]),
            int(row["id_tempo"]),
            str(row["ra"]),
            row["semana"],
            None if pd.isna(row["progresso_modulo_1"]) else float(row["progresso_modulo_1"]),
            None if pd.isna(row["progresso_modulo_2"]) else float(row["progresso_modulo_2"]),
            None if pd.isna(row["progresso_modulo_3"]) else float(row["progresso_modulo_3"]),
            None if pd.isna(row["desafio"]) else float(row["desafio"]),
            None if pd.isna(row["atividade"]) else str(row["atividade"]),
            None if pd.isna(row["desafio_ajustado"]) else int(row["desafio_ajustado"]),
            int(row["selecionou_carreira"]),
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()

    logging.info("✅ Linhas inseridas em iol_evolucional: %s", linhas_inseridas)


def processar_iol_evolucional(base_bruta: pd.DataFrame, file_name: str = None):
    conn = None

    try:
        if not file_name:
            raise ValueError("É obrigatório informar o file_name para extrair a data do arquivo.")

        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final(base_bruta, conn, file_name)
        df_final = df_final[~df_final['id_matricula'].isna()].copy()

        logging.info(
            "Linhas sem id_matricula removidas: %s",
            len(montar_dataframe_final(base_bruta, conn, file_name)) - len(df_final)
        )

        gravar_iol_evolucional(df_final, conn)

        logging.info(
            "Processamento concluído com sucesso. Arquivo: %s | Linhas gravadas: %s",
            file_name,
            len(df_final)
        )

    except Exception as e:
        logging.exception("Erro ao processar a carga: %s", str(e))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()