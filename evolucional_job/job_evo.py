import os
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
        driver = "ODBC Driver 17 for SQL Server"

        # Build connection string with SQL authentication
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
        print("Conexão com SQL estabelecida.")
        return conn

    except Exception as e:
        print(f"Erro ao conectar ao SQL Server: {e}")

def domingo_anterior(data_ref: datetime) -> datetime:
    # Ex.: se hoje = 10/03/2026 (terça), retorna 08/03/2026
    dias_desde_domingo = (data_ref.weekday() + 1) % 7
    return (data_ref - timedelta(days=dias_desde_domingo)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

def calcular_id_tempo(data_ref: datetime) -> int:
    # Se estiver nos 3 primeiros dias do mês, usa mês anterior
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

def aplicar_regra_2(base_bruta: pd.DataFrame, conn) -> pd.DataFrame:
    base = base_bruta.copy()

    query = """
    SELECT Data, Ciclo, Atividade
    FROM ismart_aux_calendario_evo
    """
    calendario = pd.read_sql(query, conn)

    if calendario.empty:
        logging.warning("Calendário vazio em ismart_aux_calendario_evo.")
        return base

    calendario["Data"] = pd.to_datetime(calendario["Data"])
    calendario = calendario.sort_values("Data").reset_index(drop=True)

    hoje = pd.Timestamp.today().normalize()
    calendario_valido = calendario[calendario["Data"] <= hoje]

    if calendario_valido.empty:
        logging.warning("Nenhuma linha válida no calendário até hoje.")
        return base

    ultima_linha_valida = calendario_valido.iloc[-1]
    linha = ultima_linha_valida.name

    dict_colunas_modulos = {
        "Módulo 1": ["Progresso até a meta no módulo 1", "Progresso geral no módulo 1"],
        "Módulo 2": ["Progresso até a meta no módulo 2", "Progresso geral no módulo 2"],
        "Módulo 3": ["Progresso até a meta no módulo 3", "Progresso geral no módulo 3"],
        "Desafio": ["Desafio"],
    }

    semana_atual = hoje - pd.Timedelta(days=(hoje.weekday() + 1) % 7)

    if ultima_linha_valida["Data"].date() >= (semana_atual.date() - timedelta(days=1)):
        for i in range(linha + 1, len(calendario)):
            ciclo = calendario.loc[i, "Ciclo"]
            atividade = calendario.loc[i, "Atividade"]

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

def montar_dataframe_final(base_bruta: pd.DataFrame, conn) -> pd.DataFrame:
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

    base = aplicar_regra_1(base_bruta)
    base = aplicar_regra_2(base, conn)

    base["RA"] = base["RA"].astype(str).str.strip()

    matriculas = buscar_matriculas_mais_recentes(conn)

    base = base.merge(
        matriculas,
        how="left",
        left_on="RA",
        right_on="ra",
    )

    agora = datetime.now()
    semana = domingo_anterior(agora)
    id_tempo = calcular_id_tempo(agora)

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
                lambda x: 1 if pd.notna(x) and x > 1 else 0
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

    # evita duplicar a mesma semana em reprocessamentos
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

    cursor.execute(delete_sql, semana_ref)

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
            int(row["desafio_ajustado"]),
            int(row["selecionou_carreira"]),
        )

    conn.commit()
    cursor.close()

def processar_iol_evolucional(base_bruta: pd.DataFrame, file_name: str = None):
    conn = None

    try:
        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final(base_bruta, conn)
        df_final = df_final[~df_final['id_matricula'].isna()]
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