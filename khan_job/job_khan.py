import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from shared_job_helpers import (
    buscar_matriculas_mais_recentes as buscar_matriculas_mais_recentes_base,
    calcular_id_tempo as calcular_id_tempo_base,
    domingo_anterior as domingo_anterior_base,
    extrair_data_do_nome_arquivo as extrair_data_do_nome_arquivo_base,
    get_connection_sqlserver,
    normalizar_numerico,
    normalizar_texto,
    remover_linhas_sem_identificador as remover_linhas_sem_identificador_base,
    validar_colunas_obrigatorias as validar_colunas_obrigatorias_base,
)


# =========================================================
# CONFIG
# =========================================================
CSV_ENCODING = "utf-8"


# =========================================================
# CONEXÃO SQL SERVER
# Reaproveitado no mesmo padrão do seu código anterior
# =========================================================
def _get_connection_sqlserver():
    return get_connection_sqlserver()


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        contexto_log="iol_khan_progresso",
    )


# =========================================================
# HELPERS DE DATA
# =========================================================
def domingo_anterior(data_ref: datetime) -> datetime:
    return domingo_anterior_base(data_ref)


def calcular_id_tempo(data_ref: datetime) -> int:
    return calcular_id_tempo_base(data_ref)


def extrair_data_do_nome_arquivo(file_name: str) -> datetime:
    return extrair_data_do_nome_arquivo_base(file_name)


def formatar_coluna_progresso(semana: datetime) -> str:
    """
    A documentação diz que a coluna de progresso vem no formato:
    'AA.MM.DD CM %'

    Ex.: semana = 15/03/2026 -> '26.03.15 CM %'
    """
    return semana.strftime("%y.%m.%d") + " CM %"


# =========================================================
# HELPERS DE DADOS
# =========================================================
def normalizar_ra(serie: pd.Series) -> pd.Series:
    return normalizar_texto(serie).fillna("")


def validar_colunas_obrigatorias(df: pd.DataFrame, semana: datetime):
    col_progresso = formatar_coluna_progresso(semana)

    colunas_obrigatorias = [
        "ID SIS do Aluno",
        "Curso KA",
        "Email do Aluno",
        col_progresso,
    ]

    validar_colunas_obrigatorias_base(df, colunas_obrigatorias, origem="o CSV do Khan")


# =========================================================
# BUSCA id_matricula MAIS RECENTE
# =========================================================
def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    return buscar_matriculas_mais_recentes_base(conn)


# =========================================================
# LEITURA CSV
# =========================================================
def ler_base_bruta_csv(caminho_arquivo: str) -> pd.DataFrame:
    """
    Lê o CSV bruto do Khan.
    """
    try:
        return pd.read_csv(caminho_arquivo, encoding=CSV_ENCODING)
    except UnicodeDecodeError:
        return pd.read_csv(caminho_arquivo, encoding="latin1")


# =========================================================
# MONTA DATAFRAME FINAL
# =========================================================
def montar_dataframe_final_khan(
    base_bruta: pd.DataFrame,
    conn,
    file_name: str,
) -> pd.DataFrame:
    """
    Monta o DataFrame final conforme documentação da carga:
    - id_matricula
    - id_tempo
    - ra
    - semana
    - curso_khan_progresso
    - curso_khan_progresso_alocado
    - progresso
    - login_aluno
    """
    data_consolidacao = extrair_data_do_nome_arquivo(file_name)
    semana = domingo_anterior(data_consolidacao)
    id_tempo = calcular_id_tempo(semana)
    coluna_progresso = formatar_coluna_progresso(semana)

    validar_colunas_obrigatorias(base_bruta, semana)

    base = base_bruta.copy()

    base["ID SIS do Aluno"] = normalizar_ra(base["ID SIS do Aluno"])
    base["Curso KA"] = normalizar_texto(base["Curso KA"])
    base["Email do Aluno"] = normalizar_texto(base["Email do Aluno"])
    base[coluna_progresso] = normalizar_numerico(base[coluna_progresso])

    matriculas = buscar_matriculas_mais_recentes(conn)

    base = base.merge(
        matriculas,
        how="left",
        left_on="ID SIS do Aluno",
        right_on="ra",
    )

    final = pd.DataFrame(
        {
            "id_matricula": pd.to_numeric(base["id_matricula"], errors="coerce"),
            "id_tempo": id_tempo,
            "ra": base["ID SIS do Aluno"],
            "semana": semana,
            "curso_khan_progresso": base["Curso KA"],
            "curso_khan_progresso_alocado": None,
            "progresso": base[coluna_progresso],
            "login_aluno": base["Email do Aluno"],
        }
    )

    return final


# =========================================================
# GRAVAÇÃO NO SQL SERVER
# =========================================================
def gravar_iol_khan_progresso(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar.")
        return

    cursor = conn.cursor()
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = """
    DELETE FROM iol_khan_progresso
    WHERE semana = ?
    """

    insert_sql = """
    INSERT INTO iol_khan_progresso (
        id_matricula,
        id_tempo,
        ra,
        semana,
        curso_khan_progresso,
        curso_khan_progresso_alocado,
        progresso,
        login_aluno
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    linhas_inseridas = 0

    for _, row in df_final.iterrows():
        cursor.execute(
            insert_sql,
            None if pd.isna(row["id_matricula"]) else int(row["id_matricula"]),
            int(row["id_tempo"]),
            None if pd.isna(row["ra"]) else str(row["ra"]),
            row["semana"],
            None if pd.isna(row["curso_khan_progresso"]) else str(row["curso_khan_progresso"]),
            None if pd.isna(row["curso_khan_progresso_alocado"]) else str(row["curso_khan_progresso_alocado"]),
            None if pd.isna(row["progresso"]) else float(row["progresso"]),
            None if pd.isna(row["login_aluno"]) else str(row["login_aluno"]),
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()

    logging.info("✅ Linhas inseridas em iol_khan_progresso: %s", linhas_inseridas)


# =========================================================
# PROCESSO PRINCIPAL
# =========================================================
def processar_iol_khan_progresso(
    base_bruta: pd.DataFrame,
    file_name: Optional[str] = None,
    remover_sem_id_matricula: bool = True,
):
    """
    Processa a carga completa do Khan Progresso.

    Parâmetros:
    - caminho_arquivo: caminho físico do CSV
    - file_name: nome lógico do arquivo; se não informado, usa o basename do caminho
    - remover_sem_id_matricula:
        True  -> remove linhas sem id_matricula antes do insert
        False -> mantém e grava NULL no id_matricula
    """
    conn = None

    try:
        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final_khan(base_bruta, conn, file_name)

        if remover_sem_id_matricula:
            df_final = remover_linhas_sem_identificador(df_final)

        gravar_iol_khan_progresso(df_final, conn)

        logging.info(
            "Processamento concluído com sucesso. Arquivo: %s | Linhas finais: %s",
            file_name,
            len(df_final),
        )

    except Exception as e:
        logging.exception("Erro ao processar a carga Khan Progresso: %s", str(e))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
