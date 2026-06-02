"""Carga da base bruta do Khan/Khanmigo para iol_khan_khanmigo.

Blob: plataformas/khan/raw_geral/row/khan_khanmigo_bruta_YYYY-MM-DD.csv
Tabela destino: iol_khan_khanmigo
Trigger: a cada novo arquivo no container (geralmente segunda-feira).

A base bruta não exige tratamentos de regra de negócio — só os mapeamentos
descritos na documentação (id_matricula via ismart_matricula, id_tempo
ajustado pelos 3 primeiros dias do mês, semana extraída do nome do arquivo).
"""
import logging
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


CSV_ENCODING = "utf-8-sig"


COL_RA = "Identificador do aluno"
COL_EMAIL = "E-mail do aluno"
COL_CONVERSAS = "Conversas Khanmigo"
COL_INTERACOES = "Interações Khanmigo"


def _get_connection_sqlserver():
    return get_connection_sqlserver()


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        colunas_identificadoras=("id_matricula",),
        contexto_log="iol_khan_khanmigo",
    )


def normalizar_ra(serie: pd.Series) -> pd.Series:
    return normalizar_texto(serie).fillna("")


def validar_colunas_obrigatorias(df: pd.DataFrame):
    validar_colunas_obrigatorias_base(
        df,
        [COL_RA, COL_EMAIL, COL_CONVERSAS, COL_INTERACOES],
        origem="o CSV do Khan/Khanmigo",
    )


def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    return buscar_matriculas_mais_recentes_base(conn)


def ler_base_bruta_csv(caminho_arquivo: str) -> pd.DataFrame:
    try:
        return pd.read_csv(caminho_arquivo, encoding=CSV_ENCODING)
    except UnicodeDecodeError:
        return pd.read_csv(caminho_arquivo, encoding="latin1")


def montar_dataframe_final_khanmigo(
    base_bruta: pd.DataFrame,
    conn,
    file_name: str,
) -> pd.DataFrame:
    data_consolidacao = extrair_data_do_nome_arquivo_base(file_name)
    semana = domingo_anterior_base(data_consolidacao)
    id_tempo = calcular_id_tempo_base(semana)

    validar_colunas_obrigatorias(base_bruta)

    base = base_bruta.copy()
    base[COL_RA] = normalizar_ra(base[COL_RA])
    base[COL_EMAIL] = normalizar_texto(base[COL_EMAIL])
    base[COL_CONVERSAS] = normalizar_numerico(base[COL_CONVERSAS])
    base[COL_INTERACOES] = normalizar_numerico(base[COL_INTERACOES])

    matriculas = buscar_matriculas_mais_recentes(conn)

    base = base.merge(
        matriculas,
        how="left",
        left_on=COL_RA,
        right_on="ra",
    )

    final = pd.DataFrame(
        {
            "id_matricula": pd.to_numeric(base["id_matricula"], errors="coerce"),
            "id_tempo": id_tempo,
            "ra": base[COL_RA],
            "semana": semana,
            "login_aluno": base[COL_EMAIL],
            "conversas_khanmigo_flagadas": None,
            "dias_usando_khanmigo": None,
            "conversas_khanmigo": base[COL_CONVERSAS],
            "interacoes_khanmigo": base[COL_INTERACOES],
            "khanmigo_token": None,
            "khanmigo_minutos_aprendizagem": None,
        }
    )

    return final


def gravar_iol_khan_khanmigo(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar em iol_khan_khanmigo.")
        return

    cursor = conn.cursor()
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = "DELETE FROM iol_khan_khanmigo WHERE semana = ?"
    insert_sql = """
    INSERT INTO iol_khan_khanmigo (
        id_matricula,
        id_tempo,
        ra,
        semana,
        login_aluno,
        conversas_khanmigo_flagadas,
        dias_usando_khanmigo,
        conversas_khanmigo,
        interacoes_khanmigo,
        khanmigo_token,
        khanmigo_minutos_aprendizagem
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s em iol_khan_khanmigo", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    def _i(v):
        return None if pd.isna(v) else int(v)

    def _f(v):
        return None if pd.isna(v) else float(v)

    def _s(v):
        return None if pd.isna(v) else str(v)

    linhas_inseridas = 0
    for _, row in df_final.iterrows():
        cursor.execute(
            insert_sql,
            _i(row["id_matricula"]),
            int(row["id_tempo"]),
            _s(row["ra"]),
            row["semana"],
            _s(row["login_aluno"]),
            None,
            None,
            _f(row["conversas_khanmigo"]),
            _f(row["interacoes_khanmigo"]),
            None,
            None,
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()
    logging.info("Linhas inseridas em iol_khan_khanmigo: %s", linhas_inseridas)


def processar_iol_khan_khanmigo(
    base_bruta: pd.DataFrame,
    file_name: Optional[str] = None,
    remover_sem_id_matricula: bool = True,
):
    conn = None
    try:
        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final_khanmigo(base_bruta, conn, file_name)

        if remover_sem_id_matricula:
            df_final = remover_linhas_sem_identificador(df_final)

        gravar_iol_khan_khanmigo(df_final, conn)

        logging.info(
            "Processamento Khan/Khanmigo concluído. Arquivo: %s | Linhas finais: %s",
            file_name,
            len(df_final),
        )

    except Exception as exc:
        logging.exception("Erro ao processar a carga Khan/Khanmigo: %s", str(exc))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
