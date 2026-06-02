"""Carga da base bruta do Khan (dados gerais) para iol_khan_dados_gerais.

Blob: plataformas/khan/raw_geral/row/khan_geral_bruta_YYYY-MM-DD.csv
Tabela destino: iol_khan_dados_gerais
Trigger: a cada novo arquivo no container (geralmente segunda-feira).

A base bruta nao exige tratamentos de regra de negocio - so os mapeamentos
descritos na documentacao (id_matricula via ismart_matricula, id_tempo
ajustado pelos 3 primeiros dias do mes, semana extraida do nome do arquivo).
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
COL_MINUTOS = "Minutos de aprendizagem"
COL_HAB_TRABALHADAS = "Habilidades trabalhadas"
COL_HAB_PROGRESSO = "Habilidades com progresso"
COL_HAB_PROFICIENCIA = "Habilidades para proficiência"
COL_CURSO = "curso_selecionado"


def _get_connection_sqlserver():
    return get_connection_sqlserver()


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        colunas_identificadoras=("id_matricula",),
        contexto_log="iol_khan_dados_gerais",
    )


def normalizar_ra(serie: pd.Series) -> pd.Series:
    return normalizar_texto(serie).fillna("")


def validar_colunas_obrigatorias(df: pd.DataFrame):
    validar_colunas_obrigatorias_base(
        df,
        [
            COL_RA,
            COL_EMAIL,
            COL_MINUTOS,
            COL_HAB_TRABALHADAS,
            COL_HAB_PROGRESSO,
            COL_HAB_PROFICIENCIA,
            COL_CURSO,
        ],
        origem="o CSV do Khan (dados gerais)",
    )


def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    return buscar_matriculas_mais_recentes_base(conn)


def ler_base_bruta_csv(caminho_arquivo: str) -> pd.DataFrame:
    try:
        return pd.read_csv(caminho_arquivo, encoding=CSV_ENCODING)
    except UnicodeDecodeError:
        return pd.read_csv(caminho_arquivo, encoding="latin1")


def montar_dataframe_final_dados_gerais(
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
    base[COL_MINUTOS] = normalizar_numerico(base[COL_MINUTOS])
    base[COL_HAB_TRABALHADAS] = normalizar_numerico(base[COL_HAB_TRABALHADAS])
    base[COL_HAB_PROGRESSO] = normalizar_numerico(base[COL_HAB_PROGRESSO])
    base[COL_HAB_PROFICIENCIA] = normalizar_numerico(base[COL_HAB_PROFICIENCIA])
    base[COL_CURSO] = normalizar_texto(base[COL_CURSO])

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
            "login_aluno": base[COL_EMAIL],
            "tempo_acesso": base[COL_MINUTOS],
            "habilidades_evolucao": base[COL_HAB_PROGRESSO],
            "habilidades_proficiencia": base[COL_HAB_PROFICIENCIA],
            "semana": semana,
            "habilidades_trabalhadas": base[COL_HAB_TRABALHADAS],
            "curso_khan_dados_gerais": base[COL_CURSO],
            "curso_khan_dados_gerais_alocado": None,
        }
    )

    return final


def gravar_iol_khan_dados_gerais(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar em iol_khan_dados_gerais.")
        return

    cursor = conn.cursor()
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = "DELETE FROM iol_khan_dados_gerais WHERE semana = ?"
    insert_sql = """
    INSERT INTO iol_khan_dados_gerais (
        id_matricula,
        id_tempo,
        ra,
        login_aluno,
        tempo_acesso,
        habilidades_evolucao,
        habilidades_proficiencia,
        semana,
        habilidades_trabalhadas,
        curso_khan_dados_gerais,
        curso_khan_dados_gerais_alocado
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s em iol_khan_dados_gerais", semana_ref)
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
            _s(row["login_aluno"]),
            _f(row["tempo_acesso"]),
            _f(row["habilidades_evolucao"]),
            _f(row["habilidades_proficiencia"]),
            row["semana"],
            _f(row["habilidades_trabalhadas"]),
            _s(row["curso_khan_dados_gerais"]),
            None,
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()
    logging.info("Linhas inseridas em iol_khan_dados_gerais: %s", linhas_inseridas)


def processar_iol_khan_dados_gerais(
    base_bruta: pd.DataFrame,
    file_name: Optional[str] = None,
    remover_sem_id_matricula: bool = True,
):
    conn = None
    try:
        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final_dados_gerais(base_bruta, conn, file_name)

        if remover_sem_id_matricula:
            df_final = remover_linhas_sem_identificador(df_final)

        gravar_iol_khan_dados_gerais(df_final, conn)

        logging.info(
            "Processamento Khan (dados gerais) concluido. Arquivo: %s | Linhas finais: %s",
            file_name,
            len(df_final),
        )

    except Exception as exc:
        logging.exception("Erro ao processar a carga Khan (dados gerais): %s", str(exc))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
