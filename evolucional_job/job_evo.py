import logging
from datetime import datetime

import pandas as pd

from shared_job_helpers import (
    buscar_matriculas_mais_recentes as buscar_matriculas_mais_recentes_base,
    calcular_id_tempo as calcular_id_tempo_base,
    domingo_anterior as domingo_anterior_base,
    extrair_data_do_nome_arquivo as extrair_data_do_nome_arquivo_base,
    get_connection_sqlserver,
    normalizar_numerico as normalizar_numerico_base,
    remover_linhas_sem_identificador as remover_linhas_sem_identificador_base,
    validar_colunas_obrigatorias,
)

def _get_connection_sqlserver():
    return get_connection_sqlserver()


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        contexto_log="iol_evolucional",
    )


def extrair_data_do_nome_arquivo(file_name: str) -> datetime:
    return extrair_data_do_nome_arquivo_base(file_name)


def domingo_anterior(data_ref: datetime) -> datetime:
    return domingo_anterior_base(data_ref)


def calcular_id_tempo(data_ref: datetime) -> int:
    return calcular_id_tempo_base(data_ref)


def normalizar_numerico(serie: pd.Series) -> pd.Series:
    return normalizar_numerico_base(serie, replacements={"Sem seleção": "0"})


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
    return buscar_matriculas_mais_recentes_base(conn)


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

    validar_colunas_obrigatorias(
        base_bruta,
        colunas_esperadas,
        origem="o Excel do Evolucional",
    )

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
        df_final = remover_linhas_sem_identificador(df_final)

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
