import logging
import re
import unicodedata
from datetime import datetime
from typing import Optional

import pandas as pd

from shared_job_helpers import (
    buscar_matriculas_mais_recentes as buscar_matriculas_mais_recentes_base,
    calcular_id_tempo as calcular_id_tempo_base,
    extrair_data_do_nome_arquivo as extrair_data_do_nome_arquivo_base,
    get_connection_sqlserver,
    normalizar_numerico as normalizar_numerico_base,
    normalizar_texto as normalizar_texto_base,
    remover_linhas_sem_identificador as remover_linhas_sem_identificador_base,
    validar_colunas_obrigatorias as validar_colunas_obrigatorias_base,
)


# =========================================================
# CONFIG
# =========================================================
EXCEL_ENGINE = "openpyxl"


# =========================================================
# CONEXÃO SQL SERVER
# Mantido no mesmo padrão do código anterior,
# mas usando variáveis de ambiente por segurança.
# =========================================================
def _get_connection_sqlserver():
    return get_connection_sqlserver()


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        colunas_identificadoras=("id_matricula",),
        contexto_log="iol_redacao",
    )


# =========================================================
# HELPERS DE DATA
# =========================================================
def extrair_data_do_nome_arquivo(file_name: str) -> datetime:
    return extrair_data_do_nome_arquivo_base(file_name)


def calcular_id_tempo(data_ref: datetime) -> int:
    return calcular_id_tempo_base(data_ref)


# =========================================================
# HELPERS DE NORMALIZAÇÃO
# =========================================================
def normalizar_texto(serie: pd.Series) -> pd.Series:
    return normalizar_texto_base(serie)


def normalizar_numerico(serie: pd.Series) -> pd.Series:
    return normalizar_numerico_base(serie)


def normalizar_data(serie: pd.Series) -> pd.Series:
    return pd.to_datetime(serie, errors="coerce", dayfirst=True)


def normalizar_inteiro(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(
        normalizar_texto_base(serie)
        .str.replace(".", "", regex=False)
        .str.replace(",", "", regex=False),
        errors="coerce",
    )


def normalizar_nome(serie: pd.Series) -> pd.Series:
    """
    Normaliza nome para facilitar matching com a tabela auxiliar.
    """
    return normalizar_texto_base(serie, lowercase=True)


def chave_coluna(nome: str) -> str:
    nome_normalizado = unicodedata.normalize("NFKD", str(nome))
    nome_sem_acento = "".join(
        caractere for caractere in nome_normalizado if not unicodedata.combining(caractere)
    )
    return " ".join(nome_sem_acento.lower().strip().split())


def padronizar_colunas_letrus(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    mapa_colunas = {
        "nome do estudante": "Nome do Estudante",
        "id externo estudante": "ID Externo Estudante",
        "id estudante": "ID Estudante",
        "id redacao": "id_redacao",
        "data de inicio": "Data de inicio",
        "data de termino": "Data de termino",
        "genero": "Genero",
        "nota da c1": "Nota da C1",
        "nota da c2": "Nota da C2",
        "nota da c3": "Nota da C3",
        "nota da c4": "Nota da C4",
        "nota da c5": "Nota da C5",
        "nota da c6": "Nota da C6",
        "nota_final": "nota_final",
        "motivo de zeramento": "Motivo de zeramento",
        "atividade": "Atividade",
        "atividade / tipo": "Atividade",
        "nome da atividade": "Atividade",
        "atividade mes": "Atividade",
        "descricao da atividade": "Atividade",
    }

    renomear = {}
    for coluna_atual in base.columns:
        coluna_padronizada = mapa_colunas.get(chave_coluna(coluna_atual))
        if coluna_padronizada and coluna_atual != coluna_padronizada and coluna_padronizada not in base.columns:
            renomear[coluna_atual] = coluna_padronizada

    if renomear:
        base = base.rename(columns=renomear)

    return base


# =========================================================
# REGRAS DE ATIVIDADE
# =========================================================
def corrigir_atvdd(string: str) -> str:
    if pd.isna(string):
        return string

    string = str(string).strip()

    atividades = [
        "Atividade Fevereiro",
        "Atividade Março",
        "Atividade Abril",
        "Atividade Maio",
        "Atividade Junho",
        "Atividade Julho",
        "Atividade Agosto",
        "Atividade Setembro",
        "Atividade Outubro",
        "Atividade Novembro",
        "Atividade Dezembro",
    ]

    tipos = ["Reescrita", "Escrita"]

    string = string.replace("ENEM", "")
    string = " ".join(string.split())

    string = string.replace("Atividade 3 - Visibilidade da Mulher na Ci?ncia", "Atividade Abril")
    string = string.replace("Atividade 3 - Visibilidade da Mulher na Ciência", "Atividade Abril")
    string = string.replace("Atividade 01", "Atividade Fevereiro")
    string = string.replace("Atividade 02", "Atividade Março")

    classificacao = None
    if re.search(r"reescrita", string, flags=re.IGNORECASE):
        classificacao = "Reescrita"
    elif re.search(r"escrita", string, flags=re.IGNORECASE):
        classificacao = "Escrita"

    for atividade in atividades:
        if atividade in string:
            if classificacao:
                return f"{atividade} - {classificacao}"
            for tipo in tipos:
                if tipo in string:
                    return f"{atividade} - {tipo}"

    return string


def separar_atividade_mes_classificacao(valor: str) -> tuple[Optional[str], Optional[str]]:
    """
    Exemplo de entrada esperada após correção:
    'Atividade Março - Escrita'
    """
    if pd.isna(valor):
        return None, None

    valor = str(valor).strip()
    if not valor:
        return None, None

    if " - " in valor:
        partes = valor.split(" - ", 1)
        return partes[0].strip() or None, partes[1].strip() or None

    # fallback caso venha só um dos lados
    if valor in {"Escrita", "Reescrita"}:
        return None, valor

    if valor.startswith("Atividade "):
        return valor, None

    return valor, None


def encontrar_coluna_atividade(df: pd.DataFrame) -> Optional[str]:
    return "Atividade" if "Atividade" in df.columns else None


# =========================================================
# VALIDAÇÃO DE COLUNAS
# =========================================================
def garantir_colunas(df: pd.DataFrame, colunas_esperadas: list[str]) -> pd.DataFrame:
    base = df.copy()
    colunas_ausentes = [col for col in colunas_esperadas if col not in base.columns]

    for coluna in colunas_ausentes:
        base[coluna] = None

    if colunas_ausentes:
        logging.warning(
            "Colunas ausentes na base da Letrus foram criadas vazias: %s",
            colunas_ausentes,
        )

    return base


def validar_colunas_obrigatorias(df: pd.DataFrame):
    colunas_obrigatorias = [
        "Nome do Estudante",
        "ID Externo Estudante",
        "ID Estudante",
        "Data de inicio",
        "Data de termino",
        "Genero",
        "Nota da C1",
        "Nota da C2",
        "Nota da C3",
        "Nota da C4",
        "Nota da C5",
        "Nota da C6",
        "nota_final",
        "Motivo de zeramento",
    ]

    validar_colunas_obrigatorias_base(
        df,
        colunas_obrigatorias,
        origem="a base bruta da Letrus",
    )


# =========================================================
# CONSULTAS AUXILIARES
# =========================================================
def buscar_matriculas_mais_recentes(conn) -> pd.DataFrame:
    return buscar_matriculas_mais_recentes_base(conn)


def buscar_ra_por_nome(conn) -> pd.DataFrame:
    """
    Busca nome -> RA a partir da data_facts_ismart_aluno_complemento.
    """
    query = """
    SELECT
        CAST(nome AS VARCHAR(255)) AS nome,
        CAST(ra AS VARCHAR(100)) AS ra
    FROM data_facts_ismart_aluno_complemento
    WHERE nome IS NOT NULL
      AND ra IS NOT NULL
    """

    df = pd.read_sql(query, conn)
    df["nome_normalizado"] = normalizar_nome(df["nome"])
    df["ra_lookup_nome"] = df["ra"].astype(str).str.strip()

    # remove duplicidades de nome, mantendo o primeiro encontrado
    df = df.dropna(subset=["nome_normalizado"]).drop_duplicates(
        subset=["nome_normalizado"], keep="first"
    )

    return df[["nome_normalizado", "ra_lookup_nome"]]


# =========================================================
# LEITURA BASE BRUTA
# =========================================================
def ler_base_bruta_excel(caminho_arquivo: str, sheet_name=0) -> pd.DataFrame:
    return pd.read_excel(caminho_arquivo, sheet_name=sheet_name, engine=EXCEL_ENGINE)


# =========================================================
# MONTA DATAFRAME FINAL
# =========================================================
def montar_dataframe_final_letrus(
    base_bruta: pd.DataFrame,
    conn,
    file_name: str,
) -> pd.DataFrame:
    """
    Monta o DataFrame final para a tabela iol_redacao.
    """
    colunas_esperadas = [
        "Nome do Estudante",
        "ID Externo Estudante",
        "ID Estudante",
        "id_redacao",
        "Data de inicio",
        "Data de termino",
        "Genero",
        "Nota da C1",
        "Nota da C2",
        "Nota da C3",
        "Nota da C4",
        "Nota da C5",
        "Nota da C6",
        "nota_final",
        "Motivo de zeramento",
    ]

    semana = extrair_data_do_nome_arquivo(file_name)
    id_tempo = calcular_id_tempo(semana)

    base = padronizar_colunas_letrus(base_bruta)
    base = garantir_colunas(base, colunas_esperadas)
    validar_colunas_obrigatorias(base)

    # -----------------------------
    # Normalizações básicas
    # -----------------------------
    base["Nome do Estudante"] = normalizar_texto(base["Nome do Estudante"])
    base["nome_normalizado"] = normalizar_nome(base["Nome do Estudante"])

    base["ID Externo Estudante"] = normalizar_texto(base["ID Externo Estudante"])
    base["ID Estudante"] = normalizar_texto(base["ID Estudante"])
    base["Data de inicio"] = normalizar_data(base["Data de inicio"])
    base["Data de termino"] = normalizar_data(base["Data de termino"])
    base["Genero"] = normalizar_texto(base["Genero"])
    base["Motivo de zeramento"] = normalizar_texto(base["Motivo de zeramento"])

    notas_cols = [
        "Nota da C1",
        "Nota da C2",
        "Nota da C3",
        "Nota da C4",
        "Nota da C5",
        "Nota da C6",
        "nota_final",
    ]
    for col in notas_cols:
        base[col] = normalizar_numerico(base[col])

    # -----------------------------
    # Regra de RA:
    # 1) tenta por nome
    # 2) fallback para ID Externo Estudante
    # -----------------------------
    ra_por_nome = buscar_ra_por_nome(conn)

    base = base.merge(
        ra_por_nome,
        how="left",
        on="nome_normalizado",
    )

    base["ra"] = base["ra_lookup_nome"]
    base.loc[base["ra"].isna(), "ra"] = base.loc[base["ra"].isna(), "ID Externo Estudante"]

    # -----------------------------
    # Busca id_matricula mais recente
    # -----------------------------
    matriculas = buscar_matriculas_mais_recentes(conn)

    base = base.merge(
        matriculas,
        how="left",
        on="ra",
    )

    # -----------------------------
    # Atividade mês / classificação
    # -----------------------------
    coluna_atividade = encontrar_coluna_atividade(base)

    if coluna_atividade:
        base[coluna_atividade] = normalizar_texto(base[coluna_atividade])
        base["atividade_tratada"] = base[coluna_atividade].apply(corrigir_atvdd)

        atividade_split = base["atividade_tratada"].apply(separar_atividade_mes_classificacao)
        base["atividade_mes"] = atividade_split.apply(lambda x: x[0])
        base["atividade_classificacao"] = atividade_split.apply(lambda x: x[1])
    else:
        logging.warning(
            "Nenhuma coluna de atividade encontrada. "
            "atividade_mes e atividade_classificacao serão gravadas como NULL."
        )
        base["atividade_mes"] = None
        base["atividade_classificacao"] = None

    # -----------------------------
    # DataFrame final
    # -----------------------------
    final = pd.DataFrame(
        {
            "id_matricula": pd.to_numeric(base["id_matricula"], errors="coerce"),
            "id_tempo": id_tempo,
            "ra": normalizar_inteiro(base["ra"]),
            "id_estudante_letrus": normalizar_inteiro(base["ID Estudante"]),
            "id_redacao_letrus": normalizar_inteiro(base["id_redacao"]),
            "data_inicio": base["Data de inicio"],
            "data_termino": base["Data de termino"],
            "semana": semana,
            "atividade_mes": base["atividade_mes"],
            "atividade_classificacao": base["atividade_classificacao"],
            "genero_textual": base["Genero"],
            "nota_c1": base["Nota da C1"],
            "nota_c2": base["Nota da C2"],
            "nota_c3": base["Nota da C3"],
            "nota_c4": base["Nota da C4"],
            "nota_c5": base["Nota da C5"],
            "nota_c6": base["Nota da C6"],
            "nota_final": base["nota_final"],
            "motivo_zeramento": base["Motivo de zeramento"],
        }
    )

    return final


# =========================================================
# GRAVAÇÃO NO SQL SERVER
# =========================================================
def gravar_iol_redacao(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar.")
        return

    cursor = conn.cursor()
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = """
    DELETE FROM iol_redacao
    WHERE semana = ?
    """

    insert_sql = """
    INSERT INTO iol_redacao (
        id_matricula,
        id_tempo,
        ra,
        id_estudante_letrus,
        id_redacao_letrus,
        data_inicio,
        data_termino,
        semana,
        atividade_mes,
        atividade_classificacao,
        genero_textual,
        nota_c1,
        nota_c2,
        nota_c3,
        nota_c4,
        nota_c5,
        nota_c6,
        nota_final,
        motivo_zeramento
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    linhas_inseridas = 0

    for _, row in df_final.iterrows():
        cursor.execute(
            insert_sql,
            None if pd.isna(row["id_matricula"]) else int(row["id_matricula"]),
            int(row["id_tempo"]),
            None if pd.isna(row["ra"]) else int(row["ra"]),
            None if pd.isna(row["id_estudante_letrus"]) else int(row["id_estudante_letrus"]),
            None if pd.isna(row["id_redacao_letrus"]) else int(row["id_redacao_letrus"]),
            None if pd.isna(row["data_inicio"]) else row["data_inicio"].to_pydatetime(),
            None if pd.isna(row["data_termino"]) else row["data_termino"].to_pydatetime(),
            row["semana"],
            None if pd.isna(row["atividade_mes"]) else str(row["atividade_mes"]),
            None if pd.isna(row["atividade_classificacao"]) else str(row["atividade_classificacao"]),
            None if pd.isna(row["genero_textual"]) else str(row["genero_textual"]),
            None if pd.isna(row["nota_c1"]) else float(row["nota_c1"]),
            None if pd.isna(row["nota_c2"]) else float(row["nota_c2"]),
            None if pd.isna(row["nota_c3"]) else float(row["nota_c3"]),
            None if pd.isna(row["nota_c4"]) else float(row["nota_c4"]),
            None if pd.isna(row["nota_c5"]) else float(row["nota_c5"]),
            None if pd.isna(row["nota_c6"]) else float(row["nota_c6"]),
            None if pd.isna(row["nota_final"]) else float(row["nota_final"]),
            None if pd.isna(row["motivo_zeramento"]) else str(row["motivo_zeramento"]),
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()

    logging.info("✅ Linhas inseridas em iol_redacao: %s", linhas_inseridas)


# =========================================================
# PROCESSO PRINCIPAL
# =========================================================
def processar_iol_redacao_letrus(
    base_bruta: pd.DataFrame,
    file_name: Optional[str] = None,
    remover_sem_id_matricula: bool = True,
):
    """
    Processa a carga completa da Letrus.

    Parâmetros:
    - base_bruta: DataFrame já lido do arquivo
    - file_name: nome lógico do arquivo
    - remover_sem_id_matricula:
        True  -> remove linhas sem id_matricula antes do insert
        False -> mantém e grava NULL no id_matricula
    """
    conn = None

    try:
        if not file_name:
            raise ValueError("O parâmetro file_name é obrigatório para extrair a semana.")

        conn = _get_connection_sqlserver()

        df_final = montar_dataframe_final_letrus(base_bruta, conn, file_name)

        if remover_sem_id_matricula:
            df_final = remover_linhas_sem_identificador(df_final)

        gravar_iol_redacao(df_final, conn)

        logging.info(
            "Processamento concluído com sucesso. Arquivo: %s | Linhas finais: %s",
            file_name,
            len(df_final),
        )

    except Exception as e:
        logging.exception("Erro ao processar a carga Letrus: %s", str(e))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
