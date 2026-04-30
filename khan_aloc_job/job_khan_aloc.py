import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd

from shared_job_helpers import (
    calcular_id_tempo as calcular_id_tempo_base,
    extrair_data_do_nome_arquivo as extrair_data_do_nome_arquivo_base,
    get_connection_sqlserver,
    normalizar_texto as normalizar_texto_base,
    remover_linhas_sem_identificador as remover_linhas_sem_identificador_base,
    validar_colunas_obrigatorias as validar_colunas_obrigatorias_base,
)


TARGET_TABLE = "iol_khan_aloc"
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


REGRAS_CURSO: dict[tuple[str, str], dict[str, int]] = {
    ("Álgebra básica", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Álgebra intermediária (parte 2)", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Aritmética", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística Avançada", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística e probabilidade", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística intermediária", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Geometria básica e medidas", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 2º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 3º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 4º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 5º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 6º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 7º Ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 8º Ano", "8º EF"): {"curso_superior": 0, "bncc": 1},
    ("Matemática EF: 9º Ano", "8º EF"): {"curso_superior": 1, "bncc": 0},
    ("Matemática EM: Álgebra 1", "8º EF"): {"curso_superior": 1, "bncc": 0},
    ("Português EF: 8º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Português EF: 9º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 5º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano (parte 3)", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano (parte 4)", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 7º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 8º ano", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 9º ano", "8º EF"): {"curso_superior": 1, "bncc": 0},
    ("Pré-álgebra", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Resolução de problemas Nível I - PMA Paraná", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Revisão de fundamentos de matemática", "8º EF"): {"curso_superior": 0, "bncc": 0},
    ("Álgebra básica", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Álgebra intermediária (parte 2)", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Aritmética", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística Avançada", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística e probabilidade", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Estatística intermediária", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Geometria básica e medidas", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 2º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 3º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 4º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 5º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 6º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 7º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 8º Ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Matemática EF: 9º Ano", "9º EF"): {"curso_superior": 0, "bncc": 1},
    ("Matemática EM: Álgebra 1", "9º EF"): {"curso_superior": 1, "bncc": 0},
    ("Português EF: 8º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Português EF: 9º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 5º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano (parte 3)", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 6º ano (parte 4)", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 7º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 8º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Prepare-se para o 9º ano", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Pré-álgebra", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Resolução de problemas Nível I - PMA Paraná", "9º EF"): {"curso_superior": 0, "bncc": 0},
    ("Revisão de fundamentos de matemática", "9º EF"): {"curso_superior": 0, "bncc": 0},
}


def _get_connection_sqlserver():
    return get_connection_sqlserver()


def extrair_data_do_nome_arquivo(file_name: str) -> date:
    return extrair_data_do_nome_arquivo_base(file_name).date()


def calcular_id_tempo(data_ref: date) -> int:
    data_base = datetime.combine(data_ref, datetime.min.time())
    return calcular_id_tempo_base(data_base)


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base.columns = [str(coluna).strip() for coluna in base.columns]
    return base


def normalizar_inteiro(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(
        serie.astype(str)
        .str.strip()
        .replace({"nan": None, "None": None, "": None})
        .str.replace(".0", "", regex=False)
        .str.replace(",", "", regex=False),
        errors="coerce",
    )


def remover_linhas_sem_identificador(df: pd.DataFrame) -> pd.DataFrame:
    return remover_linhas_sem_identificador_base(
        df,
        contexto_log=TARGET_TABLE,
    )


def converter_para_data(valor) -> Optional[date]:
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.date()
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor

    texto = str(valor).strip()
    for formato in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            pass

    convertido = pd.to_datetime(texto, dayfirst=True, errors="coerce")
    return None if pd.isna(convertido) else convertido.date()


def validar_colunas_necessarias(df: pd.DataFrame):
    base = normalizar_colunas(df)
    validar_colunas_obrigatorias_base(
        base,
        ["Data", "Nome", "Meta de domínio recomendada"],
        origem="a base bruta do Khan Aloc",
    )


def obter_semana_anterior(conn, semana_atual: date) -> Optional[date]:
    sql = f"""
        SELECT MAX(semana) AS semana_anterior
        FROM {TARGET_TABLE}
        WHERE semana < ?
    """
    row = pd.read_sql(sql, conn, params=[semana_atual]).iloc[0]
    valor = row["semana_anterior"]

    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.date()
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor

    return pd.to_datetime(valor).date()


def classificar_curso(curso_alocado: str, serie: str) -> dict[str, Optional[int]]:
    chave = (str(curso_alocado).strip(), str(serie).strip())
    if chave in REGRAS_CURSO:
        return REGRAS_CURSO[chave]
    return {"curso_superior": None, "bncc": None}


def carregar_cursos_validos(conn) -> set[str]:
    """
    Busca dinamicamente os cursos válidos a partir de iol_khan_progresso.
    Qualquer valor de 'Meta de domínio recomendada' que não estiver nesse set
    será tratado como nulo e o aluno cairá na regra da semana anterior.
    """
    sql = """
        SELECT DISTINCT curso_khan_progresso
        FROM iol_khan_progresso
        WHERE id_tempo >= 202601
          AND curso_khan_progresso IS NOT NULL
    """
    df = pd.read_sql(sql, conn)
    return set(df["curso_khan_progresso"].str.strip().dropna().tolist())


def normalizar_meta_recomendada(serie: pd.Series, cursos_validos: set[str]) -> pd.Series:
    """
    Normaliza a coluna 'Meta de domínio recomendada'.
    Retorna None para valores nulos ou que não estejam na lista de cursos válidos do SQL.
    """
    normalizada = serie.astype(str).str.strip()
    mask_invalida = ~normalizada.isin(cursos_validos) | serie.isna()
    return normalizada.mask(mask_invalida)


def carregar_dimensoes_sql(conn, id_tempo_serie: int) -> pd.DataFrame:
    sql_nome_ra = """
        SELECT
            nome,
            ra
        FROM data_facts_ismart_aluno_complemento
        WHERE nome IS NOT NULL
          AND ra IS NOT NULL
    """

    sql_ra_matricula = """
        WITH cte AS (
            SELECT
                ra,
                id_matricula,
                id_tempo,
                ROW_NUMBER() OVER (PARTITION BY ra ORDER BY id_tempo DESC) AS rn
            FROM ismart_matricula
            WHERE ra IS NOT NULL
              AND id_matricula IS NOT NULL
              AND id_tempo <= ?
        )
        SELECT
            ra,
            id_matricula
        FROM cte
        WHERE rn = 1
    """

    sql_serie = """
        SELECT
            edm.id_matricula,
            se.serie
        FROM eb_detalhamento_matricula edm
        LEFT JOIN ismart_serie se
            ON edm.id_serie = se.id_serie
    """

    df_nome_ra = pd.read_sql(sql_nome_ra, conn)
    df_ra_matricula = pd.read_sql(sql_ra_matricula, conn, params=[id_tempo_serie])
    df_serie = pd.read_sql(sql_serie, conn)

    df_lookup = (
        df_nome_ra
        .drop_duplicates(subset=["nome"])
        .merge(df_ra_matricula.drop_duplicates(subset=["ra"]), on="ra", how="left")
        .merge(df_serie.drop_duplicates(subset=["id_matricula"]), on="id_matricula", how="left")
    )

    df_lookup["nome_key"] = df_lookup["nome"].astype(str).str.strip().str.upper()
    return df_lookup[["nome_key", "ra", "id_matricula", "serie"]]


def preparar_base_atual(df_bruto: pd.DataFrame, semana_atual: date, cursos_validos: set[str]) -> pd.DataFrame:
    base = normalizar_colunas(df_bruto)
    validar_colunas_necessarias(base)
    base["data_arquivo_convertida"] = base["Data"].apply(converter_para_data)
    base = base[base["data_arquivo_convertida"] == semana_atual].copy()

    if base.empty:
        return pd.DataFrame(
            columns=["Nome", "ra", "id_matricula", "semana", "curso_alocado", "curso_superior", "bncc"]
        )

    coluna_meta = next(
        coluna for coluna in base.columns if str(coluna).strip().startswith("Meta de dom")
    )

    # 2° Tratamento: nulos reais + valores fora dos cursos válidos do SQL → tratados como nulos
    # Alunos com meta inválida são removidos aqui e cairão na regra da semana anterior (3° Tratamento)
    base[coluna_meta] = normalizar_meta_recomendada(base[coluna_meta], cursos_validos)
    base = base[base[coluna_meta].notna()].copy()

    qtd_invalidos = len(base[base[coluna_meta].isna()]) if coluna_meta in base.columns else 0
    if qtd_invalidos:
        logging.info(
            "%s alunos com meta inválida ou fora dos cursos válidos — serão replicados da semana anterior.",
            qtd_invalidos,
        )

    base["semana"] = semana_atual
    base["curso_alocado"] = base[coluna_meta].astype(str).str.strip()
    base["nome_key"] = base["Nome"].astype(str).str.strip().str.upper()

    return base


def enriquecer_com_ra_matricula_serie(df_atual: pd.DataFrame, df_lookup: pd.DataFrame) -> pd.DataFrame:
    base = df_atual.merge(df_lookup, on="nome_key", how="left")

    classificacoes = base.apply(
        lambda row: classificar_curso(row["curso_alocado"], row["serie"]),
        axis=1,
        result_type="expand",
    )

    base["curso_superior"] = classificacoes["curso_superior"]
    base["bncc"] = classificacoes["bncc"]

    sem_regra = base["curso_superior"].isna() | base["bncc"].isna()
    qtd_sem_regra = int(sem_regra.sum())
    if qtd_sem_regra:
        logging.warning(
            "Linhas com curso/serie fora da REGRAS_CURSO: %s. Serão removidas e replicadas da semana anterior.",
            qtd_sem_regra,
        )

    # Remove linhas sem classificação — bncc e curso_superior são NOT NULL no SQL Server
    # Esses alunos cairão no 3° Tratamento (replicados da semana anterior)
    return base[~sem_regra].copy()


def montar_saida_final(df_enriquecido: pd.DataFrame, semana_atual: date) -> pd.DataFrame:
    id_tempo = calcular_id_tempo(semana_atual)
    df_saida = df_enriquecido.copy()
    df_saida["id_tempo"] = id_tempo

    colunas = [
        "id_matricula",
        "id_tempo",
        "ra",
        "semana",
        "curso_alocado",
        "curso_superior",
        "bncc",
    ]

    for coluna in colunas:
        if coluna not in df_saida.columns:
            df_saida[coluna] = None

    df_saida = df_saida[colunas].copy()
    df_saida["curso_superior"] = pd.to_numeric(df_saida["curso_superior"], errors="coerce").fillna(0)
    df_saida["bncc"] = pd.to_numeric(df_saida["bncc"], errors="coerce").fillna(0)
    df_saida["id_tempo"] = pd.to_numeric(df_saida["id_tempo"], errors="coerce")
    df_saida["id_matricula"] = normalizar_inteiro(df_saida["id_matricula"])
    df_saida["ra"] = normalizar_inteiro(df_saida["ra"])

    return df_saida


def complementar_com_semana_anterior(conn, df_atual: pd.DataFrame, semana_atual: date) -> pd.DataFrame:
    semana_anterior = obter_semana_anterior(conn, semana_atual)
    if not semana_anterior:
        return df_atual

    sql_prev = f"""
        SELECT
            id_matricula,
            ra,
            semana,
            curso_alocado,
            curso_superior,
            bncc
        FROM {TARGET_TABLE}
        WHERE semana = ?
    """
    df_prev = pd.read_sql(sql_prev, conn, params=[semana_anterior])

    if df_prev.empty:
        return df_atual

    ras_atuais = set(df_atual["ra"].dropna().astype(str))
    ids_atuais = set(df_atual["id_matricula"].dropna().astype(str))

    def precisa_replicar(row):
        ra = None if pd.isna(row["ra"]) else str(row["ra"])
        id_matricula = None if pd.isna(row["id_matricula"]) else str(row["id_matricula"])

        if ra and ra not in ras_atuais:
            return True
        if (not ra) and id_matricula and id_matricula not in ids_atuais:
            return True
        return False

    df_faltantes = df_prev[df_prev.apply(precisa_replicar, axis=1)].copy()
    if df_faltantes.empty:
        return df_atual

    df_faltantes["semana"] = semana_atual

    colunas_saida = [
        "id_matricula",
        "id_tempo",
        "ra",
        "semana",
        "curso_alocado",
        "curso_superior",
        "bncc",
    ]

    if "id_tempo" not in df_faltantes.columns:
        df_faltantes["id_tempo"] = calcular_id_tempo(semana_atual)

    df_faltantes["id_matricula"] = normalizar_inteiro(df_faltantes["id_matricula"])
    df_faltantes["ra"] = normalizar_inteiro(df_faltantes["ra"])
    df_faltantes["id_tempo"] = normalizar_inteiro(df_faltantes["id_tempo"])
    df_faltantes["curso_superior"] = pd.to_numeric(df_faltantes["curso_superior"], errors="coerce")
    df_faltantes["bncc"] = pd.to_numeric(df_faltantes["bncc"], errors="coerce")

    df_final = pd.concat(
        [
            df_atual[colunas_saida].copy(),
            df_faltantes[colunas_saida].copy(),
        ],
        ignore_index=True,
    )

    return df_final.drop_duplicates(subset=["semana", "ra", "id_matricula"], keep="first")


def montar_dataframe_final_khan_aloc(
    base_bruta: pd.DataFrame,
    conn,
    file_name: str,
) -> pd.DataFrame:
    semana_atual = extrair_data_do_nome_arquivo(file_name)

    # Busca cursos válidos dinamicamente no SQL antes de qualquer tratamento
    cursos_validos = carregar_cursos_validos(conn)

    df_atual = preparar_base_atual(base_bruta, semana_atual, cursos_validos)

    if df_atual.empty:
        return pd.DataFrame(
            columns=["id_matricula", "id_tempo", "ra", "semana", "curso_alocado", "curso_superior", "bncc"]
        )

    id_tempo_referencia = calcular_id_tempo(semana_atual)
    df_lookup = carregar_dimensoes_sql(conn, id_tempo_serie=id_tempo_referencia)
    df_enriquecido = enriquecer_com_ra_matricula_serie(df_atual, df_lookup)
    df_saida_atual = montar_saida_final(df_enriquecido, semana_atual)
    return complementar_com_semana_anterior(conn, df_saida_atual, semana_atual)


def gravar_iol_khan_aloc(df_final: pd.DataFrame, conn):
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar.")
        return

    cursor = conn.cursor()
    cursor.fast_executemany = False
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = f"""
    DELETE FROM {TARGET_TABLE}
    WHERE semana = ?
    """

    insert_sql = f"""
    INSERT INTO {TARGET_TABLE} (
        id_matricula,
        id_tempo,
        ra,
        semana,
        curso_alocado,
        curso_superior,
        bncc
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    dados = []
    for _, row in df_final.iterrows():
        dados.append(
            (
                None if pd.isna(row["id_matricula"]) else int(row["id_matricula"]),
                None if pd.isna(row["id_tempo"]) else int(row["id_tempo"]),
                None if pd.isna(row["ra"]) else int(row["ra"]),
                row["semana"],
                None if pd.isna(row["curso_alocado"]) else str(row["curso_alocado"]),
                None if pd.isna(row["curso_superior"]) else int(row["curso_superior"]),
                None if pd.isna(row["bncc"]) else int(row["bncc"]),
            )
        )

    cursor.executemany(insert_sql, dados)
    conn.commit()
    cursor.close()

    logging.info("Linhas inseridas em %s: %s", TARGET_TABLE, len(dados))


def processar_iol_khan_aloc(
    base_bruta: pd.DataFrame,
    file_name: Optional[str] = None,
    remover_sem_id_matricula: bool = True,
):
    conn = None

    try:
        if not file_name:
            raise ValueError("O parâmetro file_name é obrigatório para extrair a semana.")

        df_base = normalizar_colunas(base_bruta)
        conn = _get_connection_sqlserver()
        df_final = montar_dataframe_final_khan_aloc(df_base, conn, file_name)

        if remover_sem_id_matricula:
            df_final = remover_linhas_sem_identificador(df_final)

        gravar_iol_khan_aloc(df_final, conn)

        logging.info(
            "Processamento concluído com sucesso. Arquivo: %s | Linhas finais: %s",
            file_name,
            len(df_final),
        )

    except Exception as exc:
        logging.exception("Erro ao processar a carga Khan Aloc: %s", str(exc))
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
