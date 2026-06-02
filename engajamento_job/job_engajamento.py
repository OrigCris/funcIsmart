"""Consolidação semanal dos indicadores de engajamento IOL/ADI.

Lê as 5 tabelas de origem já carregadas (iol_khan_progresso, iol_khan_aloc,
iol_evolucional, iol_missao, iol_redacao) e a presença de formação
(iol_formacao_presenca + iol_formacao_evento), calcula os indicadores por
aluno e grava em iol_engajamento.

Trigger: segundas-feiras 15h BRT (18h UTC), após o update das origens.
Ver documentacao_iol_engajamento.pdf para a regra de negócio completa.
"""
import logging
from datetime import datetime

import pandas as pd

from shared_job_helpers import (
    calcular_id_tempo,
    domingo_anterior,
    get_connection_sqlserver,
)


SERIES_KHAN = (3, 4)         # 8º EF, 9º EF
SERIES_EVOLUCIONAL = (5, 6, 7)  # 1º EM, 2º EM, 3º EM
ID_PROJETO_IOL = 2
ID_PROJETO_ADI = 5
LIMIAR_ALTO_ENGAJAMENTO = 0.6


def _id_tempo_ano(semana: datetime) -> int:
    """Retorna o id_tempo do mês 01 do ano da semana (ex.: 2026 -> 202601).

    Usado para filtrar matrículas vigentes no ano corrente conforme a doc.
    """
    return semana.year * 100 + 1


def verificar_pre_condicao(conn, semana_esperada: datetime) -> None:
    """Garante que todas as origens estão na semana esperada.

    Levanta RuntimeError se alguma tabela estiver desatualizada — o Azure
    marca a execução como falha e o alerta sobe via Application Insights.
    """
    query = """
    SELECT 'iol_missao' AS tabela, MAX(semana) AS ultima_semana FROM iol_missao
    UNION ALL SELECT 'iol_redacao',        MAX(semana) FROM iol_redacao
    UNION ALL SELECT 'iol_evolucional',    MAX(semana) FROM iol_evolucional
    UNION ALL SELECT 'iol_khan_aloc',      MAX(semana) FROM iol_khan_aloc
    UNION ALL SELECT 'iol_khan_progresso', MAX(semana) FROM iol_khan_progresso;
    """
    df = pd.read_sql(query, conn)
    df["ultima_semana"] = pd.to_datetime(df["ultima_semana"]).dt.date

    esperada = semana_esperada.date()
    atrasadas = df[df["ultima_semana"] != esperada]

    if not atrasadas.empty:
        detalhes = ", ".join(
            f"{row.tabela}={row.ultima_semana}"
            for row in atrasadas.itertuples()
        )
        msg = (
            f"Pré-condição falhou: tabelas com MAX(semana) diferente de "
            f"{esperada}. {detalhes}"
        )
        logging.error(msg)
        raise RuntimeError(msg)

    logging.info("Pré-condição OK: todas as origens em %s", esperada)


def buscar_base_alunos(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Base IOL/ADI do ano corrente com id_serie e id_projeto."""
    query = """
    SELECT
        m.id_matricula,
        m.ra,
        edm.id_serie,
        m.id_projeto
    FROM ismart_matricula m
    LEFT JOIN eb_detalhamento_matricula edm
        ON m.id_matricula = edm.id_matricula
    WHERE m.id_tempo = ?
      AND m.id_projeto IN (2, 5);
    """
    df = pd.read_sql(query, conn, params=[id_tempo_ano])
    df["ra"] = df["ra"].astype(str).str.strip()
    return df


def query_engajamento_khan(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Engajamento Khan na escala 0–100 (normalizar para 0–1 depois)."""
    query = """
    WITH base_alunos AS (
        SELECT
            m.id_matricula,
            m.ra,
            edm.id_serie
        FROM ismart_matricula m
        LEFT JOIN eb_detalhamento_matricula edm
            ON m.id_matricula = edm.id_matricula
        WHERE m.id_tempo = ?
          AND m.id_projeto IN (2, 5)
          AND edm.id_serie IN (3, 4)
    ),
    ultima_semana AS (
        SELECT MAX(semana) AS semana FROM iol_khan_progresso
    ),
    khan_filtrado AS (
        SELECT
            kp.id_matricula,
            kp.semana,
            kp.curso_khan_progresso,
            kp.progresso
        FROM iol_khan_progresso kp
        CROSS JOIN ultima_semana us
        WHERE kp.semana = us.semana
    ),
    khan_com_aloc AS (
        SELECT
            kf.id_matricula,
            kf.semana,
            kf.curso_khan_progresso,
            kf.progresso,
            ka.curso_superior,
            ka.bncc
        FROM khan_filtrado kf
        INNER JOIN iol_khan_aloc ka
            ON kf.id_matricula = ka.id_matricula
           AND kf.semana = ka.semana
           AND kf.curso_khan_progresso = ka.curso_alocado
    )
    SELECT
        b.id_matricula,
        CASE
            WHEN k.curso_khan_progresso IS NULL THEN 0
            WHEN k.curso_superior = 1 THEN 100
            WHEN k.bncc = 0 THEN 0
            ELSE TRY_CAST(k.progresso AS FLOAT)
        END AS engajamento_khan
    FROM base_alunos b
    LEFT JOIN khan_com_aloc k
        ON b.id_matricula = k.id_matricula;
    """
    return pd.read_sql(query, conn, params=[id_tempo_ano])


def query_engajamento_evolucional(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Engajamento Evolucional na escala 0–100 (normalizar para 0–1 depois)."""
    query = """
    WITH ultima_semana AS (
        SELECT MAX(semana) AS semana FROM iol_evolucional
    ),
    denominador AS (
        SELECT COUNT(*) AS qtd
        FROM ismart_aux_calendario_evo c
        WHERE c.Data < (SELECT semana FROM ultima_semana)
    ),
    base_alunos AS (
        SELECT
            m.id_matricula,
            m.ra
        FROM ismart_matricula m
        LEFT JOIN eb_detalhamento_matricula edm
            ON m.id_matricula = edm.id_matricula
        WHERE m.id_tempo = ?
          AND m.id_projeto IN (2, 5)
          AND edm.id_serie IN (5, 6, 7)
    ),
    base_evolucional AS (
        SELECT
            e.id_matricula,
            e.ra,
            e.progresso_modulo_1,
            e.progresso_modulo_2,
            e.progresso_modulo_3,
            e.desafio_ajustado
        FROM iol_evolucional e
        WHERE e.semana = (SELECT semana FROM ultima_semana)
    )
    SELECT
        b.ra,
        SUM(
            ISNULL(e.progresso_modulo_1, 0) +
            ISNULL(e.progresso_modulo_2, 0) +
            ISNULL(e.progresso_modulo_3, 0) +
            ISNULL(e.desafio_ajustado, 0)
        ) * 1.0 / d.qtd AS engajamento_evolucional
    FROM base_alunos b
    LEFT JOIN base_evolucional e
        ON b.id_matricula = e.id_matricula
    CROSS JOIN denominador d
    GROUP BY b.ra, d.qtd;
    """
    df = pd.read_sql(query, conn, params=[id_tempo_ano])
    df["ra"] = df["ra"].astype(str).str.strip()
    return df


def query_engajamento_missao(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Engajamento missão (já em 0–1)."""
    query = """
    WITH ultima_semana AS (
        SELECT MAX(semana) AS semana FROM iol_missao
    ),
    base_alunos AS (
        SELECT
            m.id_matricula,
            m.ra,
            edm.id_serie,
            m.id_projeto
        FROM ismart_matricula m
        LEFT JOIN eb_detalhamento_matricula edm
            ON m.id_matricula = edm.id_matricula
        WHERE m.id_tempo = ?
          AND m.id_projeto IN (2, 5)
    ),
    missoes_feitas AS (
        SELECT
            mi.ra,
            COUNT(*) AS total_feitas
        FROM iol_missao mi
        CROSS JOIN ultima_semana us
        WHERE mi.semana = us.semana
        GROUP BY mi.ra
    ),
    cobradas AS (
        SELECT
            am.semana,
            am.id_serie,
            am.id_projeto,
            am.missoes_cobradas
        FROM iol_aux_miss_red am
        CROSS JOIN ultima_semana us
        WHERE am.semana = us.semana
    )
    SELECT
        b.ra,
        CASE
            WHEN c.missoes_cobradas IS NULL OR c.missoes_cobradas = 0 THEN NULL
            WHEN ISNULL(mf.total_feitas, 0) >= c.missoes_cobradas THEN 1.0
            ELSE ISNULL(mf.total_feitas, 0) * 1.0 / c.missoes_cobradas
        END AS engajamento_missao
    FROM base_alunos b
    LEFT JOIN missoes_feitas mf
        ON b.ra = mf.ra
    LEFT JOIN cobradas c
        ON c.id_serie = b.id_serie
       AND c.id_projeto = b.id_projeto;
    """
    df = pd.read_sql(query, conn, params=[id_tempo_ano])
    df["ra"] = df["ra"].astype(str).str.strip()
    return df


def query_engajamento_redacao(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Engajamento redação (já em 0–1).

    Atividade só conta quando há pelo menos 1 Escrita E 1 Reescrita
    no mesmo atividade_mes. Zeros por cópia/plágio são descartados;
    zeros por outros motivos (texto insuficiente, fuga ao tema, etc.)
    contam como engajamento — baixo desempenho ≠ desengajamento.
    """
    query = """
    WITH ultima_semana AS (
        SELECT MAX(semana) AS semana FROM iol_redacao
    ),
    base_alunos AS (
        SELECT
            m.id_matricula,
            m.ra,
            edm.id_serie,
            m.id_projeto
        FROM ismart_matricula m
        LEFT JOIN eb_detalhamento_matricula edm
            ON m.id_matricula = edm.id_matricula
        WHERE m.id_tempo = ?
          AND m.id_projeto IN (2, 5)
    ),
    redacao_filtrada AS (
        -- Underscore (_) no LIKE casa 1 caractere — pega "copia"/"cópia"
        -- e "plagio"/"plágio" sem precisar normalizar acento.
        SELECT
            r.ra,
            r.atividade_mes,
            r.atividade_classificacao
        FROM iol_redacao r
        CROSS JOIN ultima_semana us
        WHERE r.semana = us.semana
          AND NOT (
                LOWER(r.motivo_zeramento) LIKE '%c_pia%'
             OR LOWER(r.motivo_zeramento) LIKE '%pl_gio%'
          )
    ),
    atividades_completas AS (
        SELECT
            ra,
            atividade_mes
        FROM redacao_filtrada
        GROUP BY ra, atividade_mes
        HAVING SUM(CASE WHEN atividade_classificacao = 'Escrita'   THEN 1 ELSE 0 END) >= 1
           AND SUM(CASE WHEN atividade_classificacao = 'Reescrita' THEN 1 ELSE 0 END) >= 1
    ),
    redacoes_feitas AS (
        SELECT
            ra,
            COUNT(*) AS total_feitas
        FROM atividades_completas
        GROUP BY ra
    ),
    cobradas AS (
        SELECT
            am.semana,
            am.id_serie,
            am.id_projeto,
            am.redacoes_cobradas
        FROM iol_aux_miss_red am
        CROSS JOIN ultima_semana us
        WHERE am.semana = us.semana
    )
    SELECT
        b.ra,
        CASE
            WHEN c.redacoes_cobradas IS NULL OR c.redacoes_cobradas = 0 THEN NULL
            WHEN ISNULL(rf.total_feitas, 0) >= c.redacoes_cobradas THEN 1.0
            ELSE ISNULL(rf.total_feitas, 0) * 1.0 / c.redacoes_cobradas
        END AS engajamento_redacao
    FROM base_alunos b
    LEFT JOIN redacoes_feitas rf
        ON b.ra = rf.ra
    LEFT JOIN cobradas c
        ON c.id_serie = b.id_serie
       AND c.id_projeto = b.id_projeto;
    """
    df = pd.read_sql(query, conn, params=[id_tempo_ano])
    df["ra"] = df["ra"].astype(str).str.strip()
    return df


def query_engajamento_formacao(conn, id_tempo_ano: int) -> pd.DataFrame:
    """Engajamento formação (já em 0–1). ADI recebe NULL aqui."""
    query = """
    WITH base_alunos AS (
        SELECT
            m.id_matricula,
            m.ra,
            m.id_projeto
        FROM ismart_matricula m
        WHERE m.id_tempo = ?
          AND m.id_projeto IN (2, 5)
    ),
    presenca_ano AS (
        SELECT
            ifp.ra,
            ifp.esteve_presente,
            ifp.justificativa_ausente
        FROM iol_formacao_presenca ifp
        INNER JOIN iol_formacao_evento ife
            ON ifp.id_iol_formacao_evento = ife.id_iol_formacao_evento
        WHERE ife.id_tempo >= ?
    ),
    formacao_agregada AS (
        SELECT
            ra,
            COUNT(*) AS total_eventos,
            SUM(
                CASE
                    WHEN esteve_presente = 1 THEN 1
                    WHEN justificativa_ausente IN ('Justificativa Aceita', 'Faltou, mas justificou') THEN 1
                    ELSE 0
                END
            ) AS total_validos
        FROM presenca_ano
        GROUP BY ra
    )
    SELECT
        b.ra,
        CASE
            WHEN b.id_projeto = 5 THEN NULL
            WHEN fa.total_eventos IS NULL OR fa.total_eventos = 0 THEN 0
            ELSE fa.total_validos * 1.0 / fa.total_eventos
        END AS engajamento_formacao
    FROM base_alunos b
    LEFT JOIN formacao_agregada fa
        ON b.ra = fa.ra;
    """
    df = pd.read_sql(query, conn, params=[id_tempo_ano, id_tempo_ano])
    df["ra"] = df["ra"].astype(str).str.strip()
    return df


def _engajamento_plataforma(row: pd.Series) -> float:
    serie = row["id_serie"]
    if serie in SERIES_KHAN:
        return row["engajamento_khan"]
    if serie in SERIES_EVOLUCIONAL:
        return row["engajamento_evolucional"]
    return None


def _media_engajamento(row: pd.Series) -> float:
    valores = [
        row["engajamento_plataforma"],
        row["engajamento_missao"],
        row["engajamento_redacao"],
        row["engajamento_formacao"],
    ]
    nao_nulos = [v for v in valores if pd.notna(v)]
    if not nao_nulos:
        return None
    return sum(nao_nulos) / len(nao_nulos)


def _flag_alto(valor) -> int:
    if pd.isna(valor):
        return None
    return 1 if valor >= LIMIAR_ALTO_ENGAJAMENTO else 0


def montar_dataframe_final(
    base: pd.DataFrame,
    khan: pd.DataFrame,
    evolucional: pd.DataFrame,
    missao: pd.DataFrame,
    redacao: pd.DataFrame,
    formacao: pd.DataFrame,
    semana: datetime,
    id_tempo: int,
) -> pd.DataFrame:
    df = base.copy()

    df = df.merge(khan, how="left", on="id_matricula")
    df = df.merge(evolucional, how="left", on="ra")
    df = df.merge(missao, how="left", on="ra")
    df = df.merge(redacao, how="left", on="ra")
    df = df.merge(formacao, how="left", on="ra")

    # Normalização para 0–1 (khan e evolucional vêm em 0–100)
    df["engajamento_khan"] = pd.to_numeric(df["engajamento_khan"], errors="coerce") / 100.0
    df["engajamento_evolucional"] = pd.to_numeric(df["engajamento_evolucional"], errors="coerce") / 100.0
    df["engajamento_missao"] = pd.to_numeric(df["engajamento_missao"], errors="coerce")
    df["engajamento_redacao"] = pd.to_numeric(df["engajamento_redacao"], errors="coerce")
    df["engajamento_formacao"] = pd.to_numeric(df["engajamento_formacao"], errors="coerce")

    df["engajamento_plataforma"] = df.apply(_engajamento_plataforma, axis=1)
    df["engajamento_redacao_old"] = None
    df["media_engajamento"] = df.apply(_media_engajamento, axis=1)

    df["plataforma_alto_engajado"] = df["engajamento_plataforma"].apply(_flag_alto)
    df["missao_alto_engajado"] = df["engajamento_missao"].apply(_flag_alto)
    df["redacao_alto_engajado"] = df["engajamento_redacao"].apply(_flag_alto)

    df["semana"] = semana
    df["id_tempo"] = id_tempo

    return df[[
        "id_matricula",
        "id_tempo",
        "ra",
        "semana",
        "engajamento_khan",
        "engajamento_evolucional",
        "engajamento_plataforma",
        "engajamento_missao",
        "engajamento_redacao_old",
        "engajamento_redacao",
        "engajamento_formacao",
        "media_engajamento",
        "plataforma_alto_engajado",
        "missao_alto_engajado",
        "redacao_alto_engajado",
    ]]


def gravar_iol_engajamento(df_final: pd.DataFrame, conn) -> None:
    if df_final.empty:
        logging.warning("DataFrame final vazio. Nada para gravar em iol_engajamento.")
        return

    cursor = conn.cursor()
    semana_ref = df_final["semana"].iloc[0]

    delete_sql = "DELETE FROM iol_engajamento WHERE semana = ?"
    insert_sql = """
    INSERT INTO iol_engajamento (
        id_matricula,
        id_tempo,
        ra,
        semana,
        engajamento_khan,
        engajamento_evolucional,
        engajamento_plataforma,
        engajamento_missao,
        engajamento_redacao_old,
        engajamento_redacao,
        engajamento_formacao,
        media_engajamento,
        plataforma_alto_engajado,
        missao_alto_engajado,
        redacao_alto_engajado_old
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logging.info("Removendo dados existentes da semana %s em iol_engajamento", semana_ref)
    cursor.execute(delete_sql, semana_ref)

    def _f(v):
        return None if pd.isna(v) else float(v)

    def _i(v):
        return None if pd.isna(v) else int(v)

    linhas_inseridas = 0
    for _, row in df_final.iterrows():
        cursor.execute(
            insert_sql,
            _i(row["id_matricula"]),
            int(row["id_tempo"]),
            str(row["ra"]),
            row["semana"],
            _f(row["engajamento_khan"]),
            _f(row["engajamento_evolucional"]),
            _f(row["engajamento_plataforma"]),
            _f(row["engajamento_missao"]),
            None,  # engajamento_redacao_old: sempre NULL (campo legado)
            _f(row["engajamento_redacao"]),
            _f(row["engajamento_formacao"]),
            _f(row["media_engajamento"]),
            _i(row["plataforma_alto_engajado"]),
            _i(row["missao_alto_engajado"]),
            _i(row["redacao_alto_engajado"]),
        )
        linhas_inseridas += 1

    conn.commit()
    cursor.close()
    logging.info("Linhas inseridas em iol_engajamento: %s", linhas_inseridas)


def run() -> None:
    """Orquestra a consolidação semanal de iol_engajamento."""
    conn = None
    try:
        agora = datetime.now()
        semana = domingo_anterior(agora)
        id_tempo = calcular_id_tempo(semana)
        id_tempo_ano = _id_tempo_ano(semana)

        logging.info(
            "Iniciando consolidação iol_engajamento. semana=%s id_tempo=%s id_tempo_ano=%s",
            semana.date(), id_tempo, id_tempo_ano,
        )

        conn = get_connection_sqlserver()

        verificar_pre_condicao(conn, semana)

        base = buscar_base_alunos(conn, id_tempo_ano)
        if base.empty:
            logging.warning("Nenhum aluno IOL/ADI encontrado para id_tempo=%s.", id_tempo_ano)
            return

        khan = query_engajamento_khan(conn, id_tempo_ano)
        evolucional = query_engajamento_evolucional(conn, id_tempo_ano)
        missao = query_engajamento_missao(conn, id_tempo_ano)
        redacao = query_engajamento_redacao(conn, id_tempo_ano)
        formacao = query_engajamento_formacao(conn, id_tempo_ano)

        df_final = montar_dataframe_final(
            base, khan, evolucional, missao, redacao, formacao,
            semana=semana,
            id_tempo=id_tempo,
        )

        gravar_iol_engajamento(df_final, conn)

        logging.info(
            "Consolidação iol_engajamento concluída. semana=%s linhas=%s",
            semana.date(), len(df_final),
        )

    except Exception as exc:
        logging.exception("Erro na consolidação iol_engajamento: %s", str(exc))
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
