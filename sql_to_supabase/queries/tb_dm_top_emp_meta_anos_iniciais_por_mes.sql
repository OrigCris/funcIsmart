WITH filtro_tempo AS (
    SELECT
        id_matricula,
        ra,
        id_tempo,
        id_es_status_meta,
        top_empresa
    FROM es_status_meta_mensal
    WHERE id_tempo >= 202401
),
com_status AS (
    -- Junta com a tabela de meta de status e já filtra id_es_status_meta = 2
    SELECT 
        f.*,
        s.status_meta
    FROM filtro_tempo AS f
    INNER JOIN es_status_meta AS s
        ON f.id_es_status_meta = s.id_es_status_meta
    WHERE f.id_es_status_meta = 2
),
agregado AS (
    -- Agrupa por id_tempo e calcula total, qtd com top_empresa = '1' e percentual
    SELECT
        id_tempo,
        COUNT(*) AS total_alunos,
        SUM(CASE WHEN top_empresa = '1' THEN 1 ELSE 0 END) AS qtd_top_empresa,
        ROUND(
            CAST(SUM(CASE WHEN top_empresa = '1' THEN 1 ELSE 0 END) AS FLOAT)
            / NULLIF(COUNT(*), 0) * 100.0,
            2
        ) AS perc_top_empresa
    FROM com_status
    GROUP BY id_tempo
)
-- Resultado final com informações da tabela tempo
SELECT
    a.id_tempo,
    t.ano,
    t.mes,
    t.mes_nome,
    t.semestre,
    t.trimestre,
    t.periodo_letivo,
    a.total_alunos,
    a.qtd_top_empresa,
    a.perc_top_empresa
FROM agregado AS a
LEFT JOIN tempo AS t
    ON a.id_tempo = t.id_tempo
ORDER BY a.id_tempo;