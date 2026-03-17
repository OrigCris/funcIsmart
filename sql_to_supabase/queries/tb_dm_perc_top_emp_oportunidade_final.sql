WITH ultimo_tempo AS (
    SELECT MAX(id_tempo) AS max_id_tempo
    FROM es_status_meta_mensal
),
dados_filtrados AS (
    SELECT 
        m.id_matricula,
        m.ra,
        m.top_empresa,
        m.id_es_status_meta,
        m.id_tempo,
        e.status_meta AS descricao_status
    FROM es_status_meta_mensal AS m
    INNER JOIN ultimo_tempo AS ut
        ON m.id_tempo = ut.max_id_tempo
    INNER JOIN es_status_meta AS e
        ON m.id_es_status_meta = e.id_es_status_meta
    WHERE m.id_es_status_meta = 1
)
SELECT 
    ROUND(
        CAST(SUM(CASE WHEN top_empresa = '1' THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(COUNT(*), 0) * 100.0,
        2
    ) AS perc_top_empresa,
    SUM(CASE WHEN top_empresa = '1' THEN 1 ELSE 0 END) AS qtd_top_empresa,
    COUNT(*) AS total_alunos
FROM dados_filtrados;