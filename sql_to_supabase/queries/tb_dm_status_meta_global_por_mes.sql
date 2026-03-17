SELECT 
    t.ano,
    t.mes,
    m.id_tempo,
    COUNT(*) AS total_alunos,
    SUM(CASE WHEN m.id_esal_status_oportunidade = 2 THEN 1 ELSE 0 END) AS alunos_oportunidade,
    ROUND(
        (CAST(SUM(CASE WHEN m.id_esal_status_oportunidade = 2 THEN 1 ELSE 0 END) AS FLOAT)
         / NULLIF(COUNT(*), 0)) * 100.0,
        2
    ) AS percentual_oportunidade
FROM es_status_meta_mensal AS m
INNER JOIN es_status_meta AS e
    ON m.id_es_status_meta = e.id_es_status_meta
INNER JOIN tempo AS t
    ON m.id_tempo = t.id_tempo
WHERE m.id_tempo >= 202401
  AND m.id_es_status_meta = 1
GROUP BY t.ano, t.mes, m.id_tempo
ORDER BY m.id_tempo;