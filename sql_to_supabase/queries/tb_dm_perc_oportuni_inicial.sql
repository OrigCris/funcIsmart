WITH ultima_data AS ( 
    SELECT MAX(id_tempo) AS id_tempo_max 
    FROM es_status_meta_mensal
), 
base_filtrada AS ( 
    SELECT 
        m.*, 
        u.id_tempo_max 
    FROM es_status_meta_mensal AS m 
    INNER JOIN ultima_data AS u 
        ON m.id_tempo = u.id_tempo_max 
    INNER JOIN es_status_meta AS e 
        ON m.id_es_status_meta = e.id_es_status_meta 
    WHERE m.id_es_status_meta = 2 
) 
SELECT  
    id_tempo_max AS id_tempo_utilizado, 
    COUNT(*) AS total_alunos, 
    SUM(CASE WHEN id_esal_status_oportunidade = 2 THEN 1 ELSE 0 END) AS alunos_oportunidade, 
    ROUND(
        CAST(SUM(CASE WHEN id_esal_status_oportunidade = 2 THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(COUNT(*), 0) * 100.0,
        2
    ) AS percentual_oportunidade
FROM base_filtrada 
GROUP BY id_tempo_max;