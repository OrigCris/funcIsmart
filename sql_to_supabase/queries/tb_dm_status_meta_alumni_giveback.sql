WITH alumni_filtrado AS (
    -- Alunos ativos a partir de 202401
    SELECT DISTINCT 
        ra,
        CAST(LEFT(CAST(id_tempo AS VARCHAR(10)), 4) AS INT) AS ano
    FROM alumni_status_anual
    WHERE id_status = 1
      AND id_tempo >= 202401
),
inscricoes_filtradas AS (
    -- Inscrições válidas
    SELECT 
        ra, 
        horas_participacao, 
        id_esal_registros_eventos_projetos
    FROM data_facts_esal_inscricoes_eventos
    WHERE id_esal_tipo_participacao_eventos = 2
),
eventos_join AS (
    -- Datas e horas das inscrições
    SELECT 
        i.ra,
        CAST(r.data_termino AS DATE) AS data_termino,
        FORMAT(CAST(r.data_termino AS DATE), 'yyyyMM') AS ano_mes,
        i.horas_participacao
    FROM inscricoes_filtradas AS i
    INNER JOIN data_facts_esal_registros_eventos_projetos AS r
        ON i.id_esal_registros_eventos_projetos = r.id_esal_registros_eventos_projetos
),
meses_ano AS (
    SELECT DISTINCT
        YEAR(CAST(data_termino AS DATE)) AS ano,
        MONTH(CAST(data_termino AS DATE)) AS mes
    FROM data_facts_esal_registros_eventos_projetos
),
ra_meses AS (
    SELECT 
        a.ra, 
        a.ano, 
        m.mes
    FROM alumni_filtrado AS a
    INNER JOIN meses_ano AS m
        ON a.ano = m.ano
),
meses_distribuidos AS (
    SELECT
        a.ra,
        a.ano,
        a.mes,
        CAST(COALESCE(SUM(r.horas_participacao), 0) AS FLOAT) AS horas_mes
    FROM ra_meses AS a
    LEFT JOIN eventos_join AS r
        ON r.ra = a.ra
        AND YEAR(r.data_termino) = a.ano
        AND MONTH(r.data_termino) = a.mes
    GROUP BY a.ra, a.ano, a.mes
),
horas_acumuladas AS (
    -- 6) Acumula horas dentro do ano por RA
    SELECT 
        ra,
        ano,
        mes,
        SUM(horas_mes) OVER (PARTITION BY ra, ano ORDER BY mes) AS horas_acumuladas
    FROM meses_distribuidos
),
meta_calculada AS (
    -- 7) Marca se atingiu meta (>= 10h) no acumulado
    SELECT
        ra,
        ano,
        mes,
        horas_acumuladas,
        CASE WHEN horas_acumuladas >= 10 THEN 'Sim' ELSE 'Não' END AS meta_atingida
    FROM horas_acumuladas
),
total_alunos_ano AS (
    -- 8) Total de alunos por ano
    SELECT 
        ano, 
        COUNT(DISTINCT ra) AS n_total
    FROM alumni_filtrado
    GROUP BY ano
)
-- 9) Resultado: % de "Sim" por mês/ano
SELECT
    m.ano,
    m.mes,
    ROUND(
        (CAST(SUM(CASE WHEN meta_atingida = 'Sim' THEN 1 ELSE 0 END) AS FLOAT) 
         / NULLIF(COUNT(DISTINCT ra), 0)) * 100.0,
        2
    ) AS percentual_meta_atingida,
    t.n_total
FROM meta_calculada AS m
INNER JOIN total_alunos_ano AS t
    ON m.ano = t.ano
GROUP BY m.ano, m.mes, t.n_total
ORDER BY m.ano, m.mes;