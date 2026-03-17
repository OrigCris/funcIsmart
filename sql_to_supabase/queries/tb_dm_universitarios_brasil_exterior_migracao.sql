WITH 
matriculas AS (
  SELECT
    id_matricula, id_tempo, id_projeto, ra
  FROM ismart_matricula
  WHERE id_tempo = '202501' AND id_projeto = '3'
),
tb_status AS (
  SELECT a.*, b.id_status
  FROM (
    SELECT MAX(id_tempo) mx_id_tempo, ra, id_matricula
    FROM ismart_status_mensal
    GROUP BY ra, id_matricula
  ) a
  INNER JOIN ismart_status_mensal b
    ON (a.mx_id_tempo = b.id_tempo AND a.ra = b.ra)
  WHERE b.id_status IN (4,7,9,10)
)
,tb_dm_universitarios as (
 SELECT
        matriculas.id_matricula,
        matriculas.ra,
        matriculas.id_tempo,
        matriculas.id_projeto,
        tb_status.id_status,
        tb_status.mx_id_tempo
    FROM matriculas
    INNER JOIN tb_status
        ON (matriculas.id_matricula = tb_status.id_matricula)   
)
,curso_max_tempo AS (
    SELECT
        e.ra,
        MAX(
            CASE 
                WHEN e.id_tempo  is null then 0
                ELSE e.id_tempo
            END
        ) AS max_id_tempo
    FROM data_facts_es_informacoes_curso e
    WHERE e.informacoes_contrato = '1'
    GROUP BY e.ra
),
curso_filtrado AS (
    SELECT
        ra,
        id_localidade_cursos,
        id_cursos_instituicoes,
        id_tempo,
        fonte_atualizacao
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY e.ra, e.id_tempo
                ORDER BY CASE WHEN fonte_atualizacao = 'Status - Atual' THEN 1 ELSE 2 END
            ) AS flag
        FROM data_facts_es_informacoes_curso e
        INNER JOIN curso_max_tempo cmt
            ON e.ra = cmt.ra 
            AND e.id_tempo = cmt.max_id_tempo
        WHERE e.informacoes_contrato = '1'
    ) t
    WHERE t.flag = 1
)
SELECT
    u.*,
    c.id_cursos_instituicoes,
    c.id_localidade_cursos,
    l.cidade,
    l.estado,
    l.pais,
    CASE
        WHEN l.id_localidade_cursos IS NULL THEN 'Não Encontrado'
        WHEN c.id_localidade_cursos = '9999' THEN 'Sem Registro'
        WHEN l.pais <> 'BRASIL' THEN 'EXTERIOR'
        ELSE 'BRASIL'
    END AS flag_localidade
FROM tb_dm_universitarios u
LEFT JOIN curso_filtrado c
    ON u.ra = c.ra
LEFT JOIN data_facts_ismart_localidade_cursos l
    ON c.id_localidade_cursos = l.id_localidade_cursos;