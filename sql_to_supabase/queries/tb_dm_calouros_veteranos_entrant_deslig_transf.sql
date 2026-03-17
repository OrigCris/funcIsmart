WITH matriculas AS (
    SELECT
        id_matricula, id_tempo, id_projeto, ra
    FROM ismart_matricula
    WHERE id_tempo = '202501' AND id_projeto = '3'
),
tb_status AS (
    SELECT a.*, b.id_status
    FROM (
        SELECT MAX(id_tempo) AS mx_id_tempo, ra, id_matricula
        FROM ismart_status_mensal
        GROUP BY ra, id_matricula
    ) AS a
    INNER JOIN ismart_status_mensal AS b
        ON a.mx_id_tempo = b.id_tempo AND a.ra = b.ra
    WHERE b.id_status IN (4,7,9,10)
),
tb_dm_universitarios AS (
    SELECT
        m.id_matricula,
        m.ra,
        m.id_tempo,
        m.id_projeto,
        s.id_status,
        s.mx_id_tempo
    FROM matriculas AS m
    INNER JOIN tb_status AS s
        ON m.id_matricula = s.id_matricula
),
base_inicial AS (
    SELECT  
        id_matricula,
        ra,
        data_inicio_curso,
        id_cursos_instituicoes AS id_curso_inicial
    FROM (
        SELECT 
            u.id_matricula,
            u.ra,
            data_inicio_curso,
            c.id_cursos_instituicoes
        FROM tb_dm_universitarios AS u
        LEFT JOIN (
            SELECT * 
            FROM data_facts_es_informacoes_curso 
            WHERE informacoes_contrato = '1'
        ) AS c
            ON u.ra = c.ra
    ) AS sub
),
tb_calouros_entrantes AS (
    SELECT
        YEAR(CONVERT(DATE, data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT ra) AS qtde,
        'calouros_entrantes' AS flag_tipo
    FROM base_inicial
    WHERE YEAR(CONVERT(DATE, data_inicio_curso, 120)) IN (2025)
    GROUP BY YEAR(CONVERT(DATE, data_inicio_curso, 120)), MONTH(CONVERT(DATE, data_inicio_curso, 120))
    UNION ALL
    SELECT
        YEAR(CONVERT(DATE, data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT ra) AS qtde,
        'calouros_entrantes' AS flag_tipo
    FROM base_inicial
    WHERE YEAR(CONVERT(DATE, data_inicio_curso, 120)) IN (2024)
    GROUP BY YEAR(CONVERT(DATE, data_inicio_curso, 120)), MONTH(CONVERT(DATE, data_inicio_curso, 120))
),
tb_calouros_desligados AS (
    SELECT
        YEAR(CONVERT(DATE, b.data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, b.data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT b.ra) AS qtde,
        'calouros_desligados' AS flag_tipo
    FROM base_inicial AS b
    INNER JOIN (
        SELECT * FROM ismart_status_mensal WHERE id_status = 2
    ) AS d
        ON b.id_matricula = d.id_matricula
    WHERE YEAR(CONVERT(DATE, b.data_inicio_curso, 120)) = 2025
    GROUP BY YEAR(CONVERT(DATE, b.data_inicio_curso, 120)), MONTH(CONVERT(DATE, b.data_inicio_curso, 120))
),
curso_recente AS (
    SELECT 
        ra,
        id_cursos_instituicoes AS id_curso_recente,
        CASE 
            WHEN data_inicio_curso IS NULL OR data_inicio_curso = 'NULL' THEN '1900-01-01'
            ELSE data_inicio_curso 
        END AS data_inicio_curso,
        id_tempo
    FROM data_facts_es_informacoes_curso AS c
    WHERE id_tempo = (
        SELECT MAX(e2.id_tempo)
        FROM data_facts_es_informacoes_curso AS e2
        WHERE e2.ra = c.ra
    )
),
calouros_transferidos AS (
    SELECT
        YEAR(CONVERT(DATE, i.data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, i.data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT i.ra) AS qtde,
        'calouros_transferidos' AS flag_tipo
    FROM (
        SELECT * 
        FROM base_inicial 
        WHERE YEAR(CONVERT(DATE, data_inicio_curso, 120)) = 2025
    ) AS i
    INNER JOIN curso_recente AS r
        ON i.ra = r.ra
    WHERE i.id_curso_inicial <> r.id_curso_recente
    GROUP BY YEAR(CONVERT(DATE, i.data_inicio_curso, 120)), MONTH(CONVERT(DATE, i.data_inicio_curso, 120))
),
veteranos_desligados AS (
    SELECT
        YEAR(CONVERT(DATE, b.data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, b.data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT b.ra) AS qtde,
        'veteranos_desligados' AS flag_tipo
    FROM base_inicial AS b
    INNER JOIN (
        SELECT * FROM ismart_status_mensal WHERE id_status = 2
    ) AS d
        ON b.id_matricula = d.id_matricula
    WHERE YEAR(CONVERT(DATE, b.data_inicio_curso, 120)) < 2025
    GROUP BY YEAR(CONVERT(DATE, b.data_inicio_curso, 120)), MONTH(CONVERT(DATE, b.data_inicio_curso, 120))
),
veteranos_transferidos AS (
    SELECT
        YEAR(CONVERT(DATE, i.data_inicio_curso, 120)) AS ano,
        MONTH(CONVERT(DATE, i.data_inicio_curso, 120)) AS mes,
        COUNT(DISTINCT i.ra) AS qtde,
        'veteranos_transferidos' AS flag_tipo
    FROM (
        SELECT * 
        FROM base_inicial 
        WHERE YEAR(CONVERT(DATE, data_inicio_curso, 120)) < 2025
    ) AS i
    INNER JOIN curso_recente AS r
        ON i.ra = r.ra
    WHERE i.id_curso_inicial <> r.id_curso_recente
    GROUP BY YEAR(CONVERT(DATE, i.data_inicio_curso, 120)), MONTH(CONVERT(DATE, i.data_inicio_curso, 120))
)
-- União final das tabelas
SELECT * FROM tb_calouros_entrantes
UNION ALL
SELECT * FROM tb_calouros_desligados
UNION ALL
SELECT * FROM calouros_transferidos
UNION ALL
SELECT * FROM veteranos_desligados
UNION ALL
SELECT * FROM veteranos_transferidos;