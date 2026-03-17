WITH matriculas AS (
    SELECT
        id_matricula, 
        id_tempo, 
        id_projeto, 
        ra
    FROM ismart_matricula
    WHERE id_tempo = '202501' 
      AND id_projeto = '3'
),
tb_status AS (
    SELECT 
        a.*, 
        b.id_status
    FROM (
        SELECT 
            MAX(id_tempo) AS mx_id_tempo, 
            ra, 
            id_matricula
        FROM ismart_status_mensal
        GROUP BY ra, id_matricula
    ) AS a
    INNER JOIN ismart_status_mensal AS b
        ON a.mx_id_tempo = b.id_tempo 
        AND a.ra = b.ra
    WHERE b.id_status IN (4,7,9,10)
),
tb_dm_universitarios AS (
    SELECT
        m.id_matricula,
        m.ra,
        m.id_tempo,
        m.id_projeto,
        s.id_status,
        s.mx_id_tempo as id_tempo_status
    FROM matriculas AS m
    INNER JOIN tb_status AS s
        ON m.id_matricula = s.id_matricula
)
SELECT 
    u.*,
    c.id_cursos_instituicoes,
    c.data_prevista_termino_curso,
    CASE 
        WHEN c.data_prevista_termino_curso IS NULL THEN 'NÃO ATIVO'
        WHEN YEAR(CAST(c.data_prevista_termino_curso AS DATE)) > 2025 THEN 'ATIVO'
        ELSE 'NÃO ATIVO'
    END AS flag_status_curso
FROM tb_dm_universitarios AS u
INNER JOIN data_facts_es_informacoes_curso AS c
    ON u.ra = c.ra
   AND c.informacoes_contrato = '1';
