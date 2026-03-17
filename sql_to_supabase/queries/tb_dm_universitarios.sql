WITH matriculas AS (
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