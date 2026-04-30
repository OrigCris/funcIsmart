WITH max_tempo AS ( 
    SELECT MAX(id_tempo) AS id_tempo_ref 
    FROM dbo.ismart_status_mensal 
),
primeira_matricula AS (
    SELECT 
        ra, 
        MIN(id_matricula) AS primeira_matricula
    FROM dbo.ismart_matricula
    WHERE id_projeto = 3
    GROUP BY ra
)

SELECT 
    ism.id_tempo, 
    ism.id_status, 
    ism.ra, 
    st.status, 
    m.id_matricula, 
    m.id_projeto, 
    eic.data_inicio_curso, 
    eic.data_prevista_termino_curso, 
    eic.informacoes_contrato, 
    t.mes, 
    t.ano, 

    -- 🔥 AJUSTADO
    CASE  
        WHEN m.id_matricula = pm.primeira_matricula 
             AND m.id_tempo = 202601
        THEN 'CALOURO' 
        ELSE 'VETERANO' 
    END AS tipo_aluno, 

    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND (
                    ism.id_status IN (7,9,10,13,16,17)
                    OR st.status IN ('CURSANDO','TRANCAMENTO','PENDENTE','PRE-MATRICULA - S1','PRE-MATRICULA - S2')
                 )
        THEN 'UNI_HJ' 
        ELSE 'UNI' 
    END AS flg_universitario, 

    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status IN (7,9,10) 
             AND eic.data_prevista_termino_curso >= CAST(GETDATE() AS date) 
        THEN 'ATIVO' 
        ELSE 'N ATIVO' 
    END AS flg_ativo_nao_ativo, 

    -- 🔥 AJUSTADO (pré-matrícula + regra 202601)
    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status IN (7,9,10,13,16,17) 
             AND m.id_matricula = pm.primeira_matricula 
             AND m.id_tempo = 202601
        THEN 'CALOURO_HJ' 

        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status IN (7,9,10) 
             AND NOT (m.id_matricula = pm.primeira_matricula AND m.id_tempo = 202601)
        THEN 'VETERANO_HJ' 

        ELSE 'NADA' 
    END AS flg_calouro_veterano, 

    -- 🔥 AJUSTADO
    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
            AND m.id_matricula = pm.primeira_matricula
            AND m.id_tempo = 202601
        THEN 'CALOURO_ENTRANTES' 
        ELSE 'NADA' 
    END AS flg_calouro_entrantes,

    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status = 2 
             AND m.id_matricula = pm.primeira_matricula 
             AND m.id_tempo = 202601
        THEN 'CALOURO_DESLIG' 

        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status = 2 
             AND NOT (m.id_matricula = pm.primeira_matricula AND m.id_tempo = 202601)
        THEN 'VETERANO_DESLIG' 

        ELSE 'NADA' 
    END AS flg_desligados, 

    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status = 11 
             AND m.id_matricula = pm.primeira_matricula 
             AND m.id_tempo = 202601
        THEN 'CALOURO_DESIST' 
        ELSE 'NADA' 
    END AS flg_desistente, 

    CASE  
        WHEN ism.id_tempo = mt.id_tempo_ref 
             AND ism.id_status = 8 
        THEN 'FORMANDOS' 
        ELSE 'NADA' 
    END AS flg_formandos, 

    CAST( 
    CASE  
        WHEN ism.ra = eic2.ra 
             AND eic2.id_tempo = ism.id_tempo 
             AND ism.id_status IN (7,9,10,13) 
             AND eic2.transferencia = 1 
        THEN 1 
        ELSE 0 
    END AS INT) AS transferencia 

FROM dbo.ismart_status_mensal AS ism 

LEFT JOIN dbo.ismart_status AS st  
    ON st.id_status = ism.id_status 

LEFT JOIN dbo.ismart_matricula AS m  
    ON m.id_matricula = ism.id_matricula 

LEFT JOIN primeira_matricula pm
    ON pm.ra = ism.ra

LEFT JOIN max_tempo mt
    ON 1 = 1

LEFT JOIN dbo.data_facts_es_informacoes_curso AS eic  
    ON eic.ra = ism.ra 

LEFT JOIN dbo.data_facts_es_informacoes_curso AS eic2  
    ON eic2.ra = ism.ra 
    AND eic2.id_tempo = ism.id_tempo 

LEFT JOIN dbo.tempo AS t  
    ON t.id_tempo = ism.id_tempo 

WHERE m.id_projeto IN (3,4)
  AND eic.informacoes_contrato = 1