WITH matricula_base AS (

    SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ra
                ORDER BY 
                    CASE 
                        WHEN id_projeto = 4 THEN 1
                        WHEN id_projeto = 3 THEN 2
                    END,
                    id_tempo DESC,      
                    id_matricula DESC   
            ) AS rn
    FROM ismart_matricula
    WHERE id_projeto IN (3,4)
),

primeira_matricula_proj3 AS (
    SELECT
        ra,
        id_matricula,
        ROW_NUMBER() OVER (
            PARTITION BY ra
            ORDER BY id_tempo ASC, id_matricula ASC
        ) AS rn
    FROM ismart_matricula
    WHERE id_projeto = 3
),

status_rank AS (
    SELECT 
        ism.*,
        ROW_NUMBER() OVER (
            PARTITION BY ism.id_matricula
            ORDER BY ism.id_tempo DESC
        ) AS rn
    FROM ismart_status_mensal ism
),

ultimo_status AS (
    SELECT *
    FROM status_rank
    WHERE rn = 1
),

curso_atual AS (
    SELECT
        eic.*,
        ROW_NUMBER() OVER (
            PARTITION BY eic.ra
            ORDER BY eic.id_tempo DESC
        ) AS rn
    FROM data_facts_es_informacoes_curso eic
),

curso_contrato AS (
    SELECT
        eic.*,
        ROW_NUMBER() OVER (
            PARTITION BY eic.ra
            ORDER BY eic.id_tempo DESC
        ) AS rn
    FROM data_facts_es_informacoes_curso eic
    WHERE eic.informacoes_contrato = 1
),

enem_rank AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ra
               ORDER BY id_tempo DESC
           ) AS rn
    FROM eb_enem
    WHERE treineiro = 0
)

SELECT

    m.id_matricula,
    m.ra,

    iac.nome,
    iac.nome_social,
    idf.cpf,
    iac.pronome,
    iac.nome_comunicacao,
    iac.orientacao_sexual,
    praca.praca_sigla,
    gen.genero,
    rac.raca,
    al.deseja_receber_comunicacao AS alumni_quer_receber_contato,

    cont.email_ismart,
    cont.email_pessoal,
    cont.celular,
    cont.telefone_fixo,
    cont.linkedin,

    eic.data_inicio_curso              AS curso_atual_inicio_curso,
    YEAR(eic.data_inicio_curso)        AS curso_atual_ano_entrada,
    eic.data_prevista_termino_curso    AS curso_atual_termino_curso,
    YEAR(eic.data_prevista_termino_curso) AS curso_atual_ano_termino,
    eic.data_termino_real              AS curso_atual_data_formacao,
    eic.ano_cursado_previsto           AS curso_atual_ano_cursado,
    eic.turno_curso                    AS curso_atual_turno_curso,
    eic.periodicidade_curso            AS curso_atual_periodicidade,
    ici.nome_curso                     AS curso_atual,
    ici.curso_agrupado                 AS curso_atual_agrupado,
    ici.area                           AS area_atual,
    ici.instituicao                    AS universidade_atual,
    ilc.cidade                         AS curso_atual_cidade_onde_estuda,
    ilc.estado                         AS curso_atual_estado_onde_estuda,
    ilc.pais                           AS curso_atual_pais_onde_estuda,
    ilc.local_agregado                 AS curso_atual_local_agregado,

    -- 🔥 AJUSTADO
    CASE 
        WHEN m.id_matricula = p3.id_matricula 
             AND m.id_tempo = 202601
        THEN 'CALOURO'
        ELSE 'VETERANO'
    END AS perfil_universidade_contrato,

    -- 🔥 AJUSTADO
    CASE 
        WHEN (m.id_matricula = p3.id_matricula AND m.id_tempo = 202601)
             OR YEAR(eic.data_inicio_curso) = YEAR(GETDATE())
        THEN 'CALOURO'
        ELSE 'VETERANO'
    END AS perfil_universidade_curso_atual,

    CASE 
        WHEN m.id_projeto = 4 THEN 'FORMADO'
        ELSE st.status
    END AS status,

    CASE 
        WHEN COALESCE(st.status,'FORMADO') IN 
        ('FORMADO','DESISTENTE','DESLIGADO','AGUARDANDO',
         'PRE-MATRICULA - S1','PRE-MATRICULA - S2')
            THEN 'NÃO SE APLICA'
        WHEN EXISTS (
            SELECT 1
            FROM es_ra_acomp a
            WHERE a.ra = m.ra
            AND a.pesquisa = '2026.1'
            AND a.respondeu = 0
        )
            THEN 'SIM'
        ELSE 'NÃO'
    END AS pendente,

    CASE 
        WHEN COALESCE(st.status,'FORMADO') IN 
        ('FORMADO','DESISTENTE','DESLIGADO','AGUARDANDO',
         'PRE-MATRICULA - S1','PRE-MATRICULA - S2')
            THEN NULL
        WHEN NOT EXISTS (
            SELECT 1
            FROM es_ra_acomp a
            WHERE a.ra = m.ra
            AND a.pesquisa = '2026.1'
            AND a.respondeu = 0
        )
            THEN 0
        ELSE
            (CASE WHEN EXISTS (
                SELECT 1 FROM es_ra_acomp 
                WHERE ra = m.ra AND pesquisa = '2026.1' AND respondeu = 0
            ) THEN 1 ELSE 0 END)
            +
            (CASE WHEN EXISTS (
                SELECT 1 FROM es_ra_acomp 
                WHERE ra = m.ra AND pesquisa = '2025.2' AND respondeu = 0
            ) THEN 1 ELSE 0 END)
            +
            (CASE WHEN EXISTS (
                SELECT 1 FROM es_ra_acomp 
                WHERE ra = m.ra AND pesquisa = '2025.1' AND respondeu = 0
            ) THEN 1 ELSE 0 END)
    END AS qtd_ras_pendente,

    CASE 
        WHEN COALESCE(st.status,'FORMADO') IN 
        ('CURSANDO','TRANCAMENTO','PENDENTE',
         'PRE-MATRICULA - S1','PRE-MATRICULA - S2') 
            THEN 'UNIVERSITÁRIO'
        WHEN COALESCE(st.status,'FORMADO') = 'DESLIGADO' 
            THEN 'DESLIGADO'
        WHEN COALESCE(st.status,'FORMADO') = 'DESISTENTE' 
            THEN 'DESISTENTE'
        ELSE 
            'GRADUADO'
    END AS perfil_real,

    CASE 
        WHEN cc.data_prevista_termino_curso >= GETDATE()
        AND COALESCE(st.status,'FORMADO') IN ('CURSANDO','TRANCAMENTO','PENDENTE')
        THEN 'ATIVO'
        ELSE 'NÃO ATIVO'
    END AS status_contrato,

    cc.data_inicio_curso                 AS contrato_inicio_curso,
    YEAR(cc.data_inicio_curso)           AS contrato_ano_inicio_entrada_es,
    cc.data_prevista_termino_curso       AS contrato_termino_previsto,
    YEAR(cc.data_prevista_termino_curso) AS contrato_ano_termino,
    ici2.nome_curso                      AS contrato_curso,
    ici2.instituicao                     AS contrato_universidade,
    ici2.area                            AS contrato_area,
    ilc2.cidade                          AS contrato_cidade,
    ilc2.estado                          AS contrato_estado,
    ilc2.pais                            AS contrato_pais,

    eb.ano_entrada_eb,
    eb.ano_formacao_eb,
    eb.projeto_entrada_eb,
    eb.projeto_formacao_eb,
    eb.escola_formacao_eb,

    en.nota_linguagens        AS enem_li,
    en.nota_ciencias_humanas  AS enem_ch,
    en.nota_ciencias_natureza AS enem_cn,
    en.nota_matematica        AS enem_mat,
    en.nota_redacao           AS enem_red,
    en.media_simples          AS enem_media,
    en.media_sem_redacao      AS enem_media_sem_redacao

FROM matricula_base m

LEFT JOIN primeira_matricula_proj3 p3
    ON p3.ra = m.ra AND p3.rn = 1

LEFT JOIN ultimo_status ism
    ON ism.id_matricula = m.id_matricula

LEFT JOIN ismart_status st
    ON st.id_status = ism.id_status

LEFT JOIN data_facts_ismart_aluno_complemento iac
    ON iac.ra = m.ra

LEFT JOIN data_facts_ismart_identificacao_aluno idf
    ON idf.ra = m.ra

LEFT JOIN alumni_cadastro al
    ON al.ra = m.ra

LEFT JOIN ismart_praca praca
    ON praca.id_praca = iac.id_praca

LEFT JOIN ismart_genero gen
    ON gen.id_genero = iac.id_genero

LEFT JOIN ismart_raca rac
    ON rac.id_raca = iac.id_raca

LEFT JOIN data_facts_ismart_contato_aluno cont
    ON cont.ra = m.ra

LEFT JOIN curso_atual eic
    ON eic.ra = m.ra AND eic.rn = 1

LEFT JOIN ismart_cursos_instituicoes ici
    ON ici.id_cursos_instituicoes = eic.id_cursos_instituicoes

LEFT JOIN data_facts_ismart_localidade_cursos ilc
    ON ilc.id_localidade_cursos = eic.id_localidade_cursos

LEFT JOIN curso_contrato cc
    ON cc.ra = m.ra AND cc.rn = 1

LEFT JOIN ismart_cursos_instituicoes ici2
    ON ici2.id_cursos_instituicoes = cc.id_cursos_instituicoes

LEFT JOIN data_facts_ismart_localidade_cursos ilc2
    ON ilc2.id_localidade_cursos = cc.id_localidade_cursos

LEFT JOIN es_aux_eb eb
    ON eb.ra = m.ra

LEFT JOIN enem_rank en
    ON en.ra = m.ra
   AND en.rn = 1

WHERE m.rn = 1