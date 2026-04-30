WITH curso_atual AS (
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
)

SELECT
    era.ra,

    /* STATUS CONTRATO */
    CASE 
        WHEN era.pesquisa = '2026.1' THEN
            CASE
                WHEN cc.data_prevista_termino_curso >= DATEFROMPARTS(2026, 3, 31)
                     AND stp.status IN (
                         'CURSANDO',
                         'TRANCAMENTO',
                         'PENDENTE',
                         'ANALISE/DESLIGADO',
                         'SUSPENSO'
                     )
                THEN 'ATIVO'
                ELSE 'INATIVO'
            END
        ELSE stc.status
    END AS status_contrato,

    stp.status AS segmentacao_pendentes,
    sm.status_meta,

    /* AJUSTE PESQUISA */
    CASE 
        WHEN era.pesquisa = '2026.1' THEN '2025.2'
        WHEN era.pesquisa = '2025.2' THEN '2025.1'
        WHEN era.pesquisa = '2025.1' THEN '2024.2'
        ELSE era.pesquisa
    END AS pesquisa,

    CASE 
        WHEN era.respondeu = 1 THEN 'Sim'
        WHEN era.respondeu = 0 THEN 'Não'
    END AS respondeu,

    CASE 
        WHEN era.comecou_pesquisa = 1 THEN 'Sim'
        WHEN era.comecou_pesquisa = 0 THEN 'Não'
    END AS comecou_pesquisa,


    era.data_resposta_ra,

    CASE 
        WHEN era.pediu_bolsa = 1 THEN 'Sim'
        WHEN era.pediu_bolsa = 0 THEN 'Não'
    END AS pediu_bolsa,

    /* 🔥 CURSO ATUAL */
    ici.area                   AS area_curso,
    ici.categoria       AS tipo_universidade,
    ici.curso_agrupado         AS curso_agregado,
    ici.instituicao            AS universidade,
    ilc.local_agregado         AS local_agregado,
    ca.turno_curso             AS turno,


    gen.genero       AS genero,
    rac.raca         AS raca,
    praca.praca_sigla AS praca_sigla


FROM es_ra_acomp era

LEFT JOIN ismart_status stc
    ON stc.id_status = era.id_status_contrato

LEFT JOIN ismart_status stp
    ON stp.id_status = era.id_segmentacao_pendentes

LEFT JOIN es_status_meta sm
    ON sm.id_es_status_meta = era.id_es_status_meta

-- CURSO ATUAL
LEFT JOIN curso_atual ca
    ON ca.ra = era.ra
   AND ca.rn = 1
   AND era.pesquisa = '2026.1'

LEFT JOIN data_facts_ismart_aluno_complemento iac
    ON iac.ra = era.ra
   AND era.pesquisa = '2026.1'

LEFT JOIN ismart_genero gen
    ON gen.id_genero = iac.id_genero

LEFT JOIN ismart_raca rac
    ON rac.id_raca = iac.id_raca

LEFT JOIN ismart_praca praca
    ON praca.id_praca = iac.id_praca

LEFT JOIN ismart_cursos_instituicoes ici
    ON ici.id_cursos_instituicoes = ca.id_cursos_instituicoes

LEFT JOIN data_facts_ismart_localidade_cursos ilc
    ON ilc.id_localidade_cursos = ca.id_localidade_cursos

-- CURSO CONTRATO (SÓ STATUS)
LEFT JOIN curso_contrato cc
    ON cc.ra = era.ra
   AND cc.rn = 1