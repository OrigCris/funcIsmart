WITH status_meta_mensal AS (
    SELECT
        smm.id_es_status_meta_mensal,
        smm.ra,
        smm.id_tempo,
        st.status,
        esm.status_meta,
        eso_atual.status_oportunidade  AS status_oportunidade_atual,
        eso_meta.status_oportunidade   AS status_oportunidade_meta,
        smm.previsao_termino_no_mes,
        smm.pendente,
        smm.top_empresa,
        smm.data_fechamento_mes,
        smm.racional
    FROM dbo.esal_status_meta_mensal smm
    LEFT JOIN dbo.es_status_meta esm
        ON smm.id_es_status_meta = esm.id_es_status_meta
    LEFT JOIN dbo.esal_status_oportunidade eso_atual
        ON smm.id_esal_status_oportunidade_atual = eso_atual.id_esal_status_oportunidade
    LEFT JOIN dbo.esal_status_oportunidade eso_meta
        ON smm.id_esal_status_oportunidade_meta = eso_meta.id_esal_status_oportunidade
    left join dbo.ismart_status st
    on smm.id_status = st.id_status
    WHERE smm.racional = 'Meta a partir de 2026'
        OR smm.racional IS NULL

)

SELECT
    smm.*,
    -- Infos do curso vigente
    eic.id_informacoes_curso,
    eic.id_cursos_instituicoes,
    eic.data_inicio_curso,
    eic.data_prevista_termino_curso,
    eic.data_termino_real,
    eic.ano_cursado_previsto,
    eic.turno_curso,
    eic.periodicidade_curso,
    -- Descrições do curso
    ici.nome_curso,
    ici.curso_agrupado,
    ici.area,
    ici.instituicao,
    ici.categoria,
    -- Localidade
    ilc.cidade,
    ilc.estado,
    ilc.pais,
    ilc.local_agregado,
    -- Oportunidade vigente no ano/mês
    ero.id_registros_oportunidades,
    eto.tipo_oportunidade,
    ero.data_registro,
    ero.fonte_registro,
    ero.cargo_oportunidade,
    ero.carreira,
    ero.trilha_carreira,
    ero.nome_organizacao,
    ero.area_core,
    ero.setor_oportunidade,
    ero.inicio_oportunidade,
    ero.termino_oportunidade,
    ero.carga_horaria_semanal,
    ero.remuneracao,
    ero.moeda_remuneracao,
    ero.indicacao_ismart,
    ero.empresa_parceira,
    ero.top_empresa          AS top_empresa_oportunidade,
    ero.comentario_oportunidade,
    ero.tag_oportunidade,
    ero.prioridade_oportunidade
FROM status_meta_mensal smm
OUTER APPLY (
    SELECT TOP 1 *
    FROM dbo.data_facts_es_informacoes_curso eic
    WHERE eic.ra = smm.ra
      AND eic.id_tempo <= smm.id_tempo
    ORDER BY eic.id_tempo DESC
) eic
LEFT JOIN dbo.ismart_cursos_instituicoes ici
    ON ici.id_cursos_instituicoes = eic.id_cursos_instituicoes
LEFT JOIN dbo.data_facts_ismart_localidade_cursos ilc
    ON ilc.id_localidade_cursos = eic.id_localidade_cursos

OUTER APPLY (
    SELECT TOP 1 *
    FROM dbo.esal_registros_oportunidades ero
    WHERE ero.ra = smm.ra
      AND ero.racional = 'Meta a partir de 2026'
      AND ero.id_tempo / 100 = smm.id_tempo / 100   -- mesmo ano
      AND ero.id_tempo <= smm.id_tempo               -- até o mês atual
    ORDER BY ero.id_tempo DESC, ero.prioridade_oportunidade ASC
) ero
LEFT JOIN dbo.esal_tipo_oportunidade eto
    ON eto.id_esal_tipo_oportunidade = ero.id_esal_tipo_oportunidade