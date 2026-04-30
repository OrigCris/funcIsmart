with base_status_mensal as (
    select 
        ism.id_matricula,
        ism.ra
    from ismart_status_mensal ism
    left join ismart_matricula m 
        on ism.id_matricula = m.id_matricula
    left join eb_detalhamento_matricula edm 
        on ism.id_matricula = edm.id_matricula
    where ism.id_tempo = 202512
      and ism.id_status = 1
      and m.id_projeto in (1, 2)
      and edm.id_serie = 7
),

base_cursinho as (
    select 
        id_matricula,
        ra
    from ismart_matricula
    where id_tempo = 202501
      and id_projeto = 6
),

base_final as (
    select id_matricula, ra from base_status_mensal
    union all
    select id_matricula, ra from base_cursinho
),

base_com_nome as (
    select 
    bf.id_matricula,
    bf.ra,
    ac.nome,
    p.praca_sigla
    from base_final as bf
    left join data_facts_ismart_aluno_complemento ac on bf.ra = ac.ra
    left join ismart_praca p on ac.id_praca = p.id_praca
),

base_projeto as(
    select 
        bcn.id_matricula,
        bcn.ra,
        bcn.nome,
        bcn.praca_sigla,
        p.projeto
    from base_com_nome bcn 
    left join ismart_matricula m on bcn.id_matricula = m.id_matricula
    left join ismart_projeto p on m.id_projeto = p.id_projeto 
),

base_fluxo_es AS (
    SELECT
        bp.id_matricula,
        bp.ra,
        bp.nome,
        bp.praca_sigla,
        bp.projeto,
        efm.recomendacao_pre_matricula AS recomendado,
        CASE 
            WHEN efm.respondeu_forms_interesse = 1 THEN 'Sim'
            ELSE 'Não'
        END AS respondeu_forms_interesse,
        efm.interesse_es AS status_interesse_es,
        efm.motivo_desinteresse AS motivo_nao_interesse
    FROM base_projeto bp
    LEFT JOIN eb_fluxo_matricula_es efm 
        ON bp.id_matricula = efm.id_matricula 
),

aprovacoes_vestibular AS (
    SELECT DISTINCT
        id_matricula,
        id_cursos_instituicoes
    FROM eb_aprovacoes_vestibular
    WHERE status_aprovacao_vestibular = 1
),

cursos_recomendados AS (
    SELECT DISTINCT
        id_cursos_instituicoes
    FROM ismart_cursos_recomendados
    WHERE id_tempo = 202501
),

base_final_es AS (
    SELECT
        bfe.*,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM aprovacoes_vestibular av
                INNER JOIN cursos_recomendados cr
                    ON av.id_cursos_instituicoes = cr.id_cursos_instituicoes
                WHERE av.id_matricula = bfe.id_matricula
            )
            THEN 'Sim'
            ELSE 'Não'
        END AS aprovacao_recomendada
    FROM base_fluxo_es bfe
),

ultimo_status AS (
    SELECT
        ism.ra,
        st.status
    FROM ismart_status_mensal ism
    LEFT JOIN ismart_status st
        ON ism.id_status = st.id_status
    WHERE ism.id_tempo = (
        SELECT MAX(id_tempo)
        FROM ismart_status_mensal
    )
),

base_final_com_status AS (
    SELECT
        bfe.*,
        us.status
    FROM base_final_es bfe
    LEFT JOIN ultimo_status us
        ON bfe.ra = us.ra
),

base_status_final as (
    select
        bfcs.id_matricula,
        bfcs.ra,
        bfcs.nome,
        bfcs.praca_sigla,
        bfcs.projeto,
        bfcs.recomendado,
        bfcs.respondeu_forms_interesse,
        bfcs.status_interesse_es,
        bfcs.motivo_nao_interesse,
        bfcs.aprovacao_recomendada,

        case
            -- se já existe status, mantém
            when bfcs.status is not null 
                then bfcs.status

            -- só entra aqui quando status é NULL
            when bfcs.recomendado = 'Não Recomendado' 
                then 'NAO RECOMENDADO'

            when bfcs.aprovacao_recomendada = 'Não' 
                then 'SEM APROVACOES RECOMENDADAS'

            when bfcs.respondeu_forms_interesse = 'Não' 
                then 'NAO RESPONDEU FORMS'

            when bfcs.status_interesse_es = 'NAO SEGUIREI' 
                then 'NAO SEGUIRA ES'

            else null
        end as status
    from base_final_com_status bfcs
),

base_com_valido_meta as (
    select
        bsf.*,
        case
            when bsf.status in (
                'NAO RECOMENDADO',
                'SEM APROVACOES RECOMENDADAS',
                'NAO RESPONDEU FORMS',
                'NAO SEGUIRA ES'
            )
            then 'Não'
            else 'Sim'
        end as valido_meta_entrantes
    from base_status_final bsf
)

select *
from base_com_valido_meta