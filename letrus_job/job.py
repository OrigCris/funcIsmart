import pandas as pd
from letrus_job import helpers
import os

BLOB_CONN_STR = os.getenv('BLOB_CONN_STR')
CONTAINER_LETRUS = '01-raw'

conn = helpers._get_connection_sqlserver()

def process_aux():
    config_subida = [
        {'arquivo':'Letrus', 'tabela':'letrus_acompanhamento_cj'},
        {'arquivo':'dicionario', 'tabela':'dicionario_engajamento_cj'},
        {'arquivo':'detalhamento', 'tabela':'ismart_letrus_cj'}
    ]

    for config in config_subida:
        df = helpers.find_files_letrus(BLOB_CONN_STR, CONTAINER_LETRUS, config['arquivo'])

        df = helpers.process_df(df)

        helpers._insert_data(conn, df, config['tabela'])

def process_silver():
    semana_atual = helpers.semana()
    id_tempo_atual = helpers.id_tempo(semana_atual)

    base_acompanhamento = pd.read_sql("SELECT * FROM letrus_acompanhamento_cj", conn)
    base_bruta = pd.read_sql("SELECT *, ID_Estudante as ID_Letrus FROM ismart_letrus_cj", conn)

    query = f"""SELECT SM.id_matricula, SM.id_status, SM.ra
    FROM ismart_status_mensal AS SM
    LEFT JOIN ismart_matricula AS M 
        ON M.id_matricula = SM.id_matricula AND M.id_tempo = 202501 AND M.id_projeto = 2
    WHERE SM.id_tempo = {id_tempo_atual};
    """

    iol_status = pd.read_sql(query, conn)

    for col in ['DataInicio', 'DataFim', 'InicioAtividade', 'FimAtividade']:
        if col in base_bruta.columns:
            base_bruta[col] = pd.to_datetime(base_bruta[col], errors='coerce')

    base_bruta["Atividade"] = base_bruta["Atividade"].apply(helpers.corrigir_atvdd)

    base_bruta["Semana"] = semana_atual

    print("Resolvendo RA via base_acompanhamento... [2/6]")

    base_letrus = base_bruta.merge(
        base_acompanhamento[['ID_Letrus', 'RA']],
        how="left",
        left_on="ID_Letrus",
        right_on="ID_Letrus"
    )

    base_letrus.columns = [x.lower() for x in base_letrus.columns]
    base_letrus['id_tempo'] = base_letrus['semana'].apply(lambda x: helpers.id_tempo(x))
    base_letrus['atividade_mes'] = base_letrus['atividade'].apply(lambda x: x.split(" ")[1])
    base_letrus['atividade_classificacao'] = base_letrus['atividade'].apply(lambda x: x.split(" ")[-1])
    base_letrus['ra'] = base_letrus['ra'].fillna(0).astype(int)

    base_letrus_merged = base_letrus.merge(iol_status, on=['ra'], how='left')
    base_letrus_merged = base_letrus_merged.dropna(subset=['id_matricula'])
    base_letrus_merged = base_letrus_merged[base_letrus_merged['id_status']==1]

    base_letrus_merged.drop(columns=['nome_do_estudante','id_status', 'inicio_da_atividade', 'fim_da_atividade',
                                    'rede', 'subrede', 'escola', 'serie', 'turma','grade', 'atividade'
                                    ], inplace=True)
    
    base_letrus_merged = base_letrus_merged.rename(columns={
        "id_estudante": "id_estudante_letrus",
        "data_de_inicio": "data_inicio",
        "data_de_termino": "data_termino",
        "genero": "genero_textual",
        "nota_da_c1": "nota_c1",
        "nota_da_c2": "nota_c2",
        "nota_da_c3": "nota_c3",
        "nota_da_c4": "nota_c4",
        "nota_da_c5": "nota_c5",
        "nota_da_c6": "nota_c6",
        "motivo_de_zeramento": "motivo_zeramento",
        "id_redacao": "id_redacao_letrus"
    })

    colunas_saida = {
        'id_matricula': 'int64',              # SQL: bigint → pandas deve ser int64
        'id_tempo': 'Int64',                  # SQL: int → pandas pode ser Int64 (nullable)
        'ra': 'int64',                        # SQL: bigint
        'id_estudante_letrus': 'int64',       # SQL: bigint
        'data_inicio': 'datetime64[ns]',      # SQL: datetime
        'data_termino': 'datetime64[ns]',     # SQL: datetime
        'semana': 'datetime64[ns]',           # SQL: date/datetime
        'atividade_mes': 'string',            # SQL: varchar
        'atividade_classificacao': 'string',  # SQL: varchar
        'genero_textual': 'string',           # SQL: varchar
        'nota_c1': 'Int64',                   # SQL: int
        'nota_c2': 'Int64',
        'nota_c3': 'Int64',
        'nota_c4': 'Int64',
        'nota_c5': 'Int64',
        'nota_c6': 'Int64',
        'nota_final': 'float64',              # SQL: float
        'motivo_zeramento': 'string',         # SQL: varchar
        'id_redacao_letrus': 'int64'          # SQL: bigint
    }

    base_letrus_merged = helpers.ajustar_tipos(base_letrus_merged, colunas_saida)
    base_letrus_merged = base_letrus_merged[colunas_saida.keys()]

    base_letrus_merged = base_letrus_merged.replace({pd.NA: None})

    helpers._insert_data(conn, base_letrus_merged, 'data_facts_iol_redacao')