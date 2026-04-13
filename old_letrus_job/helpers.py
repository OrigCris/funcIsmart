import pandas as pd
import logging, pyodbc
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
from io import BytesIO

def find_files_letrus(blob_conn_str, container, nome_arquivo):
    try:
        print("Conectando ao Blob Storage...")
        blob_service = BlobServiceClient.from_connection_string(blob_conn_str)
        container_client = blob_service.get_container_client(container)

        prefix = f"letrus/{nome_arquivo}"

        print(prefix)
        # 🔎 LISTAR TODOS OS BLOBs QUE COMEÇAM COM O PREFIXO
        blobs = list(container_client.list_blobs(name_starts_with=prefix))

        if not blobs:
            logging.error(f"Nenhum arquivo começando com '{prefix}' foi encontrado.")
            return []

        lista_dfs = []

        for blob in blobs:
            blob_name = blob.name
            logging.info(f"📥 Baixando arquivo: {blob_name}")

            blob_client = container_client.get_blob_client(blob_name)
            file_bytes = blob_client.download_blob().readall()

            # LER O ARQUIVO XLSX EM UM DATAFRAME
            df = pd.read_excel(BytesIO(file_bytes))
            lista_dfs.append(df)

            logging.info(f"✔ Arquivo {blob_name} carregado. Linhas: {len(df)}")

        print(f"Total de arquivos lidos: {len(lista_dfs)}")
        return pd.concat(lista_dfs)

    except Exception as e:
        logging.error(f"Erro ao processar blobs: {e}")
        return None

def process_df(df):
    try:
        df = df.astype(str)
        df = df.replace(["nan", "NaN", "None", ""], '')
        
        df.columns = (
            df.columns.str.strip()              # remove espaços no início/fim
                    .str.normalize('NFKD')   # remove acentos
                    .str.encode('ascii', errors='ignore')
                    .str.decode('utf-8')
                    .str.replace(' ', '_')   # troca espaço por _
                    .str.replace(r'[^A-Za-z0-9_]', '', regex=True) # tira caracteres estranhos
        )
        return df
    except Exception as e:
        print(f"Erro ao ler XLSX: {e}")
        return None
    
def _get_connection_sqlserver():
    try:
        logging.info("🟣 Conectando ao SQL Server...")

        server = 'ismart-sql-server.database.windows.net'
        database = 'dev-ismart-sql-db'
        username = 'ismart'
        password = 'th!juyep8iFr'
        driver = "ODBC Driver 18 for SQL Server"

        # Build connection string with SQL authentication
        conn_str = (
            f'Driver={{{driver}}};'
            f'Server={server};'
            f'Database={database};'
            f'UID={username};'
            f'PWD={password};'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;'
            'Login Timeout=15;'
        )

        conn = pyodbc.connect(conn_str)
        print("Conexão com SQL estabelecida.")
        return conn

    except Exception as e:
        print(f"Erro ao conectar ao SQL Server: {e}")

def _insert_data(conn, df, nome_tabela):
    try:
        cursor = conn.cursor()
        print("Inserindo dados no SQL Server...")

        # ======================================================
        # CRIAÇÃO AUTOMÁTICA DA TABELA SE NÃO EXISTIR
        # ======================================================
        print("Verificando se a tabela existe...")

        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT * FROM sysobjects WHERE name='{nome_tabela}' AND xtype='U'
            )
            SELECT 0 ELSE SELECT 1
        """)

        exists = cursor.fetchone()[0]

        if exists == 0:
            logging.info("🆕 Criando tabela automaticamente...")

            # Gera o DDL (schema SQL) baseado no DataFrame
            sql_cols = []
            for col, dtype in df.dtypes.items():
                sql_cols.append(f"[{col}] NVARCHAR(255)")

            create_sql = f"""
                CREATE TABLE {nome_tabela} (
                    {",".join(sql_cols)}
                )
            """

            cursor.execute(create_sql)
            conn.commit()

            print("Tabela criada com sucesso!")

        cursor.execute(f'TRUNCATE TABLE {nome_tabela}')
        
        cols = ",".join(df.columns)
        placeholders = ",".join(["?"] * len(df.columns))

        insert_sql = f"INSERT INTO {nome_tabela} ({cols}) VALUES ({placeholders})"

        records = list(df.values)

        to_list = [list(record) for record in records]

        cursor.fast_executemany = True  # turbo do pyodbc para SQL Server
        cursor.executemany(insert_sql, to_list)
        conn.commit()

        print("Todos os dados foram inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir dados no SQL Server: {e}")
    finally:
        cursor.close()

    print("Azure Function finalizada.")

def semana():
    dia = date.today()
    while dia.weekday() != 6:
        dia -= timedelta(days=1)
    return dia

def corrigir_atvdd(string: str) -> str:
    if pd.isna(string):
        return string
    atividades = ['Atividade Março', 'Atividade Abril', 'Atividade Maio', 'Atividade Junho',
                  'Atividade Julho', 'Atividade Agosto', 'Atividade Setembro',
                  'Atividade Outubro', 'Atividade Novembro', 'Atividade Dezembro']
    tipos = ['Reescrita', 'Escrita']

    de_para = {"Atividade 1": "Atividade Março",
               "Redação Maio": "Atividade Maio",
               "Rescrita": "Reescrita"}

    for de, para in de_para.items():
        string = string.replace(de, para)

    for atividade in atividades:
        if atividade in string:
            for tipo in tipos:
                if tipo in string:
                    return f"{atividade} - {tipo}"
    return string

def id_tempo(data, rotina='outras') -> int:
    if rotina == 'foco':
        data = data.split('_')[1].split('-')
        data = datetime(date.today().year, int(data[1]), int(data[0]))
    
    if data.day <= 4:
        id_tempo = (data - relativedelta(months=1)).strftime("%Y%m")
    else:
        id_tempo = data.strftime("%Y%m")
    return int(id_tempo)

def ajustar_tipos(df, colunas_saida):
    for col, tipo in colunas_saida.items():
        
        if tipo == "int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif tipo == "Int64":  # int nullable
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif tipo == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        elif tipo == "string":
            df[col] = df[col].astype("string")

        elif tipo == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df