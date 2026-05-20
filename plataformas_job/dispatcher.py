"""Dispatcher do blob trigger 'plataformas/*'.

Roteia cada blob recebido para o job correspondente (evolucional, khan,
khan_aloc, letrus) com base no prefixo do caminho e no nome do arquivo.
"""
import logging
from io import BytesIO

import azure.functions as func
import pandas as pd

from evolucional_job import job_evo
from khan_aloc_job import job_khan_aloc
from khan_job import job_khan
from letrus_job import job_letrus


def dispatch(inputblob: func.InputStream) -> None:
    blob_name = inputblob.name
    file_name = blob_name.split("/")[-1]

    logging.info(f"Blob recebido: {blob_name}")

    try:
        # =========================================================
        # EVOLUCIONAL
        # Ex.: plataformas/evolucional/raw/base_bruta_e.voxlsx
        # =========================================================
        if blob_name.startswith("plataformas/evolucional/raw/"):
            print('evo')
            if not file_name.startswith("base_bruta_evo"):
                logging.info(f"Arquivo ignorado no fluxo evolucional: {file_name}")
                return

            logging.info(f"Arquivo válido do evolucional detectado: {blob_name}")

            file_bytes = inputblob.read()
            base_bruta = pd.read_excel(BytesIO(file_bytes))

            job_evo.processar_iol_evolucional(base_bruta, file_name)
            logging.info(f"Processamento evolucional concluído: {file_name}")
            return

        # =========================================================
        # KHAN
        # Ex.: plataformas/khan/raw/khan_progresso_bruta_2026-03-15.csv
        # =========================================================
        if blob_name.startswith("plataformas/khan/raw/"):
            if not file_name.startswith("khan_progresso_bruta_"):
                logging.info(f"Arquivo ignorado no fluxo khan: {file_name}")
                return

            logging.info(f"Arquivo válido do khan detectado: {blob_name}")

            file_bytes = inputblob.read()

            try:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="utf-8")
            except UnicodeDecodeError:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="latin1")

            job_khan.processar_iol_khan_progresso(
                base_bruta=base_bruta,
                file_name=file_name
            )
            logging.info(f"Processamento khan concluído: {file_name}")
            return

        # =========================================================
        # KHAN ALOC
        # Ex.: plataformas/khan/raw_alocacoes/khan_alocacoes_bruta_2026-03-15.xlsx
        # =========================================================
        if blob_name.startswith("plataformas/khan/raw_alocacoes/"):
            if not file_name.startswith("khan_alocacoes_bruta_"):
                logging.info(f"Arquivo ignorado no fluxo khan_aloc: {file_name}")
                return

            logging.info(f"Arquivo válido do khan_aloc detectado: {blob_name}")

            file_bytes = inputblob.read()

            if file_name.lower().endswith((".xlsx", ".xls")):
                base_bruta = pd.read_excel(BytesIO(file_bytes))
            else:
                try:
                    base_bruta = pd.read_csv(BytesIO(file_bytes), sep=";", encoding="utf-8")
                except UnicodeDecodeError:
                    base_bruta = pd.read_csv(BytesIO(file_bytes), sep=";", encoding="latin1")

                if len(base_bruta.columns) == 1 and "," in str(base_bruta.columns[0]):
                    try:
                        base_bruta = pd.read_csv(BytesIO(file_bytes), sep=",", encoding="utf-8")
                    except UnicodeDecodeError:
                        base_bruta = pd.read_csv(BytesIO(file_bytes), sep=",", encoding="latin1")

            job_khan_aloc.processar_iol_khan_aloc(
                base_bruta=base_bruta,
                file_name=file_name
            )
            logging.info(f"Processamento khan_aloc concluído: {file_name}")
            return

        # =========================================================
        # LETRUS
        # Ex.: plataformas/letrus/raw/base_bruta_letrus_2026-03-15.csv
        # =========================================================
        if blob_name.startswith("plataformas/letrus/raw/"):
            if not file_name.startswith("base_bruta_letrus_"):
                logging.info(f"Arquivo ignorado no fluxo letrus: {file_name}")
                return

            logging.info(f"Arquivo válido do letrus detectado: {blob_name}")

            file_bytes = inputblob.read()

            try:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="utf-8")
            except UnicodeDecodeError:
                base_bruta = pd.read_csv(BytesIO(file_bytes), encoding="latin1")

            job_letrus.processar_iol_redacao_letrus(
                base_bruta=base_bruta,
                file_name=file_name
            )
            logging.info(f"Processamento letrus concluído: {file_name}")
            return

        # =========================================================
        # OUTROS ARQUIVOS
        # =========================================================
        logging.info(f"Blob ignorado fora dos diretórios mapeados: {blob_name}")

    except Exception as e:
        logging.exception(f"Erro ao processar {file_name}: {str(e)}")
        raise
