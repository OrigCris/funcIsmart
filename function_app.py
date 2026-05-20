"""Ponto de entrada do Azure Functions.

Cada trigger registrado aqui apenas delega para o módulo responsável.
A lógica de cada job vive no seu próprio pacote (`plataformas_job`,
`sql_to_supabase`, `symplicity_job`).
"""
import logging

import azure.functions as func

from engajamento_job.job_engajamento import run as engajamento_run
from plataformas_job.dispatcher import dispatch as plataformas_dispatch
from sql_to_supabase.runner import run as sql_to_supabase_run
from symplicity_job.job_symplicity import run as symplicity_run


app = func.FunctionApp()


@app.blob_trigger(
    arg_name="inputblob",
    path="plataformas/{name}",
    connection="BLOB_PLAT_CONN_STR",
    source="EventGrid",
)
def plataformas_job(inputblob: func.InputStream):
    plataformas_dispatch(inputblob)

@app.timer_trigger(
    schedule="0 0 */2 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)

def sql_to_supabase(myTimer: func.TimerRequest) -> None:
    logging.info("⏰ Iniciando execução das migrações para o Supabase")
    sql_to_supabase_run()

@app.timer_trigger(
    schedule="0 0 6 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def extractSymplicity(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")
    symplicity_run()


# Segundas 15h BRT = 18h UTC
@app.timer_trigger(
    schedule="0 0 18 * * 1",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def consolidarIolEngajamento(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")
    logging.info("Iniciando consolidação semanal de iol_engajamento")
    engajamento_run()
